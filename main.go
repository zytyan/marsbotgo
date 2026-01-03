package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/PaulSonOfLars/gotgbot/v2"
	"github.com/PaulSonOfLars/gotgbot/v2/ext"
	"github.com/PaulSonOfLars/gotgbot/v2/ext/handlers"
	"github.com/PaulSonOfLars/gotgbot/v2/ext/handlers/filters/callbackquery"
	"github.com/PaulSonOfLars/gotgbot/v2/ext/handlers/filters/chatmember"
	"github.com/PaulSonOfLars/gotgbot/v2/ext/handlers/filters/message"
	"github.com/caarlos0/env/v11"
	"github.com/mattn/go-sqlite3"
	"go.uber.org/zap"

	"main/minicv"
	"main/q"
)

type Config struct {
	BotToken string `env:"BOT_TOKEN,required,notEmpty"`

	DbPath        string `env:"MARS_DB_PATH,required,notEmpty"`
	ReportStatUrl string `env:"MARS_REPORT_STAT_URL"`
	LogLevel      string `env:"LOG_LEVEL" envDefault:"INFO"`

	BotBaseUrl     string   `env:"BOT_BASE_URL"`
	BotBaseFileUrl string   `env:"BOT_BASE_FILE_URL"`
	BotProxy       *url.URL `env:"BOT_PROXY"`

	NoBackup        bool   `env:"NO_BACKUP"`
	S3ApiEndpoint   string `env:"S3_API_ENDPOINT"`
	S3ApiKeyID      string `env:"S3_API_KEY_ID"`
	S3ApiKeySecret  string `env:"S3_API_KEY_SECRET"`
	S3Bucket        string `env:"S3_BUCKET"`
	S3BackupMinutes int    `env:"BACKUP_INTERVAL_MINUTES" envDefault:"2880"`

	DevMode bool `env:"DEV_MODE" envDefault:"false"`
}

var config Config

const (
	sqliteDriverName           = "marsbot_sqlite"
	groupedMediaWait           = 1 * time.Second
	mediaGroupLimit            = 10
	similarHDThreshold   int64 = 6
	exportCooldown             = 10 * time.Minute
	hammingDistanceError       = "dhash length mismatch"

	botRequestTimeout    = 15 * time.Second
	fileDownloadTimeout  = 20 * time.Second
	reportRequestTimeout = 10 * time.Second
	reportStatTimeout    = 5 * time.Second

	hammdistSOName = "libhammdist"
)

var (
	logger       *zap.Logger
	db           *sql.DB
	queries      *q.Queries
	fileClient   *http.Client
	reportClient *http.Client
	mediaMu      sync.Mutex
	mediaGroups  map[string]chan *gotgbot.Message

	exportMu           sync.Mutex
	exporting          map[int64]*exportState
	registerSQLiteOnce sync.Once
)

type marsResult struct {
	PrevCount     int64
	PrevLastMsgID int64
	Info          q.MarsInfo
	Skipped       bool
}

type exportState struct {
	running bool
	timer   *time.Timer
}

func main() {
	if err := env.Parse(&config); err != nil {
		fmt.Println(err.Error())
		os.Exit(2)
	}
	var err error
	logger, err = buildLogger(config.LogLevel)
	if err != nil {
		panic(err)
	}
	defer logger.Sync()
	go StartBackupThread()

	mediaGroups = make(map[string]chan *gotgbot.Message)
	exporting = make(map[int64]*exportState)

	if err := initDB(); err != nil {
		logger.Fatal("failed to start: init db", zap.Error(err))
	}
	bot, err := buildBot()
	if err != nil {
		logger.Fatal("failed to start: build bot", zap.Error(err))
	}

	dp := ext.NewDispatcher(&ext.DispatcherOpts{
		Error: func(_ *gotgbot.Bot, _ *ext.Context, err error) ext.DispatcherAction {
			logger.Warn("handler error", zap.Error(err))
			return ext.DispatcherActionNoop
		},
		Panic: func(_ *gotgbot.Bot, _ *ext.Context, r interface{}) {
			logger.Error("handler panic", zap.Any("r", r), zap.Stack("stack"))
		},
	})
	dp.AddHandler(handlers.NewMessage(message.Photo, handlePhoto).
		SetAllowChannel(true))
	dp.AddHandler(handlers.NewCallback(callbackquery.Prefix("wl:"), handleAddPicWhitelistByCallback))
	dp.AddHandler(handlers.NewCallback(callbackquery.Prefix("find:"), handleFindSimilarByCallback))
	dp.AddHandler(handlers.NewCommand("pic_info", handlePicInfo))
	dp.AddHandler(handlers.NewCommand("add_whitelist", handleAddToWhitelist))
	dp.AddHandler(handlers.NewCommand("remove_whitelist", handleRemoveFromWhitelist))
	dp.AddHandler(handlers.NewCommand("add_me_to_whitelist", handleAddUserToWhitelist))
	dp.AddHandler(handlers.NewCommand("remove_me_from_whitelist", handleRemoveUserFromWhitelist))
	dp.AddHandler(handlers.NewCommand("stat", handleBotStat))
	dp.AddHandler(handlers.NewCommand("help", handleHelp))
	dp.AddHandler(handlers.NewCommand("start", handleHelp))
	dp.AddHandler(handlers.NewCommand("mars_bot_welcome", handleCmdWelcome))
	dp.AddHandler(handlers.NewCommand("ensure_marsbot_export", handleExportData))
	dp.AddHandler(handlers.NewCommand("export", handleExportHelp))
	dp.AddHandler(handlers.NewMyChatMember(chatmember.All, handleWelcome))
	updater := ext.NewUpdater(dp, nil)

	allowed := []string{
		"callback_query",
		"channel_post",
		"message",
		"edited_message",
		"my_chat_member",
	}
	pollingOpts := &ext.PollingOpts{
		GetUpdatesOpts: &gotgbot.GetUpdatesOpts{
			AllowedUpdates: allowed,
		},
	}

	if err := updater.StartPolling(bot, pollingOpts); err != nil {
		logger.Fatal("failed to start polling", zap.Error(err))
	}
	logger.Info("marsbot is running", zap.String("username", bot.Username))
	updater.Idle()
}

func buildLogger(level string) (*zap.Logger, error) {
	var cfg zap.Config
	if config.DevMode {
		fmt.Println("Now running in dev mode")
		cfg = zap.NewDevelopmentConfig()
	} else {
		cfg = zap.NewProductionConfig()
	}
	if err := cfg.Level.UnmarshalText([]byte(strings.ToLower(level))); err != nil {
		cfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	}
	return cfg.Build()
}

func registerSQLiteDriver() {
	registerSQLiteOnce.Do(func() {
		f, err := os.Executable()
		hammdistSOFile := hammdistSOName
		if err == nil {
			hammdistSOFile = filepath.Join(filepath.Dir(f), hammdistSOName)
		}
		sql.Register(sqliteDriverName, &sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				err := conn.LoadExtension(hammdistSOFile, "sqlite3_hammdist_init")
				if err == nil {
					fmt.Println("hammdist.so loaded")
					return nil
				}
				fmt.Println("loaded hammdist.so failed, err: " + err.Error())
				return conn.RegisterFunc("hamming_distance", hammingDistance, true)
			},
		})
	})
}

func initDB() error {
	registerSQLiteDriver()
	var err error
	db, err = sql.Open(sqliteDriverName, config.DbPath)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	pragmas := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=OFF;",
		"PRAGMA cache_size=-80000;",
		"PRAGMA busy_timeout=5000;",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			db.Close()
			return fmt.Errorf("apply pragma %q: %w", p, err)
		}
	}
	queries = q.NewWithLogger(db, logger)
	return nil
}

func buildBot() (*gotgbot.Bot, error) {
	transport := &http.Transport{}
	if config.BotProxy != nil {
		transport.Proxy = http.ProxyURL(config.BotProxy)
	}

	fileClient = &http.Client{
		Timeout:   fileDownloadTimeout,
		Transport: transport,
	}
	if config.ReportStatUrl != "" {
		reportClient = &http.Client{
			Timeout: reportRequestTimeout,
		}
	}
	client := &gotgbot.BaseBotClient{
		Client: http.Client{
			Timeout:   botRequestTimeout,
			Transport: fileClient.Transport,
		},
	}
	if config.BotBaseUrl != "" {
		client.DefaultRequestOpts = &gotgbot.RequestOpts{APIURL: config.BotBaseUrl}
	}
	bot, err := gotgbot.NewBot(config.BotToken, &gotgbot.BotOpts{BotClient: client})
	if err != nil {
		return nil, fmt.Errorf("create bot: %w", err)
	}
	return bot, err
}

func handlePhoto(bot *gotgbot.Bot, ctx *ext.Context) error {
	msg := ctx.EffectiveMessage
	chat := ctx.EffectiveChat
	if msg == nil || chat == nil || len(msg.Photo) == 0 {
		return nil
	}
	// skip edited grouped media to avoid double counting
	if msg.MediaGroupId != "" && ctx.EditedMessage != nil {
		return nil
	}

	if ctx.EffectiveUser != nil {
		inWl, err := isUserInWhitelist(context.Background(), chat.Id, ctx.EffectiveUser.Id)
		if err != nil {
			logger.Warn("check user whitelist", zap.Error(err))
		}
		if inWl {
			return nil
		}
	}

	if msg.MediaGroupId != "" {
		enqueueMediaGroup(bot, msg)
		return nil
	}
	return processSinglePhoto(bot, msg)
}

func processSinglePhoto(bot *gotgbot.Bot, msg *gotgbot.Message) error {
	ctx := context.Background()
	photo := msg.Photo[len(msg.Photo)-1]
	dhash, err := getDHash(ctx, bot, photo)
	if err != nil {
		return err
	}
	result, err := recordMars(ctx, msg.Chat.Id, msg.MessageId, dhash)
	if err != nil {
		return err
	}
	if result.Skipped || result.PrevCount == 0 {
		return nil
	}

	reply := buildMarsReply(&msg.Chat, result.PrevCount, result.PrevLastMsgID)
	opt := &gotgbot.SendMessageOpts{
		ReplyParameters: replyTo(msg.MessageId),
		ParseMode:       "HTML",
	}
	if result.PrevCount > 3 {
		opt.ReplyMarkup = &gotgbot.InlineKeyboardMarkup{
			InlineKeyboard: [][]gotgbot.InlineKeyboardButton{{
				{
					Text:         "将图片添加至白名单",
					CallbackData: fmt.Sprintf("wl:%s", hex.EncodeToString(dhash)),
				},
			}},
		}
	}
	_, err = bot.SendMessage(msg.Chat.Id, reply, opt)
	if err != nil {
		logger.Warn("send mars reply", zap.Error(err))
	}
	go reportStat(msg.Chat.Id, result.PrevCount)
	return nil
}

func enqueueMediaGroup(bot *gotgbot.Bot, msg *gotgbot.Message) {
	mediaMu.Lock()
	defer mediaMu.Unlock()
	c, ok := mediaGroups[msg.MediaGroupId]
	if !ok {
		c = make(chan *gotgbot.Message, mediaGroupLimit)
		mediaGroups[msg.MediaGroupId] = c
		go flushMediaGroup(bot, msg.MediaGroupId, c)
	}
	select {
	case c <- msg:
	default:
	}
}

func flushMediaGroup(bot *gotgbot.Bot, groupID string, c chan *gotgbot.Message) {
	defer func() {
		mediaMu.Lock()
		delete(mediaGroups, groupID)
		mediaMu.Unlock()
	}()
	msgs := make([]*gotgbot.Message, 0, mediaGroupLimit)
	timer := time.NewTimer(groupedMediaWait)
	defer timer.Stop()
loop:
	for {
		select {
		case msg := <-c:
			msgs = append(msgs, msg)
			timer.Reset(groupedMediaWait)
		case <-timer.C:
			break loop
		}
	}
	if err := handleMediaGroup(bot, msgs); err != nil {
		logger.Warn("handle media group", zap.Error(err), zap.String("group_id", groupID))
	}
}

func handleMediaGroup(bot *gotgbot.Bot, msgs []*gotgbot.Message) error {
	ctx := context.Background()
	type item struct {
		msg *gotgbot.Message
		res marsResult
	}
	unique := make(map[string]*item)

	for _, msg := range msgs {
		if len(msg.Photo) == 0 {
			continue
		}
		dhash, err := getDHash(ctx, bot, msg.Photo[len(msg.Photo)-1])
		if err != nil {
			logger.Warn("get dhash for group media", zap.Error(err))
			continue
		}
		key := hex.EncodeToString(dhash)
		if _, ok := unique[key]; ok {
			continue // avoid duplicate reporting inside one album
		}
		res, err := recordMars(ctx, msg.Chat.Id, msg.MessageId, dhash)
		if err != nil {
			logger.Warn("record mars for group media", zap.Error(err))
			continue
		}
		unique[key] = &item{msg: msg, res: res}
		if res.PrevCount > 0 && !res.Skipped {
			go reportStat(msg.Chat.Id, res.PrevCount)
		}
	}

	var best *item
	for _, it := range unique {
		if it.res.Skipped || it.res.PrevCount == 0 {
			continue
		}
		if best == nil || it.res.PrevCount > best.res.PrevCount {
			best = it
		}
	}
	if best == nil {
		return nil
	}
	reply := buildGroupedReply(&best.msg.Chat, best.res.PrevCount, best.res.PrevLastMsgID)
	_, err := bot.SendMessage(best.msg.Chat.Id, reply, &gotgbot.SendMessageOpts{
		ReplyParameters: replyTo(best.msg.MessageId),
		ParseMode:       "HTML",
	})
	return err
}

func getDHash(ctx context.Context, b *gotgbot.Bot, photo gotgbot.PhotoSize) ([]byte, error) {
	dhash, err := queries.GetDhashFromFileUid(ctx, photo.FileUniqueId)
	if err == nil {
		return dhash, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}

	file, err := b.GetFile(photo.FileId, nil)
	if err != nil {
		return nil, fmt.Errorf("get file: %w", err)
	}
	var data []byte
	if data2, err := os.ReadFile(file.FilePath); err == nil {
		data = data2
	} else {
		u := file.URL(b, &gotgbot.RequestOpts{APIURL: config.BotBaseFileUrl})
		data, err = downloadFile(ctx, u)
		if err != nil {
			return nil, err
		}
	}
	dhashArr, err := minicv.DHashBytes(data)
	if err != nil {
		return nil, err
	}
	dhash = dhashArr[:]
	if err := queries.UpsertDhash(ctx, photo.FileUniqueId, dhash); err != nil {
		logger.Warn("cache dhash", zap.Error(err))
	}
	return dhash, nil
}

func downloadFile(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	resp, err := fileClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("download file: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("download failed with status %s", resp.Status)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}
	return body, nil
}

func recordMars(ctx context.Context, groupID, msgID int64, dhash []byte) (marsResult, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return marsResult{}, err
	}
	qtx := queries.WithTx(tx)

	info, err := qtx.GetMarsInfo(ctx, groupID, dhash)
	prevCount := int64(0)
	prevLastMsgID := int64(0)
	if err == nil {
		prevCount = info.Count
		prevLastMsgID = info.LastMsgID
		if info.LastMsgID == msgID || info.InWhitelist != 0 {
			_ = tx.Rollback()
			return marsResult{PrevCount: prevCount, PrevLastMsgID: prevLastMsgID, Info: info, Skipped: true}, nil
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		_ = tx.Rollback()
		return marsResult{}, err
	}

	newInfo, err := qtx.IncrementMarsInfo(ctx, groupID, dhash, msgID)
	if err != nil {
		_ = tx.Rollback()
		return marsResult{}, err
	}
	if prevCount == 0 {
		if err := qtx.IncrementGroupStat(ctx, groupID); err != nil {
			_ = tx.Rollback()
			return marsResult{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return marsResult{}, err
	}
	return marsResult{
		PrevCount:     prevCount,
		PrevLastMsgID: prevLastMsgID,
		Info:          newInfo,
	}, nil
}

func isUserInWhitelist(ctx context.Context, groupID, userID int64) (bool, error) {
	val, err := queries.IsUserInWhitelist(ctx, groupID, userID)
	if err != nil {
		return false, err
	}
	return val != 0, nil
}

func reportStat(groupID int64, marsCount int64) {
	if reportClient == nil || config.ReportStatUrl == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), reportStatTimeout)
	defer cancel()
	body, _ := json.Marshal(map[string]int64{
		"group_id":   groupID,
		"mars_count": marsCount,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, config.ReportStatUrl, bytes.NewReader(body))
	if err != nil {
		logger.Warn("build report request", zap.Error(err))
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := reportClient.Do(req)
	if err != nil {
		logger.Warn("report stat", zap.Error(err))
		return
	}
	_ = resp.Body.Close()
}

func handleAddPicWhitelistByCallback(b *gotgbot.Bot, ctx *ext.Context) error {
	if ctx.CallbackQuery == nil || ctx.EffectiveChat == nil {
		return nil
	}
	dhash, err := parseCallback(ctx.CallbackQuery.Data)
	if err != nil {
		_, err = ctx.CallbackQuery.Answer(b, &gotgbot.AnswerCallbackQueryOpts{Text: err.Error()})
		return err
	}
	if err := queries.SetMarsWhitelist(context.Background(), ctx.EffectiveChat.Id, dhash, 1); err != nil {
		return err
	}
	_, err = ctx.CallbackQuery.Answer(b, &gotgbot.AnswerCallbackQueryOpts{Text: "该图片已加入白名单"})
	return err
}

func handlePicInfo(b *gotgbot.Bot, ctx *ext.Context) error {
	msg := ctx.EffectiveMessage
	if msg == nil || ctx.EffectiveChat == nil {
		return nil
	}
	photo := getReferPhoto(msg)
	if photo == nil {
		_, err := b.SendMessage(ctx.EffectiveChat.Id, "火星车没有发现您引用了任何图片。\n尝试发送图片使用命令，或回复特定图片。",
			&gotgbot.SendMessageOpts{ReplyParameters: replyTo(msg.MessageId)})
		return err
	}

	dhash, err := getDHash(context.Background(), b, *photo)
	if err != nil {
		return err
	}
	info, err := queries.GetMarsInfo(context.Background(), ctx.EffectiveChat.Id, dhash)
	if errors.Is(err, sql.ErrNoRows) {
		info = q.MarsInfo{GroupID: ctx.EffectiveChat.Id, PicDhash: dhash, Count: 0, LastMsgID: 0, InWhitelist: 0}
	} else if err != nil {
		return err
	}

	whitelistStr := "🟢 它不在本群的火星白名单当中"
	if info.InWhitelist != 0 {
		whitelistStr = "🙈 它在本群的火星白名单中"
	}
	markup := &gotgbot.InlineKeyboardMarkup{
		InlineKeyboard: [][]gotgbot.InlineKeyboardButton{{
			{Text: "查找DHASH相似图片", CallbackData: fmt.Sprintf("find:%s", hex.EncodeToString(dhash))},
		}},
	}

	_, err = b.SendMessage(ctx.EffectiveChat.Id, fmt.Sprintf("File unique id: %s\n"+
		"dhash: %s\n在本群的火星次数:%d\n%s",
		photo.FileUniqueId, strings.ToUpper(hex.EncodeToString(dhash)), info.Count, whitelistStr),
		&gotgbot.SendMessageOpts{
			ReplyParameters: replyTo(msg.MessageId),
			ReplyMarkup:     markup,
		})
	return err
}

func handleAddToWhitelist(b *gotgbot.Bot, ctx *ext.Context) error {
	return updateWhitelistForPhoto(b, ctx, true)
}

func handleRemoveFromWhitelist(b *gotgbot.Bot, ctx *ext.Context) error {
	return updateWhitelistForPhoto(b, ctx, false)
}

func updateWhitelistForPhoto(b *gotgbot.Bot, ctx *ext.Context, toWhitelist bool) error {
	msg := ctx.EffectiveMessage
	if msg == nil || ctx.EffectiveChat == nil || b == nil {
		return nil
	}
	photo := getReferPhoto(msg)
	if photo == nil {
		_, err := b.SendMessage(ctx.EffectiveChat.Id, "火星车没有发现您引用了任何图片。\n尝试发送图片使用命令，或回复特定图片。",
			&gotgbot.SendMessageOpts{ReplyParameters: replyTo(msg.MessageId)})
		return err
	}
	dhash, err := getDHash(context.Background(), b, *photo)
	if err != nil {
		return err
	}
	info, err := queries.GetMarsInfo(context.Background(), ctx.EffectiveChat.Id, dhash)
	if errors.Is(err, sql.ErrNoRows) {
		info = q.MarsInfo{InWhitelist: 0}
	} else if err != nil {
		return err
	}
	if toWhitelist && info.InWhitelist != 0 {
		_, err := b.SendMessage(ctx.EffectiveChat.Id, "这张图片已经在白名单当中了",
			&gotgbot.SendMessageOpts{ReplyParameters: replyTo(msg.MessageId)})
		return err
	}
	if !toWhitelist && info.InWhitelist == 0 {
		_, err := b.SendMessage(ctx.EffectiveChat.Id, "这张图片并不在白名单中",
			&gotgbot.SendMessageOpts{ReplyParameters: replyTo(msg.MessageId)})
		return err
	}
	flag := int64(0)
	successMsg := "成功将图片移除白名单"
	if toWhitelist {
		flag = 1
		successMsg = "成功将图片加入白名单"
	}
	if err := queries.SetMarsWhitelist(context.Background(), ctx.EffectiveChat.Id, dhash, flag); err != nil {
		return err
	}
	_, err = b.SendMessage(ctx.EffectiveChat.Id, successMsg, &gotgbot.SendMessageOpts{ReplyParameters: replyTo(msg.MessageId)})
	return err
}

func handleAddUserToWhitelist(b *gotgbot.Bot, ctx *ext.Context) error {
	if ctx.EffectiveChat == nil || ctx.EffectiveUser == nil || ctx.EffectiveMessage == nil {
		return nil
	}
	err := queries.AddUserToWhitelist(context.Background(), ctx.EffectiveChat.Id, ctx.EffectiveUser.Id)
	if err != nil {
		var sqliteErr sqlite3.Error
		if errors.As(err, &sqliteErr) && errors.Is(sqliteErr.Code, sqlite3.ErrConstraint) {
			_, sendErr := b.SendMessage(ctx.EffectiveChat.Id, fmt.Sprintf("用户 %s 已经在本群的白名单中，您发的任何图片都不会被处理。", ctx.EffectiveMessage.GetSender().Name()),
				&gotgbot.SendMessageOpts{ReplyParameters: replyTo(ctx.EffectiveMessage.MessageId)})
			return sendErr
		}
		return err
	}
	name := ctx.EffectiveMessage.GetSender().Name()
	_, err = b.SendMessage(ctx.EffectiveChat.Id, fmt.Sprintf("已将用户 %s 加入白名单，您发的任何图片都不会被处理。", name),
		&gotgbot.SendMessageOpts{ReplyParameters: replyTo(ctx.EffectiveMessage.MessageId)})
	return err
}

func handleRemoveUserFromWhitelist(b *gotgbot.Bot, ctx *ext.Context) error {
	if ctx.EffectiveChat == nil || ctx.EffectiveUser == nil || ctx.EffectiveMessage == nil {
		return nil
	}
	err := queries.DeleteUserFromWhitelist(context.Background(), ctx.EffectiveChat.Id, ctx.EffectiveUser.Id)
	if err != nil {
		return err
	}
	_, err = b.SendMessage(ctx.EffectiveChat.Id, fmt.Sprintf("已将用户 %s 移除本群白名单，火星车会继续为您服务。",
		ctx.EffectiveMessage.GetSender().Name()),
		&gotgbot.SendMessageOpts{ReplyParameters: replyTo(ctx.EffectiveMessage.MessageId)})
	return err
}

func handleBotStat(b *gotgbot.Bot, ctx *ext.Context) error {
	if ctx.EffectiveChat == nil || ctx.EffectiveUser == nil || ctx.EffectiveMessage == nil {
		return nil
	}
	start := time.Now()
	groupCount, err := queries.CountGroups(context.Background())
	if err != nil {
		groupCount = 0
	}
	marsCount, err := queries.GetGroupMarsCount(context.Background(), ctx.EffectiveChat.Id)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	inWhitelist, err := isUserInWhitelist(context.Background(), ctx.EffectiveChat.Id, ctx.EffectiveUser.Id)
	if err != nil {
		return err
	}
	exists := "不在"
	if inWhitelist {
		exists = "在"
	}
	duration := time.Since(start)
	_, err = b.SendMessage(ctx.EffectiveChat.Id, fmt.Sprintf("火星车当前一共服务了%d个群组\n"+
		"当前群组ID: %d\n"+
		"您是 %s(id:%d)，您%s本群的白名单当中\n"+
		"本群一共记录了 %d 张不同的图片\n"+
		"本次统计共耗时 %s\n"+
		"火星车与您同在",
		groupCount, ctx.EffectiveChat.Id, ctx.EffectiveMessage.GetSender().Name(), ctx.EffectiveUser.Id, exists, marsCount, duration),
		&gotgbot.SendMessageOpts{ReplyParameters: replyTo(ctx.EffectiveMessage.MessageId)})
	return err
}

func handleHelp(b *gotgbot.Bot, ctx *ext.Context) error {
	if ctx.EffectiveChat == nil || ctx.EffectiveMessage == nil {
		return nil
	}
	if ctx.EffectiveChat.Type != "private" && strings.HasPrefix(ctx.EffectiveMessage.GetText(), "/start") {
		return nil
	}
	atSuffix := ""
	if ctx.EffectiveChat.Type != "private" && b != nil {
		atSuffix = "@" + b.Username
	}
	_, err := b.SendMessage(ctx.EffectiveChat.Id,
		strings.ReplaceAll("/help@botname 显示本帮助信息\n"+
			"/stat@botname 显示统计信息\n"+
			"/pic_info@botname 获取图片信息\n"+
			"/add_whitelist@botname 将图片添加到白名单\n"+
			"/remove_whitelist@botname 将图片移除白名单\n"+
			"/add_me_to_whitelist@botname 将用户加入群组白名单\n"+
			"/remove_me_from_whitelist@botname 将用户移出群组白名单\n"+
			"/export@botname 导出火星车的帮助信息", "@botname", atSuffix),
		&gotgbot.SendMessageOpts{ReplyParameters: replyTo(ctx.EffectiveMessage.MessageId)})
	return err
}

func handleWelcome(b *gotgbot.Bot, ctx *ext.Context) error {
	update := ctx.MyChatMember
	if update == nil || update.Chat.Type == "private" {
		return nil
	}
	if update.Chat.Type != "channel" && update.NewChatMember.GetStatus() == "administrator" {
		_, err := b.SendMessage(update.Chat.Id, "火星车的任何功能均不需要管理员权限，您无需将本bot设置为群组管理员。", nil)
		return err
	}
	oldStatus := update.OldChatMember.GetStatus()
	if oldStatus != "left" && oldStatus != "kicked" {
		return nil
	}
	if update.NewChatMember.GetStatus() == "member" {
		return sendWelcome(b, update.Chat.Id)
	}
	return nil
}

func handleCmdWelcome(b *gotgbot.Bot, ctx *ext.Context) error {
	if ctx.EffectiveChat == nil {
		return nil
	}
	return sendWelcome(b, ctx.EffectiveChat.Id)
}

func sendWelcome(bot *gotgbot.Bot, chatID int64) error {
	_, err := bot.SendMessage(chatID,
		"欢迎使用火星车。\n"+
			"本bot为 @Ytyan 为其群组开发的重复图片检测工具\n"+
			"当您将火星车加入群组或频道中后，火星车将自动开始工作。bot会实时检测群组中的图片，将其转换为DHASH，当检测到重复图片时，会回复图片的发送者。\n"+
			"bot会收集并持久保存工作需要的必要信息，包括群组ID、图片唯一ID、图片DHASH和携带图片的消息的ID。bot会在必要时下载图片，但不会持久保存\n"+
			"bot只会检查普通图片，文件形式的图片、表情包、视频等均不会被检测。\n"+
			`本bot为开源项目，您可以前往<a href="https://github.com/zytyan/pymarsbot">Github开源地址</a>自行克隆该项目。`,
		&gotgbot.SendMessageOpts{ParseMode: "HTML"})
	return err
}

func handleExportData(b *gotgbot.Bot, ctx *ext.Context) error {
	if ctx.EffectiveChat == nil || ctx.EffectiveMessage == nil {
		return nil
	}
	chatID := ctx.EffectiveChat.Id

	exportMu.Lock()
	state, ok := exporting[chatID]
	if ok {
		if state.running {
			exportMu.Unlock()
			_, err := b.SendMessage(chatID, "当前正在导出数据，请稍候再试", &gotgbot.SendMessageOpts{ReplyParameters: replyTo(ctx.EffectiveMessage.MessageId)})
			return err
		}
		exportMu.Unlock()
		_, err := b.SendMessage(chatID, "请不要短时间内重复导出，每次单个群组导出冷却时间为10分钟。", &gotgbot.SendMessageOpts{ReplyParameters: replyTo(ctx.EffectiveMessage.MessageId)})
		return err
	}
	state = &exportState{running: true}
	exporting[chatID] = state
	exportMu.Unlock()

	filePath, err := exportChatData(chatID)
	if err != nil {
		exportMu.Lock()
		delete(exporting, chatID)
		exportMu.Unlock()
		return err
	}
	defer os.Remove(filePath)

	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = b.SendDocument(chatID, gotgbot.InputFileByReader(filepath.Base(filePath), f),
		&gotgbot.SendDocumentOpts{ReplyParameters: replyTo(ctx.EffectiveMessage.MessageId)})
	if err != nil {
		exportMu.Lock()
		delete(exporting, chatID)
		exportMu.Unlock()
		return err
	}

	exportMu.Lock()
	state.running = false
	state.timer = time.AfterFunc(exportCooldown, func() {
		exportMu.Lock()
		delete(exporting, chatID)
		exportMu.Unlock()
	})
	exportMu.Unlock()
	return nil
}

func exportChatData(chatID int64) (string, error) {
	rows, err := queries.ListMarsInfoByGroup(context.Background(), chatID)
	if err != nil {
		return "", err
	}
	filename := filepath.Join(os.TempDir(), fmt.Sprintf("mars-export_%d.csv", chatID))
	file, err := os.Create(filename)
	if err != nil {
		return "", err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if err := writer.Write([]string{"group_id", "pic_dhash", "count", "last_msg_id", "in_whitelist"}); err != nil {
		return "", err
	}
	for _, row := range rows {
		record := []string{
			fmt.Sprint(row.GroupID),
			hex.EncodeToString(row.PicDhash),
			fmt.Sprint(row.Count),
			fmt.Sprint(row.LastMsgID),
			fmt.Sprint(row.InWhitelist),
		}
		if err := writer.Write(record); err != nil {
			return "", err
		}
	}
	return filename, nil
}

func handleExportHelp(b *gotgbot.Bot, ctx *ext.Context) error {
	if ctx.EffectiveMessage == nil || ctx.EffectiveChat == nil {
		return nil
	}
	_, err := b.SendMessage(ctx.EffectiveChat.Id, "想部署自己的火星车，又放不下当前数据？\n现在，您可以使用命令 /ensure_marsbot_export 导出火星车的数据，它们包括群组ID、DHASH值、火星数量、上一次消息ID及白名单状态\n这些信息将会被导出为csv格式，您可以在解压后放心地直接使用逗号分割。\n请注意，为避免无意义的性能消耗，每个群组在十分钟内只能导出一次。",
		&gotgbot.SendMessageOpts{ReplyParameters: replyTo(ctx.EffectiveMessage.MessageId)})
	return err
}

func handleFindSimilarByCallback(b *gotgbot.Bot, ctx *ext.Context) error {
	if ctx.CallbackQuery == nil || ctx.EffectiveChat == nil || ctx.EffectiveMessage == nil {
		return nil
	}
	target, err := parseCallback(ctx.CallbackQuery.Data)
	if err != nil {
		_, err = ctx.CallbackQuery.Answer(b, &gotgbot.AnswerCallbackQueryOpts{Text: err.Error()})
		return err
	}

	start := time.Now()
	items, err := queries.ListSimilarPhotos(context.Background(), target, ctx.EffectiveChat.Id, similarHDThreshold)
	if err != nil {
		return err
	}
	var textLines []string
	textLines = append(textLines, fmt.Sprintf("火星车为您找到了%d张相似的图片\n这些图片的汉明距离小于%d\n耗时:%s\n",
		len(items), similarHDThreshold, time.Since(start)))
	for i, item := range items {
		startLabel, endLabel := buildLabel(ctx.EffectiveChat, item.MarsInfo.LastMsgID)
		textLines = append(textLines, fmt.Sprintf("%s图片%d: 距离: %d 消息ID: %d%s", startLabel, i+1, item.Hd, item.MarsInfo.LastMsgID, endLabel))
	}
	_, err = b.SendMessage(ctx.EffectiveChat.Id, strings.Join(textLines, "\n"),
		&gotgbot.SendMessageOpts{
			ReplyParameters: replyTo(ctx.EffectiveMessage.MessageId),
			ParseMode:       "HTML",
		})
	if err != nil {
		return err
	}
	_, err = ctx.CallbackQuery.Answer(b, &gotgbot.AnswerCallbackQueryOpts{Text: "查找完成", ShowAlert: false})
	return err
}

func parseCallback(s string) ([]byte, error) {
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 {
		return nil, errors.New("not valid callback")
	}
	return hex.DecodeString(parts[1])
}
