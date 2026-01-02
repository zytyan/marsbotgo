package main

import (
	"github.com/PaulSonOfLars/gotgbot/v2"
	"github.com/PaulSonOfLars/gotgbot/v2/ext"
	"github.com/PaulSonOfLars/gotgbot/v2/ext/handlers"
	"github.com/PaulSonOfLars/gotgbot/v2/ext/handlers/filters/callbackquery"
	"github.com/PaulSonOfLars/gotgbot/v2/ext/handlers/filters/chatmember"
	"github.com/PaulSonOfLars/gotgbot/v2/ext/handlers/filters/message"
	"github.com/caarlos0/env/v11"
	"go.uber.org/zap"
)

type Config struct {
	BotToken string `env:"BOT_TOKEN,required,notEmpty"`

	DbPath        string `env:"MARS_DB_PATH,required,notEmpty"`
	ReportStatUrl string `env:"MARS_REPORT_STAT_URL"`
	LogLevel      string `env:"LOG_LEVEL" envDefault:"INFO"`

	BotBaseUrl     string `env:"BOT_BASE_URL"`
	BotBaseFileUrl string `env:"BOT_BASE_FILE_URL"`
	BotProxy       string `env:"BOT_PROXY"`

	S3ApiEndpoint   string `env:"S3_API_ENDPOINT"`
	S3ApiKeyID      string `env:"S3_API_KEY_ID"`
	S3ApiKeySecret  string `env:"S3_API_KEY_SECRET"`
	S3Bucket        string `env:"S3_BUCKET"`
	S3BackupMinutes int    `env:"BACKUP_INTERVAL_MINUTES"`
}

var config = env.Must(env.ParseAs[Config]())

func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}

	defer logger.Sync()

	bot, updater, err := buildApp(logger)
	if err != nil {
		logger.Fatal("failed to start", zap.Error(err))
	}

	dispatcher, ok := updater.Dispatcher.(*ext.Dispatcher)
	if !ok {
		logger.Fatal("unexpected dispatcher type")
		return
	}
	dispatcher.AddHandler(handlers.NewMessage(message.Photo, nil).
		SetAllowChannel(true))
	dispatcher.AddHandler(handlers.NewCallback(callbackquery.Prefix("wl:"), nil))
	dispatcher.AddHandler(handlers.NewCallback(callbackquery.Prefix("find:"), nil))
	dispatcher.AddHandler(handlers.NewCommand("pic_info", nil))
	dispatcher.AddHandler(handlers.NewCommand("add_whitelist", nil))
	dispatcher.AddHandler(handlers.NewCommand("remove_whitelist", nil))
	dispatcher.AddHandler(handlers.NewCommand("add_me_to_whitelist", nil))
	dispatcher.AddHandler(handlers.NewCommand("remove_me_from_whitelist", nil))
	dispatcher.AddHandler(handlers.NewCommand("stat", nil))
	dispatcher.AddHandler(handlers.NewCommand("help", nil))
	dispatcher.AddHandler(handlers.NewCommand("start", nil))
	dispatcher.AddHandler(handlers.NewCommand("mars_bot_welcome", nil))
	dispatcher.AddHandler(handlers.NewMyChatMember(chatmember.All, nil))

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

func buildApp(log *zap.Logger) (*gotgbot.Bot, *ext.Updater, error) {
	panic("TODO: ") //TODO:
}
