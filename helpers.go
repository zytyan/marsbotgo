package main

import (
	"fmt"
	"math/bits"

	"github.com/PaulSonOfLars/gotgbot/v2"
)

func buildLabel(chat *gotgbot.Chat, msgID int64) (string, string) {
	if chat == nil || msgID == 0 {
		return "", ""
	}
	var link string
	switch {
	case chat.Username != "":
		link = fmt.Sprintf("https://t.me/%s/%d", chat.Username, msgID)
	case chat.Id < 0:
		cid := -chat.Id - 1000000000000
		link = fmt.Sprintf("https://t.me/c/%d/%d", cid, msgID)
	}
	if link == "" {
		return "", ""
	}
	return fmt.Sprintf(`<a href="%s">`, link), "</a>"
}

func buildMarsReply(chat *gotgbot.Chat, count int64, lastMsgID int64) string {
	labelStart, labelEnd := buildLabel(chat, lastMsgID)
	switch {
	case count < 3:
		return fmt.Sprintf("这张图片已经%s火星%d次%s了！", labelStart, count, labelEnd)
	case count == 3:
		return fmt.Sprintf("这张图已经%s火星了%d次%s了，现在本车送你 ”火星之王“ 称号！", labelStart, count, labelEnd)
	default:
		return fmt.Sprintf("火星之王，收了你的神通吧，这张图都让您%s火星%d次%s了！", labelStart, count, labelEnd)
	}
}

func buildGroupedReply(chat *gotgbot.Chat, count int64, lastMsgID int64) string {
	labelStart, labelEnd := buildLabel(chat, lastMsgID)
	switch {
	case count < 3:
		return fmt.Sprintf("这一组图片火星了%s火星%d次%s了！", labelStart, count, labelEnd)
	case count == 3:
		return fmt.Sprintf("您这一组图片已经%s火星了%d次%s了，现在本车送你 ”火星之王“ 称号！", labelStart, count, labelEnd)
	default:
		return fmt.Sprintf("火星之王，收了你的神通吧，这些图都让您%s火星%d次%s了！", labelStart, count, labelEnd)
	}
}

func getReferPhoto(msg *gotgbot.Message) *gotgbot.PhotoSize {
	if msg == nil {
		return nil
	}
	if len(msg.Photo) > 0 {
		return &msg.Photo[len(msg.Photo)-1]
	}
	if msg.ReplyToMessage != nil && len(msg.ReplyToMessage.Photo) > 0 {
		return &msg.ReplyToMessage.Photo[len(msg.ReplyToMessage.Photo)-1]
	}
	return nil
}

func replyTo(messageID int64) *gotgbot.ReplyParameters {
	if messageID == 0 {
		return nil
	}
	return &gotgbot.ReplyParameters{MessageId: messageID}
}

func hammingDistance(a, b []byte) (int64, error) {
	if len(a) != len(b) {
		return 0, fmt.Errorf("%s: %d vs %d", hammingDistanceError, len(a), len(b))
	}
	var dist int64
	for i := range a {
		dist += int64(bits.OnesCount8(a[i] ^ b[i]))
	}
	return dist, nil
}
