package mq

import (
	"testing"
	"time"

	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/bwmarrin/snowflake"
	"github.com/nsqio/go-nsq"
)

type TestHandler struct {
	T            *testing.T
	SuccessCount int
}

func (h *TestHandler) Handle(m *datas.MMessage) error {
	defer func() {
		h.SuccessCount++
	}()
	// 基本字段检查
	if m.Message.ConnId == 0 {
		h.T.Fatal("m.Message.ConnId: ", m.Message.ConnId)
	}
	if m.Message.SenderId != "sharve" {
		h.T.Fatal("m.Message.SenderId: ", m.Message.SenderId)
	}
	if m.Message.ReceiverId != "glacc" {
		h.T.Fatal("m.Message.ReceiverId: ", m.Message.ReceiverId)
	}
	if m.Message.Type != datas.MessageType_NORMAL {
		h.T.Fatal("m.Message.Type: ", m.Message.Type)
	}
	if m.Message.CreatedTime <= 0 {
		h.T.Fatal("m.Message.CreatedTime: ", m.Message.CreatedTime)
	}

	// 检查 Type2（oneof 字段）
	normalMsg, ok := m.Message.Type2.(*datas.Message_Normal)
	if !ok {
		h.T.Fatal("m.Message.Type2 is not *datas.Message_Normal")
	}
	if normalMsg.Normal != datas.NormalType_CHAT {
		h.T.Fatal("normalMsg.Normal: ", normalMsg.Normal)
	}

	// 检查 Data（oneof 字段）
	chatData, ok := m.Message.Data.(*datas.Message_Chat)
	if !ok {
		h.T.Fatal("m.Message.Data is not *datas.Message_Chat")
	}
	if chatData.Chat == nil {
		h.T.Fatal("chatData.Chat is nil")
	}
	if chatData.Chat.DisplayName != "夏午" {
		h.T.Fatal("chatData.Chat.DisplayName: ", chatData.Chat.DisplayName)
	}

	// 检查消息链
	chain := chatData.Chat.MessageChain
	if len(chain) != 2 {
		h.T.Fatalf("MessageChain length = %d, want 2", len(chain))
	}
	if chain[0].Type != datas.MessageUnitType_TEXT || chain[0].Message != "hello " {
		h.T.Fatal("MessageChain[0] mismatch")
	}
	if chain[1].Type != datas.MessageUnitType_TEXT || chain[1].Message != "mq" {
		h.T.Fatal("MessageChain[1] mismatch")
	}
	return nil
}
func TestMq(t *testing.T) {
	node, err := snowflake.NewNode(0)
	if err != nil {
		panic(err)
	}
	datas.Ids = node
	mq := &Mq[*datas.MMessage]{
		Config:  nsq.NewConfig(),
		Topic:   "test",
		Decoder: &datas.MMessage{},
	}
	producer, err := mq.CreateProducer("localhost:4150")
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	transactionChan, err := producer.Enqueue(&datas.MMessage{
		Message: datas.Message{
			Type: datas.MessageType_NORMAL,
			Type2: &datas.Message_Normal{
				Normal: datas.NormalType_CHAT,
			},
			SenderId:    "sharve",
			ReceiverId:  "glacc",
			CreatedTime: time.Now().UnixMilli(),
			ConnId:      datas.GenId(),
			Data: &datas.Message_Chat{
				Chat: &datas.Chat{
					DisplayName: "夏午",
					MessageChain: []*datas.MessageUnit{
						{Type: datas.MessageUnitType_TEXT, Message: "hello "},
						{Type: datas.MessageUnitType_TEXT, Message: "mq"},
					},
				},
			},
		},
	})
	transaction := <-transactionChan
	if err := transaction.Error; err != nil {
		t.Fatal("enqueue failed", err)
	}
	consumer, err := mq.CreateConsumer("localhost:4161")
	if err != nil {
		t.Fatal(err)
	}
	handler := &TestHandler{T: t}
	if err := consumer.Start(handler); err != nil {
		t.Fatal(err)
	}
	<-time.After(3 * time.Second)
	if handler.SuccessCount == 0 {
		t.Fatal("no message consumed")
	}
	defer func() {
		doneChan := consumer.Close()
		<-doneChan
	}()
}
