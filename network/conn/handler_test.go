package conn

import (
	"bytes"
	"testing"

	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/XiaWuSharve/whisperly/mq"
	"github.com/bwmarrin/snowflake"
)

type handlerEnvironment struct {
	client      *Client
	sendHandler *SendHandler
	storeOut    *mq.ConsumerMock[*datas.Store]
	storeData   chan *datas.Store
}

type storeHandler struct {
	output chan<- *datas.Store
}

func (h *storeHandler) Handle(message *datas.Store) error {
	h.output <- message
	return nil
}

func prepareEnvironment(t *testing.T) *handlerEnvironment {
	t.Helper()

	node, err := snowflake.NewNode(0)
	if err != nil {
		t.Fatal(err)
	}
	datas.Ids = node

	storeOut := &mq.ConsumerMock[*datas.Store]{}
	storeData := make(chan *datas.Store, 1)
	if err := storeOut.Start(&storeHandler{output: storeData}); err != nil {
		t.Fatal(err)
	}

	sendHandler := &SendHandler{
		SendChan:     make(chan *datas.Send, 1),
		SendConsumer: &mq.ConsumerMock[*datas.Send]{},
		storeData:    &datas.Store{},
	}

	c := &Client{
		Conn: &MockConn{
			Id:     datas.GenId(),
			Reader: bytes.NewBuffer(nil),
		},
		StoreProducer: &mq.ProducerMock[*datas.Store]{Consumer: storeOut},
		SendHandler:   sendHandler,
	}

	sendHandler.Client = *c
	return &handlerEnvironment{
		client:      c,
		sendHandler: sendHandler,
		storeOut:    storeOut,
		storeData:   storeData,
	}
}

func TestSendHandlerHandleQueuesMessage(t *testing.T) {
	env := prepareEnvironment(t)
	want := &datas.Send{Type: datas.MessageType_NORMAL, MessageId: datas.GenId()}

	if err := env.sendHandler.Handle(want); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	select {
	case got := <-env.sendHandler.SendChan:
		if got != want {
			t.Fatalf("Handle() queued %p, want %p", got, want)
		}
	default:
		t.Fatal("Handle() did not queue the message")
	}
}

func TestSendHandlerCloseStoresMessage(t *testing.T) {
	env := prepareEnvironment(t)
	message := &datas.Send{
		Type: datas.MessageType_NORMAL,
	}
	env.sendHandler.SendChan <- message
	env.sendHandler.close()

	select {
	case stored := <-env.storeData:
		if stored.ReceiverId != message.ReceiverId {
			t.Fatalf("stored receiver id = %q, want %q", stored.ReceiverId, message.ReceiverId)
		}
	default:
		t.Fatal("message was not stored")
	}
}

func TestReceiveHandlerAckSending(t *testing.T) {
	env := prepareEnvironment(t)
	receiveHandler := &ReceiveHandler{Client: *env.client}
	receiveHandler.SendHandler = env.sendHandler
	receiveHandler.receiveData = &datas.Receive{MessageId: datas.GenId()}

	receiveHandler.ackSending()
	ack := <-env.sendHandler.SendChan
	if ack.Type != datas.MessageType_ACK {
		t.Fatalf("ack type = %v, want ACK", ack.Type)
	}
	if ack.MessageId != receiveHandler.receiveData.MessageId {
		t.Fatalf("ack message id = %d, want %d", ack.MessageId, receiveHandler.receiveData.MessageId)
	}
	if ack.Ack == nil || ack.Ack.Status != datas.AckStatus_SENDING {
		t.Fatalf("ack = %#v, want sending ACK", ack.Ack)
	}
}

func TestReceiveHandlerAckFail(t *testing.T) {
	env := prepareEnvironment(t)
	receiveHandler := &ReceiveHandler{Client: *env.client}
	receiveHandler.SendHandler = env.sendHandler
	receiveHandler.receiveData = &datas.Receive{MessageId: datas.GenId()}

	receiveHandler.ackFail("bad payload")
	ack := <-env.sendHandler.SendChan
	if ack.Type != datas.MessageType_ACK {
		t.Fatalf("ack type = %v, want ACK", ack.Type)
	}
	if ack.Ack == nil || ack.Ack.Status != datas.AckStatus_FAIL || ack.Ack.Reason != "bad payload" {
		t.Fatalf("ack = %#v, want failed ACK with reason", ack.Ack)
	}
}
