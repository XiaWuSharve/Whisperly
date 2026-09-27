package conn

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/XiaWuSharve/whisperly/mq"
	"github.com/XiaWuSharve/whisperly/network/router"
	"github.com/bwmarrin/snowflake"
	"google.golang.org/protobuf/proto"
)

type storeHandler struct {
	T    *testing.T
	done chan struct{}
}

func (h *storeHandler) Handle(message *datas.Store) error {
	if message.Type != datas.MessageType_NORMAL {
		h.T.Fatal(message.Type.String())
	}
	if message.SendType {
		h.T.Fatal("message.SendType != false")
	}
	if message.ReceiverId != "glacc" {
		h.T.Fatal(message.ReceiverId)
	}
	if len(message.Bytes)-message.BodyStartIdx <= 0 {
		h.T.Fatal(len(message.Bytes), message.BodyStartIdx)
	}
	close(h.done)
	return nil
}

type processHandler struct {
	T    *testing.T
	done chan struct{}
}

func (h *processHandler) Handle(message *datas.Receive) error {
	if message.Type != datas.MessageType_NORMAL {
		h.T.Fatal(message.Type.String())
	}
	if message.ReceiverId != "glacc" {
		h.T.Fatal(message.ReceiverId)
	}
	m := datas.Message{}
	if err := proto.Unmarshal(message.Bytes[message.BodyStartIdx:], &m); err != nil {
		h.T.Fatal(err)
	}
	close(h.done)
	return nil
}

type Env struct {
	c              *Client
	storeHandler   *storeHandler
	processHandler *processHandler
}

func prepareClient(t *testing.T) *Env {
	t.Helper()

	node, err := snowflake.NewNode(0)
	if err != nil {
		t.Fatal(err)
	}
	datas.Ids = node
	// 消息队列
	storeOut := &mq.ConsumerMock[*datas.Store]{}
	processOut := &mq.ConsumerMock[*datas.Receive]{}
	storeProducer := &mq.ProducerMock[*datas.Store]{Consumer: storeOut}
	sendConsumer := &mq.ConsumerMock[*datas.Send]{}

	storeHandler := &storeHandler{T: t, done: make(chan struct{})}
	processHandler := &processHandler{T: t, done: make(chan struct{})}

	if err := storeOut.Start(storeHandler); err != nil {
		t.Fatal(err)
	}
	if err := processOut.Start(processHandler); err != nil {
		t.Fatal(err)
	}
	// client 构造
	sendHandler := &SendHandler{
		SendChan: make(chan *datas.Send, 1),
	}

	receiveHandler := &ReceiveHandler{
		ProcessProducer: &mq.ProducerMock[*datas.Receive]{Consumer: processOut},
		Router:          router.NewShardRouter(128, sendConsumer, storeProducer),
	}

	pr, pw := io.Pipe()
	c := &Client{
		Conn: &MockConn{
			Id:     datas.GenId(),
			Reader: pr,
			Writer: pw,
		},
		StoreProducer:  storeProducer,
		SendHandler:    sendHandler,
		ReceiveHandler: receiveHandler,
	}
	sendHandler.Client = *c
	receiveHandler.Client = *c
	return &Env{
		c:              c,
		processHandler: processHandler,
		storeHandler:   storeHandler,
	}
}

// input: send data, output: bytes in mockconn
func TestSendHandler(t *testing.T) {
	e := prepareClient(t)
	c := e.c
	wg := sync.WaitGroup{}
	wg.Go(func() {
		if err := c.SendHandler.Start(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Fatal(err)
		}
	})
	wg.Go(func() {
		if err := c.ReceiveHandler.Start(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Fatal(err)
		}
	})
	m := datas.NewMMessage(&datas.Message{
		Type:       datas.MessageType_NORMAL,
		Sequence:   datas.GenId(),
		ReceiverId: "glacc",
		SenderId:   "sharve",
		Type2V2:    datas.NormalType_CHAT,
		Data: &datas.Message_Chat{
			Chat: &datas.Chat{
				DisplayName: "sharve",
				MessageChain: []*datas.MessageUnit{
					{
						Type:    datas.MessageUnitType_TEXT,
						Message: "hello glacc",
					},
				},
			},
		},
	}, 64)

	s := &datas.Send{}
	if err := s.FromMMessage(m); err != nil {
		t.Fatal(err)
	}

	c.SendHandler.SendChan <- s
	time.Sleep(time.Second)
	c.Conn.Close()
	wg.Wait()
	got := c.Conn.(*MockConn).Data
	// |MessageType_NORMAL|Sequence != 0|payload len 4B ！= 0|payload|
	if got[0] != byte(datas.MessageType_NORMAL<<4) {
		t.Fatal(got[0])
	}
	sq := binary.BigEndian.Uint64(got[1:9])
	if sq == 0 || sq >= uint64(datas.GenId()) {
		t.Fatal(sq)
	}
	len := binary.BigEndian.Uint32(got[9:13])
	if len == 0 {
		t.Fatal("len == 0")
	}
	d := datas.Message{}
	if err := proto.Unmarshal(got[13:], &d); err != nil {
		t.Fatal(err)
	}
}

func TestReceiveHandlerOnline(t *testing.T) {
	e := prepareClient(t)
	c := e.c

	wg := sync.WaitGroup{}
	wg.Go(func() {
		if err := c.SendHandler.Start(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Fatal(err)
		}
	})
	wg.Go(func() {
		if err := c.ReceiveHandler.Start(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Fatal(err)
		}
	})

	buf := bytes.NewBuffer(nil)
	buf.WriteByte(byte(datas.MessageType_NORMAL << 4))
	buf.Write(binary.BigEndian.AppendUint64([]byte{}, uint64(time.Now().UnixMilli())))
	buf.Write(binary.BigEndian.AppendUint64([]byte{}, uint64(datas.GenId())))
	buf.WriteByte(6)
	buf.WriteByte(5)
	m := datas.NewMMessage(&datas.Message{
		Type:       datas.MessageType_NORMAL,
		Sequence:   datas.GenId(),
		ReceiverId: "glacc",
		SenderId:   "sharve",
		Type2V2:    datas.NormalType_CHAT,
		Data: &datas.Message_Chat{
			Chat: &datas.Chat{
				DisplayName: "sharve",
				MessageChain: []*datas.MessageUnit{
					{
						Type:    datas.MessageUnitType_TEXT,
						Message: "hello glacc",
					},
				},
			},
		},
	}, 64)

	body, _ := proto.Marshal(m)
	buf.Write(binary.BigEndian.AppendUint32([]byte{}, uint32(len(body))))
	buf.WriteString("sharve")
	buf.WriteString("glacc")
	buf.Write(body)

	c.ReceiveHandler.Router.Set("glacc", make(chan *datas.Send))
	_, err := io.Copy(c.Conn.(*MockConn).Writer, buf)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)
	_, offline := c.ReceiveHandler.Router.IsOffline("sharve")
	if offline {
		t.Fatal("offline != false")
	}
	c.Conn.Close()
	wg.Wait()
	_, offline = c.ReceiveHandler.Router.IsOffline("sharve")
	if !offline {
		t.Fatal("offline == false")
	}
	<-e.processHandler.done

	gotAck := c.Conn.(*MockConn).Data
	// |MessageType_NORMAL|Sequence != 0|payload len 4B ！= 0|payload|
	if gotAck[0] != byte(datas.MessageType_ACK<<4)|byte(datas.AckStatus_SENDING) {
		t.Fatal(gotAck[0])
	}
	mId := binary.BigEndian.Uint64(gotAck[1:9])
	if mId == 0 || mId >= uint64(datas.GenId()) {
		t.Fatal(mId)
	}
}

func TestReceiveHandlerOffline(t *testing.T) {
	e := prepareClient(t)
	c := e.c

	wg := sync.WaitGroup{}
	wg.Go(func() {
		if err := c.SendHandler.Start(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Fatal(err)
		}
	})
	wg.Go(func() {
		if err := c.ReceiveHandler.Start(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Fatal(err)
		}
	})

	buf := bytes.NewBuffer(nil)
	buf.WriteByte(byte(datas.MessageType_NORMAL << 4))
	buf.Write(binary.BigEndian.AppendUint64([]byte{}, uint64(time.Now().UnixMilli())))
	buf.Write(binary.BigEndian.AppendUint64([]byte{}, uint64(datas.GenId())))
	buf.WriteByte(6)
	buf.WriteByte(5)
	m := datas.NewMMessage(&datas.Message{
		Type:       datas.MessageType_NORMAL,
		Sequence:   datas.GenId(),
		ReceiverId: "glacc",
		SenderId:   "sharve",
		Type2V2:    datas.NormalType_CHAT,
		Data: &datas.Message_Chat{
			Chat: &datas.Chat{
				DisplayName: "sharve",
				MessageChain: []*datas.MessageUnit{
					{
						Type:    datas.MessageUnitType_TEXT,
						Message: "hello glacc",
					},
				},
			},
		},
	}, 64)

	body, _ := proto.Marshal(m)
	buf.Write(binary.BigEndian.AppendUint32([]byte{}, uint32(len(body))))
	buf.WriteString("sharve")
	buf.WriteString("glacc")
	buf.Write(body)

	// c.ReceiveHandler.Router.Set("glacc", make(chan *datas.Send))
	_, err := io.Copy(c.Conn.(*MockConn).Writer, buf)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)
	_, offline := c.ReceiveHandler.Router.IsOffline("sharve")
	if offline {
		t.Fatal("offline != false")
	}
	c.Conn.Close()
	wg.Wait()
	_, offline = c.ReceiveHandler.Router.IsOffline("sharve")
	if !offline {
		t.Fatal("offline == false")
	}
	<-e.storeHandler.done
	gotAck := c.Conn.(*MockConn).Data
	// |MessageType_NORMAL|Sequence != 0|payload len 4B ！= 0|payload|
	if gotAck[0] != byte(datas.MessageType_ACK<<4)|byte(datas.AckStatus_SENDING) {
		t.Fatal(gotAck[0])
	}
	mId := binary.BigEndian.Uint64(gotAck[1:9])
	if mId == 0 || mId >= uint64(datas.GenId()) {
		t.Fatal(mId)
	}
}
