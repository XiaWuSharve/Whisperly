package conn

import (
	"bufio"
	"errors"
	"fmt"
	"log/slog"
	"net"

	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/XiaWuSharve/whisperly/mq"
)

type SendHandler struct {
	Conn
	Err           error
	SendChan      chan *datas.Send
	sendData      *datas.Send
	storeData     *datas.Store
	storeProducer mq.Producer
	SendConsumer  mq.Consumer[*datas.Send]
	routed2store  datas.Converter[*datas.Send, *datas.Store]
}

// Handle implements [mq.Handler].
func (s *SendHandler) Handle(message *datas.Send) error {
	s.SendChan <- message
	return nil
}

var _ mq.Handler[*datas.Send] = (*SendHandler)(nil)

func (s *SendHandler) close() {
	close(s.SendChan)
	var err error
	for s.sendData = range s.SendChan {
		s.storeData, err = s.routed2store.Convert(s.sendData)
		if err != nil {
			slog.Error("failed to conver routed send to cache: %w", "err", err)
		}
		_, err = s.storeProducer.Enqueue(s.storeData)
		if err != nil {
			slog.Error("failed to enqueue store mq", "err", err)
		}
	}
}

func (s *SendHandler) Start() error {
	defer s.close()
	if err := s.SendConsumer.Start(s); err != nil {
		return fmt.Errorf("cannot start send consumer: %w", err)
	}
	defer s.SendConsumer.Stop()
	for s.sendData = range s.SendChan {
		if s.Err = s.Send(s.sendData.ToByte()); s.Err != nil {
			if errors.Is(s.Err, net.ErrClosed) {
				return s.Err
			}
			return fmt.Errorf("cannot send: %w", s.Err)
		}
	}
	return nil
}

type ReceiveHandler struct {
	Conn
	ProcessProducer mq.Producer
	StoreProducer   mq.Producer
	receiveData     *datas.Receive
	streamDecoder   datas.ReceiveStreamDecoder
	storeConverter  datas.Converter[*datas.Receive, *datas.Store]
	storeData       *datas.Store
	// to self sendHandler channel
	SendChan chan *datas.Send
	ok       bool
	sendData datas.Send
	Err      error
	Pool     *Pool
}

func (c *ReceiveHandler) Start() error {
	defer close(c.SendChan)
	// 只处理与业务无关的连接相关的逻辑
	reader := bufio.NewReader(c.GetReader())
	for {
		c.receiveData, c.Err = c.streamDecoder.Parse(reader)
		if c.Err != nil {
			if errors.Is(c.Err, net.ErrClosed) {
				return c.Err
			} else if errors.Is(c.Err, datas.ErrTimeLargeOffset) {
				// send fail ACK
				c.ackFail()
				slog.Error(c.Err.Error())
				continue
			} else {
				return fmt.Errorf("failed to handle receive: %w", c.Err)
			}
		}
		// TODO 消息类型：normal/pull(ack sequence+pull count)
		switch c.receiveData.Type {
		case datas.MessageType_NORMAL:
			// TODO offline store
			_, c.ok = c.Pool.FindByUserId(c.receiveData.ReceiverId)
			if !c.ok {
				c.toStore()
			} else {
				// TODO normal ACK SENDING
				c.ProcessProducer.Enqueue(c.receiveData)
				// TODO transaction chan
			}
		case datas.MessageType_PULL:
			c.toStore()
		}
		c.ackSending()
	}
}

func (c *ReceiveHandler) ackFail() {
	c.sendData.AckStatus = datas.AckStatus_FAIL
	c.sendData.MessageId = c.receiveData.MessageId
	// TODO reason bit
	c.SendChan <- &c.sendData
}

func (c *ReceiveHandler) ackSending() {
	c.sendData.AckStatus = datas.AckStatus_SENDING
	c.sendData.MessageId = c.receiveData.MessageId
	// TODO reason bit
	c.SendChan <- &c.sendData
}

func (c *ReceiveHandler) toStore() {
	c.storeData, c.Err = c.storeConverter.Convert(c.receiveData)
	if c.Err != nil {
		c.ackFail()
		return
	}
	_, c.Err = c.StoreProducer.Enqueue(c.storeData)
	if c.Err != nil {
		c.ackFail()
		return
	}
}
