package mq

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/nsqio/go-nsq"
)

type ConsumerInt[M datas.Decodable] interface {
	Start(handler Handler[M]) error
	Close() chan int
}

type Consumer[MessType datas.Decodable] struct {
	// 交给子类初始化
	decoder           MessType
	consumer          *nsq.Consumer
	NsqLookupdAddress string
	MaxHeaderSize     int
}

var _ ConsumerInt[datas.Decodable] = (*Consumer[datas.Decodable])(nil)

type Handler[MessType any] interface {
	Handle(message MessType) error
}

var ErrRequeue = errors.New("require requeue")

func (c *Consumer[M]) Start(handler Handler[M]) error {
	h := func(message *nsq.Message) error {
		bytes := make([]byte, c.MaxHeaderSize+len(message.Body))
		copy(bytes[c.MaxHeaderSize:], message.Body)
		err := c.decoder.Parse(&datas.Payload{
			Bytes:        bytes,
			BodyStartIdx: c.MaxHeaderSize,
		})
		if err != nil {
			slog.Error("failed to parse during handling", "err", err)
			return nil
		}
		if err := handler.Handle(c.decoder); err != nil {
			if errors.Is(err, ErrRequeue) {
				return err
			}
			slog.Error("failed to handle", "err", err)
		}
		return nil
	}
	c.consumer.AddHandler(nsq.HandlerFunc(h))

	// Use nsqlookupd to discover nsqd instances.
	// See also ConnectToNSQD, ConnectToNSQDs, ConnectToNSQLookupds.
	err := c.consumer.ConnectToNSQLookupd(c.NsqLookupdAddress)
	if err != nil {
		return fmt.Errorf("consumer failed to connect to nsq lookup damon: %w", err)
	}
	return nil
}

func (c *Consumer[MessType]) Close() chan int {
	c.consumer.Stop()
	return c.consumer.StopChan
}

type ReceiveConsumer = Consumer[*datas.Receive]
type SendConsumer = Consumer[*datas.Send]
type StoreConsumer = Consumer[*datas.Store]

type ConsumerMock[M any] struct {
	handler Handler[M]
}

// Close implements [ConsumerInt].
func (c *ConsumerMock[M]) Close() chan int {
	ch := make(chan int)
	close(ch)
	return ch
}

// Start implements [ConsumerInt].
func (c *ConsumerMock[M]) Start(handler Handler[M]) error {
	c.handler = handler
	return nil
}

var _ ConsumerInt[datas.Decodable] = (*ConsumerMock[datas.Decodable])(nil)
