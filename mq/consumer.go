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
	consumer          *nsq.Consumer
	NsqLookupdAddress string
}

var _ ConsumerInt[datas.Decodable] = (*Consumer[datas.Decodable])(nil)

type Handler[MessType any] interface {
	Handle(message MessType) error
}

var ErrRequeue = errors.New("require requeue")

func (c *Consumer[M]) Start(handler Handler[M]) error {
	h := func(message *nsq.Message) error {
		// TODO 拷贝？
		var decodeData M
		decodeData.From(datas.FromByte(message.Body, 128))
		if err := handler.Handle(decodeData); err != nil {
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

type ConsumerMock[M datas.Decodable] struct {
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
