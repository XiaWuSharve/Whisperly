package mq

import (
	"fmt"

	"github.com/XiaWuSharve/whisperly/datas"

	"github.com/nsqio/go-nsq"
)

type ProducerInt interface {
	Enqueue(data datas.Encodable) (chan *nsq.ProducerTransaction, error)
	Close()
}

type Producer struct {
	Producer *nsq.Producer
	Topic    string
}

var _ ProducerInt = (*Producer)(nil)

// 入队失败了该如何处理？绕过队列直接持久化？
func (p *Producer) Enqueue(data datas.Encodable) (chan *nsq.ProducerTransaction, error) {
	doneChan := make(chan *nsq.ProducerTransaction)
	payload := data.ToByte()
	if err := p.Producer.PublishAsync(p.Topic, payload.Bytes[payload.BodyStartIdx:], doneChan); err != nil {
		return nil, fmt.Errorf("failed to publish message: %w", err)
	}
	return doneChan, nil
}

func (p *Producer) Close() {
	// Gracefully stop the producer.
	p.Producer.Stop()
}

type ProducerMock[M datas.Encodable] struct {
	Consumer *ConsumerMock[M]
}

// Close implements [ProducerInt].
func (p *ProducerMock[M]) Close() {
}

// Enqueue implements [ProducerInt].
func (p *ProducerMock[M]) Enqueue(data datas.Encodable) (chan *nsq.ProducerTransaction, error) {
	p.Consumer.handler.Handle(data.(M))
	ch := make(chan *nsq.ProducerTransaction)
	close(ch)
	return ch, nil
}

var _ ProducerInt = (*ProducerMock[datas.Encodable])(nil)
