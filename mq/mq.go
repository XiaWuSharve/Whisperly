package mq

import (
	"fmt"

	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/nsqio/go-nsq"
)

type Mq[T datas.Decodable] struct {
	Config  *nsq.Config
	Topic   string
	Decoder T
}

func (mq *Mq[T]) CreateProducer(nsqdAddress string) (*Producer, error) {
	producer, err := nsq.NewProducer(nsqdAddress, mq.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}
	return &Producer{
		Producer: producer,
		Topic:    mq.Topic,
	}, nil
}

func (mq *Mq[T]) CreateConsumer(nsqLookupdAddress string) (*Consumer[T], error) {
	consumer, err := nsq.NewConsumer(mq.Topic, "processor", mq.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}
	return &Consumer[T]{
		decoder:           mq.Decoder,
		consumer:          consumer,
		NsqLookupdAddress: nsqLookupdAddress,
	}, nil
}

// func NewMq[T datas.Decodable](topic string, decoder T) (*Mq[T], error) {
// 	// Instantiate a consumer that will subscribe to the provided channel.
// 	config := nsq.NewConfig()
// 	im := &Mq[T]{
// 		Config:  config,
// 		Topic:   topic,
// 		Decoder: decoder,
// 	}
// 	return im, nil
// }

type ReceiveMq = Mq[*datas.Receive]
type SendMq = Mq[*datas.Send]
type StoreMq = Mq[*datas.Store]
