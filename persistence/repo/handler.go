package repo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/XiaWuSharve/whisperly/mq"
)

type Handler struct {
	SyncStore
	// TODO 是否可以在外面Start（不用这个StoreConsumer了）
	StoreConsumer   mq.ConsumerInt[*datas.Store]
	SendProducer    mq.ProducerInt
	ReceiveProducer mq.ProducerInt
	cache           []*datas.Store
}

// Handle implements [mq.Handler].
func (h *Handler) Handle(message *datas.Store) error {
	// TODO 自定义超时时间
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	switch message.Type {
	case datas.MessageType_NORMAL:
		h.cache = append(h.cache, message)
	case datas.MessageType_PULL:
		if err := h.Ack(ctx, message.ReceiverId, message.AckSequence); err != nil {
			return fmt.Errorf("cannot ack: %w", err)
		}
		sequences, stores, err := h.Pull(ctx, message.ReceiverId, int(message.PullCount))
		if err != nil {
			return fmt.Errorf("cannot pull: %w", err)
		}
		for i, store := range stores {
			if store.SendType {
				sendData := &datas.Send{}
				sendData.FromStore(store)
				sendData.Sequence = sequences[i]
				if _, err := h.SendProducer.Enqueue(sendData); err != nil {
					return fmt.Errorf("cannot enqueue send producer: %w", err)
				}
			} else {
				// BUG Receive 塞不了sequence啊
				receiveData := &datas.Receive{}
				receiveData.FromStore(store)
				if _, err := h.ReceiveProducer.Enqueue(receiveData); err != nil {
					return fmt.Errorf("cannot enqueue receive producer: %w", err)
				}
			}
		}
	}
	return nil
}

func (h *Handler) Flush() error {
	errs, err := h.Push(context.Background(), h.cache)
	if err != nil {
		if errors.Is(err, ErrEmptyStoreArray) {
			return nil
		}
		return fmt.Errorf("flush failed: %w", err)
	}
	for i, err := range errs {
		if err != nil {
			fail := datas.Send{}
			store := h.cache[i]
			var messId int64
			if store.SendType {
				s := datas.Send{}
				s.FromStore(store)
				messId = s.MessageId
			} else {
				r := datas.Receive{}
				r.FromStore(store)
				messId = r.MessageId
			}
			fail.FromAck(datas.AckStatus_FAIL, messId, err.Error())
			_, err := h.SendProducer.Enqueue(&fail)
			if err != nil {
				return fmt.Errorf("cannot enqueue send: %w", err)
			}
		}
	}
	h.cache = h.cache[:0]
	return nil
}

var _ mq.Handler[*datas.Store] = (*Handler)(nil)

// TODO 是否可以在外面Start（不用这个Start了）
func (h *Handler) Start() error {
	if err := h.StoreConsumer.Start(h); err != nil {
		return fmt.Errorf("cannot start repo handler: %w", err)
	}
	for {
		<-time.After(time.Second)
		if err := h.Flush(); err != nil {
			return err
		}
	}
}
