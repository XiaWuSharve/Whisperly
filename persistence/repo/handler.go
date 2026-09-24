package repo

import (
	"fmt"

	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/XiaWuSharve/whisperly/mq"
)

type Handler struct {
	SyncStore
	// TODO 是否可以在外面Start（不用这个StoreConsumer了）
	StoreConsumer   mq.Consumer[*datas.Store]
	SendProducer    mq.Producer
	ReceiveProducer mq.Producer
}

// Handle implements [mq.Handler].
func (h *Handler) Handle(message *datas.Store) error {
	if message.SendType {
		sendData := &datas.Send{}
		sendData.FromStore(message)
		if _, err := h.SendProducer.Enqueue(sendData); err != nil {
			return fmt.Errorf("cannot enqueue send producer: %w", err)
		}
	} else {
		receiveData := &datas.Receive{}
		receiveData.FromStore(message)
		if _, err := h.ReceiveProducer.Enqueue(receiveData); err != nil {
			return fmt.Errorf("cannot enqueue receive producer: %w", err)
		}
	}
	return nil
}

var _ mq.Handler[*datas.Store] = (*Handler)(nil)

// TODO 是否可以在外面Start（不用这个Start了）
func (h *Handler) Start() error {
	if err := h.StoreConsumer.Start(h); err != nil {
		return fmt.Errorf("cannot start repo handler: %w", err)
	}
	return nil
}
