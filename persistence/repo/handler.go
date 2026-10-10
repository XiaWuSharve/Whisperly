package repo

import (
	"context"
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
	if message.Type == datas.MessageType_PULL {
		h.Ack(context.Background(), message.ReceiverId, message.AckSequence)
		h.Pull(context.Background(), message.ReceiverId, int(message.PullCount))
	} else {
		// TODO 批量刷写
		h.Push(context.Background(), []*datas.Store{message})
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
