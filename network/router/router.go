package router

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/XiaWuSharve/whisperly/mq"
	"github.com/XiaWuSharve/whisperly/network/conn"
)

type Router struct {
	SendConsumer  mq.Consumer[*datas.Send]
	Pool          *conn.Pool
	StoreProducer mq.Producer
	Send2store    datas.Converter[*datas.Send, *datas.Store]
}

var _ mq.Handler[*datas.Send] = (*Router)(nil)

var ErrConnNotFound = errors.New("conn not exist")

func (r *Router) Start() error {
	return r.SendConsumer.Start(r)
}

// var ErrWaitingRetry = errors.New("send channel is full")

func (r *Router) Handle(d *datas.Send) error {
	handler, ok := r.Pool.Get(d.ReceiverId)
	if !ok {
		storeData, err := r.Send2store.Convert(d)
		if err != nil {
			return fmt.Errorf("failed to convert send to store data: %w", err)
		}
		if _, err := r.StoreProducer.Enqueue(storeData); err != nil {
			return fmt.Errorf("cannot enqueue store producer: %w", err)
		}
		return nil
	}
	// r.HeaderBytes[0] = byte(frame.AckStatus)
	// binary.BigEndian.PutUint64(r.HeaderBytes[1:9], uint64(frame.ConnId))
	// binary.BigEndian.PutUint32(r.HeaderBytes[9:13], uint32(len(frame.Payload)))
	slog.Debug("client sending frame")

	select {
	case handler.SendChan <- d:
	default:
		return mq.ErrRequeue
	}

	return nil
}
