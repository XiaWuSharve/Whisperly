package router

import (
	"errors"
	"fmt"
	"log/slog"
	"math/bits"
	"sync"
	"unsafe"

	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/XiaWuSharve/whisperly/mq"
	"github.com/cespare/xxhash"
)

// use NewShardRouter(bucketMaxNum uint64)
type ShardRouter struct {
	shards        []*ConcurrentRouter
	SendConsumer  mq.ConsumerInt[*datas.Send]
	StoreProducer mq.ProducerInt
	store         datas.Store
	hashFunc      func(k string) uint64
	mask          uint64
}

// 同一连接的（和所拥有的SendChan）下线和改变Id一定不会并发
func (sm *ShardRouter) ChangeId(s string, d string) {
	// 可放心创建局部变量
	ch, _ := sm.IsOffline(s)
	sm.Set(d, ch)
	sm.Delete(s)
}

type ConcurrentRouter struct {
	rwMu sync.RWMutex
	m    map[string]chan *datas.Send
}

func NewShardRouter(bucketMaxNum uint64, sendConsumer mq.ConsumerInt[*datas.Send], storeProducer mq.ProducerInt) *ShardRouter {
	if bucketMaxNum == 0 {
		return nil
	}
	// 不超过n的最大2^k-1数
	mask := uint64((1 << bits.Len(uint(bucketMaxNum))) - 1)
	if mask > bucketMaxNum {
		mask >>= 1
	}

	sm := &ShardRouter{
		shards:        make([]*ConcurrentRouter, mask+1),
		hashFunc:      HashFunc[string],
		mask:          mask,
		SendConsumer:  sendConsumer,
		StoreProducer: storeProducer,
	}
	for i := range sm.shards {
		sm.shards[i] = &ConcurrentRouter{
			m: make(map[string]chan *datas.Send),
		}
	}

	return sm
}

func (sm *ShardRouter) Send(k string, data *datas.Send) (bool, bool) {
	return sm.shards[sm.hashFunc(k)&sm.mask].Send(k, data)
}

func (sm *ShardRouter) Set(k string, v chan *datas.Send) {
	sm.shards[sm.hashFunc(k)&sm.mask].Set(k, v)
}

func (sm *ShardRouter) Delete(k string) {
	sm.shards[sm.hashFunc(k)&sm.mask].Delete(k)
}

func (sm *ShardRouter) IsOffline(id string) (chan *datas.Send, bool) {
	return sm.shards[sm.hashFunc(id)&sm.mask].IsOffline(id)
}

func (cr *ConcurrentRouter) IsOffline(id string) (chan *datas.Send, bool) {
	d, ok := cr.m[id]
	return d, !ok
}

// returns: offline, full
func (cr *ConcurrentRouter) Send(k string, data *datas.Send) (bool, bool) {
	cr.rwMu.RLock()
	defer cr.rwMu.RUnlock()
	v, ok := cr.m[k]
	if !ok {
		return true, false
	}
	select {
	case v <- data:
	default:
		return false, true
	}
	return false, false
}

func (cr *ConcurrentRouter) Set(k string, v chan *datas.Send) {
	cr.rwMu.Lock()
	defer cr.rwMu.Unlock()
	cr.m[k] = v
}

func (cr *ConcurrentRouter) Delete(k string) {
	cr.rwMu.Lock()
	defer cr.rwMu.Unlock()
	delete(cr.m, k)
}

func HashFunc[T any](t T) uint64 {
	return xxhash.Sum64(*(*[]byte)(unsafe.Pointer(&t)))
}

var _ mq.Handler[*datas.Send] = (*ShardRouter)(nil)

var ErrConnNotFound = errors.New("conn not exist")

func (r *ShardRouter) Start() error {
	return r.SendConsumer.Start(r)
}

// var ErrWaitingRetry = errors.New("send channel is full")

func (r *ShardRouter) Handle(d *datas.Send) error {
	slog.Debug("client sending frame")
	offline, full := r.Send(d.ReceiverId, d)
	if offline {
		r.store.FromSend(d)
		if _, err := r.StoreProducer.Enqueue(&r.store); err != nil {
			return fmt.Errorf("cannot enqueue store producer: %w", err)
		}
		return nil
	}
	if full {
		return mq.ErrRequeue
	}
	return nil
}
