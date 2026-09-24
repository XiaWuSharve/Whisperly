package repo

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/XiaWuSharve/whisperly/config"
	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/aliyun/aliyun-tablestore-go-sdk/timeline"
	"github.com/aliyun/aliyun-tablestore-go-sdk/timeline/promise"
)

type SyncStore struct {
	syncStore   timeline.MessageStore
	adapter     timeline.MessageAdapter
	BufChanSize int
}

type MyAdapter struct {
	MaxHeaderSize int
}

const (
	PAYLOAD = "payload"
)

// Marshal implements [timeline.MessageAdapter].
func (m *MyAdapter) Marshal(msg timeline.Message) (*timeline.ColumnMap, error) {
	store, ok := msg.(*datas.Store)
	if !ok {
		return nil, timeline.ErrUnexpected
	}
	col := timeline.NewColumnMap()
	payload := store.ToByte()
	col.AddBytesColumn(PAYLOAD, payload.Bytes[payload.BodyStartIdx:])
	return col, nil
}

type ErrFieldNotFound struct {
	FieldName string
}

func (efn *ErrFieldNotFound) Error() string {
	return efn.FieldName + " not found"
}

var ErrPayloadNotFound = &ErrFieldNotFound{PAYLOAD}

// Unmarshal implements [timeline.MessageAdapter].
func (m *MyAdapter) Unmarshal(cols *timeline.ColumnMap) (timeline.Message, error) {
	mp := cols.ToMap()
	v, ok := mp[PAYLOAD]
	if !ok {
		return nil, ErrPayloadNotFound
	}
	store := &datas.Store{}
	body := v.([]byte)
	// TODO 可能可以封装？
	bytes := make([]byte, len(body)+m.MaxHeaderSize)
	copy(bytes[m.MaxHeaderSize:], body)
	store.Parse(&datas.Payload{
		Bytes:        bytes,
		BodyStartIdx: m.MaxHeaderSize,
	})

	return store, nil
}

var _ timeline.MessageAdapter = (*MyAdapter)(nil)

func NewSyncStore() (*SyncStore, error) {
	cfg := config.Tablestore
	option := timeline.StoreOption{
		Endpoint:  cfg.Endpoint,
		Instance:  cfg.Instance,
		TableName: "sync",
		AkId:      cfg.AkId,
		AkSecret:  cfg.AkSecret,
		TTL:       -1, // Data time to alive, eg: almost one year
	}
	syncStore, err := timeline.NewDefaultStore(option)
	if err != nil {
		return nil, fmt.Errorf("cannot create sync store: %w", err)
	}
	if err := syncStore.Sync(); err != nil {
		return nil, fmt.Errorf("cannot sync sync store: %w", err)
	}
	return &SyncStore{syncStore: syncStore, adapter: &MyAdapter{}, BufChanSize: cfg.BatchChanSize}, nil
}

var ErrEmptyStoreArray = errors.New("the len of store array is 0")

// 支持不同ReceiverIds
func (st *SyncStore) Push(ctx context.Context, stores []*datas.Store) ([]error, error) {
	if len(stores) == 0 {
		return nil, ErrEmptyStoreArray
	}
	futures := make([]*promise.Future, len(stores))
	for i, store := range stores {
		tm, err := timeline.NewTmLine(store.ReceiverId, st.adapter, st.syncStore)
		if err != nil {
			return nil, fmt.Errorf("cannot create timeline: %w", err)
		}
		f, err := tm.BatchStore(store)
		if err != nil {
			return nil, fmt.Errorf("cannot store message: %w", err)
		}
		futures[i] = f
	}
	fanInFuture := promise.FanIn(futures...)
	results, err := fanInFuture.FanInGet()
	if err != nil {
		return nil, fmt.Errorf("cannot get fanin: %w", err)
	}
	errs := make([]error, len(stores))
	for i, res := range results {
		errs[i] = res.Err
	}
	return errs, nil
}

func (st *SyncStore) Pull(ctx context.Context, receiverId string, msgCount int) ([]*datas.Store, error) {
	tm, err := timeline.NewTmLine(receiverId, st.adapter, st.syncStore)
	if err != nil {
		return nil, fmt.Errorf("cannot create timeline: %w", err)
	}
	it := tm.Scan(&timeline.ScanParameter{
		From:        math.MaxInt64,
		To:          0,
		MaxCount:    msgCount,
		BufChanSize: st.BufChanSize,
	})
	defer it.Close()
	stores := make([]*datas.Store, msgCount)
	trueCount := 0
	for i := range stores {
		entry, err := it.Next()
		if err != nil {
			if errors.Is(err, timeline.ErrorDone) {
				break
			}
			return nil, fmt.Errorf("cannot scan timeline: %w", err)
		}
		store, ok := entry.Message.(*datas.Store)
		if !ok {
			return nil, fmt.Errorf("unexpected message type: %T", entry.Message)
		}
		store.ReceiverId = receiverId
		store.Sequence = entry.Sequence
		stores[i] = store
		trueCount++
	}
	return stores[:trueCount], nil
}

func (st *SyncStore) Ack(ctx context.Context, receiverId string, minSequence int64) error {
	tm, err := timeline.NewTmLine(receiverId, st.adapter, st.syncStore)
	if err != nil {
		return fmt.Errorf("cannot create timeline: %w", err)
	}
	it := tm.Scan(&timeline.ScanParameter{
		From:        math.MaxInt64,
		To:          minSequence - 1,
		BufChanSize: st.BufChanSize,
	})
	defer it.Close()
	for {
		entry, err := it.Next()
		if err != nil {
			if errors.Is(err, timeline.ErrorDone) {
				return nil
			} else {
				return fmt.Errorf("cannot scan timeline: %w", err)
			}
		}
		if err := tm.Delete(entry.Sequence); err != nil {
			return fmt.Errorf("cannot delete %d: %w", entry.Sequence, err)
		}
	}
}

func (st *SyncStore) Close() {
	st.syncStore.Close()
}
