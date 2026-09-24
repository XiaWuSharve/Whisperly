package repo

import (
	"context"
	"testing"
	"time"

	"github.com/XiaWuSharve/whisperly/config"
	"github.com/XiaWuSharve/whisperly/datas"
)

func TestTableStore(t *testing.T) {
	config.Tablestore = &config.TablestoreConfig{
		Endpoint:      "http://101.37.76.38:8084",
		Instance:      "x02caat39505",
		AkId:          "abcdef",
		AkSecret:      "abcdef",
		BatchChanSize: 50,
	}
	store, err := NewSyncStore()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer func() {
		store.Ack(ctx, "sharve", 0)
		store.Ack(ctx, "glacc", 0)
		store.Close()
	}()
	want := []*datas.Store{
		{
			SendType:   true,
			ReceiverId: "sharve",
			Payload: datas.Payload{
				Bytes:        []byte("test timeline"),
				BodyStartIdx: 256,
			},
		},
		{
			ReceiverId: "sharve",
			Payload: datas.Payload{
				Bytes:        []byte("hello sharve"),
				BodyStartIdx: 256,
			},
		},
		{
			ReceiverId: "glacc",
			Payload: datas.Payload{
				Bytes:        []byte("hello glacc"),
				BodyStartIdx: 256,
			},
		},
	}
	errs, err := store.Push(ctx, want)
	if err != nil {
		t.Fatal(err)
	}
	for i, err := range errs {
		if err != nil {
			t.Error(err, want[i])
		}
	}
	out, err := store.Pull(ctx, "sharve", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 {
		t.Fatal("len(out): ", len(out))
	}
	for i, o := range out {
		if o.Sequence == 0 {
			t.Fatal("o.Sequence == 0")
		}
		if o.ReceiverId != "sharve" {
			t.Fatal(o.ReceiverId)
		}
		if string(o.Payload.Bytes) != string(want[1-i].Payload.Bytes) {
			t.Fatal(string(o.Payload.Bytes))
		}
	}
	out, err = store.Pull(ctx, "sharve", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatal("len(out): ", len(out))
	}
	for _, o := range out {
		if o.Sequence == 0 {
			t.Fatal("o.Sequence == 0")
		}
		if o.ReceiverId != "sharve" {
			t.Fatal(o.ReceiverId)
		}
		if string(o.Payload.Bytes) != string(want[1].Payload.Bytes) {
			t.Fatal(string(o.Payload.Bytes))
		}
	}
	if err := store.Ack(ctx, "sharve", out[0].Sequence); err != nil {
		t.Fatal(err)
	}
	out, err = store.Pull(ctx, "sharve", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatal("len(out): ", len(out))
	}
	for _, o := range out {
		if o.Sequence == 0 {
			t.Fatal("o.Sequence == 0")
		}
		if o.ReceiverId != "sharve" {
			t.Fatal(o.ReceiverId)
		}
		if string(o.Payload.Bytes) != string(want[0].Payload.Bytes) {
			t.Fatal(string(o.Payload.Bytes))
		}
	}
	cancel()
}
