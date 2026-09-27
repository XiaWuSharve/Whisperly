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
			Type:       datas.MessageType_NORMAL,
			SendType:   true,
			ReceiverId: "sharve",
			Payload:    *datas.FromByte([]byte("test timeline"), 128),
		},
		{
			Type:       datas.MessageType_NORMAL,
			ReceiverId: "sharve",
			Payload:    *datas.FromByte([]byte("hello sharve"), 128),
		},
		{
			Type:       datas.MessageType_NORMAL,
			ReceiverId: "glacc",
			Payload:    *datas.FromByte([]byte("hello glacc"), 128),
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
	outSeq, outStore, err := store.Pull(ctx, "sharve", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(outStore) != 2 {
		t.Fatal("len(out): ", len(outStore))
	}
	for i, o := range outStore {
		if outSeq[i] == 0 {
			t.Fatal("outSeq[i] == 0")
		}
		if o.ReceiverId != "sharve" {
			t.Fatal(o.ReceiverId)
		}
		if string(datas.ToByte(&o.Payload)) != string(datas.ToByte(&want[1-i].Payload)) {
			t.Fatal(string(o.Payload.Bytes))
		}
	}
	outSeq, outStore, err = store.Pull(ctx, "sharve", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(outStore) != 1 {
		t.Fatal("len(out): ", len(outStore))
	}
	for i, o := range outStore {
		if outSeq[i] == 0 {
			t.Fatal("outSeq[i] == 0")
		}
		if o.ReceiverId != "sharve" {
			t.Fatal(o.ReceiverId)
		}
		if string(datas.ToByte(&o.Payload)) != string(datas.ToByte(&want[1].Payload)) {
			t.Fatal(string(o.Payload.Bytes))
		}
	}
	if err := store.Ack(ctx, "sharve", outSeq[0]); err != nil {
		t.Fatal(err)
	}
	outSeq, outStore, err = store.Pull(ctx, "sharve", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(outStore) != 1 {
		t.Fatal("len(out): ", len(outStore))
	}
	for i, o := range outStore {
		if outSeq[i] == 0 {
			t.Fatal("outSeq[i] == 0")
		}
		if o.ReceiverId != "sharve" {
			t.Fatal(o.ReceiverId)
		}
		if string(datas.ToByte(&o.Payload)) != string(datas.ToByte(&want[0].Payload)) {
			t.Fatal(string(o.Payload.Bytes))
		}
	}
	cancel()
}

func TestHandler(t *testing.T) {
	// TODO
}
