package listener

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/XiaWuSharve/whisperly/network/conn"
	"github.com/bwmarrin/snowflake"
	"github.com/xtaci/kcp-go/v5"
)

func TestKcp(t *testing.T) {
	node, err := snowflake.NewNode(0)
	if err != nil {
		panic(err)
	}
	datas.Ids = node

	listener, err := kcp.Listen("0.0.0.0:3001")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	lis := &KcpListener{Listener: listener}
	connChan := make(chan conn.Conn, 10)
	go func() {
		defer lis.Close()
		if err := lis.Listen(connChan); err != nil {
			if !errors.Is(err, net.ErrClosed) {
				t.Fatal(err)
			}
		}
	}()
	sess, err := kcp.Dial("127.0.0.1:3001")
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	n, err := sess.Write([]byte("hello server"))
	if n != 12 {
		t.Fatal(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	bytes := make([]byte, 12)
	select {
	case conn := <-connChan:
		id := conn.GetId()
		if id == 0 {
			t.Fatal(id == 0)
		}
		n, err := conn.GetReader().Read(bytes)
		if n != 12 {
			t.Fatal(n)
		}
		if string(bytes) != "hello server" {
			t.Fatal(string(bytes))
		}
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("time.After(5 * time.Second)")
	}
}

// func TestKCP(t *testing.T) {
// 	listener, err := kcp.Listen("0.0.0.0:3001")
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	s := NewKcpServer()
// 	ctx, cancel := context.WithCancel(context.Background())
// 	ctx, cancelTimeout := context.WithTimeout(ctx, 3*time.Second)
// 	go func() {
// 		if err := s.Start(ctx, listener); err != listener.Close() {
// 			t.Log(err)
// 		}
// 		t.Log("shutdown")
// 	}()
// 	defer listener.Close()
// 	time.Sleep(time.Second)
// 	sess, err := kcp.Dial("127.0.0.1:3001")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	defer sess.Close()

// 	client.NewClient()
// 	c := client.NewKcpClient(sess)

// 	payload, _ := proto.Marshal(&datas.Message{
// 		Data: &datas.Message_ConnectMessageRequest{
// 			ConnectMessageRequest: &datas.ConnectMessageRequest{
// 				Id:          "sharve",
// 				DisplayName: "夏午",
// 			},
// 		},
// 	})
// 	c.Send(&datas.ReceiveFrame{
// 		CreatedTime: time.Now().UnixMilli(),
// 		Payload:     payload,
// 	})

// 	f, err := c.Receive()
// 	if err != nil {
// 		t.Error(err)
// 	}

// 	var m datas.Message
// 	if err := proto.Unmarshal(f.Payload, &m); err != nil {
// 		t.Error(err)
// 	}

// 	if m.GetConnectMessageResponse().GetStatus() != datas.ConnectStatus_SUCCESS {
// 		t.Error("failed")
// 	}

// 	time.Sleep(time.Second)

// 	dto := &datas.Message{
// 		Data: &datas.Message_ChatMessage{ChatMessage: &datas.ChatMessage{
// 			RemoteId: "sharve",
// 			MessageChain: []*datas.MessageUnit{
// 				{
// 					Type:    datas.MessageUnitType_TEXT,
// 					Message: "hello kcp+protobuf",
// 				},
// 			},
// 		}},
// 	}

// 	payload, _ = proto.Marshal(dto)
// 	jsonPayload, _ := json.Marshal(dto)

// 	c.Send(&datas.ReceiveFrame{
// 		CreatedTime: time.Now().UnixMilli(),
// 		Payload:     payload,
// 	})

// 	f, err = c.Receive()
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	if err := proto.Unmarshal(f.Payload, &m); err != nil {
// 		t.Error(err)
// 	}

// 	if m.GetChatMessage().GetDisplayName() != "夏午" || m.GetChatMessage().GetMessageChain()[0].Message != "hello kcp+protobuf" {
// 		t.Errorf("failed received: %s %s", m.GetChatMessage().GetDisplayName(), m.GetChatMessage().GetMessageChain()[0].Message)
// 	}

// 	slog.Info("compression rate (byte)", "before", len(jsonPayload), "after", len(payload)+12)
// 	cancelTimeout()
// 	cancel()
// }
