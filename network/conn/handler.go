package conn

import (
	"bufio"
	"errors"
	"fmt"
	"log/slog"
	"net"

	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/XiaWuSharve/whisperly/mq"
	"github.com/XiaWuSharve/whisperly/network/router"
)

type Client struct {
	Conn
	StoreProducer  mq.ProducerInt
	SendHandler    *SendHandler
	ReceiveHandler *ReceiveHandler
	id             string
}

type ReceiveHandler struct {
	Client
	ProcessProducer mq.ProducerInt
	receiveData     datas.Receive
	storeData       datas.Store
	Router          *router.ShardRouter
	offline         bool
	Err             error
}

type SendHandler struct {
	Client
	Err       error
	SendChan  chan *datas.Send
	sendData  *datas.Send
	storeData datas.Store
}

// Handle implements [mq.Handler].
// func (s *SendHandler) Handle(message *datas.Send) error {
// 	s.SendChan <- message
// 	return nil
// }

// var _ mq.Handler[*datas.Send] = (*SendHandler)(nil)
// 发送chan不能由自己（消费者）关闭
func (s *SendHandler) close() {
	// 非正常退出要将最后一个保存
	if s.sendData != nil {
		s.storeData.FromSend(s.sendData)
		_, err := s.StoreProducer.Enqueue(&s.storeData)
		if err != nil {
			slog.Error("failed to enqueue store mq", "err", err)
		}
	}
	for s.sendData = range s.SendChan {
		s.storeData.FromSend(s.sendData)
		_, err := s.StoreProducer.Enqueue(&s.storeData)
		if err != nil {
			slog.Error("failed to enqueue store mq", "err", err)
		}
	}
}

func (s *SendHandler) Start() error {
	defer s.close()
	for s.sendData = range s.SendChan {
		// TODO 需要加一个ToByte接口（这个改为ToPayload）
		if s.Err = s.Send(datas.ToByte(s.sendData.ToPayload())); s.Err != nil {
			if errors.Is(s.Err, net.ErrClosed) {
				break
			}
			return fmt.Errorf("cannot send: %w", s.Err)
		}
	}
	s.sendData = nil
	return nil
}

var ErrIdNotFound = errors.New("id not found, try adding first")

// 只处理与业务无关的连接相关的逻辑
func (r *ReceiveHandler) Start() error {
	defer func() {
		if r.id != "" {
			r.Router.Delete(r.id)
		}
		close(r.SendHandler.SendChan)
		r.Conn.Close()
	}()
	reader := bufio.NewReader(r.GetReader())
	for {
		//解析
		// TODO config
		r.Err = r.receiveData.FromStream(reader, 128)
		if r.Err != nil {
			if errors.Is(r.Err, net.ErrClosed) {
				return r.Err
			} else if errors.Is(r.Err, datas.ErrTimeLargeOffset) {
				// send fail ACK
				r.ackFail(fmt.Sprintf("请校对时钟：%s", r.Err))
				slog.Error(r.Err.Error())
				continue
			} else {
				return fmt.Errorf("failed to handle receive: %w", r.Err)
			}
		}
		// 更新id
		if r.id == "" {
			r.Router.Set(r.receiveData.SenderId, r.SendHandler.SendChan)
		} else if r.id != r.receiveData.SenderId {
			r.Router.ChangeId(r.id, r.receiveData.SenderId)
		}
		r.id = r.receiveData.SenderId
		// 状态机
		// 消息类型：normal/pull(ack sequence+pull count)
		switch r.receiveData.Type {
		case datas.MessageType_NORMAL:
			// offline store
			_, r.offline = r.Router.IsOffline(r.receiveData.ReceiverId)
			if r.offline {
				r.toStore()
			} else {
				// normal ACK SENDING
				r.ProcessProducer.Enqueue(&r.receiveData)
				// TODO transaction chan
			}
		case datas.MessageType_PULL:
			r.toStore()
		}
		r.ackSending()
	}
}

// receiver 发送给 sender 的数据结构体一律不能复用（原地修改）
// TODO datas.Send.FromAck(status, messId, reason)
func (r *ReceiveHandler) ackFail(reason string) {
	// TODO reason bit
	f := &datas.Send{
		Payload: datas.Payload{
			Bytes:        make([]byte, 13),
			BodyStartIdx: 13,
		},
		Type:      datas.MessageType_ACK,
		AckStatus: datas.AckStatus_FAIL,
		MessageId: r.receiveData.MessageId,
	}
	r.SendHandler.SendChan <- f
}

func (r *ReceiveHandler) ackSending() {
	// TODO reason bit
	f := &datas.Send{
		Payload: datas.Payload{
			Bytes:        make([]byte, 13),
			BodyStartIdx: 13,
		},
		Type:      datas.MessageType_ACK,
		AckStatus: datas.AckStatus_SENDING,
		MessageId: r.receiveData.MessageId,
	}
	r.SendHandler.SendChan <- f
}

func (r *ReceiveHandler) toStore() {
	r.storeData.FromReceive(&r.receiveData)
	_, r.Err = r.StoreProducer.Enqueue(&r.storeData)
	if r.Err != nil {
		r.ackFail(fmt.Sprintf("无法暂存数据：%s", r.Err))
		return
	}
}
