package conn

import (
	"bufio"
	"errors"
	"fmt"
	"log/slog"
	"net"

	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/XiaWuSharve/whisperly/mq"
	"github.com/XiaWuSharve/whisperly/utils"
)

type Pool = utils.ShardMap[string, *SendHandler]

type Client struct {
	Conn
	StoreProducer  mq.ProducerInt
	SendHandler    *SendHandler
	ReceiveHandler *ReceiveHandler
	Pool           *Pool
}

type ReceiveHandler struct {
	Client
	ProcessProducer mq.ProducerInt
	receiveData     *datas.Receive
	StreamDecoder   datas.ReceiveStreamDecoder
	storeData       datas.Store
	// to self sendHandler channel
	ok       bool
	sendData datas.Send
	Err      error
}

type SendHandler struct {
	Client
	Err          error
	SendChan     chan *datas.Send
	sendData     *datas.Send
	storeData    *datas.Store
	SendConsumer mq.ConsumerInt[*datas.Send]
	id           *string
	payload      *datas.Payload
}

// Handle implements [mq.Handler].
func (s *SendHandler) Handle(message *datas.Send) error {
	s.SendChan <- message
	return nil
}

var _ mq.Handler[*datas.Send] = (*SendHandler)(nil)

func (s *SendHandler) close() {
	close(s.SendChan)
	for s.sendData = range s.SendChan {
		s.storeData.FromSend(s.sendData)
		_, err := s.StoreProducer.Enqueue(s.storeData)
		if err != nil {
			slog.Error("failed to enqueue store mq", "err", err)
		}
	}
}

func (s *SendHandler) Start() error {
	defer s.close()
	if err := s.SendConsumer.Start(s); err != nil {
		return fmt.Errorf("cannot start send consumer: %w", err)
	}
	defer s.SendConsumer.Close()
	for s.sendData = range s.SendChan {
		// TODO 需要加一个ToByte接口（这个改为ToPayload）
		s.payload = s.sendData.ToByte()
		if s.Err = s.Send(s.payload.Bytes[s.payload.BodyStartIdx:]); s.Err != nil {
			if errors.Is(s.Err, net.ErrClosed) {
				return s.Err
			}
			return fmt.Errorf("cannot send: %w", s.Err)
		}
	}
	return nil
}

var ErrIdNotFound = errors.New("id not found, try adding first")

func (r *ReceiveHandler) Start() error {
	defer close(r.SendHandler.SendChan)
	// 只处理与业务无关的连接相关的逻辑
	reader := bufio.NewReader(r.GetReader())
	for {
		r.receiveData, r.Err = r.StreamDecoder.Parse(reader)
		if r.Err != nil {
			if errors.Is(r.Err, net.ErrClosed) {
				return r.Err
			} else if errors.Is(r.Err, datas.ErrTimeLargeOffset) {
				// send fail ACK
				r.ackFail(fmt.Sprintf("数据格式转换失败：%s", r.Err))
				slog.Error(r.Err.Error())
				continue
			} else {
				return fmt.Errorf("failed to handle receive: %w", r.Err)
			}
		}
		if r.SendHandler.id == nil {
			r.SendHandler.id = &r.receiveData.SenderId
			r.Pool.Set(*r.SendHandler.id, r.SendHandler)
		} else if *r.SendHandler.id != r.receiveData.SenderId {
			handler, ok := r.Pool.Get(r.receiveData.SenderId)
			if !ok {
				return ErrIdNotFound
			}
			r.Pool.Set(r.receiveData.SenderId, handler)
			r.Pool.Delete(*r.SendHandler.id)
		}
		// 消息类型：normal/pull(ack sequence+pull count)
		switch r.receiveData.Type {
		case datas.MessageType_NORMAL:
			// offline store
			_, r.ok = r.Pool.Get(r.receiveData.ReceiverId)
			if !r.ok {
				r.toStore()
			} else {
				// normal ACK SENDING
				r.ProcessProducer.Enqueue(r.receiveData)
				// TODO transaction chan
			}
		case datas.MessageType_PULL:
			r.toStore()
		}
		r.ackSending()
	}
}

func (r *ReceiveHandler) ackFail(reason string) {
	r.sendData.Type = datas.MessageType_ACK
	r.sendData.Ack = &datas.Ack{
		Reason: reason,
		Status: datas.AckStatus_FAIL,
	}
	r.sendData.MessageId = r.receiveData.MessageId
	// TODO reason bit
	r.SendHandler.SendChan <- &r.sendData
}

func (r *ReceiveHandler) ackSending() {
	r.sendData.Type = datas.MessageType_ACK
	r.sendData.Ack = &datas.Ack{
		Status: datas.AckStatus_SENDING,
	}
	r.sendData.MessageId = r.receiveData.MessageId
	// TODO reason bit
	r.SendHandler.SendChan <- &r.sendData
}

func (r *ReceiveHandler) toStore() {
	r.storeData.FromReceive(r.receiveData)
	if r.Err != nil {
		r.ackFail(fmt.Sprintf("数据格式转换失败：%s", r.Err))
		return
	}
	_, r.Err = r.StoreProducer.Enqueue(&r.storeData)
	if r.Err != nil {
		r.ackFail(fmt.Sprintf("无法暂存数据：%s", r.Err))
		return
	}
}
