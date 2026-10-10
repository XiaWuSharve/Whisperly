package processor

import (
	"fmt"
	"log/slog"

	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/XiaWuSharve/whisperly/mq"
)

type Processor struct {
	SendProduer     mq.ProducerInt
	ReceiveConsumer mq.ConsumerInt[*datas.Receive]
	// Receive2Message datas.Converter[*datas.Receive, *datas.MMessage]
	// Message2Send    datas.Converter[*datas.MMessage, *datas.Send]
}

var _ mq.Handler[*datas.Receive] = (*Processor)(nil)

func (p *Processor) Start() error {
	if err := p.ReceiveConsumer.Start(p); err != nil {
		return fmt.Errorf("cannot start processor: %w", err)
	}
	return nil
}

func (p *Processor) send(m *datas.MMessage) error {
	send := &datas.Send{}
	send.FromMMessage(m)
	_, err := p.SendProduer.Enqueue(send)
	if err != nil {
		return fmt.Errorf("cannot enqueue sender: %w", err)
	}
	// TODO 异常处理
	return nil
}

func (p *Processor) ackSent(m *datas.MMessage) error {
	m.Type = datas.MessageType_ACK
	m.Data = &datas.Message_Ack{
		Ack: &datas.Ack{
			Status: datas.AckStatus_SENT,
		},
	}
	m.ReceiverId = m.SenderId
	return p.send(m)
}

func (p *Processor) ackFail(m *datas.MMessage, reason string) error {
	// FAIL message
	m.Type = datas.MessageType_ACK
	m.Type2 = &datas.Message_AckStatus{
		AckStatus: datas.AckStatus_FAIL,
	}
	m.Data = &datas.Message_Ack{Ack: &datas.Ack{
		Reason: reason,
	}}
	m.ReceiverId = m.SenderId
	return p.send(m)
}

// TODO 抽出发送入队逻辑
func (p *Processor) Handle(frame *datas.Receive) error {
	slog.Debug("received raw message", "payload", frame.Payload)
	mMessage := datas.NewMMessage(&datas.Message{}, 128)
	mMessage.FromReceive(frame)
	slog.Debug("message", "mess", mMessage.String())
	switch mMessage.Type2V2 {
	case datas.NormalType_CANDIDATE:
		// TODO check candiMess info
	case datas.NormalType_CHAT:
		chatMess := mMessage.GetChat()
		for _, v := range chatMess.MessageChain {
			if v.Type != datas.MessageUnitType_TEXT &&
				v.Type != datas.MessageUnitType_CALL_UNIT &&
				v.Type != datas.MessageUnitType_ANSWER_UNIT &&
				v.Type != datas.MessageUnitType_ESTABLISH {
				p.ackFail(mMessage, fmt.Sprintf("包含不支持的消息类型: %s", v.Type.String()))
				// TODO 之后用于精细化控制队列行为
				return nil
			}
		}
	case datas.NormalType_CALL:
	case datas.NormalType_ANSWER:
	}
	if err := p.send(mMessage); err != nil {
		err = p.ackFail(mMessage, fmt.Sprintf("发送失败：%s", err))
		return fmt.Errorf("cannot send normal message: %w", err)
	}
	if err := p.ackSent(mMessage); err != nil {
		if err := p.ackFail(mMessage, fmt.Sprint("无法确认收到: %w", err)); err != nil {
			err = fmt.Errorf("cannot send fail ack: %w", err)
		}
		return fmt.Errorf("cannot ack sent: %w", err)
	}
	return nil
}
