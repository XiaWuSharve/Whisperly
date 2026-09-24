package datas

import (
	"github.com/bwmarrin/snowflake"
	"google.golang.org/protobuf/proto"
)

type MMessage struct {
	Payload
	Message
}

// Parse implements [Decodable].
func (p *MMessage) Parse(*Payload) error {
	panic("unimplemented")
}

var _ Encodable = (*MMessage)(nil)

func (p *MMessage) ToByte() *Payload {
	p.Bytes, _ = proto.Marshal(p)
	return &p.Payload
}

type MessageDecoder struct {
	message MMessage
}

// TODO 改为 Decodable，Parse(ToByte?)时指定底层数组偏移量
var _ Decodable = (*MMessage)(nil)

func (mp *MessageDecoder) Parse(data []byte) (*MMessage, error) {
	if err := proto.Unmarshal(data, &mp.message); err != nil {
		return nil, err
	}
	mp.message.Bytes = data
	return &mp.message, nil
}

type MMessage2Send struct {
	frame Send
}

var _ Converter[*MMessage, *Send] = (*MMessage2Send)(nil)

func (m2f *MMessage2Send) Convert(mess *MMessage) (*Send, error) {
	m2f.frame.Type = mess.Type
	m2f.frame.ReceiverId = mess.ReceiverId
	m2f.frame.MessageId = mess.MessageId
	m2f.frame.ConnId = mess.ConnId
	if mess.Type == MessageType_ACK {
		m2f.frame.Ack = mess.GetAck()
	}
	m2f.frame.Payload = *(mess.ToByte())
	return &m2f.frame, nil
}

func GenId() int64 {
	return Ids.Generate().Int64()
}

var Ids *snowflake.Node
