package datas

import (
	"github.com/bwmarrin/snowflake"
	"google.golang.org/protobuf/proto"
)

// use NewMMessage(message *Message, headerSize int)
type MMessage struct {
	Payload
	*Message
}

// From implements [Decodable].
func (p *MMessage) From(*Payload) {
	panic("unimplemented")
}

func NewMMessage(message *Message, headerSize int) *MMessage {
	m := &MMessage{
		Message: message,
	}
	m.BodyStartIdx = headerSize
	return m
}

// ToPayload implements [Encodable].
func (p *MMessage) ToPayload() *Payload {
	bytes, _ := proto.Marshal(p)
	p.Bytes = make([]byte, p.BodyStartIdx+len(bytes))
	copy(p.Bytes[p.BodyStartIdx:], bytes)
	return &p.Payload
}

var _ Encodable = (*MMessage)(nil)

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
	m2f.frame.Payload = *mess.ToPayload()
	return &m2f.frame, nil
}

func GenId() int64 {
	return Ids.Generate().Int64()
}

var Ids *snowflake.Node
