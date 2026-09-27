package datas

import (
	"encoding/binary"
	"fmt"
)

// send frame:
// | MessageType type = NORMAL 4b | reserved 4b | sequence 8B | payload len 4B | payload... |
// | MessageType type = ACK 4b | AckStatus 4b | mess id 8B | payload len 4B | payload... |
type Send struct {
	Payload
	Type      MessageType
	AckStatus AckStatus
	// CreatedTime int64
	// Ack        *Ack
	ReceiverId string // 换成发送者Id不挺好？
	MessageId  int64
	Sequence   int64
	// ConnId     int64 // deprecated?
}

type ErrUnsupportedType struct {
	Type MessageType
}

func (e *ErrUnsupportedType) Error() string {
	return fmt.Sprintf("unsupported type: %s", e.Type.String())
}

var _ error = (*ErrUnsupportedType)(nil)

// ToPayload implements [Encodable].
func (s *Send) ToPayload() *Payload {
	switch s.Type {
	case MessageType_NORMAL:
		s.Bytes[s.BodyStartIdx-13] = byte(s.Type << 4)
		binary.BigEndian.PutUint64(s.Bytes[s.BodyStartIdx-12:s.BodyStartIdx-4], uint64(s.Sequence))
	case MessageType_ACK:
		s.Bytes[s.BodyStartIdx-13] = (byte(s.Type<<4) | byte(s.AckStatus))
		binary.BigEndian.PutUint64(s.Bytes[s.BodyStartIdx-12:s.BodyStartIdx-4], uint64(s.MessageId))
	default:
		panic(&ErrUnsupportedType{s.Type})
	}
	binary.BigEndian.PutUint32(s.Bytes[s.BodyStartIdx-4:s.BodyStartIdx], uint32(len(s.Bytes)))
	s.BodyStartIdx -= 13
	return &s.Payload
}

func (s *Send) FromStore(store *Store) {
	// s.Parse(&store.Payload)
	panic("unimplemented")
}

func (s *Send) From(p *Payload) {
	panic("unimplemented")
}

func (s *Send) FromMMessage(m *MMessage) error {
	s.Payload = *m.ToPayload()
	s.Type = m.Type
	switch m.Type {
	case MessageType_NORMAL:
		s.Sequence = m.GetSequence()
	case MessageType_ACK:
		s.AckStatus = m.GetAck().GetStatus()
		s.MessageId = m.GetMessageId()
	default:
		return &ErrUnsupportedType{s.Type}
	}
	return nil
}

var _ Encodable = (*Send)(nil)
var _ Decodable = (*Send)(nil)
