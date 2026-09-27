package datas

import (
	"encoding/binary"

	"github.com/XiaWuSharve/whisperly/utils"
)

type Store struct {
	Payload
	Type        MessageType
	AckSequence int64
	PullCount   int32
	SendType    bool
	ReceiverId  string
	len         int
}

// ToPayload implements [Encodable].
func (s *Store) ToPayload() *Payload {
	s.len = len(s.ReceiverId)
	copy(s.Bytes[s.BodyStartIdx-s.len:s.BodyStartIdx], s.ReceiverId)
	s.Bytes[s.BodyStartIdx-s.len-1] = byte(s.len)
	switch s.Type {
	case MessageType_PULL:
		s.Bytes[s.BodyStartIdx-s.len-(1+4+8+1)] = byte(s.Type << 4)
		binary.BigEndian.PutUint64(s.Bytes[s.BodyStartIdx-s.len-(1+4+8):s.BodyStartIdx-s.len-(1+4)], uint64(s.AckSequence))
		binary.BigEndian.PutUint32(s.Bytes[s.BodyStartIdx-s.len-(1+4):s.BodyStartIdx-s.len-1], uint32(s.PullCount))
		return &Payload{
			Bytes:        s.Bytes,
			BodyStartIdx: s.BodyStartIdx - s.len - (1 + 4 + 8 + 1),
		}
	case MessageType_NORMAL:
		s.Bytes[s.BodyStartIdx-s.len-2] = byte(s.Type<<4) | utils.ToByte(s.SendType)
		return &Payload{
			Bytes:        s.Bytes,
			BodyStartIdx: s.BodyStartIdx - s.len - 2,
		}
	}
	return nil
}

// Parse implements [Decodable].
func (s *Store) From(d *Payload) {
	s.Payload = *d
	s.Type = MessageType(s.Bytes[s.BodyStartIdx] >> 4)
	switch s.Type {
	case MessageType_PULL:
		s.AckSequence = int64(binary.BigEndian.Uint64(s.Bytes[s.BodyStartIdx+1 : s.BodyStartIdx+9]))
		s.PullCount = int32(binary.BigEndian.Uint32(s.Bytes[s.BodyStartIdx+9 : s.BodyStartIdx+9+4]))
		s.ReceiverId = string(s.Bytes[s.BodyStartIdx+9+4+1 : s.BodyStartIdx+9+4+1+int(s.Bytes[s.BodyStartIdx+9+4])])
		s.BodyStartIdx += 9 + 4 + 1 + int(s.Bytes[s.BodyStartIdx+9+4])
	case MessageType_NORMAL:
		s.SendType = utils.ToBool(s.Bytes[s.BodyStartIdx] & 0xf)
		s.ReceiverId = string(s.Bytes[s.BodyStartIdx+2 : s.BodyStartIdx+2+int(s.Bytes[s.BodyStartIdx+1])])
		s.BodyStartIdx += 2 + int(s.Bytes[s.BodyStartIdx+1])
	}
}

func (s *Store) FromSend(d *Send) {
	s.Type = MessageType_NORMAL
	s.SendType = true
	s.ReceiverId = d.ReceiverId
	s.Payload = *d.ToPayload()
}

func (s *Store) FromReceive(d *Receive) {
	if d.Type == MessageType_PULL {
		s.Type = MessageType_PULL
		s.AckSequence = d.AckSequence
		s.PullCount = d.PullCount
		s.ReceiverId = d.SenderId
	} else {
		s.Type = MessageType_NORMAL
		s.SendType = false
		s.ReceiverId = d.ReceiverId
	}
	s.Payload = *d.ToPayload()
}

var _ Encodable = (*Store)(nil)
var _ Decodable = (*Store)(nil)
