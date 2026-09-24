package datas

import "github.com/XiaWuSharve/whisperly/utils"

type Store struct {
	// Type        MessageType
	// AckSequence int64
	// PullCount   int32
	Payload
	SendType   bool
	ReceiverId string
	Sequence   int64
	len        int
}

// Parse implements [Decodable].
func (s *Store) Parse(d *Payload) error {
	s.Payload = *d
	s.SendType = utils.ToBool(s.Bytes[s.BodyStartIdx])
	s.len = int(s.Bytes[s.BodyStartIdx+1])
	s.ReceiverId = string(s.Bytes[s.BodyStartIdx+2 : s.BodyStartIdx+2+s.len])
	s.BodyStartIdx += 2 + s.len
	return nil
}

// ToByte implements [Encodable].
func (s *Store) ToByte() *Payload {
	s.len = len(s.ReceiverId)
	copy(s.Bytes[s.BodyStartIdx-s.len:s.BodyStartIdx], s.ReceiverId)
	s.Bytes[s.BodyStartIdx-s.len-1] = byte(s.len)
	s.Bytes[s.BodyStartIdx-s.len-2] = utils.ToByte(s.SendType)
	return &Payload{
		Bytes:        s.Bytes,
		BodyStartIdx: s.BodyStartIdx - s.len - 2,
	}
}

func (s *Store) FromSend(d *Send) {
	s.Payload = *(d.ToByte())
	s.ReceiverId = d.ReceiverId
	s.SendType = true
}

func (s *Store) FromReceive(d *Receive) {
	panic("unimplemented")
}

var _ Encodable = (*Store)(nil)
var _ Decodable = (*Store)(nil)
