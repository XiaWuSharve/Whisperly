package datas

type Send struct {
	Payload
	Type        MessageType
	CreatedTime int64
	Ack         *Ack
	ReceiverId  string // 换成发送者Id不挺好？
	MessageId   int64
	Sequence    int64
	ConnId      int64 // deprecated?
}

// Parse implements [Decodable].
func (s *Send) Parse(d *Payload) error {
	panic("unimplemented")
}

// ToByte implements [Encodable].
func (s *Send) ToByte() *Payload {
	if s.Type == MessageType_NORMAL {

	}
	return &s.Payload
}

func (s *Send) FromStore(store *Store) {
	s.Parse(&store.Payload)
}

var _ Encodable = (*Send)(nil)
var _ Decodable = (*Send)(nil)
