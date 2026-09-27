package datas

type Encodable interface {
	ToPayload() *Payload
	// GetHeaderLen() int
}

type Converter[S, D any] interface {
	Convert(source S) (D, error)
}

type EncodableDecodable interface {
	Encodable
	Decodable
}

type Decodable interface {
	From(*Payload)
}

func ToByte(p *Payload) []byte {
	return p.Bytes[p.BodyStartIdx:]
}

func FromByte(data []byte, headerBufSize int) *Payload {
	bytes := make([]byte, headerBufSize+len(data))
	copy(bytes[headerBufSize:], data)
	return &Payload{
		Bytes:        bytes,
		BodyStartIdx: headerBufSize,
	}
}
