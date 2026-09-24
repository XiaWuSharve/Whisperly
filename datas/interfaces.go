package datas

type Encodable interface {
	ToByte() *Payload
	// GetHeaderLen() int
}

type Converter[S, D any] interface {
	Convert(source S) (D, error)
}

type Decodable interface {
	Parse(*Payload) error
}
