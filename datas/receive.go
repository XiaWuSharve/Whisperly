package datas

import (
	"errors"
	"io"
	"time"

	"github.com/XiaWuSharve/whisperly/config"
)

type Receive struct {
	Payload
	Type        MessageType
	CreatedTime int64
	ReceiverId  string
	SenderId    string
	MessageId   int64
	AckSequence int64
	PullCount   int32
}

// Parse implements [Decodable].
func (r *Receive) Parse(data *Payload) error {
	panic("unimplemented")
	// r.CreatedTime = int64(binary.BigEndian.Uint64(data[0:8]))
	// r.Payload = data
	// return nil
}

func (r *Receive) FromStore(s *Store) {

}

var _ Encodable = (*Receive)(nil)

// GetHeaderLen implements [Encodable].
func (r *Receive) GetHeaderLen() int {
	return 8
}

// ToByte implements [Encodable].
func (r *Receive) ToByte() *Payload {
	panic("unimplemented")
	// binary.BigEndian.PutUint64(r.Payload[0:8], uint64(r.CreatedTime))
	// return r.Payload
}

type ReceiveDecoder struct {
	frame Receive
}

var _ Decodable = (*Receive)(nil)

type ReceiveStreamDecoder struct {
	rBuf       [12]byte
	wBuf       [12]byte
	frame      Receive
	PayloadLen int
	Err        error
}

var ErrTimeLargeOffset = errors.New("client time too fast or slow")

func ValidateTime(createdTime int64) bool {
	diff := createdTime - time.Now().UnixMilli()
	if diff < 0 {
		diff = -diff
	}
	if diff > config.Server.TimeTolerance*1000 { // 5分钟对应的毫秒数
		return false
	}
	return true
}

func (fsd *ReceiveStreamDecoder) Parse(r io.Reader) (*Receive, error) {
	panic("unimplemented")
	// receive frame |CreatedTime 8B|Len 4B -> 1Unit = 1B|
	// send frame |AckType 1B|MessageId 8B|Len 4B -> 1Unit = 1B|
	// if _, fsd.Err = io.ReadFull(r, fsd.Rbuf[:]); fsd.Err != nil {
	// 	if errors.Is(fsd.Err, io.ErrClosedPipe) {
	// 		return nil, net.ErrClosed
	// 	}
	// 	return nil, fmt.Errorf("failed to read header: %w", fsd.Err)
	// }
	// fsd.frame.CreatedTime = int64(binary.BigEndian.Uint64(fsd.Rbuf[0:8]))
	// fsd.PayloadLen = int(binary.BigEndian.Uint32(fsd.Rbuf[8:12]))
	// slog.Debug("received", "header", fsd.Rbuf[:], "created time", time.UnixMilli(fsd.frame.CreatedTime).String(), "payload length (Bytes)", fsd.PayloadLen)
	// if !ValidateTime(fsd.frame.CreatedTime) {
	// io.CopyN(io.Discard, r, int64(fsd.PayloadLen))
	// 	return nil, ErrTimeLargeOffset
	// }
	// 预留给生产者字节编码的8字节用于存放created time，避免编码的时候拷贝payload
	// fsd.frame.Payload = make([]byte, 8+fsd.PayloadLen)
	// fsd.frame.Payload = fsd.frame.Payload[8:]
	// if _, fsd.Err = io.ReadFull(r, fsd.frame.Payload); fsd.Err != nil {
	// 	if errors.Is(fsd.Err, io.ErrClosedPipe) {
	// 		return nil, net.ErrClosed
	// 	}
	// 	return nil, fmt.Errorf("failed to read payload: %w", fsd.Err)
	// }
	// return &fsd.frame, nil
}

type Receive2MMessage struct {
	MMessage MMessage
}

// Convert implements [Converter].
func (r *Receive2MMessage) Convert(source *Receive) (*MMessage, error) {
	panic("unimplemented")
	// if err := proto.Unmarshal(source.Payload, &r.MMessage.Message); err != nil {
	// 	return nil, err
	// }
	// /*
	// 		type Receive struct {
	// 	    CreatedTime int64
	// 	    Type        MessageType
	// 	    ReceiverId  string
	// 	    SenderId    string
	// 	    MessageId   int64
	// 	    AckSequence int64
	// 	    PullCount   int32
	// 	    FullBuf     []byte
	// 	    Payload     []byte
	// 	}
	// */
	// r.MMessage.Type = source.Type
	// r.MMessage.CreatedTime = source.CreatedTime
	// r.MMessage.ReceiverId = source.ReceiverId
	// r.MMessage.SenderId = source.SenderId
	// r.MMessage.MessageId = source.MessageId
	// switch source.Type {
	// case MessageType_PULL:
	// 	r.MMessage.Data = &Message_Pull{
	// 		Pull: &Pull{
	// 			AckSequence: source.AckSequence,
	// 			PullCount:   source.PullCount,
	// 		},
	// 	}
	// }
	// return &r.MMessage, nil
}

var _ Converter[*Receive, *MMessage] = (*Receive2MMessage)(nil)
