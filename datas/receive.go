package datas

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/XiaWuSharve/whisperly/config"
)

// TODO 对pull类型是否也要messId or messId = 0？
// receive frame:
// | MessageType type = NORMAL 4b | reserved 4b | created time 8B | mess id 8B | sender id len 1B | receiver id len 1B |
// | payload len 4B | sender id...| receiver id... | payload... |
// | MessageType type = PULL 4b | reserved 4b | created time 8B | ack sequence 8B | pull count 4B | sender id len 1B | sender id... |
type Receive struct {
	Payload
	Type        MessageType
	CreatedTime int64
	ReceiverId  string
	SenderId    string
	MessageId   int64
	AckSequence int64
	PullCount   int32
	cursor      int
	lenReceiver int
	lenSender   int
	buf         [16]byte
	err         error
}

// ToPayload implements [Encodable].
func (re *Receive) ToPayload() *Payload {
	re.cursor = re.BodyStartIdx
	switch re.Type {
	case MessageType_NORMAL:
		re.lenReceiver = len(re.ReceiverId)
		copy(re.Bytes[re.cursor-re.lenReceiver:re.cursor], re.ReceiverId)
		re.cursor -= re.lenReceiver
		re.lenSender = len(re.SenderId)
		copy(re.Bytes[re.cursor-re.lenSender:re.cursor], re.SenderId)
		re.cursor -= re.lenSender
		binary.BigEndian.PutUint32(re.Bytes[re.cursor-len(re.Bytes[re.BodyStartIdx:]):re.cursor], uint32(len(re.Bytes[re.BodyStartIdx:])))
		re.cursor -= 4
		re.Bytes[re.cursor-1] = byte(re.lenReceiver)
		re.Bytes[re.cursor-2] = byte(re.lenSender)
		re.cursor -= 2
		binary.BigEndian.PutUint64(re.Bytes[re.cursor-8:re.cursor], uint64(re.MessageId))
		re.cursor -= 8
		binary.BigEndian.PutUint64(re.Bytes[re.cursor-8:re.cursor], uint64(re.CreatedTime))
		re.cursor -= 8
	case MessageType_PULL:
		re.lenSender = len(re.SenderId)
		copy(re.Bytes[re.cursor-re.lenSender:re.cursor], re.SenderId)
		re.cursor -= re.lenSender
		re.Bytes[re.cursor-1] = byte(re.lenSender)
		re.cursor--
		binary.BigEndian.PutUint32(re.Bytes[re.cursor-4:re.cursor], uint32(re.PullCount))
		re.cursor -= 4
		binary.BigEndian.PutUint64(re.Bytes[re.cursor-8:re.cursor], uint64(re.AckSequence))
		re.cursor -= 8
		binary.BigEndian.PutUint64(re.Bytes[re.cursor-8:re.cursor], uint64(re.CreatedTime))
		re.cursor -= 8
	}
	re.Bytes[re.cursor-1] = byte(re.Type << 4)
	re.cursor--
	re.BodyStartIdx = re.cursor
	return &re.Payload
}

// Parse implements [Decodable].
func (re *Receive) From(data *Payload) {
	panic("unimplemented")
	// r.CreatedTime = int64(binary.BigEndian.Uint64(data[0:8]))
	// r.Payload = data
	// return nil
}

func (re *Receive) FromStore(s *Store) {
	panic("unimplemented")
}

type ErrWrongType struct {
	Type MessageType
}

// Error implements [error].
func (e *ErrWrongType) Error() string {
	return fmt.Sprintf("wrong message type: %s", e.Type.String())
}

var _ error = (*ErrWrongType)(nil)

func (re *Receive) throw() error {
	if errors.Is(re.err, io.EOF) {
		return net.ErrClosed
	}
	return fmt.Errorf("failed to read header: %w", re.err)
}

func (re *Receive) readN(r io.Reader, n int) error {
	_, re.err = io.ReadFull(r, re.buf[:n])
	return re.err
}

func (re *Receive) read(r io.Reader, buf []byte) error {
	_, re.err = io.ReadFull(r, buf)
	return re.err
}

func (re *Receive) discard(r io.Reader, n int) error {
	_, re.err = io.CopyN(io.Discard, r, int64(n))
	return re.err
}

func (re *Receive) FromStream(r io.Reader, headerBufSize int) error {
	if re.readN(r, 1) != nil {
		return re.throw()
	}
	re.Type = MessageType(re.buf[0] >> 4)
	if re.Type != MessageType_NORMAL && re.Type != MessageType_PULL {
		return &ErrWrongType{re.Type}
	}
	if re.readN(r, 8) != nil {
		return re.throw()
	}
	re.CreatedTime = int64(binary.BigEndian.Uint64(re.buf[:8]))
	if ValidateTime(re.CreatedTime) {
		switch re.Type {
		case MessageType_NORMAL:
			if re.readN(r, 8) != nil {
				return re.throw()
			}
			re.MessageId = int64(binary.BigEndian.Uint64(re.buf[:8]))
			if re.readN(r, 6) != nil {
				return re.throw()
			}
			// TODO 过滤空ID
			senderIdBuf := make([]byte, re.buf[0])
			receiverIdBuf := make([]byte, re.buf[1])
			payloadBuf := make([]byte, headerBufSize+int(binary.BigEndian.Uint32(re.buf[2:6])))
			if re.read(r, senderIdBuf) != nil {
				return re.throw()
			}
			re.SenderId = string(senderIdBuf)
			if re.read(r, receiverIdBuf) != nil {
				return re.throw()
			}
			re.ReceiverId = string(receiverIdBuf)
			if re.read(r, payloadBuf[headerBufSize:]) != nil {
				return re.throw()
			}
			re.Payload.Bytes = payloadBuf
		case MessageType_PULL:
			re.MessageId = 0
			if re.readN(r, 8) != nil {
				return re.throw()
			}
			re.AckSequence = int64(binary.BigEndian.Uint64(re.buf[:8]))
			if re.readN(r, 4) != nil {
				return re.throw()
			}
			re.PullCount = int32(binary.BigEndian.Uint32(re.buf[:4]))
			if re.readN(r, 1) != nil {
				return re.throw()
			}
			senderIdBuf := make([]byte, re.buf[0])
			if re.read(r, senderIdBuf) != nil {
				return re.throw()
			}
			re.SenderId = string(senderIdBuf)
			re.Payload.Bytes = make([]byte, headerBufSize)
		}
		re.Payload.BodyStartIdx = headerBufSize
		slog.Debug("received", "created time", time.UnixMilli(re.CreatedTime).String(), "payload length (Bytes)", len(re.Payload.Bytes)-headerBufSize)
	} else {
		switch re.Type {
		case MessageType_NORMAL:
			if re.discard(r, 8) != nil {
				return re.throw()
			}
			if re.readN(r, 6) != nil {
				return re.throw()
			}
			if re.discard(r, int(re.buf[0])+int(re.buf[1])+int(binary.BigEndian.Uint32(re.buf[2:6]))) != nil {
				return re.throw()
			}
		case MessageType_PULL:
			if re.discard(r, 12) != nil {
				return re.throw()
			}
			if re.readN(r, 1) != nil {
				return re.throw()
			}
			if re.discard(r, int(re.buf[0])) != nil {
				return re.throw()
			}
		}
		return ErrTimeLargeOffset
	}
	return nil
}

var _ Encodable = (*Receive)(nil)

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
