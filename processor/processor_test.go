package processor

import (
	"testing"
	"time"

	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/XiaWuSharve/whisperly/mq"
	"github.com/bwmarrin/snowflake"
)

type handler struct {
	T *testing.T
}

var f = false
var messId int64

func (h *handler) Handle(s *datas.Send) error {
	if f {
		if s.Type != datas.MessageType_ACK {
			h.T.Fatal(s.Type)
		}
		if s.ReceiverId != "sharve" {
			h.T.Fatal("s.ReceiverId != sharve: ", s.ReceiverId)
		}
		if s.AckStatus != datas.AckStatus_SENT {
			h.T.Fatal(s.AckStatus.String())
		}
		if s.MessageId != messId {
			h.T.Fatal(s.MessageId)
		}
	} else {
		if s.Type != datas.MessageType_NORMAL {
			h.T.Fatal(s.Type)
		}
		if s.ReceiverId != "processor" {
			h.T.Fatal(s.ReceiverId)
		}
		if len(s.Payload.Bytes) == 0 {
			h.T.Fatal("len(s.Payload) == 0")
		}
		f = true
	}
	return nil
}
func TestProcessor(t *testing.T) {
	node, err := snowflake.NewNode(0)
	if err != nil {
		panic(err)
	}
	datas.Ids = node

	inputC := &mq.ConsumerMock[*datas.Receive]{}
	defer inputC.Close()
	inputP := &mq.ProducerMock[*datas.Receive]{Consumer: inputC}
	defer inputP.Close()
	outputC := &mq.ConsumerMock[*datas.Send]{}
	handler := &handler{T: t}
	outputC.Start(handler)
	defer outputC.Close()
	outputP := &mq.ProducerMock[*datas.Send]{Consumer: outputC}
	pc := &Processor{
		SendProduer:     outputP,
		ReceiveConsumer: inputC,
	}
	inputC.Start(pc)
	defer inputC.Close()
	messId = datas.GenId()
	inputP.Enqueue(&datas.Receive{
		CreatedTime: time.Now().UnixMilli(),
		Type:        datas.MessageType_NORMAL,
		ReceiverId:  "processor",
		SenderId:    "sharve",
		MessageId:   messId,
		Payload: *datas.NewMMessage(&datas.Message{
			MessageId: messId,
			Type2: &datas.Message_Normal{
				Normal: datas.NormalType_CHAT,
			},
			Data: &datas.Message_Chat{
				Chat: &datas.Chat{
					DisplayName: "夏午",
					MessageChain: []*datas.MessageUnit{
						{
							Type:    datas.MessageUnitType_TEXT,
							Message: "hello processor",
						},
					},
				},
			},
		}, 128).ToPayload(),
	})
}
