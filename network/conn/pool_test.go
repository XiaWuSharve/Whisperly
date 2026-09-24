package conn

import (
	"math"
	"math/rand/v2"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/XiaWuSharve/whisperly/utils"
	"github.com/bwmarrin/snowflake"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
func generateRandomString() string {
    b := make([]byte, rand.IntN(255)+1)
    for i := range b {
        // 从字符集中随机选取一个字符
        b[i] = charset[rand.IntN(len(charset))]
    }
    return string(b)
}

func initTest() {
	node, err := snowflake.NewNode(0)
	if err != nil {
		panic(err)
	}
	datas.Ids = node
}

// type data struct {
// 	op func()
// 	connId int64
// 	userId string
// 	handler *SendHandler
// 	findRes bool
// }

// func createFuncSet(pool *Pool) []func(data) {
// 	return []func(d data)bool {
// 		func(d data) {
// 			pool.AddSendHandler(d.handler)
// 			return true
// 		},
// 		func(d data) {
// 			pool.UpdateUserId(d.connId, d.userId)
// 			return true
// 		},
// 		func (d data)  {
// 			_, ok := pool.FindByUserId(d.userId)
// 			return d.
// 		},
// 		func (d data)  {
// 			_, ok := pool.Remove
// 		},
// 	}
// }

func TestConsistence(t *testing.T) {
	initTest()

	pool := &Pool{
		SendHandlersByConnId: utils.NewShardMap[int64, *SendHandler](4, utils.HashFunc),
		SendHandlersByUserId: utils.NewShardMap[string, *SendHandler](4, utils.HashFunc),
		UserIdByConnId: utils.NewShardMap[int64, string](4, utils.HashFunc),
	}
	wg := sync.WaitGroup{}
	wg.Go(func() {
		pool.AddSendHandler(&SendHandler{Conn: &KcpConn{Id: 0}})
	})
	wg.Go(func() {
		pool.AddSendHandler(&SendHandler{Conn: &KcpConn{Id: 1}})
	})
	wg.Go(func() {
		pool.AddSendHandler(&SendHandler{Conn: &KcpConn{Id: 2}})
	})
	wg.Wait()
	if len() != 3 {
		t.Fatal("len(pool.SendHandlersByConnId) != 3")
	}
	wg.Go(func() {
		pool.UpdateUserId(1, "sharve")
	})
	wg.Go(func() {
		pool.AddSendHandler(&SendHandler{Conn: &KcpConn{Id: 3}})
	})
	wg.Go(func() {
		pool.RemoveSendHandler(0)
	})
	wg.Go(func() {
		pool.FindByUserId("glacc")
	})
	wg.Wait()
	wg.Go(func() {
		pool.UpdateUserId(0, "err")
	})
	wg.Go(func() {
		pool.FindByUserId("sharve")
	})
	wg.Go(func() {
		pool.UpdateUserId(2, "glacc")
	})
	wg.Go(func() {
		pool.RemoveSendHandler(5)
	})
	wg.Wait()
	wg.Go(func() {
		pool.UpdateUserId(1, "commie")
	})
	wg.Go(func() {
		pool.RemoveSendHandler(2)
	})
	wg.Wait()
}

func TestPool(t *testing.T) {
	initTest()
	qps := 10000000
	interval := time.Duration(float64(time.Second) / float64(qps))	
		synctest.Test(t, func(t *testing.T) {
		for i := 0; i < qps*3; i++ {
			op := rand.IntN(4)
			go func ()  {
				
			}
			time.Sleep(interval)
		}
		synctest.Wait()
	})
}
