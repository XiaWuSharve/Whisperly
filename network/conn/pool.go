package conn

import (
	"sync"

	"github.com/XiaWuSharve/whisperly/utils"
)

// 要解决的是同步问题不是互斥问题mutex (x)
type Pool struct {
	// only shared memory structure are allowed to be coroutine safe (to avoid lock acquiring)
	SendHandlersByConnId *utils.ShardMap[int64, *SendHandler]
	SendHandlersByUserId *utils.ShardMap[string, *SendHandler]
	UserIdByConnId       *utils.ShardMap[int64, string]
	muSendHandler        sync.Mutex
	muUserId             sync.Mutex
}

// coroutine safe
func (p *Pool) AddSendHandler(c *SendHandler) {
	p.SendHandlersByConnId.Set(c.GetId(), c)
}

func (p *Pool) RemoveSendHandler(id int64) {
	con, ok := p.SendHandlersByConnId.Get(id)
	if ok {
		con.Close()
		p.muSendHandler.Lock()
		p.SendHandlersByConnId.Delete(id)
		p.muSendHandler.Unlock()
	}
	p.muUserId.Lock()
	userId, ok := p.UserIdByConnId.Get(id)
	if ok {
		p.UserIdByConnId.Delete(id)
		// 此时UpdateUserId：p.SendHandlersByUserId.Set(userId, sendHandler)
		p.SendHandlersByUserId.Delete(userId)
	}
	p.muUserId.Unlock()
}

func (p *Pool) FindByUserId(id string) (*SendHandler, bool) {
	return p.SendHandlersByUserId.Get(id)
}

func (p *Pool) UpdateUserId(connId int64, userId string) {
	uid, ok := p.UserIdByConnId.Get(connId)
	if ok {
		p.SendHandlersByUserId.Delete(uid)
	}
	p.muSendHandler.Lock()
	sendHandler, ok2 := p.SendHandlersByConnId.Get(connId)
	if !ok2 {
		p.muSendHandler.Unlock()
		p.RemoveSendHandler(connId)
		return
	}
	//此时RemoveSendHandler: p.SendHandlersByConnId.Delete(id)
	p.muUserId.Lock()
	p.SendHandlersByUserId.Set(userId, sendHandler)
	p.muUserId.Unlock()
	p.muSendHandler.Unlock()
	p.UserIdByConnId.Set(connId, userId)
}
