package internal

import (
	"fmt"
	"github.com/qingyu31/lightkv/internal/common"
	"github.com/qingyu31/lightkv/internal/db"
	"github.com/qingyu31/resp"
)

type CommandHandler struct {
	handlerMap map[string]common.Handler
	DB db.DB
}

func (c *CommandHandler) Register(command string, handler common.Handler) {
	if c.handlerMap == nil {
		c.handlerMap = make(map[string]common.Handler)
	}
	c.handlerMap[command] = handler
}

func (c *CommandHandler) Handle(session *common.Session,request *resp.Request) (resp.Message, error) {
	h, ok := c.handlerMap[request.GetCommand()]
	if ok {
		return h.Handle(session, request)
	}
	return nil, fmt.Errorf("ERR unknown command `%s`", request.GetCommand())
}
