package common

import "github.com/qingyu31/resp"


type Handler interface {
	Handle(session *Session, request *resp.Request) (resp.Message, error)
}

type HandleFunc func(session *Session, request *resp.Request) (resp.Message, error)

func (h HandleFunc) Handle(session *Session, request *resp.Request) (resp.Message, error)  {
	return h(session, request)
}