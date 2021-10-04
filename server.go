package main

import (
	"github.com/qingyu31/lightkv/internal/common"
	"github.com/qingyu31/lightkv/internal/db"
	"github.com/qingyu31/resp"
	"log"
	"net"
)

type server struct {
	Addr       net.Addr
	Handler    common.Handler
	handlerMap map[string]common.Handler
	DB         db.DB
}

func (srv *server) ListenAndServe() error {
	listener, err := net.Listen(srv.Addr.Network(), srv.Addr.String())
	if err != nil {
		return err
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		go srv.handleConn(conn)
	}
}

func (srv *server) Handle(command string, handler common.Handler) {
	if srv.handlerMap == nil {
		srv.handlerMap = make(map[string]common.Handler)
	}
	srv.handlerMap[command] = handler
}

func (srv *server) handleConn(conn net.Conn) {
	requestReader := resp.NewRequestReader(conn)
	responseWriter := resp.NewResponseWriter(conn)
	session := new(common.Session)
	session.DB = srv.DB
	session.Namespace = []byte("default")
	for {
		req, err := requestReader.Next()
		if err != nil {
			log.Printf("connection %s %s\n", conn.RemoteAddr().String(), err)
			return
		}
		msg, err := srv.Handler.Handle(session, req)
		if err != nil {
			responseWriter.Write(resp.NewError(err))
			continue
		}
		responseWriter.Write(msg)
	}
}
