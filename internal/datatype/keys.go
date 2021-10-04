package datatype

import (
	"fmt"
	"github.com/qingyu31/lightkv/internal/common"
	"github.com/qingyu31/lightkv/internal/db"
	"github.com/qingyu31/resp"
)

type Keys struct {
}

func (Keys) Get(session *common.Session, request *resp.Request) (resp.Message, error) {
	arguments := request.GetArguments()
	if len(arguments) != 1 {
		return nil, fmt.Errorf("ERR wrong number of arguments for '%s' command", request.GetCommand())
	}
	var result []byte
	var err error
	err = session.DB.Exec(session.Namespace, func(tx db.Tx) error {
		result, err = tx.Get(arguments[0])
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resp.NewBulkStringsWithBytes(result), nil
}

func (Keys) Set(session *common.Session, request *resp.Request) (resp.Message, error) {
	arguments := request.GetArguments()
	if len(arguments) < 2 {
		return nil, fmt.Errorf("ERR wrong number of arguments for '%s' command", request.GetCommand())
	}
	if len(arguments) > 2 {
		//todo
		return nil, fmt.Errorf("ERR wrong number of arguments for '%s' command", request.GetCommand())
	}
	err := session.DB.Exec(session.Namespace, func(tx db.Tx) error {
		return tx.Set(arguments[0], arguments[1])
	})
	if err != nil {
		return nil, err
	}
	return resp.NewSimpleStrings("OK"), nil
}

func (Keys) Del(session *common.Session, request *resp.Request) (resp.Message, error) {
	arguments := request.GetArguments()
	if len(arguments) != 1 {
		return nil, fmt.Errorf("ERR wrong number of arguments for '%s' command", request.GetCommand())
	}
	var result []byte
	var err error
	err = session.DB.Exec(session.Namespace, func(tx db.Tx) error {
		result, err = tx.Get(arguments[0])
		if err != nil {
			return err
		}
		return tx.Del(arguments[0])
	})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return resp.NewInteger(0), nil
	}
	return resp.NewInteger(1), nil
}
