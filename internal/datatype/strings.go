package datatype

import (
	"errors"
	"fmt"
	"github.com/qingyu31/lightkv/internal/common"
	"github.com/qingyu31/lightkv/internal/db"
	"github.com/qingyu31/resp"
)

type Strings struct {
}

func (Strings) Get(session *common.Session, request *resp.Request) (resp.Message, error) {
	arguments := request.GetArguments()
	if len(arguments) != 1 {
		return nil, fmt.Errorf("ERR wrong number of arguments for '%s' command", request.GetCommand())
	}
	key := arguments[0]
	var result []byte
	var err error
	err = session.DB.Exec(session.Namespace, func(tx db.Tx) error {
		data, err2 := getData(tx, key)
		if err2 != nil {
			return err2
		}
		if data.Type != dataTypeString {
			err = errors.New("unmatched key type")
			return nil
		}
		result = data.Body
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resp.NewBulkStringsWithBytes(result), nil
}

func (Strings) Set(session *common.Session, request *resp.Request) (resp.Message, error) {
	arguments := request.GetArguments()
	if len(arguments) < 2 {
		return nil, fmt.Errorf("ERR wrong number of arguments for '%s' command", request.GetCommand())
	}
	if len(arguments) > 2 {
		//todo
		return nil, fmt.Errorf("ERR wrong number of arguments for '%s' command", request.GetCommand())
	}
	key := arguments[0]
	var err error
	err = session.DB.Exec(session.Namespace, func(tx db.Tx) error {
		data, err2 := getData(tx, key)
		if err2 != nil {
			return err2
		}
		if data.Type != dataTypeString {
			err = errors.New("unmatched key type")
			return nil
		}
		data.Body = arguments[1]
		return setData(tx, key, data)
	})
	if err != nil {
		return nil, err
	}
	return resp.NewSimpleStrings("OK"), nil
}
