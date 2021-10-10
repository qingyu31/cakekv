package datatype

import (
	"fmt"
	"github.com/qingyu31/lightkv/internal/common"
	"github.com/qingyu31/lightkv/internal/db"
	"github.com/qingyu31/resp"
)

type Keys struct {
}

func (Keys) Del(session *common.Session, request *resp.Request) (resp.Message, error) {
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
		if data == nil {
			return nil
		}
		return getDataTable(tx).Key(key).Del()
	})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return resp.NewInteger(0), nil
	}
	return resp.NewInteger(1), nil
}
