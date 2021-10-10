package datatype

import (
	"github.com/golang/protobuf/proto"
	"github.com/qingyu31/lightkv/internal/db"
	"github.com/qingyu31/lightkv/internal/idl"
)

const dataTypeString = 1
const dataTypeHash = 2
const dataTypeList = 3
const dataTypeSet = 4
const dataTypeSortedSet = 5
const dataTypeBitmap = 6

const dataTable = "data"

func getDataTable(tx db.Tx) *db.Table {
	return tx.Table([]byte(dataTable))
}

func getData(tx db.Tx, key []byte) (*idl.Data, error) {
	b, err := getDataTable(tx).Key(key).Get()
	if err != nil {
		return nil, err
	}
	if len(b) == 0 {
		return nil, nil
	}
	data := new(idl.Data)
	err = proto.Unmarshal(b, data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func setData(tx db.Tx, key []byte, data *idl.Data) error {
	b, err := proto.Marshal(data)
	if err != nil {
		return err
	}
	return getDataTable(tx).Key(key).Set(b)
}
