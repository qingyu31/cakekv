package db

import (
	"github.com/boltdb/bolt"
	"github.com/golang/protobuf/proto"
	"github.com/qingyu31/lightkv/internal/idl"
	"time"
)

type boltDb struct {
	db *bolt.DB
}

func NewBoltDB(path string) (*boltDb, error) {
	b := new(boltDb)
	var err error
	b.db, err = bolt.Open(path, 0660, bolt.DefaultOptions)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (b boltDb) Exec(namespace []byte, function func(tx Tx) error) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(namespace)
		if err != nil {
			return err
		}
		bTx := new(boltTx)
		bTx.bucket = bucket
		return function(bTx)
	})
}

type boltTx struct {
	bucket *bolt.Bucket
}

func (b boltTx) Get(key []byte) ([]byte, error) {
	val, err := b.get(key)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	return val.GetBody(), nil
}

func (b boltTx) Set(key []byte, value []byte) error {
	val, err := b.get(key)
	if err != nil {
		return err
	}
	if val == nil {
		val = new(idl.Value)
	}
	val.Body = value
	return b.set(key, val)
}

func (b boltTx) Del(key []byte) error {
	return b.bucket.Delete(key)
}

func (b boltTx) ExpireAt(key []byte, expireAt time.Time) error {
	val, err := b.get(key)
	if err != nil {
		return err
	}
	if val == nil {
		return nil
	}
	val.ExpireAt = uint64(expireAt.Unix())
	return b.set(key, val)
}

func (b boltTx) set(key []byte, val *idl.Value) error {
	result, err := proto.Marshal(val)
	if err != nil {
		return err
	}
	return b.bucket.Put(key, result)
}

func (b boltTx) get(key []byte) (*idl.Value, error) {
	val := b.bucket.Get(key)
	if val == nil {
		return nil, nil
	}
	v := new(idl.Value)
	err := proto.Unmarshal(val, v)
	if err != nil {
		return nil, err
	}
	if v.ExpireAt <= 0 || v.ExpireAt > uint64(time.Now().Unix()) {
		return v, nil
	}
	return nil, b.bucket.Delete(key)
}
