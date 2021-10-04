package db

import "time"

type Tx interface {
	Get(key []byte)([]byte, error)
	Set(key, value []byte) error
	Del(key []byte) error
	ExpireAt(key []byte, expireAt time.Time) error
}
