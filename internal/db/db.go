package db

import "time"

type DB interface {
	Exec(namespace []byte, function func(tx Tx) error) error
}

type Tx interface {
	Get(table, key []byte) ([]byte, error)
	Set(table, key, value []byte) error
	Del(table, key []byte) error
	ExpireAt(table, key []byte, expireAt time.Time) error
	Table(table []byte) *Table
}

type Table struct {
	table []byte
	tx    Tx
}

func (t *Table) Key(key []byte) *Key {
	k := new(Key)
	k.table = t
	k.key = key
	return k
}

type Key struct {
	key   []byte
	table *Table
}

func (k Key) Get() ([]byte, error) {
	return k.table.tx.Get(k.table.table, k.key)
}

func (k Key) Set(value []byte) error {
	return k.table.tx.Set(k.table.table, k.key, value)
}

func (k Key) Del() error {
	return k.table.tx.Del(k.table.table, k.key)
}

func (k Key) ExpireAt(expireAt time.Time) error {
	return k.table.tx.ExpireAt(k.table.table, k.key, expireAt)
}
