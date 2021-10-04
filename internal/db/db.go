package db

type DB interface {
	Exec(namespace []byte, function func(tx Tx) error) error
}