package common

import "github.com/qingyu31/lightkv/internal/db"

type Session struct {
	DB        db.DB
	Namespace []byte
}
