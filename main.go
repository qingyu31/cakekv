package main

import (
	"flag"
	"fmt"
	"github.com/qingyu31/lightkv/internal"
	"github.com/qingyu31/lightkv/internal/common"
	"github.com/qingyu31/lightkv/internal/datatype"
	"github.com/qingyu31/lightkv/internal/db"
	"net"
	"os"
	"strconv"
)

const dbFileName = "main.db"

var port = flag.Int("p", 6379, "port")
var dataDir = flag.String("d", "/var/lib/cakekv/data/", "data directory")

func main() {
	srv := new(server)
	srv.Addr, _ = net.ResolveTCPAddr("tcp", ":"+strconv.Itoa(*port))
	handler := new(internal.CommandHandler)
	srv.Handler = handler
	err := os.MkdirAll(*dataDir, 0640)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return
	}
	srv.DB, err = db.NewBoltDB("./test.db")
	if err != nil {
		fmt.Fprintf(os.Stderr, "open db %v\n", err)
		return
	}
	keyValue := new(datatype.Keys)
	handler.Register("get", common.HandleFunc(keyValue.Get))
	handler.Register("set", common.HandleFunc(keyValue.Set))
	handler.Register("del", common.HandleFunc(keyValue.Del))
	err = srv.ListenAndServe()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ListenAndServe %v\n", err)
		return
	}
}
