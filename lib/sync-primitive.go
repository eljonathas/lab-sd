package lib

import (
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

type SyncPrimitive struct {
	conn *zk.Conn
	root string
}

func NewSyncPrimitive(address string) (*SyncPrimitive, error) {
	c, _, err := zk.Connect([]string{address}, 3*time.Second)
	if err != nil {
		return nil, err
	}
	return &SyncPrimitive{conn: c}, nil
}
