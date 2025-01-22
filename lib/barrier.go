package lib

import (
	"fmt"
	"os"

	"github.com/samuel/go-zookeeper/zk"
)

type Barrier struct {
	*SyncPrimitive
	size int
	name string
}

func NewBarrier(address, root string, size int) (*Barrier, error) {
	sp, err := NewSyncPrimitive(address)
	if err != nil {
		return nil, err
	}
	hostname, _ := os.Hostname()
	b := &Barrier{
		SyncPrimitive: sp,
		size:          size,
		name:          hostname,
	}
	b.root = root
	exists, _, err := b.conn.Exists(root)
	if err == nil && !exists {
		_, _ = b.conn.Create(root, []byte{}, 0, zk.WorldACL(zk.PermAll))
	}
	return b, nil
}

func (b *Barrier) Enter() error {
	_, err := b.conn.Create(b.root+"/"+b.name, []byte{}, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}
	for {
		children, _, ch, err := b.conn.ChildrenW(b.root)
		if err != nil {
			return err
		}
		if len(children) >= b.size {
			break
		}
		<-ch
	}
	fmt.Println("Entrou na barreira.")
	return nil
}

func (b *Barrier) Leave() error {
	err := b.conn.Delete(b.root+"/"+b.name, -1)
	if err != nil {
		return err
	}
	for {
		children, _, ch, err := b.conn.ChildrenW(b.root)
		if err != nil {
			return err
		}
		if len(children) == 0 {
			break
		}
		<-ch
	}
	fmt.Println("Saiu da barreira.")
	return nil
}
