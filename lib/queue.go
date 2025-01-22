package lib

import (
	"bytes"
	"encoding/binary"
	"strconv"

	"github.com/samuel/go-zookeeper/zk"
)

type Queue struct {
	*SyncPrimitive
}

func NewQueue(address, root string) (*Queue, error) {
	sp, err := NewSyncPrimitive(address)
	if err != nil {
		return nil, err
	}
	q := &Queue{SyncPrimitive: sp}
	q.root = root
	exists, _, err := q.conn.Exists(root)
	if err == nil && !exists {
		_, _ = q.conn.Create(root, []byte{}, 0, zk.WorldACL(zk.PermAll))
	}
	return q, nil
}

func (q *Queue) Produce(i int) error {
	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.BigEndian, int32(i))
	_, err := q.conn.Create(
		q.root+"/element",
		buf.Bytes(),
		zk.FlagSequence,
		zk.WorldACL(zk.PermAll),
	)
	return err
}

func (q *Queue) Consume() (int, error) {
	for {
		children, _, ch, err := q.conn.ChildrenW(q.root)
		if err != nil {
			return -1, err
		}
		if len(children) == 0 {
			<-ch
			continue
		}
		minNode := children[0]
		minVal, _ := strconv.Atoi(minNode[len("element"):])
		for _, c := range children {
			v, _ := strconv.Atoi(c[len("element"):])
			if v < minVal {
				minVal = v
				minNode = c
			}
		}
		data, stat, err := q.conn.Get(q.root + "/" + minNode)
		if err != nil {
			continue
		}
		err = q.conn.Delete(q.root+"/"+minNode, stat.Version)
		if err != nil {
			continue
		}
		var val int32
		_ = binary.Read(bytes.NewReader(data), binary.BigEndian, &val)
		return int(val), nil
	}
}
