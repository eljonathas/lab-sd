package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"strconv"
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
	return nil
}

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
	_, err := q.conn.Create(q.root+"/element", buf.Bytes(), zk.FlagSequence, zk.WorldACL(zk.PermAll))
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

func queueTest(args []string) {
	if len(args) < 4 {
		fmt.Println("Exemplo: qTest 127.0.0.1:2181 10 p")
		return
	}
	q, err := NewQueue(args[0], "/app1")
	if err != nil {
		fmt.Println("Erro ao criar Queue:", err)
		return
	}
	max, _ := strconv.Atoi(args[1])
	if args[2] == "p" {
		for i := 0; i < max; i++ {
			err := q.Produce(10 + i)
			if err != nil {
				fmt.Println("Erro ao produzir:", err)
			}
		}
		fmt.Println("Produção concluída.")
	} else {
		for i := 0; i < max; i++ {
			val, err := q.Consume()
			if err != nil {
				fmt.Println("Erro ao consumir:", err)
				i--
			} else {
				fmt.Println("Item consumido:", val)
			}
		}
		fmt.Println("Consumo concluído.")
	}
}

func barrierTest(args []string) {
	if len(args) < 2 {
		fmt.Println("Exemplo: bTest 127.0.0.1:2181 2")
		return
	}
	size, _ := strconv.Atoi(args[1])
	b, err := NewBarrier(args[0], "/b1", size)
	if err != nil {
		fmt.Println("Erro ao criar Barrier:", err)
		return
	}
	if err := b.Enter(); err != nil {
		fmt.Println("Erro ao entrar na barreira:", err)
	}
	fmt.Println("Barreira atingida.")
	r := rand.Intn(100)
	time.Sleep(time.Duration(r) * time.Millisecond)
	if err := b.Leave(); err != nil {
		fmt.Println("Erro ao sair da barreira:", err)
	}
	fmt.Println("Saiu da barreira.")
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Use:")
		fmt.Println("  qTest [host:port] [quantidade] [p|c]")
		fmt.Println("  bTest [host:port] [tamanho]")
		return
	}
	switch os.Args[1] {
	case "qTest":
		if len(os.Args) < 5 {
			fmt.Println("Exemplo: go run main.go qTest 127.0.0.1:2181 10 p")
			return
		}
		queueTest(os.Args[2:5])
	case "bTest":
		if len(os.Args) < 4 {
			fmt.Println("Exemplo: go run main.go bTest 127.0.0.1:2181 2")
			return
		}
		barrierTest(os.Args[2:4])
	default:
		fmt.Println("Opção inválida.")
	}
}
