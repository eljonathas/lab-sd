package main

import (
	"fmt"
	"lab-sd/lib"
	"math/rand"
	"os"
	"strconv"
	"time"
)

func queueTest(args []string) {
	q, err := lib.NewQueue(args[0], "/app1")
	if err != nil {
		fmt.Println("Erro ao criar fila:", err)
		return
	}
	max, _ := strconv.Atoi(args[1])

	switch args[2] {
	case "p":
		for i := 0; i < max; i++ {
			if err := q.Produce(10 + i); err != nil {
				fmt.Println("Erro ao produzir:", err)
			}
		}
		fmt.Println("Produção concluída.")
	case "c":
		for i := 0; i < max; i++ {
			val, err := q.Consume()
			if err != nil {
				fmt.Println("Erro ao consumir:", err)
				i--
			} else {
				fmt.Printf("Consumindo item %d/%d: %d\n", i+1, max, val)
			}
		}
		fmt.Println("Consumo concluído.")
	}
}

func barrierTest(args []string) {
	size, _ := strconv.Atoi(args[1])
	b, err := lib.NewBarrier(args[0], "/b1", size)
	if err != nil {
		fmt.Println("Erro ao criar barreira:", err)
		return
	}
	if err := b.Enter(); err != nil {
		fmt.Println("Erro ao entrar na barreira:", err)
		return
	}
	r := rand.Intn(100)
	time.Sleep(time.Duration(r) * time.Millisecond)
	if err := b.Leave(); err != nil {
		fmt.Println("Erro ao sair da barreira:", err)
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Exemplos de uso:")
		fmt.Println("  qTest <host:port> <quantidade> <p|c>")
		fmt.Println("  bTest <host:port> <tamanho>")
		return
	}

	switch os.Args[1] {
	case "qTest":
		if len(os.Args) < 5 {
			fmt.Println("Use: qTest <host:port> <quantidade> <p|c>")
			return
		}
		queueTest(os.Args[2:5])
	case "bTest":
		if len(os.Args) < 4 {
			fmt.Println("Use: bTest <host:port> <tamanho>")
			return
		}
		barrierTest(os.Args[2:4])
	default:
		fmt.Println("Opção inválida.")
	}
}
