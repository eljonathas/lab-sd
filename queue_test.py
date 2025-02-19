from lib.queue import Queue

def main():
    if len(sys.argv) != 5:
        print("Uso: queue_test.py <endereço_zk> <nó_raiz> <modo> <elementos>")
        sys.exit(1)

    zk_address = sys.argv[1]
    root = sys.argv[2]
    mode = sys.argv[3]
    elementos = int(sys.argv[4])

    queue = Queue(zk_address, root)

    if mode == "p":
        print("[PRODUTOR] Produzindo elementos...")
        for i in range(elementos):
            queue.produce(i)
            print(f"Elemento {i} adicionado.")
    elif mode == "c":
        print("[CONSUMIDOR] Consumindo elementos...")
        for _ in range(elementos):
            item = queue.consume()
            print(f"Consumido: {item}")
    else:
        print("Modo inválido. Use 'p' (produtor) ou 'c' (consumidor).")

if __name__ == "__main__":
    main()