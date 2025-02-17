import sys
from lib.sync_primitive import SyncPrimitive
import time

class Queue(SyncPrimitive):
    def __init__(self, address, root):
        super().__init__(address)
        self.root = root
        self._ensure_root()

    def _ensure_root(self):
        self.zk.ensure_path(self.root)

    def produce(self, value):
        self.zk.create(f"{self.root}/element-", str(value).encode(), sequence=True)

    def consume(self):
        while True:
            children = self.zk.get_children(self.root)
            if not children:
                time.sleep(1)
                continue
            sorted_children = sorted(children)
            first_child = sorted_children[0]
            data, _ = self.zk.get(f"{self.root}/{first_child}")
            self.zk.delete(f"{self.root}/{first_child}")
            return int(data.decode())

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