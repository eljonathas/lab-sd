import sys
import time
from lib.sync_primitive import SyncPrimitive

class Queue(SyncPrimitive):
    def __init__(self, address, root):
        super().__init__(address)  # Inicializa a conexão com o ZooKeeper
        self.root = root  # Caminho do nó raiz da fila
        self._ensure_root()  # Garante que o nó raiz existe

    def _ensure_root(self):
        self.zk.ensure_path(self.root)  # Cria o nó raiz se ele não existir

    def produce(self, value):
        # Adiciona um elemento à fila criando um nó sequencial
        self.zk.create(f"{self.root}/element-", str(value).encode(), sequence=True)

    def consume(self):
        while True:
            children = self.zk.get_children(self.root)  # Obtém os nós filhos (elementos da fila)
            if not children:
                time.sleep(1)  # Se a fila estiver vazia, aguarda 1 segundo
                continue
            sorted_children = sorted(children)  # Ordena os nós filhos
            first_child = sorted_children[0]  # Pega o nó mais antigo (menor sequência)
            data, _ = self.zk.get(f"{self.root}/{first_child}")  # Obtém o valor do nó
            self.zk.delete(f"{self.root}/{first_child}")  # Remove o nó da fila
            return int(data.decode())  # Retorna o valor consumido