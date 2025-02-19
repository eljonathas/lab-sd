import socket
import uuid
from lib.base_barrier import BaseBarrier
from lib.sync_primitive import SyncPrimitive

class DoubleBarrier(BaseBarrier):
    def __init__(self, address, root, size):
        super().__init__(address, root)  # Inicializa a conexão com o ZooKeeper e o nó raiz
        self.size = size  # Número de participantes esperados
        self.node_path = None  # Caminho do nó efêmero deste participante

    def enter(self):
        hostname = socket.gethostname()  # Obtém o nome do host
        unique_id = f"{hostname}-{uuid.uuid4()}"  # Gera um ID único para o participante
        self.node_path = f"{self.path}/{unique_id}"  # Define o caminho do nó efêmero
        self.zk.create(self.node_path, ephemeral=True)  # Cria o nó efêmero no ZooKeeper

        while True:
            with SyncPrimitive._mutex:
                children = self.zk.get_children(self.path, watch=self._watch_callback)  # Obtém os nós filhos
                if len(children) < self.size:
                    SyncPrimitive._mutex.wait()  # Aguarda notificação se o número de participantes for insuficiente
                else:
                    return True  # Todos os participantes entraram na barreira

    def leave(self):
        if self.node_path:
            with SyncPrimitive._mutex:
                if self.zk.exists(self.node_path):
                    self.zk.delete(self.node_path)  # Remove o nó efêmero deste participante
                SyncPrimitive._mutex.notify_all()  # Notifica todas as threads em espera

        while True:
            with SyncPrimitive._mutex:
                children = self.zk.get_children(self.path, watch=self._watch_callback)  # Obtém os nós filhos
                if len(children) > 0:
                    SyncPrimitive._mutex.wait()  # Aguarda notificação se ainda houver participantes
                else:
                    return True  # Todos os participantes saíram da barreira