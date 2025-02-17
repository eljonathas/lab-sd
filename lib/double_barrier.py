import socket
import uuid
from lib.base_barrier import BaseBarrier
from lib.sync_primitive import SyncPrimitive

# barreira dupla para sincronizar a entrada e a saída de participantes.
class DoubleBarrier(BaseBarrier):
    def __init__(self, address, root, size):
        # 'root' é o caminho do nó que conterá os nós dos participantes;
        # 'size' é o número total esperado de participantes.
        super().__init__(address, root)
        self.size = size
        self.node_path = None  # Caminho do nó efêmero deste participante

    def enter(self):
        # registra este participante criando um nó efêmero com identificador único
        hostname = socket.gethostname()
        unique_id = f"{hostname}-{uuid.uuid4()}"
        self.node_path = f"{self.path}/{unique_id}"
        self.zk.create(self.node_path, ephemeral=True)

        # espera até que o número de participantes seja igual ou superior a self.size
        while True:
            with SyncPrimitive._mutex:
                children = self.zk.get_children(self.path, watch=self._watch_callback)
                if len(children) < self.size:
                    # ainda não atingiu o número esperado; aguarda notificação
                    SyncPrimitive._mutex.wait()
                else:
                    # número esperado de participantes atingido; libera a execução
                    return True

    def leave(self):
        # remove o nó efêmero deste participante para sinalizar sua saída
        if self.node_path:
            with SyncPrimitive._mutex:
                if self.zk.exists(self.node_path):
                    self.zk.delete(self.node_path)
                SyncPrimitive._mutex.notify_all()

        # aguarda até que nenhum participante permaneça (todos saíram)
        while True:
            with SyncPrimitive._mutex:
                children = self.zk.get_children(self.path, watch=self._watch_callback)
                if len(children) > 0:
                    # ainda existem nós de participantes; aguarda notificação
                    SyncPrimitive._mutex.wait()
                else:
                    # todos os participantes saíram; finaliza o método
                    return True
