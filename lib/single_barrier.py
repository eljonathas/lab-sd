from lib.base_barrier import BaseBarrier
from lib.sync_primitive import SyncPrimitive

class SingleBarrier(BaseBarrier):
    def enter(self):
        # Loop que espera a remoção do nó de barreira
        while True:
            with SyncPrimitive._mutex:
                # Se o nó não existir, a barreira já foi liberada
                if not self.zk.exists(self.path, watch=self._watch_callback):
                    return  # Libera a execução
                # Caso o nó ainda exista, aguarda notificação de alteração
                SyncPrimitive._mutex.wait()

    def leave(self):
        with SyncPrimitive._mutex:
            # Se o nó de barreira existir, remove-o (libera a barreira)
            if self.zk.exists(self.path):
                self.zk.delete(self.path, recursive=True)
            # Notifica todas as threads/processos que possam estar esperando
            SyncPrimitive._mutex.notify_all()
