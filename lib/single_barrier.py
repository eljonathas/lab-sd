from lib.base_barrier import BaseBarrier
from lib.sync_primitive import SyncPrimitive

class SingleBarrier(BaseBarrier):
    def enter(self):
        while True:
            with SyncPrimitive._mutex:
                if not self.zk.exists(self.path, watch=self._watch_callback):
                    return  # Sai do loop se o nó da barreira não existir
                SyncPrimitive._mutex.wait()  # Aguarda notificação de mudança no nó

    def leave(self):
        with SyncPrimitive._mutex:
            if self.zk.exists(self.path):
                self.zk.delete(self.path, recursive=True)  # Remove o nó da barreira
            SyncPrimitive._mutex.notify_all()  # Notifica todas as threads em espera