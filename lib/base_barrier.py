from lib.sync_primitive import SyncPrimitive

class BaseBarrier(SyncPrimitive):
    def __init__(self, address, path):
        super().__init__(address)
        self.path = path  # Caminho do nó que representa a barreira
        self._ensure_path()  # Cria o nó caso não exista

    def _ensure_path(self):
        if not self.zk.exists(self.path):
            self.zk.ensure_path(self.path)

    def _watch_callback(self, event):
        # Método callback invocado quando há alguma alteração (evento) no nó observado
        with SyncPrimitive._mutex:
            SyncPrimitive._mutex.notify_all()