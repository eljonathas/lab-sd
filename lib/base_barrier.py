from lib.sync_primitive import SyncPrimitive

class BaseBarrier(SyncPrimitive):
    def __init__(self, address, path):
        super().__init__(address)  # Inicializa a conexão com o ZooKeeper
        self.path = path  # Caminho do nó que representa a barreira
        self._ensure_path()  # Garante que o nó da barreira existe

    def _ensure_path(self):
        if not self.zk.exists(self.path):
            self.zk.ensure_path(self.path)  # Cria o nó se ele não existir

    def _watch_callback(self, event):
        # Callback chamado quando há mudanças no nó observado
        with SyncPrimitive._mutex:
            SyncPrimitive._mutex.notify_all()  # Notifica todas as threads em espera