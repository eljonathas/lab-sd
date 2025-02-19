from kazoo.client import KazooClient
import threading

class SyncPrimitive:
    _zk: KazooClient = None  # Instância compartilhada do cliente ZooKeeper
    _mutex = threading.Condition()  # Mutex para sincronização entre threads

    def __init__(self, address):
        if SyncPrimitive._zk is None:
            SyncPrimitive._zk = KazooClient(address)  # Conecta ao ZooKeeper
            SyncPrimitive._zk.start()  # Inicia a conexão
        self.address = address

    @property
    def zk(self):
        return SyncPrimitive._zk  # Retorna a instância do ZooKeeper