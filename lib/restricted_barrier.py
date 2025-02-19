from lib.base_barrier import BaseBarrier

class RestrictedBarrier(BaseBarrier):
    def __init__(self, address, path, max_clients):
        super().__init__(address, path)
        self.max_clients = max_clients
        self.client_path = f"{self.path}/clients"
        self._ensure_path()
        
    def enter(self):
        if not self.zk.exists(self.client_path):
            self.zk.create(self.client_path, makepath=True)
        children = self.zk.get_children(self.client_path)
        if len(children) < self.max_clients:
            node_path = f"{self.client_path}/client-"
            self.zk.create(node_path, ephemeral=True, sequence=True)
        else:
            raise Exception("Número máximo de clientes na barreira atingido.")

    
    def leave(self):
        children = self.zk.get_children(self.client_path)
        for child in children:
            full_path = f"{self.client_path}/{child}"
            if self.zk.exists(full_path):
                self.zk.delete(full_path)
                break
