class ReusableBarrier(BaseBarrier):
    def __init__(self, address, root, size):
        super().__init__(address, root)
        self.size = size
        self._nodes = {}  # Dicionário para armazenar, por thread, o caminho do nó efêmero

    def enter(self):
        """
        Cada thread cria seu próprio nó efêmero e aguarda até que o número de nós
        no caminho da barreira seja igual ou superior ao tamanho esperado.
        """
        hostname = socket.gethostname()
        unique_id = f"{hostname}-{uuid.uuid4()}"
        node_path = f"{self.path}/{unique_id}"
        # Armazena o nó criado para a thread atual
        self._nodes[threading.get_ident()] = node_path
        self.zk.create(node_path, ephemeral=True)

        while True:
            with SyncPrimitive._mutex:
                children = self.zk.get_children(self.path, watch=self._watch_callback)
                if len(children) >= self.size:
                    return True
                SyncPrimitive._mutex.wait()

    def leave(self):
        """
        A thread remove seu próprio nó efêmero se ele existir e notifica as demais.
        """
        node_path = self._nodes.pop(threading.get_ident(), None)
        if node_path and self.zk.exists(node_path):
            try:
                self.zk.delete(node_path)
            except Exception:
                # Se o nó já foi removido, ignoramos o erro
                pass
        with SyncPrimitive._mutex:
            SyncPrimitive._mutex.notify_all()

    def reset(self):
        """
        Reinicia completamente a barreira, removendo recursivamente o nó raiz e recriando-o.
        """
        self.zk.delete(self.path, recursive=True)
        self._ensure_path()