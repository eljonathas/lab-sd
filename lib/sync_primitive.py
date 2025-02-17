from kazoo.client import KazooClient
import threading
import socket
import uuid
import logging

logging.basicConfig()

class SyncPrimitive:
    _zk = None
    _mutex = threading.Condition()

    def __init__(self, address):
        if SyncPrimitive._zk is None:
            SyncPrimitive._zk = KazooClient(address)
            SyncPrimitive._zk.start()
        self.address = address

    @property
    def zk(self):
        return SyncPrimitive._zk