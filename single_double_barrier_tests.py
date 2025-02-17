import threading
import time

from lib.single_barrier import SingleBarrier
from lib.double_barrier import DoubleBarrier

def test_single_barrier():
    address = "127.0.0.1:2181"
    barrier_path = "/single_barrier_test"

    barrier = SingleBarrier(address, barrier_path)

    def worker(idx):
        print(f"[Thread-{idx}] Aguardando liberação da barreira...")
        barrier.enter()
        print(f"[Thread-{idx}] Passou da barreira!")

    # Cria algumas threads que aguardam a barreira
    threads = []
    for i in range(3):
        t = threading.Thread(target=worker, args=(i,))
        t.start()
        threads.append(t)

    # Aguarda um tempo, depois remove a barreira, liberando todos
    time.sleep(2)
    print("[Main] Removendo a barreira...")
    barrier.leave()

    for t in threads:
        t.join()

    print("[Main] Todas as threads passaram da barreira (teste SingleBarrier concluído).")


def test_double_barrier():
    address = "127.0.0.1:2181"
    root = "/double_barrier_test"
    size = 3

    def worker(idx):
        barrier = DoubleBarrier(address, root, size)
        print(f"[Thread-{idx}] -> enter()")
        barrier.enter()
        print(f"[Thread-{idx}] Todos chegaram. Fazendo algo importante...")

        time.sleep(1 + idx * 0.2)

        print(f"[Thread-{idx}] -> leave()")
        barrier.leave()
        print(f"[Thread-{idx}] Saiu da barreira!")

    threads = []
    for i in range(size):
        t = threading.Thread(target=worker, args=(i,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    print("[Main] Todas as threads entraram e saíram da barreira (teste DoubleBarrier concluído).")


if __name__ == "__main__":
    test_single_barrier()
    test_double_barrier()
