import threading
import time

from lib.restricted_barrier import RestrictedBarrier

def test_restricted_barrier():
    address = "127.0.0.1:2181"
    barrier_path = "/restricted_barrier_test"   
    max_size = 3

    barrier = RestrictedBarrier(address, barrier_path, max_size)

    def worker(idx):
        print(f"[Thread-{idx}] Tentando entrar na barreira...")
        barrier.enter()
        print(f"[Thread-{idx}] Passou da barreira e está executando a tarefa...")
        
        time.sleep(1 + idx * 0.2)
        
        print(f"[Thread-{idx}] Saindo da barreira...")
        barrier.leave()
        print(f"[Thread-{idx}] Saiu da barreira com sucesso!")

    threads = []
    for i in range(max_size):
        t = threading.Thread(target=worker, args=(i,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    print("[Main] Todas as threads entraram e saíram da barreira (teste RestrictedBarrier concluído).")

if __name__ == "__main__":
    test_restricted_barrier()
