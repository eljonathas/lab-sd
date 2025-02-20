import sys
import threading
import time
from lib.reusable_barriers import ReusableBarrier

def test_cycle(barrier, cycle_num):
    print(f"\n=== Ciclo {cycle_num} Iniciado ===")

    def worker():
        print(f"[Thread {threading.get_ident()}] Tentando entrar")
        barrier.enter()
        print(f"[Thread {threading.get_ident()}] Dentro da barreira")
        time.sleep(1)
        barrier.leave()
        print(f"[Thread {threading.get_ident()}] Saiu")

    threads = []
    for _ in range(barrier.size):
        t = threading.Thread(target=worker)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print(f"=== Ciclo {cycle_num} Concluído ===\n")


def main():
    if len(sys.argv) != 4:
        print("Uso: test_reusable_barrier.py <zk_address> <nó_raiz> <tamanho_grupo>")
        sys.exit(1)

    zk_address = sys.argv[1]
    root_path = sys.argv[2]
    group_size = int(sys.argv[3])

    barrier = ReusableBarrier(zk_address, root_path, group_size)

    # Executar 3 ciclos completos de uso
    for cycle in range(1, 4):
        test_cycle(barrier, cycle)

    # Reset final para limpeza
    barrier.reset()
    print("✅ Todos os ciclos foram concluídos com sucesso!")


if __name__ == "__main__":
    main()