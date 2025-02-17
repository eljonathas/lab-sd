import sys
from lib.double_barrier import DoubleBarrier
import time
import random

def main():
    if len(sys.argv) != 4:
        print("Uso: barrier_test.py <endereço_zk> <nó_raiz> <tamanho_grupo>")
        sys.exit(1)

    zk_address = sys.argv[1]
    root_path = sys.argv[2]
    size = int(sys.argv[3])

    barrier = DoubleBarrier(zk_address, root_path, size)
    
    print("[BARREIRA] Entrando na barreira...")
    barrier.enter()
    print("[BARREIRA] Todos os participantes chegaram. Iniciando processamento...")
    
    tempo_processamento = random.randint(1, 5)
    time.sleep(tempo_processamento)
    
    print("[BARREIRA] Saindo da barreira...")
    barrier.leave()
    print("[BARREIRA] Saiu da barreira.")

if __name__ == "__main__":
    main()