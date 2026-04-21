import zmq
import os
import sys

def main():
    """
    Broker ZeroMQ:
    Atua como um Load Balancer e roteador entre Clientes (REQ) e Servidores/Workers (REP).
    Utiliza o padrão ROUTER para receber conexões dos clientes, e DEALER para distribuir para os servidores.
    """
    context = zmq.Context()
    
    # Frontend escuta as requisições dos clientes (REQ)
    frontend = context.socket(zmq.ROUTER)
    frontend.bind("tcp://*:5555")
    
    # Backend repassa as requisições para os servidores disponíveis (REP)
    backend = context.socket(zmq.DEALER)
    backend.bind("tcp://*:5556")
    
    print("[Broker] ✅ Iniciado. Roteando tráfego entre Clientes (5555) e Servidores (5556)...")
    try:
        zmq.proxy(frontend, backend)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"[Broker] ❌ Erro: {e}")
    finally:
        frontend.close()
        backend.close()
        context.term()

if __name__ == "__main__":
    main()
