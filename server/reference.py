import zmq
import time
import msgpack
import threading

servers = {}
rank_counter = 0
lock = threading.Lock()

HEARTBEAT_TIMEOUT = 60  

def get_current_time_ms():
    return int(time.time() * 1000)

def cleanup_loop():
    """Remove servidores que nao mandaram heartbeat no tempo limite."""
    while True:
        time.sleep(10)
        now = time.time()
        with lock:
            to_remove = [name for name, info in servers.items()
                         if now - info["last_heartbeat"] > HEARTBEAT_TIMEOUT]
            for name in to_remove:
                print(f"[Reference] ⚠️  Servidor '{name}' removido por timeout de heartbeat.")
                del servers[name]

def main():
    global rank_counter

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:5560")
    print("[Reference] ✅ Iniciado na porta 5560.")

    t = threading.Thread(target=cleanup_loop, daemon=True)
    t.start()

    while True:
        try:
            raw = socket.recv()
            msg = msgpack.unpackb(raw, raw=False)
            action = msg.get("action", "")

            if action == "register":
                name = msg.get("name", "unknown")
                with lock:
                    if name not in servers:
                        rank_counter += 1
                        servers[name] = {
                            "name": name,
                            "rank": rank_counter,
                            "last_heartbeat": time.time()
                        }
                        print(f"[Reference] ✅ Servidor '{name}' registrado com rank {rank_counter}.")
                    else:
                        servers[name]["last_heartbeat"] = time.time()

                    rank = servers[name]["rank"]

                resp = msgpack.packb({"rank": rank}, use_bin_type=True)
                socket.send(resp)

            elif action == "list":
                with lock:
                    lista = [{"name": s["name"], "rank": s["rank"]} for s in servers.values()]

                resp = msgpack.packb({"servers": lista}, use_bin_type=True)
                socket.send(resp)
                print(f"[Reference] 📋 Lista enviada: {[s['name'] for s in lista]}")

            elif action == "heartbeat":
                name = msg.get("name", "unknown")
                with lock:
                    if name in servers:
                        servers[name]["last_heartbeat"] = time.time()
                        print(f"[Reference] 💓 Heartbeat de '{name}'.")
                    else:
                        print(f"[Reference] ⚠️  Heartbeat de '{name}' desconhecido, ignorando.")

                resp = msgpack.packb({
                    "status": "OK",
                    "current_time": get_current_time_ms()
                }, use_bin_type=True)
                socket.send(resp)

            else:
                socket.send(msgpack.packb({"error": "acao desconhecida"}, use_bin_type=True))

        except Exception as e:
            print(f"[Reference] ❌ Erro: {e}")
            try:
                socket.send(msgpack.packb({"error": str(e)}, use_bin_type=True))
            except:
                pass

if __name__ == "__main__":
    main()
