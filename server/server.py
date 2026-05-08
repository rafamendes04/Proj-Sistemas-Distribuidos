import zmq
import time
import sqlite3
import os
import msgpack
import threading
import sys

sys.stdout.reconfigure(line_buffering=True)

# Relógios
lamport_clock = 0
lamport_lock = threading.Lock()
clock_offset = 0  # Para o Algoritmo de Berkeley (ajuste do relógio físico)

coordinator = None
coordinator_lock = threading.Lock()

# Contexto global para evitar vazamento de memória/recursos
context = zmq.Context()

def get_current_time_ms():
    return int(time.time() * 1000) + clock_offset

def lamport_send():
    global lamport_clock
    with lamport_lock:
        lamport_clock += 1
        return lamport_clock

def lamport_receive(recv):
    global lamport_clock
    with lamport_lock:
        lamport_clock = max(lamport_clock, recv) + 1
        return lamport_clock

def init_db(path):
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.execute("CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, timestamp INTEGER)")
    conn.execute("CREATE TABLE IF NOT EXISTS channels (channel_name TEXT PRIMARY KEY, created_at INTEGER)")
    conn.execute("""CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        channel TEXT, username TEXT, content TEXT,
        timestamp INTEGER, lamport_clock INTEGER)""")
    conn.commit()
    return conn

# --- Handlers de Mensagens (Mantidos conforme original) ---
def handle_login(conn, payload):
    ts = get_current_time_ms()
    try:
        conn.execute("INSERT OR IGNORE INTO users (username, timestamp) VALUES (?, ?)", (payload['username'], ts))
        conn.commit()
        return "SUCCESS", f"User {payload['username']} logged in."
    except Exception as e:
        return "ERROR", str(e)

def handle_create_channel(conn, payload):
    ts = get_current_time_ms()
    try:
        conn.execute("INSERT INTO channels (channel_name, created_at) VALUES (?, ?)", (payload['channel_name'], ts))
        conn.commit()
        return "SUCCESS", f"Channel '{payload['channel_name']}' created."
    except sqlite3.IntegrityError:
        return "ERROR", f"Channel '{payload['channel_name']}' already exists."
    except Exception as e:
        return "ERROR", str(e)

def handle_list_channels(conn):
    rows = conn.execute("SELECT channel_name FROM channels").fetchall()
    return "SUCCESS", [r[0] for r in rows]

def handle_publish(conn, pub_socket, server_id, payload, clock):
    channel = payload.get('channel', '')
    username = payload.get('username', '')
    content = payload.get('message', '')
    ts = get_current_time_ms()

    if not channel or not content:
        return "ERROR", "Canal ou mensagem vazia."

    if not conn.execute("SELECT 1 FROM channels WHERE channel_name = ?", (channel,)).fetchone():
        return "ERROR", f"Canal '{channel}' nao existe."

    conn.execute(
        "INSERT INTO messages (channel, username, content, timestamp, lamport_clock) VALUES (?, ?, ?, ?, ?)",
        (channel, username, content, ts, clock)
    )
    conn.commit()

    lc = lamport_send()
    pub_socket.send_multipart([channel.encode(), msgpack.packb({
        "channel": channel, "username": username,
        "message": content, "timestamp": ts, "lamport_clock": lc
    }, use_bin_type=True)])
    return "SUCCESS", "Mensagem publicada."

# --- Lógica de Sincronização e Eleição ---

def novo_ref_socket(ref_url):
    s = context.socket(zmq.REQ)
    s.setsockopt(zmq.RCVTIMEO, 2000)
    s.setsockopt(zmq.LINGER, 0)
    s.connect(ref_url)
    return s

def pedir_hora(host, port, server_id):
    # Usa o contexto global
    s = context.socket(zmq.REQ)
    s.setsockopt(zmq.RCVTIMEO, 2000)
    s.setsockopt(zmq.LINGER, 0)
    # IMPORTANTE: Corrige o nome do host para o Docker (troca _ por - se necessário)
    docker_host = host.replace("_", "-")
    try:
        s.connect(f"tcp://{docker_host}:{port}")
        s.send(msgpack.packb({"action": "get_time", "name": server_id}, use_bin_type=True))
        resp = msgpack.unpackb(s.recv(), raw=False)
        return resp.get("current_time")
    except:
        return None
    finally:
        s.close()

def sincronizar(server_id, peers):
    global clock_offset
    with coordinator_lock:
        coord = coordinator

    if not coord or coord == server_id:
        return

    info = next((p for p in peers if p["name"] == coord), None)
    if not info:
        iniciar_eleicao(server_id, peers)
        return

    coord_time = pedir_hora(info["host"], info["port"], server_id)
    if coord_time is None:
        print(f"[{server_id}] Coordenador inativo, iniciando eleicao.")
        iniciar_eleicao(server_id, peers)
    else:
        local_now = int(time.time() * 1000)
        # Berkeley Simplificado: Ajusta a diferença para bater com o coordenador
        new_offset = coord_time - local_now
        clock_offset = new_offset
        print(f"[{server_id}] Relogio Sincronizado. Offset: {clock_offset}ms")

def iniciar_eleicao(server_id, peers):
    print(f"[{server_id}] Iniciando eleicao...")
    maiores = [p for p in peers if p["name"] > server_id]
    algum_superior_respondeu = False

    for peer in maiores:
        s = context.socket(zmq.REQ)
        s.setsockopt(zmq.RCVTIMEO, 2000)
        s.setsockopt(zmq.LINGER, 0)
        docker_host = peer['host'].replace("_", "-")
        try:
            s.connect(f"tcp://{docker_host}:{peer['port']}")
            s.send(msgpack.packb({"action": "election", "from": server_id}, use_bin_type=True))
            resp = msgpack.unpackb(s.recv(), raw=False)
            if resp.get("status") == "OK":
                algum_superior_respondeu = True
                break
        except:
            continue
        finally:
            s.close()

    if not algum_superior_respondeu:
        virar_coordenador(server_id)

def virar_coordenador(server_id):
    global coordinator
    with coordinator_lock:
        coordinator = server_id
    
    print(f"[{server_id}] Eu sou o novo coordenador!")
    pub = context.socket(zmq.PUB)
    pub.connect(os.getenv("PUBSUB_URL", "tcp://pubsub-proxy:5557"))
    time.sleep(0.5) # Aguarda "slow joiners" do ZMQ
    pub.send_multipart([b"servers", msgpack.packb({"coordinator": server_id}, use_bin_type=True)])
    pub.close()

def election_loop(port, server_id, peers):
    s = context.socket(zmq.REP)
    s.bind(f"tcp://*:{port}")
    
    while True:
        try:
            raw = s.recv()
            msg = msgpack.unpackb(raw, raw=False)
            action = msg.get("action", "")

            if action == "get_time":
                s.send(msgpack.packb({"current_time": get_current_time_ms()}, use_bin_type=True))
            
            elif action == "election":
                s.send(msgpack.packb({"status": "OK"}, use_bin_type=True))
                threading.Thread(target=iniciar_eleicao, args=(server_id, peers), daemon=True).start()
            
            else:
                s.send(msgpack.packb({"error": "unknown"}, use_bin_type=True))
        except:
            continue

def servers_subscriber(server_id):
    global coordinator
    sub = context.socket(zmq.SUB)
    sub.connect(os.getenv("PUBSUB_URL_SUB", "tcp://pubsub-proxy:5558"))
    sub.setsockopt_string(zmq.SUBSCRIBE, "servers")

    while True:
        try:
            topic = sub.recv()
            data = sub.recv()
            msg = msgpack.unpackb(data, raw=False)
            novo = msg.get("coordinator")
            if novo:
                with coordinator_lock:
                    coordinator = novo
                print(f"[{server_id}] Novo coordenador anunciado: {novo}")
        except:
            continue

def main():
    server_id = os.getenv("SERVER_ID", "server_1")
    os.makedirs("/app/data", exist_ok=True)
    conn = init_db(f"/app/data/{server_id}.db")

    election_port = os.getenv("ELECTION_PORT", "5570")
    
    # Processa peers e garante nomes limpos
    raw_peers = os.getenv("PEERS", "").split(",")
    peers = []
    for p in raw_peers:
        if ":" in p:
            name, port = p.split(":")
            if name != server_id:
                peers.append({"name": name, "port": port, "host": name})

    # Sockets principais
    rep = context.socket(zmq.REP)
    rep.connect(os.getenv("BROKER_URL", "tcp://broker:5556"))
    pub = context.socket(zmq.PUB)
    pub.connect(os.getenv("PUBSUB_URL", "tcp://pubsub-proxy:5557"))

    # Threads de suporte
    threading.Thread(target=election_loop, args=(election_port, server_id, peers), daemon=True).start()
    threading.Thread(target=servers_subscriber, args=(server_id,), daemon=True).start()

    # Registro inicial
    ref_url = os.getenv("REFERENCE_URL", "tcp://reference:5560")
    ref_socket = novo_ref_socket(ref_url)
    try:
        ref_socket.send(msgpack.packb({"action": "register", "name": server_id}, use_bin_type=True))
        ref_socket.recv()
    except:
        pass

    # No início, o de maior ID assume
    todos = [p["name"] for p in peers] + [server_id]
    if server_id == max(todos):
        time.sleep(1)
        virar_coordenador(server_id)

    print(f"[{server_id}] Online.")
    
    msg_count = 0
    while True:
        try:
            raw = rep.recv()
            msg = msgpack.unpackb(raw, raw=False)
            msg_type = msg.get("type", "")
            
            # Sincronia Lamport (Lógica)
            lc = lamport_receive(msg.get("lamport_clock", 0))
            
            resp = {"timestamp": get_current_time_ms()}
            
            if msg_type == "LOGIN_REQ":
                s, t = handle_login(conn, msg['payload'])
                resp.update({"type": "LOGIN_RESP", "payload": {"status": s, "message": t}})
            elif msg_type == "PUBLISH_REQ":
                s, t = handle_publish(conn, pub, server_id, msg['payload'], lc)
                resp.update({"type": "PUBLISH_RESP", "payload": {"status": s, "message": t}})
            elif msg_type == "LIST_CHANNELS_REQ":
                s, c = handle_list_channels(conn)
                resp.update({"type": "LIST_CHANNELS_RESP", "payload": {"status": s, "channels": c}})
            elif msg_type == "CREATE_CHANNEL_REQ":
                s, t = handle_create_channel(conn, msg['payload'])
                resp.update({"type": "CREATE_CHANNEL_RESP", "payload": {"status": s, "message": t}})

            resp["lamport_clock"] = lamport_send()
            rep.send(msgpack.packb(resp, use_bin_type=True))

            # Regra das 15 mensagens
            msg_count += 1
            if msg_count >= 15:
                msg_count = 0
                # Heartbeat
                try:
                    ref_socket.send(msgpack.packb({"action": "heartbeat", "name": server_id}, use_bin_type=True))
                    ref_socket.recv()
                except:
                    ref_socket = novo_ref_socket(ref_url)
                # Sincronia de Relógio
                sincronizar(server_id, peers)

        except Exception as e:
            print(f"Erro no loop principal: {e}")

if __name__ == "__main__":
    main()