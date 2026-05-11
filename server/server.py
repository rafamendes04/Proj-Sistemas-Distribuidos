import zmq
import time
import sqlite3
import os
import msgpack
import threading
import sys

sys.stdout.reconfigure(line_buffering=True)

lamport_clock = 0
lamport_lock = threading.Lock()
clock_offset = 0
coordinator = None
coordinator_lock = threading.Lock()

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

def handle_login(conn, payload):
    ts = get_current_time_ms()
    try:
        conn.execute("INSERT OR IGNORE INTO users (username, timestamp) VALUES (?, ?)", (payload['username'], ts))
        conn.commit()
        return "SUCCESS", f"User {payload['username']} logged in."
    except Exception as e:
        return "ERROR", str(e)

def handle_create_channel(conn, payload, peers_push=None, server_id=None):
    ts = get_current_time_ms()
    try:
        conn.execute("INSERT INTO channels (channel_name, created_at) VALUES (?, ?)", (payload['channel_name'], ts))
        conn.commit()

        if peers_push:
            replicar(peers_push, server_id, {
                "action": "replicate_channel",
                "channel_name": payload['channel_name'],
                "created_at": ts
            })

        return "SUCCESS", f"Channel '{payload['channel_name']}' created."
    except sqlite3.IntegrityError:
        return "ERROR", f"Channel '{payload['channel_name']}' already exists."
    except Exception as e:
        return "ERROR", str(e)

def handle_list_channels(conn):
    rows = conn.execute("SELECT channel_name FROM channels").fetchall()
    return "SUCCESS", [r[0] for r in rows]

def handle_publish(conn, pub_socket, server_id, payload, clock, peers_push=None):
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

    if peers_push:
        replicar(peers_push, server_id, {
            "action": "replicate_message",
            "channel": channel,
            "username": username,
            "content": content,
            "timestamp": ts,
            "lamport_clock": lc
        })

    return "SUCCESS", "Mensagem publicada."

def replicar(peers_push, server_id, data):
    data["origin"] = server_id
    packed = msgpack.packb(data, use_bin_type=True)
    for sock in peers_push:
        try:
            sock.send(packed, zmq.NOBLOCK)
        except Exception as e:
            print(f"[{server_id}] [Replicacao] Erro ao enviar para peer: {e}")

def replication_receiver(port, conn, server_id):
    s = context.socket(zmq.PULL)
    s.bind(f"tcp://*:{port}")
    print(f"[{server_id}] [Replicacao] Escutando na porta {port}")

    while True:
        try:
            raw = s.recv()
            msg = msgpack.unpackb(raw, raw=False)
            action = msg.get("action", "")

            if action == "replicate_message":
                try:
                    conn.execute(
                        "INSERT INTO messages (channel, username, content, timestamp, lamport_clock) VALUES (?, ?, ?, ?, ?)",
                        (msg["channel"], msg["username"], msg["content"], msg["timestamp"], msg["lamport_clock"])
                    )
                    conn.commit()
                    print(f"[{server_id}] [Replicacao] Mensagem replicada | canal={msg['channel']} | de={msg['username']} | origin={msg.get('origin')}")
                except Exception as e:
                    print(f"[{server_id}] [Replicacao] Erro ao gravar mensagem: {e}")

            elif action == "replicate_channel":
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO channels (channel_name, created_at) VALUES (?, ?)",
                        (msg["channel_name"], msg["created_at"])
                    )
                    conn.commit()
                    print(f"[{server_id}] [Replicacao] Canal replicado | canal={msg['channel_name']} | origin={msg.get('origin')}")
                except Exception as e:
                    print(f"[{server_id}] [Replicacao] Erro ao gravar canal: {e}")

        except Exception as e:
            print(f"[{server_id}] [Replicacao] Erro no receiver: {e}")

def peer_host(name):
    return name.replace("_", "-")

def pedir_hora(host, port, server_id):
    s = context.socket(zmq.REQ)
    s.setsockopt(zmq.RCVTIMEO, 2000)
    s.setsockopt(zmq.LINGER, 0)
    try:
        s.connect(f"tcp://{peer_host(host)}:{port}")
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
        print(f"[{server_id}] Coordenador '{coord}' nao esta na lista de peers, iniciando eleicao.")
        threading.Thread(target=iniciar_eleicao, args=(server_id, peers), daemon=True).start()
        return

    t_antes = int(time.time() * 1000)
    coord_time = pedir_hora(info["host"], info["port"], server_id)
    t_depois = int(time.time() * 1000)

    if coord_time is None:
        print(f"[{server_id}] [Berkeley] Coordenador '{coord}' nao respondeu. Iniciando eleicao.")
        threading.Thread(target=iniciar_eleicao, args=(server_id, peers), daemon=True).start()
        return

    rtt = t_depois - t_antes
    local_now = t_antes + rtt // 2
    new_offset = coord_time - local_now
    clock_offset = new_offset
    print(f"[{server_id}] [Berkeley] Sincronizado com '{coord}'. Offset={clock_offset}ms RTT={rtt}ms")

def iniciar_eleicao(server_id, peers):
    print(f"[{server_id}] [Bully] Iniciando eleicao...")
    maiores = [p for p in peers if p["name"] > server_id]
    algum_respondeu = False

    for peer in maiores:
        s = context.socket(zmq.REQ)
        s.setsockopt(zmq.RCVTIMEO, 2000)
        s.setsockopt(zmq.LINGER, 0)
        try:
            s.connect(f"tcp://{peer_host(peer['host'])}:{peer['port']}")
            s.send(msgpack.packb({"action": "election", "from": server_id}, use_bin_type=True))
            resp = msgpack.unpackb(s.recv(), raw=False)
            if resp.get("status") == "OK":
                algum_respondeu = True
                print(f"[{server_id}] [Bully] Peer '{peer['name']}' respondeu, ele assume.")
                break
        except:
            continue
        finally:
            s.close()

    if not algum_respondeu:
        virar_coordenador(server_id)

def virar_coordenador(server_id):
    global coordinator
    with coordinator_lock:
        coordinator = server_id

    print(f"[{server_id}] [Bully] Sou o novo coordenador!")
    pub = context.socket(zmq.PUB)
    pub.connect(os.getenv("PUBSUB_URL", "tcp://pubsub-proxy:5557"))
    time.sleep(0.5)
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
        except Exception as e:
            print(f"[{server_id}] election_loop erro: {e}")
            continue

def servers_subscriber(server_id):
    global coordinator
    sub = context.socket(zmq.SUB)
    sub.connect(os.getenv("PUBSUB_URL_SUB", "tcp://pubsub-proxy:5558"))
    sub.setsockopt_string(zmq.SUBSCRIBE, "servers")

    while True:
        try:
            sub.recv()
            data = sub.recv()
            msg = msgpack.unpackb(data, raw=False)
            novo = msg.get("coordinator")
            if novo:
                with coordinator_lock:
                    coordinator = novo
                print(f"[{server_id}] [Bully] Novo coordenador via PubSub: {novo}")
        except:
            continue

def novo_ref_socket(ref_url):
    s = context.socket(zmq.REQ)
    s.setsockopt(zmq.RCVTIMEO, 2000)
    s.setsockopt(zmq.LINGER, 0)
    s.connect(ref_url)
    return s

def main():
    server_id = os.getenv("SERVER_ID", "server_1")
    os.makedirs("/app/data", exist_ok=True)
    conn = init_db(f"/app/data/{server_id}.db")

    election_port = os.getenv("ELECTION_PORT", "5570")
    replication_port = os.getenv("REPLICATION_PORT", "5580")

    raw_peers = os.getenv("PEERS", "").split(",")
    peers = []
    for p in raw_peers:
        if ":" in p:
            name, port = p.split(":")
            name = name.strip()
            if name != server_id:
                peers.append({"name": name, "port": port.strip(), "host": name})

    # Sockets PUSH para enviar replicacao aos outros servidores
    raw_rep_peers = os.getenv("REPLICATION_PEERS", "").split(",")
    peers_push = []
    for entry in raw_rep_peers:
        entry = entry.strip()
        if ":" in entry:
            host, port = entry.split(":")
            host = host.strip()
            if host != server_id:
                s = context.socket(zmq.PUSH)
                s.setsockopt(zmq.LINGER, 0)
                s.setsockopt(zmq.SNDHWM, 100)
                s.connect(f"tcp://{peer_host(host)}:{port.strip()}")
                peers_push.append(s)
                print(f"[{server_id}] [Replicacao] Conectado ao peer {host}:{port.strip()}")

    rep = context.socket(zmq.REP)
    rep.connect(os.getenv("BROKER_URL", "tcp://broker:5556"))

    pub = context.socket(zmq.PUB)
    pub.connect(os.getenv("PUBSUB_URL", "tcp://pubsub-proxy:5557"))

    threading.Thread(target=election_loop, args=(election_port, server_id, peers), daemon=True).start()
    threading.Thread(target=servers_subscriber, args=(server_id,), daemon=True).start()
    threading.Thread(target=replication_receiver, args=(replication_port, conn, server_id), daemon=True).start()

    ref_url = os.getenv("REFERENCE_URL", "tcp://reference:5560")
    ref_socket = novo_ref_socket(ref_url)
    try:
        ref_socket.send(msgpack.packb({"action": "register", "name": server_id}, use_bin_type=True))
        ref_socket.recv()
        print(f"[{server_id}] Registrado no servico de referencia.")
    except Exception as e:
        print(f"[{server_id}] Falha ao registrar na referencia: {e}")

    todos = [p["name"] for p in peers] + [server_id]
    if server_id == max(todos):
        time.sleep(1)
        virar_coordenador(server_id)

    print(f"[{server_id}] Online. Porta de eleicao: {election_port} | Porta de replicacao: {replication_port}")

    msg_count = 0
    while True:
        try:
            raw = rep.recv()
            msg = msgpack.unpackb(raw, raw=False)
            msg_type = msg.get("type", "")

            lc = lamport_receive(msg.get("lamport_clock", 0))

            print(f"[{server_id}] RECEBIDO | tipo={msg_type} | LC={lc}")

            resp = {"timestamp": get_current_time_ms()}

            if msg_type == "LOGIN_REQ":
                s, t = handle_login(conn, msg['payload'])
                resp.update({"type": "LOGIN_RESP", "payload": {"status": s, "message": t}})
            elif msg_type == "PUBLISH_REQ":
                s, t = handle_publish(conn, pub, server_id, msg['payload'], lc, peers_push=peers_push)
                resp.update({"type": "PUBLISH_RESP", "payload": {"status": s, "message": t}})
            elif msg_type == "LIST_CHANNELS_REQ":
                s, c = handle_list_channels(conn)
                resp.update({"type": "LIST_CHANNELS_RESP", "payload": {"status": s, "channels": c}})
            elif msg_type == "CREATE_CHANNEL_REQ":
                s, t = handle_create_channel(conn, msg['payload'], peers_push=peers_push, server_id=server_id)
                resp.update({"type": "CREATE_CHANNEL_RESP", "payload": {"status": s, "message": t}})
            else:
                resp.update({"type": "ERROR_RESP", "payload": {"status": "ERROR", "message": "Tipo desconhecido"}})

            resp["lamport_clock"] = lamport_send()
            rep.send(msgpack.packb(resp, use_bin_type=True))

            msg_count += 1
            if msg_count >= 15:
                msg_count = 0
                try:
                    ref_socket.send(msgpack.packb({"action": "heartbeat", "name": server_id}, use_bin_type=True))
                    ref_socket.recv()
                except:
                    ref_socket = novo_ref_socket(ref_url)
                threading.Thread(target=sincronizar, args=(server_id, peers), daemon=True).start()

        except Exception as e:
            print(f"[{server_id}] Erro no loop principal: {e}")

if __name__ == "__main__":
    main()