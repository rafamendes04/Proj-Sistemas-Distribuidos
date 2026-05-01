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

coordinator = None
coordinator_lock = threading.Lock()


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
    ts = int(time.time() * 1000)
    try:
        conn.execute("INSERT OR IGNORE INTO users (username, timestamp) VALUES (?, ?)", (payload['username'], ts))
        conn.commit()
        return "SUCCESS", f"User {payload['username']} logged in."
    except Exception as e:
        return "ERROR", str(e)


def handle_create_channel(conn, payload):
    ts = int(time.time() * 1000)
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
    ts = int(time.time() * 1000)

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
    print(f"[{server_id}] --> PUB '{channel}': [{username}] {content} | LC={lc}")
    return "SUCCESS", "Mensagem publicada."


def novo_ref_socket(context, ref_url):
    s = context.socket(zmq.REQ)
    s.setsockopt(zmq.RCVTIMEO, 5000)
    s.setsockopt(zmq.LINGER, 0)
    s.connect(ref_url)
    return s


def registrar_referencia(context, ref_url, server_id):
    s = novo_ref_socket(context, ref_url)
    try:
        s.send(msgpack.packb({"action": "register", "name": server_id}, use_bin_type=True))
        resp = msgpack.unpackb(s.recv(), raw=False)
        rank = resp.get("rank", -1)
        print(f"[{server_id}] Registrado. Rank={rank}")
        return rank, s
    except Exception as e:
        print(f"[{server_id}] Falha ao registrar: {e}")
        s.close()
        return -1, novo_ref_socket(context, ref_url)


def heartbeat(context, ref_socket, ref_url, server_id):
    try:
        ref_socket.send(msgpack.packb({"action": "heartbeat", "name": server_id}, use_bin_type=True))
        ref_socket.recv()
        print(f"[{server_id}] Heartbeat OK")
        return ref_socket
    except Exception as e:
        print(f"[{server_id}] Heartbeat falhou: {e}")
        ref_socket.close()
        return novo_ref_socket(context, ref_url)


def get_peers(server_id):
    peers = []
    for entry in os.getenv("PEERS", "").split(","):
        entry = entry.strip()
        if not entry:
            continue
        parts = entry.split(":")
        if len(parts) == 2 and parts[0].strip() != server_id:
            peers.append({"name": parts[0].strip(), "port": parts[1].strip(), "host": parts[0].strip()})
    return peers


def pedir_hora(host, port, server_id):
    ctx = zmq.Context()
    s = ctx.socket(zmq.REQ)
    s.setsockopt(zmq.RCVTIMEO, 5000)
    s.setsockopt(zmq.LINGER, 0)
    try:
        s.connect(f"tcp://{host}:{port}")
        s.send(msgpack.packb({"action": "get_time", "name": server_id}, use_bin_type=True))
        resp = msgpack.unpackb(s.recv(), raw=False)
        return resp.get("current_time")
    except:
        print(f"[{server_id}] Coordenador {host}:{port} nao respondeu.")
        return None
    finally:
        s.close()


def sincronizar(server_id, peers):
    global lamport_clock

    with coordinator_lock:
        coord = coordinator

    if coord is None:
        print(f"[{server_id}] Sem coordenador, pulando sincronia.")
        return

    if coord == server_id:
        print(f"[{server_id}] Sou o coordenador, nada a sincronizar.")
        return

    info = next((p for p in peers if p["name"] == coord), None)
    if info is None:
        print(f"[{server_id}] Coordenador '{coord}' sumiu, iniciando eleicao.")
        iniciar_eleicao(server_id, peers)
        return

    coord_time = pedir_hora(info["host"], info["port"], server_id)
    if coord_time is None:
        iniciar_eleicao(server_id, peers)
        return

    local_time = int(time.time() * 1000)
    diff = coord_time - local_time
    print(f"[{server_id}] Berkeley | coord={coord_time} local={local_time} diff={diff}ms")

    with lamport_lock:
        lamport_clock = max(lamport_clock, coord_time // 1000)


def iniciar_eleicao(server_id, peers):
    print(f"[{server_id}] Iniciando eleicao...")

    maiores = [p for p in peers if p["name"] > server_id]
    algum_vivo = False

    for peer in maiores:
        ctx = zmq.Context()
        s = ctx.socket(zmq.REQ)
        s.setsockopt(zmq.RCVTIMEO, 5000)
        s.setsockopt(zmq.LINGER, 0)
        try:
            s.connect(f"tcp://{peer['host']}:{peer['port']}")
            s.send(msgpack.packb({"action": "election", "from": server_id}, use_bin_type=True))
            resp = msgpack.unpackb(s.recv(), raw=False)
            if resp.get("status") == "OK":
                print(f"[{server_id}] '{peer['name']}' ta vivo, ele assume.")
                algum_vivo = True
                break
        except:
            print(f"[{server_id}] '{peer['name']}' nao respondeu.")
        finally:
            s.close()

    if not algum_vivo:
        virar_coordenador(server_id, os.getenv("PUBSUB_URL", "tcp://pubsub-proxy:5557"))


def virar_coordenador(server_id, pubsub_url):
    global coordinator

    with coordinator_lock:
        coordinator = server_id

    print(f"[{server_id}] Sou o novo coordenador!")

    ctx = zmq.Context()
    pub = ctx.socket(zmq.PUB)
    pub.connect(pubsub_url)
    time.sleep(0.2)
    pub.send_multipart([b"servers", msgpack.packb({"coordinator": server_id}, use_bin_type=True)])
    print(f"[{server_id}] --> PUB [servers] coordenador={server_id}")
    pub.close()


def election_loop(server_id, port, peers):
    global coordinator

    ctx = zmq.Context()
    s = ctx.socket(zmq.REP)
    s.bind(f"tcp://*:{port}")
    print(f"[{server_id}] Eleicao escutando na porta {port}")

    while True:
        try:
            msg = msgpack.unpackb(s.recv(), raw=False)
            action = msg.get("action", "")

            if action == "get_time":
                ts = int(time.time() * 1000)
                s.send(msgpack.packb({"current_time": ts}, use_bin_type=True))
                print(f"[{server_id}] get_time -> {ts}")

            elif action == "election":
                sender = msg.get("from", "?")
                s.send(msgpack.packb({"status": "OK"}, use_bin_type=True))
                print(f"[{server_id}] Eleicao recebida de '{sender}'")
                threading.Thread(target=iniciar_eleicao, args=(server_id, peers), daemon=True).start()

            elif action == "new_coordinator":
                novo = msg.get("coordinator", "")
                with coordinator_lock:
                    coordinator = novo
                print(f"[{server_id}] Coordenador atualizado: {novo}")
                s.send(msgpack.packb({"status": "OK"}, use_bin_type=True))

            else:
                s.send(msgpack.packb({"error": "acao desconhecida"}, use_bin_type=True))

        except Exception as e:
            print(f"[{server_id}] Erro no election_loop: {e}")
            try:
                s.send(msgpack.packb({"error": str(e)}, use_bin_type=True))
            except:
                pass


def servers_subscriber(server_id):
    global coordinator

    ctx = zmq.Context()
    sub = ctx.socket(zmq.SUB)
    sub.connect(os.getenv("PUBSUB_URL_SUB", "tcp://pubsub-proxy:5558"))
    sub.setsockopt_string(zmq.SUBSCRIBE, "servers")

    while True:
        try:
            sub.recv()
            msg = msgpack.unpackb(sub.recv(), raw=False)
            novo = msg.get("coordinator", "")
            if novo:
                with coordinator_lock:
                    coordinator = novo
                print(f"[{server_id}] [servers] coordenador={novo}")
        except Exception as e:
            print(f"[{server_id}] Erro no servers_subscriber: {e}")


def main():
    server_id = os.getenv("SERVER_ID", "server_1")
    os.makedirs("/app/data", exist_ok=True)
    conn = init_db(f"/app/data/{server_id}.db")

    election_port = os.getenv("ELECTION_PORT", "5570")
    peers = get_peers(server_id)

    context = zmq.Context()
    ref_url = os.getenv("REFERENCE_URL", "tcp://reference:5560")
    broker_url = os.getenv("BROKER_URL", "tcp://broker:5556")
    pubsub_url = os.getenv("PUBSUB_URL", "tcp://pubsub-proxy:5557")

    rep = context.socket(zmq.REP)
    rep.connect(broker_url)

    pub = context.socket(zmq.PUB)
    pub.connect(pubsub_url)

    time.sleep(1)
    rank, ref_socket = registrar_referencia(context, ref_url, server_id)

    threading.Thread(target=election_loop, args=(server_id, election_port, peers), daemon=True).start()
    threading.Thread(target=servers_subscriber, args=(server_id,), daemon=True).start()

    # o servidor com nome maior vira coordenador direto no inicio
    # eleicao so acontece quando o coordenador atual cair
    todos = [p["name"] for p in peers] + [server_id]
    coordenador_inicial = max(todos)
    with coordinator_lock:
        coordinator = coordenador_inicial
    if server_id == coordenador_inicial:
        def anunciar():
            time.sleep(0.5)
            virar_coordenador(server_id, os.getenv("PUBSUB_URL", "tcp://pubsub-proxy:5557"))
        threading.Thread(target=anunciar, daemon=True).start()

    print(f"[{server_id}] Pronto. Rank={rank}")

    msgs = 0
    while True:
        try:
            raw = rep.recv()
            msg = msgpack.unpackb(raw, raw=False)
            msg_type = msg.get("type", "UNKNOWN")
            payload = msg.get("payload", {})

            lc = lamport_receive(msg.get("lamport_clock", 0))
            print(f"[{server_id}] <-- {msg_type} | LC={lc} | {payload}")

            resp = {"timestamp": int(time.time() * 1000)}

            if msg_type == "LOGIN_REQ":
                status, text = handle_login(conn, payload)
                resp["type"] = "LOGIN_RESP"
                resp["payload"] = {"status": status, "message": text}
                msgs += 1

            elif msg_type == "CREATE_CHANNEL_REQ":
                status, text = handle_create_channel(conn, payload)
                resp["type"] = "CREATE_CHANNEL_RESP"
                resp["payload"] = {"status": status, "message": text}
                msgs += 1

            elif msg_type == "LIST_CHANNELS_REQ":
                status, channels = handle_list_channels(conn)
                resp["type"] = "LIST_CHANNELS_RESP"
                resp["payload"] = {"status": status, "channels": channels}
                msgs += 1

            elif msg_type == "PUBLISH_REQ":
                status, text = handle_publish(conn, pub, server_id, payload, lc)
                resp["type"] = "PUBLISH_RESP"
                resp["payload"] = {"status": status, "message": text}
                msgs += 1

            else:
                resp["type"] = "ERROR_RESP"
                resp["payload"] = {"message": "Tipo desconhecido."}

            send_lc = lamport_send()
            resp["lamport_clock"] = send_lc
            print(f"[{server_id}] --> {resp['type']} | LC={send_lc} | {resp['payload']}")
            rep.send(msgpack.packb(resp, use_bin_type=True))

            if msgs >= 15:
                msgs = 0
                ref_socket = heartbeat(context, ref_socket, ref_url, server_id)
                sincronizar(server_id, peers)

        except Exception as e:
            print(f"[{server_id}] Erro: {e}")


if __name__ == "__main__":
    main()