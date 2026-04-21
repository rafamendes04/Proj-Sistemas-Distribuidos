import zmq
import time
import sqlite3
import os
import msgpack
import threading

lamport_clock = 0
lamport_lock = threading.Lock()

def lamport_send():
    global lamport_clock
    with lamport_lock:
        lamport_clock += 1
        return lamport_clock

def lamport_receive(received_clock):
    global lamport_clock
    with lamport_lock:
        lamport_clock = max(lamport_clock, received_clock) + 1
        return lamport_clock

def init_db(db_name):
    conn = sqlite3.connect(db_name, check_same_thread=False)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        username TEXT PRIMARY KEY, timestamp INTEGER)''')
    c.execute('''CREATE TABLE IF NOT EXISTS channels (
        channel_name TEXT PRIMARY KEY, created_at INTEGER)''')
    c.execute('''CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        channel TEXT, username TEXT, content TEXT,
        timestamp INTEGER, lamport_clock INTEGER)''')
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

    row = conn.execute("SELECT 1 FROM channels WHERE channel_name = ?", (channel,)).fetchone()
    if not row:
        return "ERROR", f"Canal '{channel}' nao existe."

    conn.execute(
        "INSERT INTO messages (channel, username, content, timestamp, lamport_clock) VALUES (?, ?, ?, ?, ?)",
        (channel, username, content, ts, clock)
    )
    conn.commit()

    send_clock = lamport_send()
    pub_msg = msgpack.packb({
        "channel": channel, "username": username,
        "message": content, "timestamp": ts,
        "lamport_clock": send_clock
    }, use_bin_type=True)

    pub_socket.send_multipart([channel.encode(), pub_msg])
    print(f"[{server_id}] --> PUB '{channel}': [{username}] {content} | LC={send_clock}")
    return "SUCCESS", "Mensagem publicada."

def criar_ref_socket(context, ref_url):
    s = context.socket(zmq.REQ)
    s.setsockopt(zmq.RCVTIMEO, 5000)
    s.setsockopt(zmq.LINGER, 0)
    s.connect(ref_url)
    return s

def register_on_reference(context, ref_url, server_id):
    """Tenta registrar, recriando o socket se necessário."""
    ref_socket = criar_ref_socket(context, ref_url)
    try:
        ref_socket.send(msgpack.packb({"action": "register", "name": server_id}, use_bin_type=True))
        raw = ref_socket.recv()
        resp = msgpack.unpackb(raw, raw=False)
        rank = resp.get("rank", -1)
        print(f"[{server_id}] Registrado na Referencia. Rank={rank}")
        return rank, ref_socket
    except Exception as e:
        print(f"[{server_id}] Erro ao registrar na Referencia: {e}")
        ref_socket.close()
        return -1, criar_ref_socket(context, ref_url)

def send_heartbeat(context, ref_socket, ref_url, server_id):
    """Envia heartbeat. Se der erro, recria o socket e retorna o novo."""
    try:
        ref_socket.send(msgpack.packb({"action": "heartbeat", "name": server_id}, use_bin_type=True))
        raw = ref_socket.recv()
        resp = msgpack.unpackb(raw, raw=False)
        ref_time = resp.get("current_time", None)
        local_time = int(time.time() * 1000)
        diff = (ref_time - local_time) if ref_time else 0
        print(f"[{server_id}] Heartbeat OK | ref_time={ref_time} | local={local_time} | diff={diff}ms")
        return ref_socket
    except Exception as e:
        print(f"[{server_id}] Erro no heartbeat: {e} — recriando socket da Referencia.")
        ref_socket.close()
        return criar_ref_socket(context, ref_url)

def main():
    server_id = os.getenv('SERVER_ID', 'server_1')
    db_name = f"/app/data/{server_id}.db"
    os.makedirs("/app/data", exist_ok=True)
    conn = init_db(db_name)

    context = zmq.Context()
    ref_url = os.getenv('REFERENCE_URL', 'tcp://reference:5560')

    rep_socket = context.socket(zmq.REP)
    broker_url = os.getenv('BROKER_URL', 'tcp://broker:5556')
    rep_socket.connect(broker_url)

    pub_socket = context.socket(zmq.PUB)
    proxy_url = os.getenv('PUBSUB_URL', 'tcp://pubsub-proxy:5557')
    pub_socket.connect(proxy_url)

    time.sleep(1)

    rank, ref_socket = register_on_reference(context, ref_url, server_id)
    print(f"[{server_id}] Pronto. Rank={rank} | Broker={broker_url} | PubSub={proxy_url}")

    msgs_recebidas = 0

    while True:
        try:
            raw = rep_socket.recv()
            msg = msgpack.unpackb(raw, raw=False)
            msg_type = msg.get('type', 'UNKNOWN')
            payload = msg.get('payload', {})

            received_clock = msg.get('lamport_clock', 0)
            current_clock = lamport_receive(received_clock)

            print(f"[{server_id}] <-- {msg_type} | LC={current_clock} | {payload}")

            resp = {"timestamp": int(time.time() * 1000)}

            if msg_type == "LOGIN_REQ":
                status, text = handle_login(conn, payload)
                resp["type"] = "LOGIN_RESP"
                resp["payload"] = {"status": status, "message": text}
                msgs_recebidas += 1

            elif msg_type == "CREATE_CHANNEL_REQ":
                status, text = handle_create_channel(conn, payload)
                resp["type"] = "CREATE_CHANNEL_RESP"
                resp["payload"] = {"status": status, "message": text}
                msgs_recebidas += 1

            elif msg_type == "LIST_CHANNELS_REQ":
                status, channels = handle_list_channels(conn)
                resp["type"] = "LIST_CHANNELS_RESP"
                resp["payload"] = {"status": status, "channels": channels}
                msgs_recebidas += 1

            elif msg_type == "PUBLISH_REQ":
                status, text = handle_publish(conn, pub_socket, server_id, payload, current_clock)
                resp["type"] = "PUBLISH_RESP"
                resp["payload"] = {"status": status, "message": text}
                msgs_recebidas += 1

            else:
                resp["type"] = "ERROR_RESP"
                resp["payload"] = {"message": "Tipo desconhecido."}

            send_clock = lamport_send()
            resp["lamport_clock"] = send_clock

            print(f"[{server_id}] --> {resp['type']} | LC={send_clock} | {resp['payload']}")
            rep_socket.send(msgpack.packb(resp, use_bin_type=True))

            if msgs_recebidas >= 10:
                msgs_recebidas = 0
                ref_socket = send_heartbeat(context, ref_socket, ref_url, server_id)

        except Exception as e:
            print(f"[{server_id}] Erro: {e}")

if __name__ == "__main__":
    main()