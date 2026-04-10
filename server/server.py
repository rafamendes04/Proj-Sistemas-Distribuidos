import zmq
import time
import sqlite3
import os
import msgpack

def init_db(db_name):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            timestamp INTEGER
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS channels (
            channel_name TEXT PRIMARY KEY,
            created_at INTEGER
        )
    ''')
    # Armazena todas as mensagens publicadas nos canais
    c.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel TEXT,
            username TEXT,
            content TEXT,
            timestamp INTEGER
        )
    ''')
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

def handle_publish(conn, pub_socket, server_id, payload):
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
        "INSERT INTO messages (channel, username, content, timestamp) VALUES (?, ?, ?, ?)",
        (channel, username, content, ts)
    )
    conn.commit()

    pub_msg = msgpack.packb({
        "channel": channel,
        "username": username,
        "message": content,
        "timestamp": ts
    }, use_bin_type=True)

    pub_socket.send_multipart([channel.encode(), pub_msg])
    print(f"[{server_id}] --> PUB '{channel}': [{username}] {content}")

    return "SUCCESS", "Mensagem publicada."

def main():
    server_id = os.getenv('SERVER_ID', 'server_1')
    db_name = f"/app/data/{server_id}.db"
    os.makedirs("/app/data", exist_ok=True)
    conn = init_db(db_name)

    context = zmq.Context()

    rep_socket = context.socket(zmq.REP)
    broker_url = os.getenv('BROKER_URL', 'tcp://broker:5556')
    rep_socket.connect(broker_url)

    pub_socket = context.socket(zmq.PUB)
    proxy_url = os.getenv('PUBSUB_URL', 'tcp://pubsub-proxy:5557')
    pub_socket.connect(proxy_url)

    time.sleep(0.5)

    print(f"[{server_id}] Pronto. Broker={broker_url} | PubSub={proxy_url}")

    while True:
        try:
            raw = rep_socket.recv()
            msg = msgpack.unpackb(raw, raw=False)
            msg_type = msg.get('type', 'UNKNOWN')
            payload = msg.get('payload', {})

            print(f"[{server_id}] <-- {msg_type} | {payload}")

            resp = {"timestamp": int(time.time() * 1000)}

            if msg_type == "LOGIN_REQ":
                status, text = handle_login(conn, payload)
                resp["type"] = "LOGIN_RESP"
                resp["payload"] = {"status": status, "message": text}

            elif msg_type == "CREATE_CHANNEL_REQ":
                status, text = handle_create_channel(conn, payload)
                resp["type"] = "CREATE_CHANNEL_RESP"
                resp["payload"] = {"status": status, "message": text}

            elif msg_type == "LIST_CHANNELS_REQ":
                status, channels = handle_list_channels(conn)
                resp["type"] = "LIST_CHANNELS_RESP"
                resp["payload"] = {"status": status, "channels": channels}

            elif msg_type == "PUBLISH_REQ":
                status, text = handle_publish(conn, pub_socket, server_id, payload)
                resp["type"] = "PUBLISH_RESP"
                resp["payload"] = {"status": status, "message": text}

            else:
                resp["type"] = "ERROR_RESP"
                resp["payload"] = {"message": "Tipo desconhecido."}

            print(f"[{server_id}] --> {resp['type']} | {resp['payload']}")
            rep_socket.send(msgpack.packb(resp, use_bin_type=True))

        except Exception as e:
            print(f"[{server_id}] Erro: {e}")

if __name__ == "__main__":
    main()
