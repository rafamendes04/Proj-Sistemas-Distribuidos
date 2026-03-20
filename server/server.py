import zmq
import time
import sqlite3
import os
import msgpack

def init_db(db_name):
    """
    Inicializa o banco de dados SQLite para persistência dos dados.
    Armazena usuários (com timestamp do login) e canais criados.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            timestamp INTEGER
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS channels (
            channel_name TEXT PRIMARY KEY,
            created_at INTEGER
        )
    ''')
    conn.commit()
    return conn

def handle_login(conn, req):
    timestamp = int(time.time() * 1000)
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT OR IGNORE INTO users (username, timestamp) VALUES (?, ?)", (req['username'], timestamp))
        conn.commit()
        return "SUCCESS", f"User {req['username']} logged in."
    except Exception as e:
        return "ERROR", str(e)

def handle_create_channel(conn, req):
    timestamp = int(time.time() * 1000)
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO channels (channel_name, created_at) VALUES (?, ?)", (req['channel_name'], timestamp))
        conn.commit()
        return "SUCCESS", f"Channel '{req['channel_name']}' created."
    except sqlite3.IntegrityError:
        return "ERROR", f"Channel '{req['channel_name']}' already exists."
    except Exception as e:
        return "ERROR", str(e)

def handle_list_channels(conn, req):
    cursor = conn.cursor()
    cursor.execute("SELECT channel_name FROM channels")
    rows = cursor.fetchall()
    channels = [row[0] for row in rows]
    return "SUCCESS", channels, "Channels listed successfully."

def main():
    server_id = os.getenv('SERVER_ID', 'server_1')
    db_name = f"/app/data/{server_id}.db"
    
    # Garante que o diretório de dados existe para montagem de volume no Docker
    os.makedirs("/app/data", exist_ok=True)
    conn = init_db(db_name)
    
    context = zmq.Context()
    # Padrão REP para receber requisições do Broker e enviar respostas
    socket = context.socket(zmq.REP)
    
    # O broker está no endereço tcp://broker:5556 (porta DEALER)
    broker_url = os.getenv('BROKER_URL', 'tcp://broker:5556')
    print(f"[{server_id}] Conectando ao broker em {broker_url}...")
    
    socket.connect(broker_url)
    print(f"[{server_id}] Servidor pronto e escutando. Utilizando banco de dados local: {db_name}")

    while True:
        try:
            message = socket.recv()
            
            # Deserializa a mensagem utilizando MessagePack
            wrapper = msgpack.unpackb(message, raw=False)
            
            msg_type_name = wrapper.get('type', 'UNKNOWN')
                
            print(f"[{server_id}] <-- Requisição recebida: {msg_type_name} | Conteúdo Completo: {wrapper}")
            
            # Prepara a estrutura da resposta
            response = {}
            response["timestamp"] = int(time.time() * 1000)
            
            # Processa de acordo com o tipo da mensagem
            if msg_type_name == "LOGIN_REQ":
                status, msg = handle_login(conn, wrapper.get('payload', {}))
                response["type"] = "LOGIN_RESP"
                response["payload"] = {"status": status, "message": msg}
                
            elif msg_type_name == "CREATE_CHANNEL_REQ":
                status, msg = handle_create_channel(conn, wrapper.get('payload', {}))
                response["type"] = "CREATE_CHANNEL_RESP"
                response["payload"] = {"status": status, "message": msg}
                
            elif msg_type_name == "LIST_CHANNELS_REQ":
                status, channels, msg = handle_list_channels(conn, wrapper.get('payload', {}))
                response["type"] = "LIST_CHANNELS_RESP"
                response["payload"] = {"status": status, "message": msg, "channels": channels}
                
            else:
                response["type"] = "UNKNOWN"
            
            print(f"[{server_id}] --> Enviando resposta: {response['type']} | Conteúdo Completo: {response}")
            
            # Serializa a resposta para binário com MessagePack e envia
            socket.send(msgpack.packb(response, use_bin_type=True))
            
        except Exception as e:
            print(f"[{server_id}] ❌ Erro processando a mensagem: {e}")
            pass

if __name__ == "__main__":
    main()
