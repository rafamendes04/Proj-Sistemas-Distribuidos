import zmq
import time
import sqlite3
import os
import sys

# Adiciona o diretório proto gerado ao path para importar as classes do Protobuf
sys.path.append(os.path.join(os.path.dirname(__file__), 'proto'))
import chat_pb2

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
        cursor.execute("INSERT OR IGNORE INTO users (username, timestamp) VALUES (?, ?)", (req.username, timestamp))
        conn.commit()
        return chat_pb2.SUCCESS, f"User {req.username} logged in."
    except Exception as e:
        return chat_pb2.ERROR, str(e)

def handle_create_channel(conn, req):
    timestamp = int(time.time() * 1000)
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO channels (channel_name, created_at) VALUES (?, ?)", (req.channel_name, timestamp))
        conn.commit()
        return chat_pb2.SUCCESS, f"Channel '{req.channel_name}' created."
    except sqlite3.IntegrityError:
        return chat_pb2.ERROR, f"Channel '{req.channel_name}' already exists."
    except Exception as e:
        return chat_pb2.ERROR, str(e)

def handle_list_channels(conn, req):
    cursor = conn.cursor()
    cursor.execute("SELECT channel_name FROM channels")
    rows = cursor.fetchall()
    channels = [row[0] for row in rows]
    return chat_pb2.SUCCESS, channels, "Channels listed successfully."

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
            
            # Deserializa a mensagem utilizando Protobuf
            wrapper = chat_pb2.Wrapper()
            wrapper.ParseFromString(message)
            
            try:
                msg_type_name = chat_pb2.MessageType.Name(wrapper.type)
            except ValueError:
                msg_type_name = "UNKNOWN"
                
            print(f"[{server_id}] <-- Requisição recebida: {msg_type_name}")
            
            # Prepara a estrutura da resposta
            response = chat_pb2.Wrapper()
            response.timestamp = int(time.time() * 1000)
            
            # Processa de acordo com o tipo da mensagem
            if wrapper.type == chat_pb2.LOGIN_REQ:
                status, msg = handle_login(conn, wrapper.login_req)
                response.type = chat_pb2.LOGIN_RESP
                response.login_resp.status = status
                response.login_resp.message = msg
                
            elif wrapper.type == chat_pb2.CREATE_CHANNEL_REQ:
                status, msg = handle_create_channel(conn, wrapper.create_channel_req)
                response.type = chat_pb2.CREATE_CHANNEL_RESP
                response.create_channel_resp.status = status
                response.create_channel_resp.message = msg
                
            elif wrapper.type == chat_pb2.LIST_CHANNELS_REQ:
                status, channels, msg = handle_list_channels(conn, wrapper.list_channels_req)
                response.type = chat_pb2.LIST_CHANNELS_RESP
                response.list_channels_resp.status = status
                response.list_channels_resp.message = msg
                response.list_channels_resp.channels.extend(channels)
                
            else:
                response.type = chat_pb2.UNKNOWN
            
            try:
                resp_type_name = chat_pb2.MessageType.Name(response.type)
            except ValueError:
                resp_type_name = "UNKNOWN"

            print(f"[{server_id}] --> Enviando resposta: {resp_type_name}")
            
            # Serializa a resposta para binário e envia
            socket.send(response.SerializeToString())
            
        except Exception as e:
            print(f"[{server_id}] ❌ Erro processando a mensagem: {e}")
            # Em caso de erro muito grave (ex: format exception), 
            # é importante não deixar o socket ZMQ REQ/REP num estado travado.
            # Aqui, para fins didáticos, lidamos enviando um UNKNOWN ou reiniciando o socket, mas try/catch geral absorve.
            pass

if __name__ == "__main__":
    main()
