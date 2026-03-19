FROM python:3.9-slim

WORKDIR /app

# Instala dependências de compilação essenciais e limpa cache do apt
RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*

# Copia dependências Python
COPY server/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia código fonte
COPY server/server.py server/broker.py ./
COPY proto/chat.proto ./proto/

# Compila o arquivo proto usando o plugin Python
RUN python -m grpc_tools.protoc -I./proto --python_out=./proto ./proto/chat.proto

# Permite acesso à porta do broker externamente e internamente para Docker
EXPOSE 5555 5556

# Definimos nos comandos de inicialização dos containers se o script será server.py ou broker.py
CMD ["python", "server.py"]
