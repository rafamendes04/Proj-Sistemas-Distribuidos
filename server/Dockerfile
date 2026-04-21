FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*

COPY server/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY server/server.py server/broker.py server/pubsub_proxy.py server/reference.py ./

EXPOSE 5555 5556

CMD ["python", "server.py"]