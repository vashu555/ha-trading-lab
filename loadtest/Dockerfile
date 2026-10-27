# Load generator: Locust (HTTP users + charts) and websocket-client (the wss:// price stream).
FROM python:3.11-slim
RUN pip install --no-cache-dir locust==2.31.8 websocket-client==1.8.0
WORKDIR /mnt/locust
