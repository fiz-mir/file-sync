FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY server.py .
COPY cli.py .

RUN mkdir -p /downloads

EXPOSE 8765 8080

CMD ["python", "server.py"]
