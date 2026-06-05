# How to Run

Two ways to run the system — choose one.

---

## Option A: Docker

### Prerequisites

Make sure Docker is installed:

```bash
docker --version
docker compose version
```

If not installed, download it from [docker.com](https://docker.com).

### Step 1 — Generate a Test File

```bash
cd file-sync
bash scripts/generate_test_file.sh ./demo_data/restaurant_data.db
```

### Step 2 — Start the Containers

```bash
docker compose up --build
```

You should see:
```
server      | Server ready. Ctrl-C to stop.
client-demo | Registered with server. Assigned id: restaurant-demo
```

### Step 3 — Trigger the Download

Open a second terminal and choose one:

**CLI:**
```bash
python server/cli.py download restaurant-demo \
  --path /var/data/restaurant_data.db
```

**REST API:**
```bash
curl -X POST http://localhost:8080/download \
  -H "Content-Type: application/json" \
  -d '{"client_id": "restaurant-demo", "path": "/var/data/restaurant_data.db"}'
```

### Step 4 — Check the Downloaded File

```bash
ls -lh downloads/
```

---

## Option B: Manual Installation

### Prerequisites

Make sure Python 3.9+ is installed, then install dependencies:

```bash
pip install aiohttp aiofiles websockets requests
```

### Step 1 — Generate a Test File

```bash
dd if=/dev/urandom of=/tmp/restaurant_data.db bs=1M count=100
```

### Step 2 — Start the Server

Open a terminal:

```bash
cd file-sync
python server/server.py
```

You should see:
```
Starting WebSocket server on ws://0.0.0.0:8765
Starting REST API on http://0.0.0.0:8080
Server ready. Ctrl-C to stop.
```

### Step 3 — Start the Client

Open a second terminal:

```bash
cd file-sync
CLIENT_ID=restaurant-001 \
TARGET_FILE=/tmp/restaurant_data.db \
python client/client.py
```

You should see:
```
Connecting to server: ws://localhost:8765
Registered with server. Assigned id: restaurant-001
```

### Step 4 — Trigger the Download

Open a third terminal and choose one:

**CLI:**
```bash
cd file-sync
python server/cli.py download restaurant-001 --path /tmp/restaurant_data.db
```

**REST API:**
```bash
curl -X POST http://localhost:8080/download \
  -H "Content-Type: application/json" \
  -d '{"client_id": "restaurant-001", "path": "/tmp/restaurant_data.db"}'
```

### Step 5 — Check the Downloaded File

```bash
ls -lh downloads/
```

---

## Useful Commands

| Command | Description |
|---------|-------------|
| `python server/cli.py list` | List all connected clients |
| `python server/cli.py download <client_id>` | Download file from a client |
| `docker compose down` | Stop all containers |
| `docker compose logs -f` | View container logs |
| `docker ps` | Check running containers |
