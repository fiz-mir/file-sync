# How to Run

## Prerequisites

```bash
pip install aiohttp aiofiles websockets requests
```

---

## Step 1 — Create a Test File

```bash
dd if=/dev/urandom of=/tmp/restaurant_data.db bs=1M count=100
```

---

## Step 2 — Start the Server

Open a terminal:

```bash
cd file-sync
python server/server.py
```

---

## Step 3 — Start the Client

Open a second terminal:

```bash
cd file-sync
CLIENT_ID=restaurant-001 \
TARGET_FILE=/tmp/restaurant_data.db \
python client/client.py
```

---

## Step 4 — Trigger the Download

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

The downloaded file will be saved in the `file-sync/downloads/` folder.
