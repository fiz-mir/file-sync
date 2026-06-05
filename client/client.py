"""
On-Premise Client - Connects outbound to the cloud server via WebSocket.
Waits for download_request commands and streams the requested file back.
"""

import asyncio
import json
import logging
import os
import struct
import time
from pathlib import Path

import aiofiles
import websockets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("client")

# ── Config ────────────────────────────────────────────────────────────────────
SERVER_WS_URL  = os.getenv("SERVER_WS_URL",  "ws://localhost:8765")
CLIENT_ID      = os.getenv("CLIENT_ID",      "restaurant-001")
SHARED_SECRET  = os.getenv("SHARED_SECRET",  "change-me-in-production")
TARGET_FILE    = os.getenv("TARGET_FILE",     "/var/data/restaurant_data.db")
RECONNECT_DELAY = int(os.getenv("RECONNECT_DELAY", "5"))   # seconds
CHUNK_SIZE      = 256 * 1024   # 256 KB — server may override per request


# ══════════════════════════════════════════════════════════════════════════════
# File streamer
# ══════════════════════════════════════════════════════════════════════════════
async def stream_file(ws, transfer_id: str, path: str, chunk_size: int):
    """Read the file in chunks and send each as a binary WebSocket frame."""
    fpath = Path(path)

    if not fpath.exists():
        await ws.send(json.dumps({
            "type":        "transfer_error",
            "transfer_id": transfer_id,
            "reason":      f"File not found: {path}",
        }))
        return

    total_bytes = fpath.stat().st_size
    log.info("Starting transfer %s  file=%s  size=%d bytes", transfer_id, path, total_bytes)

    await ws.send(json.dumps({
        "type":        "transfer_start",
        "transfer_id": transfer_id,
        "filename":    fpath.name,
        "total_bytes": total_bytes,
    }))

    tid_bytes = transfer_id.encode("utf-8").ljust(36, b"\x00")[:36]
    seq       = 0
    sent      = 0
    t0        = time.time()

    try:
        async with aiofiles.open(fpath, "rb") as f:
            while True:
                chunk = await f.read(chunk_size)
                if not chunk:
                    break
                # Frame: [36-byte tid][4-byte seq big-endian][payload]
                frame = tid_bytes + struct.pack(">I", seq) + chunk
                await ws.send(frame)
                sent += len(chunk)
                seq  += 1

                # Progress log every 10 MB
                if sent % (10 * 1024 * 1024) < chunk_size:
                    pct = sent / total_bytes * 100 if total_bytes else 0
                    log.info("  %.1f%%  (%d / %d bytes)", pct, sent, total_bytes)

        elapsed = time.time() - t0
        speed   = sent / elapsed / 1024 / 1024 if elapsed > 0 else 0
        log.info(
            "Transfer complete: %s  %d bytes in %.2fs  (%.1f MB/s)",
            transfer_id, sent, elapsed, speed
        )

        await ws.send(json.dumps({
            "type":        "transfer_end",
            "transfer_id": transfer_id,
            "bytes_sent":  sent,
        }))

    except Exception as e:
        log.exception("Error streaming file: %s", e)
        await ws.send(json.dumps({
            "type":        "transfer_error",
            "transfer_id": transfer_id,
            "reason":      str(e),
        }))


# ══════════════════════════════════════════════════════════════════════════════
# Main connection loop
# ══════════════════════════════════════════════════════════════════════════════
async def connect_and_serve():
    log.info("Connecting to server: %s  as client_id=%s", SERVER_WS_URL, CLIENT_ID)

    async with websockets.connect(
        SERVER_WS_URL,
        ping_interval=30,
        ping_timeout=10,
        close_timeout=10,
        max_size=None,          # no message-size limit (large binary frames)
    ) as ws:
        # ── Register ───────────────────────────────────────────────────────
        await ws.send(json.dumps({
            "type":      "register",
            "client_id": CLIENT_ID,
            "secret":    SHARED_SECRET,
        }))
        ack = json.loads(await ws.recv())
        assert ack.get("type") == "registered", f"Unexpected ack: {ack}"
        assigned_id = ack.get("client_id", CLIENT_ID)
        log.info("Registered with server. Assigned id: %s", assigned_id)

        # ── Message loop ───────────────────────────────────────────────────
        async for raw in ws:
            if isinstance(raw, bytes):
                continue   # server won't send binary; ignore just in case

            msg = json.loads(raw)
            mtype = msg.get("type")

            if mtype == "download_request":
                path        = msg.get("path", TARGET_FILE)
                transfer_id = msg["transfer_id"]
                chunk_size  = int(msg.get("chunk_size", CHUNK_SIZE))
                # Run transfer in background so we can handle new commands
                asyncio.create_task(
                    stream_file(ws, transfer_id, path, chunk_size)
                )

            elif mtype == "pong":
                pass

            elif mtype == "error":
                log.error("Server error: %s", msg.get("reason"))

            else:
                log.debug("Unknown message type: %s", mtype)


async def run():
    while True:
        try:
            await connect_and_serve()
        except (OSError, websockets.exceptions.ConnectionClosed,
                websockets.exceptions.WebSocketException) as e:
            log.warning("Connection lost: %s  — reconnecting in %ds", e, RECONNECT_DELAY)
        except AssertionError as e:
            log.error("Auth/handshake failed: %s  — reconnecting in %ds", e, RECONNECT_DELAY)
        except Exception as e:
            log.exception("Unexpected error: %s", e)
        await asyncio.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("Client shutting down.")
