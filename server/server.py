"""
Cloud Server - Receives file uploads from on-premise clients via reverse tunnel / WebSocket.
Clients connect outbound to this server; the server can then request file downloads on demand.
"""

import asyncio
import json
import logging
import os
import time
import uuid
from pathlib import Path
from typing import Dict, Optional

import aiofiles
import websockets
from aiohttp import web

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("server")

# ── Config ────────────────────────────────────────────────────────────────────
WS_HOST   = os.getenv("WS_HOST", "0.0.0.0")
WS_PORT   = int(os.getenv("WS_PORT", "8765"))
API_HOST  = os.getenv("API_HOST", "0.0.0.0")
API_PORT  = int(os.getenv("API_PORT", "8080"))
DOWNLOADS = Path(os.getenv("DOWNLOAD_DIR", "./downloads"))
CHUNK_SIZE = 256 * 1024   # 256 KB chunks
SHARED_SECRET = os.getenv("SHARED_SECRET", "change-me-in-production")

DOWNLOADS.mkdir(parents=True, exist_ok=True)

# ── State ─────────────────────────────────────────────────────────────────────
# client_id -> websocket connection
connected_clients: Dict[str, websockets.WebSocketServerProtocol] = {}
# transfer_id -> asyncio.Future (resolves when transfer completes)
pending_transfers: Dict[str, asyncio.Future] = {}


# ══════════════════════════════════════════════════════════════════════════════
# WebSocket handler  (clients connect here)
# ══════════════════════════════════════════════════════════════════════════════
async def ws_handler(ws: websockets.WebSocketServerProtocol):
    client_id: Optional[str] = None
    try:
        # ── Handshake ──────────────────────────────────────────────────────
        raw = await asyncio.wait_for(ws.recv(), timeout=10)
        msg = json.loads(raw)
        assert msg.get("type") == "register", "Expected register message"
        assert msg.get("secret") == SHARED_SECRET, "Bad secret"

        client_id = msg.get("client_id") or str(uuid.uuid4())
        connected_clients[client_id] = ws
        log.info("Client registered: %s", client_id)
        await ws.send(json.dumps({"type": "registered", "client_id": client_id}))

        # ── Message loop ───────────────────────────────────────────────────
        async for raw_msg in ws:
            # Binary frame → file chunk
            if isinstance(raw_msg, bytes):
                await _handle_chunk(raw_msg)
            else:
                data = json.loads(raw_msg)
                mtype = data.get("type")
                if mtype == "transfer_start":
                    await _handle_transfer_start(data)
                elif mtype == "transfer_end":
                    await _handle_transfer_end(data)
                elif mtype == "transfer_error":
                    await _handle_transfer_error(data)
                elif mtype == "ping":
                    await ws.send(json.dumps({"type": "pong"}))

    except asyncio.TimeoutError:
        log.warning("Handshake timeout")
    except AssertionError as e:
        log.warning("Auth failed: %s", e)
        try:
            await ws.send(json.dumps({"type": "error", "reason": str(e)}))
        except Exception:
            pass
    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as e:
        log.exception("WS handler error: %s", e)
    finally:
        if client_id and client_id in connected_clients:
            del connected_clients[client_id]
            log.info("Client disconnected: %s", client_id)


# ── Transfer state per in-flight download ─────────────────────────────────────
class TransferState:
    def __init__(self, transfer_id: str, filename: str, total_bytes: int):
        self.transfer_id = transfer_id
        self.filename    = filename
        self.total_bytes = total_bytes
        self.received    = 0
        self.path        = DOWNLOADS / f"{transfer_id}_{filename}"
        self.fh          = None   # opened in _handle_transfer_start

active_transfers: Dict[str, TransferState] = {}


async def _handle_transfer_start(data: dict):
    tid = data["transfer_id"]
    ts  = TransferState(tid, data["filename"], data["total_bytes"])
    ts.fh = await aiofiles.open(ts.path, "wb")
    active_transfers[tid] = ts
    log.info("Transfer started: %s  (%s bytes)", tid, data["total_bytes"])


async def _handle_chunk(raw: bytes):
    """
    Binary frame layout:
      [36 bytes transfer_id UTF-8] [4 bytes uint32 seq] [rest = payload]
    """
    if len(raw) < 40:
        return
    tid  = raw[:36].decode("utf-8").rstrip("\x00")
    # seq = int.from_bytes(raw[36:40], "big")   # available for ordering/ack
    payload = raw[40:]
    ts = active_transfers.get(tid)
    if ts and ts.fh:
        await ts.fh.write(payload)
        ts.received += len(payload)


async def _handle_transfer_end(data: dict):
    tid = data["transfer_id"]
    ts  = active_transfers.pop(tid, None)
    if ts and ts.fh:
        await ts.fh.close()
        log.info(
            "Transfer complete: %s  saved to %s  (%d bytes)",
            tid, ts.path, ts.received
        )
        fut = pending_transfers.pop(tid, None)
        if fut and not fut.done():
            fut.set_result({
                "transfer_id": tid,
                "path": str(ts.path),
                "bytes": ts.received,
            })


async def _handle_transfer_error(data: dict):
    tid = data["transfer_id"]
    ts  = active_transfers.pop(tid, None)
    if ts and ts.fh:
        await ts.fh.close()
        ts.path.unlink(missing_ok=True)
    fut = pending_transfers.pop(tid, None)
    if fut and not fut.done():
        fut.set_exception(RuntimeError(data.get("reason", "unknown error")))
    log.error("Transfer error from client: %s", data.get("reason"))


# ══════════════════════════════════════════════════════════════════════════════
# Core download trigger
# ══════════════════════════════════════════════════════════════════════════════
async def request_download(client_id: str, remote_path: str, timeout: int = 300) -> dict:
    ws = connected_clients.get(client_id)
    if ws is None:
        raise ValueError(f"Client '{client_id}' is not connected")

    transfer_id = str(uuid.uuid4())
    loop = asyncio.get_event_loop()
    fut  = loop.create_future()
    pending_transfers[transfer_id] = fut

    cmd = {
        "type":        "download_request",
        "transfer_id": transfer_id,
        "path":        remote_path,
        "chunk_size":  CHUNK_SIZE,
    }
    await ws.send(json.dumps(cmd))
    log.info("Download requested from %s  tid=%s  path=%s", client_id, transfer_id, remote_path)

    result = await asyncio.wait_for(fut, timeout=timeout)
    return result


# ══════════════════════════════════════════════════════════════════════════════
# REST API
# ══════════════════════════════════════════════════════════════════════════════
async def api_list_clients(request: web.Request) -> web.Response:
    clients = list(connected_clients.keys())
    return web.json_response({"clients": clients, "count": len(clients)})


async def api_download(request: web.Request) -> web.Response:
    body = await request.json()
    client_id   = body.get("client_id")
    remote_path = body.get("path")
    timeout     = int(body.get("timeout", 300))

    if not client_id or not remote_path:
        return web.json_response(
            {"error": "client_id and path are required"}, status=400
        )

    try:
        t0     = time.time()
        result = await request_download(client_id, remote_path, timeout)
        elapsed = round(time.time() - t0, 2)
        return web.json_response({
            "status":      "ok",
            "transfer_id": result["transfer_id"],
            "saved_to":    result["path"],
            "bytes":       result["bytes"],
            "elapsed_s":   elapsed,
        })
    except ValueError as e:
        return web.json_response({"error": str(e)}, status=404)
    except asyncio.TimeoutError:
        return web.json_response({"error": "Transfer timed out"}, status=504)
    except Exception as e:
        log.exception("Download failed")
        return web.json_response({"error": str(e)}, status=500)


async def api_download_status(request: web.Request) -> web.Response:
    tid = request.match_info["transfer_id"]
    if tid in pending_transfers:
        return web.json_response({"status": "in_progress", "transfer_id": tid})
    saved = list(DOWNLOADS.glob(f"{tid}_*"))
    if saved:
        return web.json_response({
            "status": "complete",
            "transfer_id": tid,
            "path": str(saved[0]),
            "bytes": saved[0].stat().st_size,
        })
    return web.json_response({"status": "not_found"}, status=404)


def build_app() -> web.Application:
    app = web.Application()
    app.router.add_get( "/clients",                    api_list_clients)
    app.router.add_post("/download",                   api_download)
    app.router.add_get( "/download/{transfer_id}",     api_download_status)
    return app


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════
async def main():
    log.info("Starting WebSocket server on ws://%s:%s", WS_HOST, WS_PORT)
    log.info("Starting REST API on http://%s:%s",       API_HOST, API_PORT)

    ws_server  = websockets.serve(ws_handler, WS_HOST, WS_PORT)
    app        = build_app()
    runner     = web.AppRunner(app)
    await runner.setup()
    site       = web.TCPSite(runner, API_HOST, API_PORT)

    async with ws_server:
        await site.start()
        log.info("Server ready. Ctrl-C to stop.")
        await asyncio.Future()   # run forever


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Shutting down.")
