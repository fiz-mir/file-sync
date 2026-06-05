#!/usr/bin/env python3
"""
CLI tool to trigger file downloads from connected on-premise clients.

Usage examples:
  python cli.py list
  python cli.py download restaurant-001
  python cli.py download restaurant-001 --path /var/data/custom.db
  python cli.py download restaurant-001 --timeout 600
"""

import argparse
import json
import sys
import time

import requests


def get_api_base(args) -> str:
    return f"http://{args.host}:{args.port}"


def cmd_list(args):
    r = requests.get(f"{get_api_base(args)}/clients", timeout=10)
    r.raise_for_status()
    data = r.json()
    clients = data.get("clients", [])
    if not clients:
        print("No clients currently connected.")
    else:
        print(f"{len(clients)} client(s) connected:")
        for c in clients:
            print(f"  • {c}")


def cmd_download(args):
    payload = {
        "client_id": args.client_id,
        "path":      args.path,
        "timeout":   args.timeout,
    }

    print(f"Requesting download from '{args.client_id}'  path='{args.path}' …")
    t0 = time.time()

    try:
        r = requests.post(
            f"{get_api_base(args)}/download",
            json=payload,
            timeout=args.timeout + 10,
        )
    except requests.exceptions.ConnectionError:
        print(f"ERROR: Cannot reach server at {get_api_base(args)}")
        sys.exit(1)

    elapsed = round(time.time() - t0, 2)

    if r.status_code == 200:
        data = r.json()
        mb   = data["bytes"] / 1024 / 1024
        speed = mb / data["elapsed_s"] if data.get("elapsed_s") else 0
        print("✓ Download complete!")
        print(f"  Transfer ID : {data['transfer_id']}")
        print(f"  Saved to    : {data['saved_to']}")
        print(f"  Size        : {mb:.2f} MB ({data['bytes']:,} bytes)")
        print(f"  Duration    : {data['elapsed_s']} s")
        print(f"  Speed       : {speed:.1f} MB/s")
    else:
        try:
            err = r.json().get("error", r.text)
        except Exception:
            err = r.text
        print(f"✗ Download failed (HTTP {r.status_code}): {err}")
        sys.exit(1)


def cmd_status(args):
    r = requests.get(f"{get_api_base(args)}/download/{args.transfer_id}", timeout=10)
    r.raise_for_status()
    print(json.dumps(r.json(), indent=2))


def main():
    parser = argparse.ArgumentParser(
        description="Trigger file downloads from connected on-premise clients."
    )
    parser.add_argument("--host", default="localhost", help="Server API host (default: localhost)")
    parser.add_argument("--port", default=8080,        help="Server API port (default: 8080)", type=int)

    sub = parser.add_subparsers(dest="command", required=True)

    # list
    sub.add_parser("list", help="List connected clients")

    # download
    dl = sub.add_parser("download", help="Download a file from a client")
    dl.add_argument("client_id", help="Client ID to download from")
    dl.add_argument(
        "--path",
        default="/var/data/restaurant_data.db",
        help="Remote file path on the client (default: /var/data/restaurant_data.db)"
    )
    dl.add_argument(
        "--timeout", type=int, default=300,
        help="Max seconds to wait for transfer (default: 300)"
    )

    # status
    st = sub.add_parser("status", help="Check status of a transfer")
    st.add_argument("transfer_id", help="Transfer ID to query")

    args = parser.parse_args()

    if args.command == "list":
        cmd_list(args)
    elif args.command == "download":
        cmd_download(args)
    elif args.command == "status":
        cmd_status(args)


if __name__ == "__main__":
    main()
