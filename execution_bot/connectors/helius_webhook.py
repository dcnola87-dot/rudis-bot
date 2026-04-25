from __future__ import annotations

import json
import os
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


HOST = (os.getenv("HELIUS_WEBHOOK_HOST") or "0.0.0.0").strip()
PORT = int((os.getenv("HELIUS_WEBHOOK_PORT") or "8080").strip() or "8080")
WEBHOOK_PATH = (os.getenv("HELIUS_WEBHOOK_PATH") or "/helius-webhook").strip() or "/helius-webhook"
PUMP_PROGRAM = (os.getenv("PUMPFUN_PROGRAM_ID") or "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P").strip()
GRADUATED_MINTS_FILE = Path(
    os.getenv("GRADUATED_MINTS_FILE")
    or (Path(__file__).resolve().parents[2] / "logs" / "graduated_mints.txt")
)

_WRITE_LOCK = threading.Lock()


def _json_response(handler: BaseHTTPRequestHandler, status: int, payload: dict):
    data = json.dumps(payload).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


def _flatten_logs(tx: dict) -> list[str]:
    logs: list[str] = []

    meta = tx.get("meta")
    if isinstance(meta, dict):
        for log in meta.get("logMessages") or []:
            if isinstance(log, str):
                logs.append(log)

    for log in tx.get("logMessages") or []:
        if isinstance(log, str):
            logs.append(log)

    for event in tx.get("events") or []:
        if isinstance(event, dict):
            for value in event.values():
                if isinstance(value, str):
                    logs.append(value)

    return logs


def _flatten_accounts(tx: dict) -> list[str]:
    accounts: list[str] = []

    for key in ("accountData", "accounts"):
        value = tx.get(key)
        if not isinstance(value, list):
            continue
        for item in value:
            if isinstance(item, str):
                accounts.append(item)
                continue
            if not isinstance(item, dict):
                continue
            for field in ("account", "pubkey", "address", "mint", "tokenAddress"):
                raw = item.get(field)
                if isinstance(raw, str) and raw.strip():
                    accounts.append(raw.strip())
                    break

    instructions = tx.get("instructions")
    if isinstance(instructions, list):
        for ix in instructions:
            if not isinstance(ix, dict):
                continue
            for account in ix.get("accounts") or []:
                if isinstance(account, str) and account.strip():
                    accounts.append(account.strip())
                elif isinstance(account, dict):
                    for field in ("pubkey", "address", "account"):
                        raw = account.get(field)
                        if isinstance(raw, str) and raw.strip():
                            accounts.append(raw.strip())
                            break

    return accounts


def _looks_like_graduation(tx: dict) -> bool:
    logs = [log.lower() for log in _flatten_logs(tx)]
    if not logs:
        return False

    graduation_markers = (
        "withdrawevent",
        "migrate",
        "migration",
        "complete",
        "initialize pool",
    )
    return any(any(marker in log for marker in graduation_markers) for log in logs)


def _extract_candidate_mints(tx: dict) -> list[str]:
    accounts = _flatten_accounts(tx)
    out: list[str] = []
    seen = set()
    for account in accounts:
        if (
            not account
            or account == PUMP_PROGRAM
            or len(account) < 32
            or len(account) > 44
        ):
            continue
        if account not in seen:
            out.append(account)
            seen.add(account)
    return out


def append_mints(mints: list[str]):
    if not mints:
        return
    GRADUATED_MINTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with _WRITE_LOCK:
        with GRADUATED_MINTS_FILE.open("a", encoding="utf-8") as fh:
            for mint in mints:
                fh.write(f"{mint}\n")


def extract_graduated_mints(payload) -> list[str]:
    txs = payload if isinstance(payload, list) else [payload]
    out: list[str] = []
    seen = set()
    for tx in txs:
        if not isinstance(tx, dict):
            continue
        if not _looks_like_graduation(tx):
            continue
        for mint in _extract_candidate_mints(tx):
            if mint not in seen:
                out.append(mint)
                seen.add(mint)
    return out


class HeliusWebhookHandler(BaseHTTPRequestHandler):
    server_version = "RudisHeliusWebhook/0.1"

    def do_GET(self):
        if self.path != WEBHOOK_PATH:
            _json_response(self, HTTPStatus.NOT_FOUND, {"ok": False, "error": "not_found"})
            return
        _json_response(
            self,
            HTTPStatus.OK,
            {
                "ok": True,
                "path": WEBHOOK_PATH,
                "pump_program": PUMP_PROGRAM,
                "graduated_mints_file": str(GRADUATED_MINTS_FILE),
            },
        )

    def do_POST(self):
        if self.path != WEBHOOK_PATH:
            _json_response(self, HTTPStatus.NOT_FOUND, {"ok": False, "error": "not_found"})
            return

        try:
            content_length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            content_length = 0

        raw = self.rfile.read(content_length) if content_length > 0 else b""
        try:
            payload = json.loads(raw.decode("utf-8") if raw else "[]")
        except json.JSONDecodeError:
            _json_response(self, HTTPStatus.BAD_REQUEST, {"ok": False, "error": "invalid_json"})
            return

        mints = extract_graduated_mints(payload)
        append_mints(mints)
        _json_response(self, HTTPStatus.OK, {"ok": True, "queued": len(mints), "mints": mints[:10]})

    def log_message(self, fmt: str, *args):
        return


def serve():
    server = ThreadingHTTPServer((HOST, PORT), HeliusWebhookHandler)
    print(f"Helius webhook listening on http://{HOST}:{PORT}{WEBHOOK_PATH}")
    print(f"Writing graduated mints to: {GRADUATED_MINTS_FILE}")
    server.serve_forever()


if __name__ == "__main__":
    serve()
