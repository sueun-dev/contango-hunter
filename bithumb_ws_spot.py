#!/usr/bin/env python3
"""Stream Bithumb KRW order books via WebSocket to compute USD-converted prices."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import sys
import time
import uuid
from typing import Dict, List

import ccxt
import websockets


BITHUMB_WS_URL = "wss://ws-api.bithumb.com/websocket/v1"
CHUNK_SIZE = 50


def normalize_base(symbol: str) -> str:
    return symbol.split("/")[0]


def symbol_to_code(symbol: str) -> str:
    base, quote = symbol.split("/")
    return f"{quote}-{base}"


def safe_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def load_bithumb_krw_markets() -> List[str]:
    client = ccxt.bithumb({"enableRateLimit": True})
    markets = client.load_markets()
    codes: List[str] = []
    for market in markets.values():
        if market.get("quote") != "KRW":
            continue
        codes.append(symbol_to_code(market["symbol"]))
    return sorted(set(codes))


class BithumbWebSocketClient:
    def __init__(self, codes: List[str]):
        self.codes = codes
        self.cache: Dict[str, Dict[str, float]] = {}
        self._usdt_krw: float | None = None
        self._lock = asyncio.Lock()

    async def run(self):
        while True:
            try:
                async with websockets.connect(
                    BITHUMB_WS_URL,
                    ping_interval=50,
                    ping_timeout=20,
                    max_size=None,
                ) as ws:
                    await self._subscribe(ws)
                    async for message in ws:
                        await self._handle_message(message)
            except Exception as exc:  # noqa: BLE001
                print(f"Bithumb WS error: {exc}", file=sys.stderr)
                await asyncio.sleep(3)

    async def _subscribe(self, ws):
        for i in range(0, len(self.codes), CHUNK_SIZE):
            chunk = self.codes[i : i + CHUNK_SIZE]
            payload = [
                {"ticket": str(uuid.uuid4())},
                {"type": "orderbook", "codes": chunk, "is_only_realtime": True},
                {"format": "DEFAULT"},
            ]
            await ws.send(json.dumps(payload))
            await asyncio.sleep(0.2)

    async def _handle_message(self, message):
        if isinstance(message, bytes):
            message = message.decode("utf-8")
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return
        if data.get("type") != "orderbook":
            return
        market = data.get("code")
        units = data.get("orderbook_units")
        if not market or not units:
            return
        ask_price = safe_float(units[0].get("ask_price"))
        if ask_price is None:
            return
        await self._update_cache(market, ask_price)

    async def _update_cache(self, market: str, ask_price: float):
        async with self._lock:
            if market == "KRW-USDT":
                self._usdt_krw = ask_price
            base = market.split("-")[1]
            entry = self.cache.setdefault(base, {})
            entry["krw"] = ask_price
            entry["timestamp"] = time.time()
            if self._usdt_krw:
                entry["usd"] = ask_price / self._usdt_krw

    async def snapshot(self) -> Dict[str, Dict[str, float]]:
        async with self._lock:
            return {base: data.copy() for base, data in self.cache.items()}

    async def usd_rate(self) -> float | None:
        async with self._lock:
            return self._usdt_krw


def render_snapshot(snapshot: Dict[str, Dict[str, float]], top_n: int) -> str:
    lines = []
    for base in sorted(snapshot.keys())[:top_n]:
        entry = snapshot[base]
        usd = entry.get("usd")
        lines.append(
            f"{base:8s} | ask {entry.get('krw', 0):12.2f} KRW | "
            f"{'%.6f' % usd if usd else 'n/a'} USD"
        )
    return "\n".join(lines) if lines else "No data yet."


async def monitor_loop(client: BithumbWebSocketClient, interval: float, top_n: int, run_once: bool):
    await asyncio.sleep(1.0)
    while True:
        snapshot = await client.snapshot()
        rate = await client.usd_rate()
        rate_str = f"{rate:.2f}" if rate else "n/a"
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print(f"\n[{timestamp}] Bithumb KRW spot (USDT/KRW={rate_str}):")
        print(render_snapshot(snapshot, top_n))
        if run_once:
            break
        await asyncio.sleep(max(0.5, interval))


async def main():
    parser = argparse.ArgumentParser(description="Stream Bithumb KRW orderbooks via WebSocket.")
    parser.add_argument("--interval", type=float, default=2.0, help="Seconds between snapshots.")
    parser.add_argument("--top", type=int, default=20, help="Number of markets to print.")
    parser.add_argument("--once", action="store_true", help="Print once and exit.")
    args = parser.parse_args()

    codes = load_bithumb_krw_markets()
    client = BithumbWebSocketClient(codes)
    ws_task = asyncio.create_task(client.run())
    try:
        await monitor_loop(client, args.interval, args.top, args.once)
    finally:
        ws_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await ws_task


if __name__ == "__main__":
    asyncio.run(main())
