#!/usr/bin/env python3
"""Monitor contango opportunities using OKX WebSocket data for rapid futures updates."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
import contextlib
from typing import Dict, List, Sequence

import ccxt
import websockets

from price_fetcher import ExchangeConfig, fetch_prices_for_exchange


OKX_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
SUB_CHUNK = 20  # max args per subscribe request
OKX_FEE = 0.0005  # 0.05% taker/maker approximation

SPOT_CONFIGS: List[ExchangeConfig] = [
    ExchangeConfig(
        exchange_id="upbit",
        label="Upbit KRW Spot",
        market_type="spot",
        quote="KRW",
        short_label="Up",
    ),
    ExchangeConfig(
        exchange_id="bithumb",
        label="Bithumb KRW Spot",
        market_type="spot",
        quote="KRW",
        short_label="Bit",
    ),
]

SPOT_FEES = {
    "upbit": 0.0005,
    "bithumb": 0.0004,
}


def chunked(seq: Sequence[str], size: int):
    for i in range(0, len(seq), size):
        yield list(seq[i : i + size])


def safe_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_base(symbol: str) -> str:
    return symbol.replace("-", "").upper()


class USDKRWCache:
    def __init__(self, ttl_seconds: float = 30.0):
        self.ttl = ttl_seconds
        self._rates: Dict[str, Dict[str, float]] = {}

    def get_rate(self, exchange_id: str, prices: Dict[str, Dict[str, float]]) -> float:
        info = prices.get("USDT/KRW")
        if not info:
            raise RuntimeError("USDT/KRW ticker missing.")
        raw_value = info.get("ask") or info.get("last")
        if raw_value is None:
            raise RuntimeError("USDT/KRW ticker lacks price data.")
        rate = float(raw_value)
        now = time.time()
        record = self._rates.get(exchange_id)
        if record and record["raw"] == rate and now - record["timestamp"] < self.ttl:
            return record["rate"]
        self._rates[exchange_id] = {"rate": rate, "timestamp": now, "raw": rate}
        return rate


usdkrw_cache = USDKRWCache()


def build_spot_usd_maps() -> Dict[str, Dict[str, Dict[str, float]]]:
    spot_maps: Dict[str, Dict[str, Dict[str, float]]] = {}
    for cfg in SPOT_CONFIGS:
        payload = fetch_prices_for_exchange(cfg)
        if "error" in payload:
            print(
                f"Warning: {cfg.exchange_id} unavailable ({payload['error']})",
                file=sys.stderr,
            )
            continue
        prices = payload["prices"]
        try:
            usdkrw = usdkrw_cache.get_rate(cfg.exchange_id, prices)
        except RuntimeError as exc:
            print(f"Warning: {cfg.exchange_id} USDT/KRW issue ({exc})", file=sys.stderr)
            continue
        spot_usd: Dict[str, float] = {}
        for symbol, ticker in prices.items():
            if not symbol.endswith("/KRW"):
                continue
            ask = ticker.get("ask") or ticker.get("last")
            if ask is None:
                continue
            base = normalize_base(symbol.split("/")[0])
            spot_usd[base] = float(ask) / usdkrw
        if spot_usd:
            spot_maps[cfg.exchange_id] = {
                "label": cfg.short_label or cfg.label,
                "prices": spot_usd,
            }
    return spot_maps


def load_okx_instruments() -> Dict[str, Dict[str, str]]:
    client = ccxt.okx({"enableRateLimit": True})
    markets = client.fetch_markets()
    mapping: Dict[str, Dict[str, str]] = {}
    for market in markets:
        if not market.get("swap"):
            continue
        if market.get("settle") not in ("USDT", None):
            continue
        inst_id = market["id"]
        mapping[inst_id] = {
            "base": normalize_base(market["base"]),
            "symbol": market["symbol"],
        }
    return mapping


class OKXWebSocketClient:
    def __init__(self, instruments: Dict[str, Dict[str, str]]):
        self.instruments = instruments
        self.cache: Dict[str, Dict[str, float]] = {}
        self._lock = asyncio.Lock()

    async def run(self):
        inst_ids = list(self.instruments.keys())
        while True:
            try:
                async with websockets.connect(OKX_WS_URL) as ws:
                    await self._subscribe(ws, inst_ids)
                    while True:
                        raw = await ws.recv()
                        await self._handle_message(ws, raw)
            except Exception as exc:  # noqa: BLE001
                print(f"OKX WS error: {exc}", file=sys.stderr)
                await asyncio.sleep(3)

    async def _subscribe(self, ws, inst_ids: List[str]):
        channels = ("tickers", "books5", "funding-rate")
        for channel in channels:
            for chunk in chunked(inst_ids, SUB_CHUNK):
                payload = {
                    "op": "subscribe",
                    "args": [{"channel": channel, "instId": inst_id} for inst_id in chunk],
                }
                await ws.send(json.dumps(payload))
                await asyncio.sleep(0.2)

    async def _handle_message(self, ws, raw: str):
        if raw == "pong":
            return
        if raw == "ping":
            await ws.send("pong")
            return
        try:
            message = json.loads(raw)
        except json.JSONDecodeError:
            return
        if isinstance(message, dict):
            if message.get("event") in {"subscribe", "error"}:
                if message.get("event") == "error":
                    print(f"OKX WS error: {message}", file=sys.stderr)
                return
            if message.get("op") == "ping":
                await ws.send(json.dumps({"op": "pong"}))
                return
            arg = message.get("arg")
            data = message.get("data")
            if not arg or not data:
                return
            channel = arg.get("channel")
            inst_id = arg.get("instId")
            if channel == "books5":
                for entry in data:
                    await self._update_from_books(entry)
            elif channel == "tickers":
                for entry in data:
                    await self._update_from_ticker(entry)
            elif channel == "funding-rate":
                for entry in data:
                    await self._update_funding(entry)

    async def _update_from_books(self, entry: Dict[str, str]):
        inst_id = entry.get("instId")
        if not inst_id:
            return
        bids = entry.get("bids") or []
        asks = entry.get("asks") or []
        bid = safe_float(bids[0][0]) if bids else None
        ask = safe_float(asks[0][0]) if asks else None
        async with self._lock:
            record = self.cache.setdefault(inst_id, {})
            if bid is not None:
                record["bid"] = bid
            if ask is not None:
                record["ask"] = ask
            record["timestamp"] = time.time()

    async def _update_from_ticker(self, entry: Dict[str, str]):
        inst_id = entry.get("instId")
        if not inst_id:
            return
        bid = safe_float(entry.get("bidPx"))
        ask = safe_float(entry.get("askPx"))
        mark = safe_float(entry.get("markPx"))
        async with self._lock:
            record = self.cache.setdefault(inst_id, {})
            if bid is not None:
                record["bid"] = bid
            if ask is not None:
                record["ask"] = ask
            if mark is not None:
                record["mark"] = mark
            record["timestamp"] = time.time()

    async def _update_funding(self, entry: Dict[str, str]):
        inst_id = entry.get("instId")
        if not inst_id:
            return
        funding = safe_float(entry.get("fundingRate"))
        async with self._lock:
            record = self.cache.setdefault(inst_id, {})
            if funding is not None:
                record["funding_rate"] = funding
            record["timestamp"] = time.time()

    async def snapshot(self) -> Dict[str, Dict[str, float]]:
        async with self._lock:
            return {inst: data.copy() for inst, data in self.cache.items()}


def identify_contango(
    spot_maps: Dict[str, Dict[str, Dict[str, float]]],
    okx_data: Dict[str, Dict[str, float]],
    instruments: Dict[str, Dict[str, str]],
    min_spread_pct: float,
) -> List[Dict[str, float]]:
    rows: List[Dict[str, float]] = []
    for spot_id, payload in spot_maps.items():
        label = payload["label"]
        spot_prices = payload["prices"]
        for inst_id, data in okx_data.items():
            info = instruments.get(inst_id)
            if not info:
                continue
            base = info["base"]
            spot_price = spot_prices.get(base)
            if not spot_price:
                continue
            bid = data.get("bid")
            if bid is None:
                continue
            spread = bid - spot_price
            if spread <= 0:
                continue
            pct = (spread / spot_price) * 100
            if pct < min_spread_pct:
                continue
            funding = data.get("funding_rate")
            spot_fee = SPOT_FEES.get(spot_id, 0.0)
            total_fee_pct = (spot_fee * 2 + OKX_FEE * 2) * 100
            net_pct = pct - total_fee_pct
            rows.append(
                {
                    "base": base,
                    "spot_label": label,
                    "spot_price": spot_price,
                    "futures_price": bid,
                    "spread": spread,
                    "pct": pct,
                    "funding_rate": funding,
                    "net_pct": net_pct,
                }
            )
    rows.sort(key=lambda row: row["pct"], reverse=True)
    return rows


def render_rows(rows: List[Dict[str, float]], top_n: int) -> str:
    if not rows:
        return "No contango opportunities available."
    lines = []
    for row in rows[:top_n]:
        funding = row.get("funding_rate")
        funding_str = "n/a" if funding is None else f"{funding * 100:.4f}%"
        lines.append(
            f"{row['base']:8s} | Long {row['spot_label']} (ask) @{row['spot_price']:.8f} USD | "
            f"Short OKX perp (bid) @{row['futures_price']:.8f} USD "
            f"(spread {row['spread']:.6f} USD, {row['pct']:.3f}%, funding {funding_str}, net {row['net_pct']:.3f}%)"
        )
    return "\n".join(lines)


async def monitor_loop(
    okx_client: OKXWebSocketClient,
    instruments,
    interval: float,
    min_pct: float,
    top_n: int,
    run_once: bool,
):
    while True:
        spot_maps = await asyncio.to_thread(build_spot_usd_maps)
        okx_snapshot = await okx_client.snapshot()
        rows = identify_contango(spot_maps, okx_snapshot, instruments, min_pct)
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print(f"\n[{timestamp}] OKX contango (min {min_pct:.3f}%):")
        print(render_rows(rows, top_n))
        if run_once:
            break
        await asyncio.sleep(max(0.5, interval))


async def main():
    parser = argparse.ArgumentParser(description="Monitor OKX contango via WebSocket.")
    parser.add_argument("--interval", type=float, default=2.0, help="Seconds between spot refreshes.")
    parser.add_argument("--min-pct", type=float, default=0.1, help="Minimum spread percentage.")
    parser.add_argument("--top", type=int, default=10, help="Rows to display.")
    parser.add_argument("--once", action="store_true", help="Run a single evaluation and exit.")
    args = parser.parse_args()

    instruments = load_okx_instruments()
    okx_client = OKXWebSocketClient(instruments)
    ws_task = asyncio.create_task(okx_client.run())
    try:
        await monitor_loop(
            okx_client,
            instruments,
            args.interval,
            args.min_pct,
            args.top,
            args.once,
        )
    finally:
        ws_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await ws_task


if __name__ == "__main__":
    asyncio.run(main())
