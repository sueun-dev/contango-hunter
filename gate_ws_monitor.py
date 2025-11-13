#!/usr/bin/env python3
"""Gate.io perpetual futures contango monitor using WebSocket streams."""

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


GATE_WS_URL = "wss://fx-ws.gateio.ws/v4/ws/usdt"
SUB_CHUNK = 30
GATE_FEE = 0.0005

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


def normalize_base(symbol: str) -> str:
    return symbol.replace("-", "").upper()


def safe_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


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


def load_gate_instruments() -> Dict[str, Dict[str, str]]:
    client = ccxt.gateio({"enableRateLimit": True})
    markets = client.fetch_markets()
    mapping: Dict[str, Dict[str, str]] = {}
    for market in markets:
        if not market.get("swap"):
            continue
        if market.get("settle") != "USDT":
            continue
        contract = market.get("info", {}).get("contract") or market["id"]
        mapping[contract] = {
            "base": normalize_base(market["base"]),
            "symbol": market["symbol"],
        }
    return mapping


class GateWebSocketClient:
    def __init__(self, instruments: Dict[str, Dict[str, str]]):
        self.instruments = instruments
        self.cache: Dict[str, Dict[str, float]] = {}
        self._lock = asyncio.Lock()

    async def run(self):
        contracts = list(self.instruments.keys())
        while True:
            try:
                async with websockets.connect(GATE_WS_URL) as ws:
                    await self._subscribe(ws, contracts)
                    while True:
                        raw = await ws.recv()
                        await self._handle_message(ws, raw)
            except Exception as exc:  # noqa: BLE001
                print(f"Gate WS error: {exc}", file=sys.stderr)
                await asyncio.sleep(3)

    async def _subscribe(self, ws, contracts: List[str]):
        channels = [
            "futures.tickers",
            "futures.order_book",
            "futures.funding_rate",
        ]
        for channel in channels:
            for chunk in chunked(contracts, SUB_CHUNK):
                payload = {
                    "time": int(time.time()),
                    "channel": channel,
                    "event": "subscribe",
                    "payload": chunk if channel != "futures.order_book" else [[c, "20", "0"] for c in chunk],
                }
                if channel == "futures.order_book":
                    payload["payload"] = [[c, "20", "0"] for c in chunk]
                await ws.send(json.dumps(payload))
                await asyncio.sleep(0.2)

    async def _handle_message(self, ws, raw: str):
        try:
            message = json.loads(raw)
        except json.JSONDecodeError:
            return
        if not isinstance(message, dict):
            return
        event = message.get("event")
        channel = message.get("channel")
        if event in {"subscribe", "ping", "pong"}:
            if event == "ping":
                await ws.send(json.dumps({"time": int(time.time()), "channel": "futures.ping"}))
            return
        result = message.get("result")
        if not result:
            return
        if channel == "futures.tickers":
            await self._update_from_ticker(result)
        elif channel == "futures.order_book":
            await self._update_from_order_book(result)
        elif channel == "futures.funding_rate":
            await self._update_from_funding(result)

    async def _update_from_ticker(self, result: Dict[str, str]):
        contract = result.get("contract")
        if not contract:
            return
        bid = safe_float(result.get("best_bid"))
        ask = safe_float(result.get("best_ask"))
        mark = safe_float(result.get("mark_price"))
        async with self._lock:
            record = self.cache.setdefault(contract, {})
            if bid is not None:
                record["bid"] = bid
            if ask is not None:
                record["ask"] = ask
            if mark is not None:
                record["mark"] = mark
            record["timestamp"] = time.time()

    async def _update_from_order_book(self, result: Dict[str, str]):
        contract = result.get("contract")
        if not contract:
            return
        bids = result.get("bids") or []
        asks = result.get("asks") or []
        bid = safe_float(bids[0][0]) if bids else None
        ask = safe_float(asks[0][0]) if asks else None
        async with self._lock:
            record = self.cache.setdefault(contract, {})
            if bid is not None:
                record["bid"] = bid
            if ask is not None:
                record["ask"] = ask
            record["timestamp"] = time.time()

    async def _update_from_funding(self, result: Dict[str, str]):
        contract = result.get("contract")
        if not contract:
            return
        funding = safe_float(result.get("funding_rate"))
        async with self._lock:
            record = self.cache.setdefault(contract, {})
            if funding is not None:
                record["funding_rate"] = funding
            record["timestamp"] = time.time()

    async def snapshot(self) -> Dict[str, Dict[str, float]]:
        async with self._lock:
            return {contract: data.copy() for contract, data in self.cache.items()}


def identify_contango(
    spot_maps: Dict[str, Dict[str, Dict[str, float]]],
    gate_data: Dict[str, Dict[str, float]],
    instruments: Dict[str, Dict[str, str]],
    min_spread_pct: float,
) -> List[Dict[str, float]]:
    rows: List[Dict[str, float]] = []
    for spot_id, payload in spot_maps.items():
        label = payload["label"]
        spot_prices = payload["prices"]
        for contract, data in gate_data.items():
            info = instruments.get(contract)
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
            total_fee_pct = (spot_fee * 2 + GATE_FEE * 2) * 100
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
            f"Short Gate perp (bid) @{row['futures_price']:.8f} USD "
            f"(spread {row['spread']:.6f} USD, {row['pct']:.3f}%, funding {funding_str}, net {row['net_pct']:.3f}%)"
        )
    return "\n".join(lines)


async def monitor_loop(
    gate_client: GateWebSocketClient,
    instruments,
    interval: float,
    min_pct: float,
    top_n: int,
    run_once: bool,
):
    while True:
        spot_maps = await asyncio.to_thread(build_spot_usd_maps)
        gate_snapshot = await gate_client.snapshot()
        rows = identify_contango(spot_maps, gate_snapshot, instruments, min_pct)
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print(f"\n[{timestamp}] Gate contango (min {min_pct:.3f}%):")
        print(render_rows(rows, top_n))
        if run_once:
            break
        await asyncio.sleep(max(0.5, interval))


async def main():
    parser = argparse.ArgumentParser(description="Monitor Gate.io contango via WebSocket.")
    parser.add_argument("--interval", type=float, default=2.0, help="Seconds between spot refreshes.")
    parser.add_argument("--min-pct", type=float, default=0.1, help="Minimum spread percentage.")
    parser.add_argument("--top", type=int, default=10, help="Rows to display.")
    parser.add_argument("--once", action="store_true", help="Run once and exit.")
    args = parser.parse_args()

    instruments = load_gate_instruments()
    gate_client = GateWebSocketClient(instruments)
    ws_task = asyncio.create_task(gate_client.run())
    try:
        await monitor_loop(gate_client, instruments, args.interval, args.min_pct, args.top, args.once)
    finally:
        ws_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await ws_task


if __name__ == "__main__":
    asyncio.run(main())
