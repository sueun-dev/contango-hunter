#!/usr/bin/env python3
"""Hyperliquid perp contango monitor using exchange WebSocket streams."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import sys
import time
from typing import Dict, List, Sequence

import ccxt
import websockets

from price_fetcher import ExchangeConfig, fetch_prices_for_exchange


HL_WS_URL = "wss://api.hyperliquid.xyz/ws"
HL_FEE = 0.00035  # approximate taker fee
SUB_CHUNK = 40

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
        entries: Dict[str, float] = {}
        for symbol, ticker in prices.items():
            if not symbol.endswith("/KRW"):
                continue
            ask = ticker.get("ask") or ticker.get("last")
            if ask is None:
                continue
            base = normalize_base(symbol.split("/")[0])
            entries[base] = float(ask) / usdkrw
        if entries:
            spot_maps[cfg.exchange_id] = {
                "label": cfg.short_label or cfg.label,
                "prices": entries,
            }
    return spot_maps


def load_hl_coins() -> List[str]:
    client = ccxt.hyperliquid({"enableRateLimit": True})
    markets = client.fetch_markets()
    coins: List[str] = []
    for market in markets:
        if not market.get("swap"):
            continue
        coins.append(normalize_base(market["base"]))
    return sorted(set(coins))


class HyperliquidWebSocketClient:
    def __init__(self, coins: List[str]):
        self.coins = coins
        self.cache: Dict[str, Dict[str, float]] = {}
        self._lock = asyncio.Lock()

    async def run(self):
        while True:
            try:
                async with websockets.connect(HL_WS_URL) as ws:
                    await self._subscribe(ws, self.coins)
                    ping_task = asyncio.create_task(self._ping_loop(ws))
                    try:
                        while True:
                            raw = await ws.recv()
                            await self._handle_message(raw)
                    finally:
                        ping_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await ping_task
            except Exception as exc:  # noqa: BLE001
                print(f"Hyperliquid WS error: {exc}", file=sys.stderr)
                await asyncio.sleep(3)

    async def _ping_loop(self, ws):
        while True:
            await asyncio.sleep(30)
            try:
                await ws.send(json.dumps({"method": "ping"}))
            except Exception:  # noqa: BLE001
                break

    async def _subscribe(self, ws, coins: List[str]):
        # subscribe bbo and activeAssetCtx per coin
        for chunk in chunked(coins, SUB_CHUNK):
            for coin in chunk:
                msg = {
                    "method": "subscribe",
                    "subscription": {"type": "bbo", "coin": coin},
                }
                await ws.send(json.dumps(msg))
            await asyncio.sleep(0.2)
        for chunk in chunked(coins, SUB_CHUNK):
            for coin in chunk:
                msg = {
                    "method": "subscribe",
                    "subscription": {"type": "activeAssetCtx", "coin": coin},
                }
                await ws.send(json.dumps(msg))
            await asyncio.sleep(0.2)

    async def _handle_message(self, raw: str):
        try:
            message = json.loads(raw)
        except json.JSONDecodeError:
            return
        if not isinstance(message, dict):
            return
        channel = message.get("channel")
        data = message.get("data")
        if channel == "subscriptionResponse":
            return
        if channel == "pong":
            return
        if channel == "bbo" and isinstance(data, dict):
            await self._update_bbo(data)
        elif channel == "activeAssetCtx" and isinstance(data, dict):
            await self._update_ctx(data)

    async def _update_bbo(self, data: Dict[str, any]):
        coin = normalize_base(data.get("coin", ""))
        if not coin:
            return
        bbo = data.get("bbo", [])
        bid = bbo[0] if bbo else None
        ask = bbo[1] if len(bbo) > 1 else None
        bid_price = safe_float(bid[0]) if bid else None
        ask_price = safe_float(ask[0]) if ask else None
        async with self._lock:
            record = self.cache.setdefault(coin, {})
            if bid_price is not None:
                record["bid"] = bid_price
            if ask_price is not None:
                record["ask"] = ask_price
            record["timestamp"] = time.time()

    async def _update_ctx(self, data: Dict[str, any]):
        coin = normalize_base(data.get("coin", ""))
        ctx = data.get("ctx") or {}
        funding = safe_float(ctx.get("funding"))
        async with self._lock:
            record = self.cache.setdefault(coin, {})
            if funding is not None:
                record["funding_rate"] = funding
            record["timestamp"] = time.time()

    async def snapshot(self) -> Dict[str, Dict[str, float]]:
        async with self._lock:
            return {coin: info.copy() for coin, info in self.cache.items()}


def identify_contango(
    spot_maps: Dict[str, Dict[str, Dict[str, float]]],
    hl_data: Dict[str, Dict[str, float]],
    min_spread_pct: float,
) -> List[Dict[str, float]]:
    rows: List[Dict[str, float]] = []
    for spot_id, payload in spot_maps.items():
        label = payload["label"]
        spot_prices = payload["prices"]
        for coin, data in hl_data.items():
            spot_price = spot_prices.get(coin)
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
            total_fee_pct = (spot_fee * 2 + HL_FEE * 2) * 100
            net_pct = pct - total_fee_pct
            rows.append(
                {
                    "base": coin,
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
            f"Short Hyperliquid perp (bid) @{row['futures_price']:.8f} USD "
            f"(spread {row['spread']:.6f} USD, {row['pct']:.3f}%, funding {funding_str}, net {row['net_pct']:.3f}%)"
        )
    return "\n".join(lines)


async def monitor_loop(
    hl_client: HyperliquidWebSocketClient,
    interval: float,
    min_pct: float,
    top_n: int,
    run_once: bool,
):
    while True:
        spot_maps = await asyncio.to_thread(build_spot_usd_maps)
        hl_snapshot = await hl_client.snapshot()
        rows = identify_contango(spot_maps, hl_snapshot, min_pct)
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print(f"\n[{timestamp}] Hyperliquid contango (min {min_pct:.3f}%):")
        print(render_rows(rows, top_n))
        if run_once:
            break
        await asyncio.sleep(max(0.5, interval))


async def main():
    parser = argparse.ArgumentParser(description="Monitor Hyperliquid contango via WebSocket.")
    parser.add_argument("--interval", type=float, default=2.0, help="Seconds between spot refreshes.")
    parser.add_argument("--min-pct", type=float, default=0.1, help="Minimum spread percentage.")
    parser.add_argument("--top", type=int, default=10, help="Rows to display.")
    parser.add_argument("--once", action="store_true", help="Run once and exit.")
    args = parser.parse_args()

    coins = load_hl_coins()
    hl_client = HyperliquidWebSocketClient(coins)
    ws_task = asyncio.create_task(hl_client.run())
    try:
        await monitor_loop(hl_client, args.interval, args.min_pct, args.top, args.once)
    finally:
        ws_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await ws_task


if __name__ == "__main__":
    asyncio.run(main())
