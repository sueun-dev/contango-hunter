#!/usr/bin/env python3
"""Unified WebSocket contango engine combining spot streams and futures streams."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Sequence

from upbit_ws_spot import UpbitWebSocketClient, load_upbit_krw_markets
from bithumb_ws_spot import BithumbWebSocketClient, load_bithumb_krw_markets

from okx_ws_monitor import (
    OKXWebSocketClient,
    load_okx_instruments,
    OKX_FEE,
)
from gate_ws_monitor import (
    GateWebSocketClient,
    load_gate_instruments,
    GATE_FEE,
)
from hyperliquid_ws_monitor import (
    HyperliquidWebSocketClient,
    load_hl_coins,
    HL_FEE,
)


SPOT_FEES = {
    "upbit": 0.0005,
    "bithumb": 0.0004,
}

FUTURES_LABELS = {
    "okx": "OKX",
    "gate": "Gate",
    "hyper": "Hyperliquid",
}


@dataclass
class FuturesStream:
    name: str
    fee: float
    snapshot: Dict[str, Dict[str, float]]
    resolver: Callable[[str], Optional[str]]


def filter_spot_prices(snapshot: Dict[str, Dict[str, float]]) -> Dict[str, float]:
    prices: Dict[str, float] = {}
    for base, entry in snapshot.items():
        usd = entry.get("usd")
        if usd:
            prices[base.upper()] = usd
    return prices


async def build_spot_sources(
    up_client: Optional[UpbitWebSocketClient],
    bit_client: Optional[BithumbWebSocketClient],
) -> Dict[str, Dict[str, Dict[str, float]]]:
    sources: Dict[str, Dict[str, Dict[str, float]]] = {}
    if up_client:
        up_snapshot = await up_client.snapshot()
        up_prices = filter_spot_prices(up_snapshot)
        if up_prices:
            sources["upbit"] = {"label": "Up", "prices": up_prices}
    if bit_client:
        bit_snapshot = await bit_client.snapshot()
        bit_prices = filter_spot_prices(bit_snapshot)
        if bit_prices:
            sources["bithumb"] = {"label": "Bit", "prices": bit_prices}
    return sources


def compute_contango(
    spot_sources: Dict[str, Dict[str, Dict[str, float]]],
    futures_streams: Sequence[FuturesStream],
    min_spread_pct: float,
) -> List[Dict[str, float]]:
    rows: List[Dict[str, float]] = []
    for spot_id, payload in spot_sources.items():
        label = payload["label"]
        spot_prices = payload["prices"]
        spot_fee = SPOT_FEES.get(spot_id, 0.0)
        for stream in futures_streams:
            for key, data in stream.snapshot.items():
                base = stream.resolver(key)
                if not base:
                    continue
                spot_price = spot_prices.get(base.upper())
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
                total_fee_pct = (spot_fee * 2 + stream.fee * 2) * 100
                net_pct = pct - total_fee_pct
                rows.append(
                    {
                        "base": base.upper(),
                        "spot_label": label,
                        "futures": stream.name,
                        "spot_price": spot_price,
                        "futures_price": bid,
                        "spread": spread,
                        "pct": pct,
                        "net_pct": net_pct,
                        "funding_rate": funding,
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
            f"{row['base']:8s} | Long {row['spot_label']} (ask) @{row['spot_price']:.6f} USD | "
            f"Short {row['futures']} perp (bid) @{row['futures_price']:.6f} USD "
            f"(spread {row['spread']:.6f} USD, {row['pct']:.3f}%, net {row['net_pct']:.3f}%, funding {funding_str})"
        )
    return "\n".join(lines)


async def contango_loop(
    spot_clients,
    futures_clients,
    futures_meta,
    interval: float,
    min_pct: float,
    top_n: int,
    run_once: bool,
):
    await asyncio.sleep(1.0)
    while True:
        spot_sources = await build_spot_sources(*spot_clients)
        futures_streams: List[FuturesStream] = []
        for key, meta in futures_meta.items():
            client = futures_clients.get(key)
            if not client:
                continue
            snapshot = await client.snapshot()
            if not snapshot:
                continue
            futures_streams.append(
                FuturesStream(
                    name=meta["label"],
                    fee=meta["fee"],
                    snapshot=snapshot,
                    resolver=meta["resolver"],
                )
            )
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        if not spot_sources:
            print(f"\n[{timestamp}] Waiting for spot data...")
        elif not futures_streams:
            print(f"\n[{timestamp}] Waiting for futures data...")
        else:
            rows = compute_contango(spot_sources, futures_streams, min_pct)
            print(f"\n[{timestamp}] Unified WS contango (min {min_pct:.3f}%):")
            print(render_rows(rows, top_n))
        if run_once:
            break
        await asyncio.sleep(max(0.5, interval))


async def main():
    parser = argparse.ArgumentParser(description="Unified WebSocket contango monitor.")
    parser.add_argument("--interval", type=float, default=2.0, help="Seconds between evaluations.")
    parser.add_argument("--min-pct", type=float, default=0.2, help="Minimum futures premium percentage.")
    parser.add_argument("--top", type=int, default=10, help="Rows to display per refresh.")
    parser.add_argument(
        "--futures",
        type=str,
        default="okx,gate,hyper",
        help="Comma-separated futures exchanges to include (okx, gate, hyper).",
    )
    parser.add_argument("--no-upbit", action="store_true", help="Disable Upbit spot source.")
    parser.add_argument("--no-bithumb", action="store_true", help="Disable Bithumb spot source.")
    parser.add_argument("--once", action="store_true", help="Run a single evaluation and exit.")
    args = parser.parse_args()

    futures_selection = {name.strip() for name in args.futures.split(",") if name.strip()}

    up_client = None
    if not args.no_upbit:
        up_codes = load_upbit_krw_markets()
        up_client = UpbitWebSocketClient(up_codes)

    bit_client = None
    if not args.no_bithumb:
        bit_codes = load_bithumb_krw_markets()
        bit_client = BithumbWebSocketClient(bit_codes)

    okx_client = gate_client = hyper_client = None
    futures_meta = {}

    if "okx" in futures_selection:
        okx_inst = load_okx_instruments()
        okx_client = OKXWebSocketClient(okx_inst)
        futures_meta["okx"] = {
            "label": FUTURES_LABELS["okx"],
            "fee": OKX_FEE,
            "resolver": lambda inst_id, m=okx_inst: m.get(inst_id, {}).get("base"),
        }

    if "gate" in futures_selection:
        gate_inst = load_gate_instruments()
        gate_client = GateWebSocketClient(gate_inst)
        futures_meta["gate"] = {
            "label": FUTURES_LABELS["gate"],
            "fee": GATE_FEE,
            "resolver": lambda contract, m=gate_inst: m.get(contract, {}).get("base"),
        }

    if "hyper" in futures_selection:
        hl_coins = load_hl_coins()
        hyper_client = HyperliquidWebSocketClient(hl_coins)
        futures_meta["hyper"] = {
            "label": FUTURES_LABELS["hyper"],
            "fee": HL_FEE,
            "resolver": lambda coin: coin,
        }

    tasks = []
    if up_client:
        tasks.append(asyncio.create_task(up_client.run()))
    if bit_client:
        tasks.append(asyncio.create_task(bit_client.run()))
    if okx_client:
        tasks.append(asyncio.create_task(okx_client.run()))
    if gate_client:
        tasks.append(asyncio.create_task(gate_client.run()))
    if hyper_client:
        tasks.append(asyncio.create_task(hyper_client.run()))

    try:
        await contango_loop(
            (up_client, bit_client),
            {"okx": okx_client, "gate": gate_client, "hyper": hyper_client},
            futures_meta,
            args.interval,
            args.min_pct,
            args.top,
            args.once,
        )
    finally:
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task


if __name__ == "__main__":
    asyncio.run(main())
