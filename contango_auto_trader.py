#!/usr/bin/env python3
"""Automated contango hedger using the REST-based monitor outputs.

The script continuously:
 1. Fetches every spot/futures price via contango_monitor helpers.
 2. Picks the best opportunity (highest spread, funding >= 0).
 3. Adds/removes 50 USD notional tranches per trade leg while obeying:
      - Entry threshold >= 1% to add exposure.
      - Exit threshold <= 0.2% to remove exposure.
      - Max exposure per leg = 2,000 USD.
 4. Places futures first (short), then spot (KRW buy) using ccxt.

Set LIVE trading only after configuring API credentials as environment variables:
  <EXCHANGE>_API_KEY, <EXCHANGE>_API_SECRET, [<EXCHANGE>_API_PASSWORD]

The script defaults to dry-run (no orders).
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Dict, Optional, List
import json

import ccxt

import contango_monitor as monitor

TRANCHE_USD = 50.0
MAX_PER_LEG_USD = 2000.0
LOG_FILE = "trade_cycles.jsonl"


@dataclass
class HedgePosition:
    base: str
    spot_exchange: str
    futures_exchange: str
    futures_symbol: str
    spot_symbol: str
    notional_usd: float = 0.0
    tranches: List[dict] = field(default_factory=list)

    @property
    def remaining_capacity(self) -> float:
        return max(0.0, MAX_PER_LEG_USD - self.notional_usd)

    def record_entry(self, usd: float, opportunity: dict):
        usd_added = min(usd, self.remaining_capacity)
        if usd_added <= 0:
            return 0.0
        self.tranches.append(
            {
                "usd": usd_added,
                "futures_price": opportunity["futures_price"],
                "spot_price": opportunity["spot_price"],
                "timestamp": time.time(),
            }
        )
        self.notional_usd += usd_added
        return usd_added

    def record_exit(self, usd: float, exit_data: dict):
        usd_target = min(usd, self.notional_usd)
        if usd_target <= 0:
            return 0.0, 0.0, []
        usd_remaining = usd_target
        pnl_total = 0.0
        details = []
        exit_fut = exit_data["futures_price"]
        exit_spot = exit_data["spot_price"]
        while usd_remaining > 1e-9 and self.tranches:
            tranche = self.tranches[0]
            available = tranche["usd"]
            portion = min(usd_remaining, available)
            qty = portion / tranche["futures_price"]
            pnl = qty * (
                (tranche["futures_price"] - exit_fut)
                + (exit_spot - tranche["spot_price"])
            )
            details.append(
                {
                    "portion_usd": portion,
                    "qty": qty,
                    "entry_futures_price": tranche["futures_price"],
                    "entry_spot_price": tranche["spot_price"],
                    "entry_timestamp": tranche["timestamp"],
                    "exit_futures_price": exit_fut,
                    "exit_spot_price": exit_spot,
                    "exit_timestamp": time.time(),
                    "pnl_usd": pnl,
                }
            )
            pnl_total += pnl
            tranche["usd"] -= portion
            usd_remaining -= portion
            if tranche["usd"] <= 1e-9:
                self.tranches.pop(0)
        actual_closed = usd_target - usd_remaining
        self.notional_usd = max(0.0, self.notional_usd - actual_closed)
        return actual_closed, pnl_total, details


def fetch_rows(min_pct: float) -> list[dict]:
    with ThreadPoolExecutor(max_workers=len(monitor.SPOT_CONFIGS) + len(monitor.FUTURES_CONFIGS)) as executor:
        spot_maps = monitor.build_spot_usd_maps(executor)
        futures_maps = monitor.build_futures_maps(executor)
        rows = monitor.identify_contango(
            spot_maps,
            futures_maps,
            min_spread_pct=min_pct,
            require_nonnegative_funding=False,
        )
    return rows


def pick_best(rows: list[dict], entry_threshold: float) -> Optional[dict]:
    for row in rows:
        if row["pct"] >= entry_threshold and row.get("funding_rate") is not None and row["funding_rate"] >= 0:
            return row
    return None


def create_client(exchange_id: str, is_futures: bool):
    class_name = getattr(ccxt, exchange_id)
    params = {"enableRateLimit": True}
    if is_futures:
        params["options"] = {"defaultType": "swap"}
    api_key_var = f"{exchange_id.upper()}_API_KEY"
    secret_var = f"{exchange_id.upper()}_API_SECRET"
    password_var = f"{exchange_id.upper()}_API_PASSWORD"
    api_key = os.getenv(api_key_var)
    api_secret = os.getenv(secret_var)
    if not api_key or not api_secret:
        raise RuntimeError(f"Missing credentials for {exchange_id}. Set {api_key_var}/{secret_var}.")
    params["apiKey"] = api_key
    params["secret"] = api_secret
    api_password = os.getenv(password_var)
    if api_password:
        params["password"] = api_password
    return class_name(params)


def place_market_order(client, symbol: str, side: str, amount: float):
    return client.create_order(symbol=symbol, type="market", side=side, amount=amount)


def ensure_position(opportunity: dict, positions: Dict[str, HedgePosition]) -> HedgePosition:
    key = f"{opportunity['spot_exchange']}::{opportunity['exchange']}::{opportunity['base']}"
    if key not in positions:
        positions[key] = HedgePosition(
            base=opportunity["base"],
            spot_exchange=opportunity["spot_exchange"],
            futures_exchange=opportunity["exchange"],
            futures_symbol=opportunity.get("futures_symbol", f"{opportunity['base']}/USDT:USDT"),
            spot_symbol=f"{opportunity['base']}/KRW",
            notional_usd=0.0,
        )
    return positions[key]


def hedged_amount_qty(opportunity: dict, usd_amount: float) -> float:
    futures_price = opportunity["futures_price"]
    if futures_price <= 0:
        raise ValueError("Invalid futures price for quantity calculation.")
    return usd_amount / futures_price


def execute_tranche(
    position: HedgePosition,
    opportunity: dict,
    usd_amount: float,
    action: str,
    dry_run: bool,
):
    qty = hedged_amount_qty(opportunity, usd_amount)
    futures_client = create_client(position.futures_exchange, is_futures=True) if not dry_run else None
    spot_client = create_client(position.spot_exchange, is_futures=False) if not dry_run else None

    if action == "open":
        print(
            f"Opening {usd_amount:.2f} USD tranche ({qty:.6f} {position.base}): "
            f"short {position.futures_exchange} {position.futures_symbol} / "
            f"buy {position.spot_exchange} {position.spot_symbol}"
        )
        if dry_run:
            return {"qty": qty, "usd_amount": usd_amount, "mode": "DRY_RUN"}
        fut_order = place_market_order(futures_client, position.futures_symbol, "sell", qty)
        spot_order = place_market_order(spot_client, position.spot_symbol, "buy", qty)
        return {"qty": qty, "usd_amount": usd_amount, "futures_order": fut_order, "spot_order": spot_order}
    elif action == "close":
        print(
            f"Closing {usd_amount:.2f} USD tranche ({qty:.6f} {position.base}): "
            f"buy to cover {position.futures_exchange} / "
            f"sell spot at {position.spot_exchange}"
        )
        if dry_run:
            return {"qty": qty, "usd_amount": usd_amount, "mode": "DRY_RUN"}
        fut_order = place_market_order(futures_client, position.futures_symbol, "buy", qty)
        spot_order = place_market_order(spot_client, position.spot_symbol, "sell", qty)
        return {"qty": qty, "usd_amount": usd_amount, "futures_order": fut_order, "spot_order": spot_order}
    else:
        raise ValueError("Invalid action.")


def log_event(event: str, payload: dict):
    record = {"event": event, "timestamp": time.time()}
    record.update(payload)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
    except OSError as exc:
        print(f"Warning: failed to write log {exc}")


def auto_trade_loop(args):
    positions: Dict[str, HedgePosition] = {}
    dry_run = not args.live
    print(f"Starting auto-trader | Entry>={args.entry_threshold}% | Exit<={args.exit_threshold}% "
          f"| tranche={TRANCHE_USD} USD | max per leg={MAX_PER_LEG_USD} USD | mode={'LIVE' if args.live else 'DRY-RUN'}")
    while True:
        try:
            rows = fetch_rows(min_pct=0.0)
        except monitor.ContangoError as exc:
            print(f"Data fetch error: {exc}")
            time.sleep(args.interval)
            continue

        best = pick_best(rows, entry_threshold=args.entry_threshold)
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        if best:
            print(
                f"\n[{timestamp}] Top spread {best['pct']:.3f}% | net {best['net_pct']:.3f}% "
                f"| funding {best['funding_rate'] * 100:.4f}% | "
                f"{best['spot_label']} vs {best['exchange']} ({best['base']})"
            )
        else:
            print(f"\n[{timestamp}] No eligible entries >= {args.entry_threshold:.2f}%")

        # Entry logic
        if best:
            position = ensure_position(best, positions)
            usd_to_add = min(TRANCHE_USD, position.remaining_capacity)
            if usd_to_add > 0 and best["pct"] >= args.entry_threshold:
                execution = execute_tranche(position, best, usd_to_add, "open", dry_run)
                recorded = position.record_entry(usd_to_add, best)
                log_event(
                    "entry",
                    {
                        "base": best["base"],
                        "spot_exchange": best["spot_exchange"],
                        "futures_exchange": best["exchange"],
                        "usd": recorded,
                        "spread_pct": best["pct"],
                        "net_pct": best["net_pct"],
                        "funding_rate": best["funding_rate"],
                        "execution": execution,
                    },
                )

        # Exit logic for every open position
        for key, pos in list(positions.items()):
            # Find current spread info for same base/exchange if available
            current = next((row for row in rows if row["base"] == pos.base
                            and row["spot_exchange"] == pos.spot_exchange
                            and row["exchange"] == pos.futures_exchange), None)
            if not current:
                continue
            if pos.notional_usd <= 0:
                the_pos = positions.pop(key)
                continue
            if current["pct"] <= args.exit_threshold and pos.notional_usd > 0:
                usd_request = min(TRANCHE_USD, pos.notional_usd)
                closed_usd, pnl_usd, details = pos.record_exit(usd_request, current)
                if closed_usd > 0:
                    execution = execute_tranche(pos, current, closed_usd, "close", dry_run)
                    log_event(
                        "exit",
                        {
                            "base": pos.base,
                            "spot_exchange": pos.spot_exchange,
                            "futures_exchange": pos.futures_exchange,
                            "usd": closed_usd,
                            "spread_pct": current["pct"],
                            "net_pct": current["net_pct"],
                            "funding_rate": current["funding_rate"],
                            "pnl_usd": pnl_usd,
                            "portions": details,
                            "execution": execution,
                        },
                    )
                if pos.notional_usd <= 1e-9:
                    positions.pop(key, None)

        time.sleep(args.interval)


def main():
    parser = argparse.ArgumentParser(description="Automated contango hedger.")
    parser.add_argument("--entry-threshold", type=float, default=1.0, help="Minimum spread %% to add exposure.")
    parser.add_argument("--exit-threshold", type=float, default=0.2, help="Spread %% to remove exposure.")
    parser.add_argument("--interval", type=float, default=10.0, help="Seconds between re-evaluations.")
    parser.add_argument("--live", action="store_true", help="Send live orders. Default is dry-run.")
    args = parser.parse_args()

    try:
        auto_trade_loop(args)
    except KeyboardInterrupt:
        print("\nStopped by user.")


if __name__ == "__main__":
    main()
