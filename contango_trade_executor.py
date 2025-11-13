#!/usr/bin/env python3
"""Identify tradable contango opportunities and outline/execute the hedge legs."""

from __future__ import annotations

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Tuple

import ccxt

import contango_monitor as monitor


def fetch_opportunities(min_pct: float) -> List[Dict[str, float]]:
    with ThreadPoolExecutor(max_workers=len(monitor.SPOT_CONFIGS) + len(monitor.FUTURES_CONFIGS)) as executor:
        spot_maps = monitor.build_spot_usd_maps(executor)
        futures_maps = monitor.build_futures_maps(executor)
        rows = monitor.identify_contango(spot_maps, futures_maps, min_spread_pct=0.0)
    return rows


def select_opportunity(rows: List[Dict[str, float]], min_pct: float) -> Dict[str, float] | None:
    for row in rows:
        if row["pct"] >= min_pct:
            return row
    return None


def compute_quantities(opportunity: Dict[str, float], notional_usd: float) -> Tuple[float, float]:
    futures_price = opportunity["futures_price"]
    if futures_price <= 0:
        raise ValueError("Invalid futures price.")
    base_qty = notional_usd / futures_price
    return base_qty, futures_price


def create_exchange_client(exchange_id: str, is_futures: bool):
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
        raise RuntimeError(
            f"Missing credentials for {exchange_id}. "
            f"Set {api_key_var} and {secret_var} environment variables."
        )
    params["apiKey"] = api_key
    params["secret"] = api_secret
    api_password = os.getenv(password_var)
    if api_password:
        params["password"] = api_password
    return class_name(params)


def execute_live_trade(
    opportunity: Dict[str, float],
    base_qty: float,
    dry_run: bool,
) -> None:
    futures_id = opportunity["exchange"]
    spot_id = opportunity["spot_exchange"]
    futures_symbol = opportunity.get("futures_symbol") or f"{opportunity['base']}/USDT:USDT"
    spot_symbol = f"{opportunity['base']}/KRW"

    print(f"\nPlanned Orders ({'DRY-RUN' if dry_run else 'LIVE'}):")
    print(
        f" 1) Short {base_qty:.6f} {opportunity['base']} on {futures_id} ({futures_symbol}) at market.\n"
        f" 2) Buy  {base_qty:.6f} {opportunity['base']} on {spot_id} ({spot_symbol}) at market."
    )

    if dry_run:
        print("Dry-run mode: no orders were sent. Use --live to enable trading once credentials are configured.")
        return

    futures_client = create_exchange_client(futures_id, is_futures=True)
    spot_client = create_exchange_client(spot_id, is_futures=False)

    try:
        futures_order = futures_client.create_order(
            symbol=futures_symbol,
            type="market",
            side="sell",
            amount=base_qty,
        )
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Failed to place futures order: {exc}") from exc

    try:
        spot_order = spot_client.create_order(
            symbol=spot_symbol,
            type="market",
            side="buy",
            amount=base_qty,
        )
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Failed to place spot order: {exc}") from exc

    print("Futures order response:", futures_order)
    print("Spot order response:", spot_order)
    print("Hedge opened. Monitor spread and unwind when thresholds are met.")


def main():
    parser = argparse.ArgumentParser(description="Select and (optionally) execute best contango hedge.")
    parser.add_argument("--entry-threshold", type=float, default=1.0, help="Minimum spread %% to open hedge.")
    parser.add_argument("--exit-threshold", type=float, default=0.2, help="Target spread %% to close hedge (informational).")
    parser.add_argument("--notional-usd", type=float, default=1000.0, help="USD notional size for the hedge.")
    parser.add_argument("--once", action="store_true", help="Evaluate once (default behavior).")
    parser.add_argument("--live", action="store_true", help="Send market orders (requires API credentials).")
    parser.add_argument("--min-pct-scan", type=float, default=0.2, help="Minimum spread %% to keep in the scan list.")
    args = parser.parse_args()

    try:
        rows = fetch_opportunities(min_pct=args.min_pct_scan)
    except monitor.ContangoError as exc:
        print(f"Failed to gather market data: {exc}")
        sys.exit(1)

    opportunity = select_opportunity(rows, min_pct=args.entry_threshold)
    if not opportunity:
        print(f"No opportunities meet the entry threshold of {args.entry_threshold:.3f}%.")
        sys.exit(0)

    base_qty, futures_price = compute_quantities(opportunity, args.notional_usd)
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    print(f"\n[{timestamp}] Selected opportunity:")
    print(
        f"  Spot: {opportunity['spot_label']} ({opportunity['spot_exchange']}) "
        f"@ {opportunity['spot_price']:.6f} USD"
    )
    print(
        f"  Futures: {opportunity['exchange']} "
        f"{opportunity.get('futures_symbol', opportunity['base'] + '/USDT:USDT')} "
        f"@ {opportunity['futures_price']:.6f} USD"
    )
    print(
        f"  Spread: {opportunity['spread']:.6f} USD ({opportunity['pct']:.3f}%) "
        f"| Net after fees: {opportunity['net_pct']:.3f}% "
        f"| Funding: {opportunity['funding_rate'] * 100:.4f}%"
    )
    print(
        f"\nTrade plan:\n"
        f"  - Short {base_qty:.6f} {opportunity['base']} on {opportunity['exchange']} first.\n"
        f"  - Immediately buy the same {base_qty:.6f} {opportunity['base']} on {opportunity['spot_exchange']}.\n"
        f"  - Monitor spread and close both legs when spread falls below {args.exit_threshold:.3f}%.\n"
    )

    execute_live_trade(opportunity, base_qty, dry_run=not args.live)


if __name__ == "__main__":
    main()
