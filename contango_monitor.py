#!/usr/bin/env python3
"""Monitor contango opportunities between Upbit spot (KRW) and futures venues."""

from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List

import ccxt

from price_fetcher import ExchangeConfig, fetch_prices_for_exchange


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

FUTURES_CONFIGS: List[ExchangeConfig] = [
    ExchangeConfig(
        exchange_id="hyperliquid",
        label="Hyperliquid Perpetuals",
        market_type="swap",
    ),
    ExchangeConfig(
        exchange_id="gateio",
        label="Gate.io Perpetuals",
        market_type="swap",
        options={"defaultType": "swap"},
    ),
    ExchangeConfig(
        exchange_id="okx",
        label="OKX Perpetuals",
        market_type="swap",
        options={"defaultType": "swap"},
    ),
    # Bybit temporarily disabled due to regional 403 blocks.
    # ExchangeConfig(
    #     exchange_id="bybit",
    #     label="Bybit Perpetuals",
    #     market_type="swap",
    #     options={"defaultType": "swap"},
    # ),
]

SPOT_FEES = {
    "upbit": 0.0005,  # 0.05%
    "bithumb": 0.0004,  # 0.04%
}

FUTURES_FEES = {
    "gateio": 0.0005,  # 0.05%
    "hyperliquid": 0.00035,  # 0.035%
    "okx": 0.0005,  # 0.05%
    "bybit": 0.0055,  # 0.55%
}


class ContangoError(RuntimeError):
    """Raised when we cannot compute required prices."""


class USDKRWCache:
    def __init__(self, ttl_seconds: float = 30.0):
        self.ttl = ttl_seconds
        self._rates: Dict[str, Dict[str, float]] = {}

    def get_rate(self, exchange_id: str, prices: Dict[str, Dict[str, float]]) -> float:
        info = prices.get("USDT/KRW")
        if not info:
            raise ContangoError("USDT/KRW ticker missing.")
        raw_value = info.get("ask") or info.get("last")
        if raw_value is None:
            raise ContangoError("USDT/KRW ticker has no price data.")
        rate = float(raw_value)
        now = time.time()
        record = self._rates.get(exchange_id)
        if record:
            cached_rate = record["rate"]
            cached_value = record["raw"]
            if rate == cached_value and now - record["timestamp"] < self.ttl:
                return cached_rate
        self._rates[exchange_id] = {"rate": rate, "timestamp": now, "raw": rate}
        return rate


usdkrw_cache = USDKRWCache()


def _fetch_exchange(cfg: ExchangeConfig, include_funding: bool) -> Dict[str, Dict[str, float]]:
    return fetch_prices_for_exchange(cfg, include_funding=include_funding)


def fetch_exchange_group(
    configs: List[ExchangeConfig],
    include_funding: bool,
    executor: ThreadPoolExecutor,
) -> Dict[str, Dict[str, Dict[str, float]]]:
    results: Dict[str, Dict[str, Dict[str, float]]] = {}
    if not configs:
        return results
    future_to_cfg = {
        executor.submit(_fetch_exchange, cfg, include_funding): cfg for cfg in configs
    }
    for future in as_completed(future_to_cfg):
        cfg = future_to_cfg[future]
        try:
            payload = future.result()
        except Exception as exc:  # noqa: BLE001
            results[cfg.exchange_id] = {"error": f"{type(exc).__name__}: {exc}"}
        else:
            results[cfg.exchange_id] = payload
    return results


def normalize_base(symbol: str) -> str:
    base = symbol.split("/")[0]
    return base.replace("-", "").upper()


def build_spot_usd_maps(executor: ThreadPoolExecutor) -> Dict[str, Dict[str, Dict[str, float]]]:
    spot_maps: Dict[str, Dict[str, Dict[str, float]]] = {}
    payloads = fetch_exchange_group(SPOT_CONFIGS, include_funding=False, executor=executor)
    for cfg in SPOT_CONFIGS:
        payload = payloads.get(cfg.exchange_id)
        if not payload:
            continue
        if "error" in payload:
            print(
                f"Warning: {cfg.exchange_id} unavailable ({payload['error']})",
                file=sys.stderr,
            )
            continue
        prices = payload["prices"]
        try:
            usdkrw = usdkrw_cache.get_rate(cfg.exchange_id, prices)
        except ContangoError as exc:
            print(f"Warning: {cfg.exchange_id} missing USDT/KRW ({exc})", file=sys.stderr)
            continue
        spot_usd: Dict[str, float] = {}
        for symbol, ticker in prices.items():
            if not symbol.endswith("/KRW"):
                continue
            price = ticker.get("ask") or ticker.get("last")
            if price is None:
                continue
            base = normalize_base(symbol)
            spot_usd[base] = float(price) / usdkrw
        if spot_usd:
            spot_maps[cfg.exchange_id] = {
                "label": cfg.short_label or cfg.label,
                "prices": spot_usd,
            }
    if not spot_maps:
        raise ContangoError("No KRW spot prices available.")
    return spot_maps


def build_futures_maps(executor: ThreadPoolExecutor) -> Dict[str, Dict[str, Dict[str, float]]]:
    maps: Dict[str, Dict[str, Dict[str, float]]] = {}
    payloads = fetch_exchange_group(FUTURES_CONFIGS, include_funding=True, executor=executor)
    for cfg in FUTURES_CONFIGS:
        payload = payloads.get(cfg.exchange_id)
        if not payload:
            continue
        if "error" in payload:
            print(
                f"Warning: {cfg.exchange_id} unavailable ({payload['error']})",
                file=sys.stderr,
            )
            maps[cfg.exchange_id] = {}
            continue
        exchange_prices: Dict[str, Dict[str, float]] = {}
        for symbol, ticker in payload["prices"].items():
            sell_price = ticker.get("bid") or ticker.get("mark") or ticker.get("last")
            if sell_price is None:
                continue
            base = normalize_base(symbol)
            funding_rate = ticker.get("fundingRate")
            try:
                funding_value = float(funding_rate) if funding_rate is not None else None
            except (TypeError, ValueError):
                funding_value = None
            exchange_prices[base] = {
                "price": float(sell_price),
                "funding_rate": funding_value,
                "symbol": symbol,
            }
        maps[cfg.exchange_id] = exchange_prices
    return maps


def identify_contango(
    spot_maps: Dict[str, Dict[str, Dict[str, float]]],
    futures_maps: Dict[str, Dict[str, Dict[str, float]]],
    min_spread_pct: float,
    require_nonnegative_funding: bool = True,
) -> List[Dict[str, float]]:
    rows = []
    for spot_id, spot_payload in spot_maps.items():
        spot_label = spot_payload.get("label", spot_id)
        spot_prices = spot_payload.get("prices", {})
        for futures_id, futures_prices in futures_maps.items():
            for base, payload in futures_prices.items():
                futures_price = payload.get("price")
                spot_price = spot_prices.get(base)
                if not spot_price:
                    continue
                if futures_price is None:
                    continue
                spread = futures_price - spot_price
                if spread <= 0:
                    continue
                pct = (spread / spot_price) * 100
                if pct < min_spread_pct:
                    continue
                funding_rate = payload.get("funding_rate")
                if funding_rate is None:
                    continue
                if require_nonnegative_funding and funding_rate < 0:
                    continue
                spot_fee = SPOT_FEES.get(spot_id, 0.0)
                fut_fee = FUTURES_FEES.get(futures_id, 0.0)
                total_fee_pct = (spot_fee * 2 + fut_fee * 2) * 100
                net_pct = pct - total_fee_pct
                net_after_0_2 = pct - total_fee_pct - 0.2
                net_after_0_4 = pct - total_fee_pct - 0.4
                rows.append(
                    {
                        "base": base,
                        "spot_exchange": spot_id,
                        "spot_label": spot_label,
                        "exchange": futures_id,
                        "spot_price": spot_price,
                        "futures_price": futures_price,
                        "spread": spread,
                        "pct": pct,
                        "futures_symbol": payload.get("symbol"),
                        "funding_rate": funding_rate,
                        "net_pct": net_pct,
                        "net_pct_minus_0_2": net_after_0_2,
                        "net_pct_minus_0_4": net_after_0_4,
                    }
                )
    rows.sort(key=lambda row: row["pct"], reverse=True)
    return rows


def _fmt_net(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.3f}%"


def render_opportunities(rows: List[Dict[str, float]], top_n: int) -> str:
    if not rows:
        return "No contango opportunities above threshold."
    above = []
    below = []
    for row in rows[:top_n]:
        base = row["base"]
        pct = row["pct"]
        spread = row["spread"]
        funding = row.get("funding_rate")
        net_pct = row.get("net_pct")
        net_02 = row.get("net_pct_minus_0_2")
        net_04 = row.get("net_pct_minus_0_4")
        funding_str = "funding n/a" if funding is None else f"funding {funding * 100:.4f}%"
        net0 = _fmt_net(net_pct)
        net02 = _fmt_net(net_02)
        net04 = _fmt_net(net_04)
        line = (
            f"{base:8s} | Long {row['spot_label']} (ask) @{row['spot_price']:.8f} USD | "
            f"Short {row['exchange']} perp (bid) @{row['futures_price']:.8f} USD "
            f"(spread {spread:.6f} USD, {pct:.3f}%, {funding_str}, net0 {net0}, net0.2 {net02}, net0.4 {net04})"
        )
        if pct >= 1.0:
            above.append(line)
        else:
            below.append(line)
    output_lines = []
    if above:
        output_lines.append("차익 1% 이상")
        output_lines.extend(above)
    if below:
        if output_lines:
            output_lines.append("-" * 80)
        output_lines.append("차익 1% 이하")
        output_lines.extend(below)
    return "\n".join(output_lines)


def main():
    parser = argparse.ArgumentParser(description="Monitor contango opportunities.")
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds between refreshes.")
    parser.add_argument(
        "--min-pct",
        type=float,
        default=0.1,
        help="Minimum futures premium percentage to report.",
    )
    parser.add_argument("--top", type=int, default=10, help="Maximum rows per refresh.")
    parser.add_argument("--once", action="store_true", help="Run a single evaluation and exit.")
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear the terminal before printing each refresh.",
    )
    args = parser.parse_args()

    try:
        max_workers = max(1, min(8, len(SPOT_CONFIGS) + len(FUTURES_CONFIGS)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while True:
                try:
                    spot_maps = build_spot_usd_maps(executor)
                    futures_maps = build_futures_maps(executor)
                    rows = identify_contango(spot_maps, futures_maps, args.min_pct)
                    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    if args.clear:
                        print("\033[2J\033[H", end="")
                    print(f"\n[{timestamp}] Top contango spreads (min {args.min_pct:.3f}%):")
                    print(render_opportunities(rows, args.top))
                except (ContangoError, ccxt.BaseError) as exc:
                    print(f"Error while computing contango: {exc}", file=sys.stderr)
                if args.once:
                    break
                time.sleep(max(0.1, args.interval))
    except KeyboardInterrupt:
        print("\nStopped by user.")


if __name__ == "__main__":
    main()
