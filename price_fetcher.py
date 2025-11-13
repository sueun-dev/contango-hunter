#!/usr/bin/env python3
"""Fetch prices for all supported coins across specific exchanges using ccxt."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import ccxt


@dataclass
class ExchangeConfig:
    exchange_id: str
    label: str
    market_type: str  # "spot" or "swap"
    quote: Optional[str] = None
    settle: Optional[str] = None
    options: Dict[str, Any] = field(default_factory=dict)
    params: Dict[str, Any] = field(default_factory=dict)
    short_label: Optional[str] = None


EXCHANGE_CONFIGS: List[ExchangeConfig] = [
    ExchangeConfig(
        exchange_id="upbit",
        label="Upbit KRW Spot",
        market_type="spot",
        quote="KRW",
    ),
    ExchangeConfig(
        exchange_id="bithumb",
        label="Bithumb KRW Spot",
        market_type="spot",
        quote="KRW",
    ),
    ExchangeConfig(
        exchange_id="lighter",
        label="Lighter Futures",
        market_type="swap",
    ),
    ExchangeConfig(
        exchange_id="extended",
        label="Extended Futures",
        market_type="swap",
    ),
    ExchangeConfig(
        exchange_id="based",
        label="Based Futures",
        market_type="swap",
    ),
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
        exchange_id="bybit",
        label="Bybit Perpetuals",
        market_type="swap",
        options={"defaultType": "swap"},
    ),
    ExchangeConfig(
        exchange_id="okx",
        label="OKX Perpetuals",
        market_type="swap",
        options={"defaultType": "swap"},
    ),
]


def build_exchange(cfg: ExchangeConfig):
    if not hasattr(ccxt, cfg.exchange_id):
        raise AttributeError(f"ccxt has no exchange named '{cfg.exchange_id}'")
    exchange_class = getattr(ccxt, cfg.exchange_id)
    exchange = exchange_class({"enableRateLimit": True})
    if cfg.options:
        exchange.options = {**exchange.options, **cfg.options}
    return exchange


def filter_markets(exchange, cfg: ExchangeConfig) -> List[Dict[str, Any]]:
    markets = exchange.load_markets()
    filtered: List[Dict[str, Any]] = []
    for market in markets.values():
        if cfg.market_type == "spot" and not market.get("spot"):
            continue
        if cfg.market_type == "swap" and not market.get("swap"):
            continue
        if cfg.quote and market.get("quote") != cfg.quote:
            continue
        if cfg.settle and market.get("settle") != cfg.settle:
            continue
        filtered.append(market)
    return filtered


def fetch_tickers(exchange, symbols: List[str], params: Dict[str, Any]):
    try:
        return exchange.fetch_tickers(symbols, params=params)
    except TypeError:
        # exchange version may not accept both symbols + params
        return exchange.fetch_tickers(params=params)


def fetch_funding_rates(exchange, symbols: List[str]):
    if not getattr(exchange, "has", {}).get("fetchFundingRates"):
        return None
    try:
        return exchange.fetch_funding_rates(symbols)
    except TypeError:
        return exchange.fetch_funding_rates()


def format_ticker(symbol: str, ticker: Dict[str, Any]) -> Dict[str, Any]:
    ts = ticker.get("timestamp")
    iso_ts: Optional[str] = None
    if ts is not None:
        iso_ts = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat()
    return {
        "symbol": symbol,
        "last": ticker.get("last"),
        "bid": ticker.get("bid"),
        "ask": ticker.get("ask"),
        "mark": ticker.get("info", {}).get("markPrice"),
        "timestamp": ts,
        "isoTime": iso_ts,
    }


def fetch_prices_for_exchange(cfg: ExchangeConfig, include_funding: bool = False) -> Dict[str, Any]:
    try:
        exchange = build_exchange(cfg)
    except AttributeError as exc:
        return {"error": str(exc)}

    try:
        markets = filter_markets(exchange, cfg)
    except Exception as exc:  # noqa: BLE001
        return {"error": f"failed to load markets: {exc}"}

    if not markets:
        return {"error": "no markets matched the requested filters"}

    symbols = [market["symbol"] for market in markets]
    prices: Dict[str, Dict[str, Any]] = {}

    try:
        tickers = fetch_tickers(exchange, symbols, cfg.params)
    except ccxt.BaseError:
        tickers = {}

    if tickers:
        for symbol in symbols:
            ticker = tickers.get(symbol)
            if ticker:
                prices[symbol] = format_ticker(symbol, ticker)

    missing_symbols = [symbol for symbol in symbols if symbol not in prices]
    for symbol in missing_symbols:
        try:
            ticker = exchange.fetch_ticker(symbol, params=cfg.params)
        except ccxt.BaseError as exc:
            prices[symbol] = {"symbol": symbol, "error": str(exc)}
            continue
        prices[symbol] = format_ticker(symbol, ticker)

    if include_funding and cfg.market_type == "swap":
        funding_rates: Dict[str, float] = {}
        try:
            raw_rates = fetch_funding_rates(exchange, symbols)
        except (ccxt.BaseError, AttributeError):
            raw_rates = None
        if raw_rates:
            if isinstance(raw_rates, dict):
                iterable = raw_rates.items()
            else:
                iterable = [(entry.get("symbol"), entry) for entry in raw_rates]
            for symbol, entry in iterable:
                if not symbol or symbol not in prices:
                    continue
                rate = entry.get("fundingRate")
                if rate is None and isinstance(entry.get("info"), dict):
                    rate = entry["info"].get("fundingRate")
                if rate is None:
                    continue
                try:
                    funding_rates[symbol] = float(rate)
                except (TypeError, ValueError):
                    continue
        for symbol, rate in funding_rates.items():
            prices[symbol]["fundingRate"] = rate

    return {
        "label": cfg.label,
        "type": cfg.market_type,
        "quote": cfg.quote,
        "settle": cfg.settle,
        "count": len(prices),
        "prices": prices,
    }


def main():
    aggregated: Dict[str, Any] = {}
    for cfg in EXCHANGE_CONFIGS:
        aggregated[cfg.exchange_id] = fetch_prices_for_exchange(cfg)
    print(json.dumps(aggregated, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
