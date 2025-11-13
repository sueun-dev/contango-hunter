# contango-hunter

Tools for scanning and hedging contango opportunities across KRW spot venues (Upbit, Bithumb) and major perp venues (Gate.io, Hyperliquid, OKX).

## Overview

| Script | Description |
|--------|-------------|
| `contango_monitor.py` | Periodically loads every KRW spot and perp market via REST, filters by spread/funding, and prints ranked opportunities. |
| `contango_trade_executor.py` | Single-shot planner: fetches the current best spread, computes the hedge size for a chosen USD notional, and optionally places the futures-first/spot-second market orders (dry-run by default). |
| `contango_auto_trader.py` | Continuous hedger that adds 50 USD tranches whenever spread ≥ entry threshold (default 1%) and unwinds tranches when spread ≤ exit threshold (default 0.2%). Futures legs are opened first, then spot. Supports dry-run and live modes. |
| `.env.example` | Template for required API keys (Upbit, Bithumb, Gate, Hyperliquid, OKX) and auto-trader parameters. Copy to `.env` and fill before enabling live trading. |

> ⚠️ **Important**: Live trading requires API keys with trading permissions. Use the `--live` flag only after setting the environment variables described in `.env.example`. Always test with the default dry-run mode first.

## Installation

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

### 1. Monitor (REST-based)
```bash
python3 contango_monitor.py --interval 10 --min-pct 0.1 --top 10 --clear
```

### 2. One-off Trade Plan / Execution
```bash
# Dry-run (prints plan only)
python3 contango_trade_executor.py --entry-threshold 1.0 --min-pct-scan 0.2 --notional-usd 500

# Live mode (requires API keys in env vars)
python3 contango_trade_executor.py --entry-threshold 1.0 --min-pct-scan 0.2 --notional-usd 500 --live
```

### 3. Continuous Auto Trader
```bash
# Dry-run (default)
python3 contango_auto_trader.py --entry-threshold 1.0 --exit-threshold 0.2 --interval 10

# Live mode (after sourcing .env with credentials)
python3 contango_auto_trader.py --entry-threshold 1.0 --exit-threshold 0.2 --interval 10 --live
```

The auto-trader opens hedges in 50 USD increments up to 2 000 USD per leg, always shorting the perp first and buying spot second. It unwinds the same 50 USD tranches (or whatever remains) when spread falls below the exit threshold.

## Environment Variables

Copy `.env.example` to `.env` and fill in:

```
UPBIT_API_KEY=...
UPBIT_API_SECRET=...
BITHUMB_API_KEY=...
BITHUMB_API_SECRET=...
OKX_API_KEY=...
OKX_API_SECRET=...
OKX_API_PASSWORD=...
GATEIO_API_KEY=...
GATEIO_API_SECRET=...
HYPERLIQUID_API_KEY=...
HYPERLIQUID_API_SECRET=...
```

Then load them before running with `--live`.

```bash
export $(grep -v '^#' .env | xargs)
```

## Disclaimer

This repository is provided for research purposes only. You are responsible for all trading decisions, API key management, and risk controls. Always validate strategies in dry-run environments before going live.***


## Short term
python3 contango_auto_trader.py --entry-threshold 1.0 --exit-threshold 0.2 --interval 5 --live
