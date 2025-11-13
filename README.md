# contango-hunter

## Scripts

- `contango_monitor.py`: REST 기반 전체 거래소(Upbit, Bithumb, Gate, Hyperliquid, OKX) 콘탱고 스캐너.
- `okx_ws_monitor.py`: OKX 무기한 선물 데이터를 WebSocket으로 실시간 스트리밍해 보다 빠른 콘탱고 감시를 수행.
- `gate_ws_monitor.py`: Gate.io USDT 무기한 선물을 WebSocket으로 모니터링.

## Usage

```bash
# REST 기반 모니터
python3 contango_monitor.py --interval 10 --min-pct 0.1 --top 10

# OKX WebSocket 기반 모니터
python3 okx_ws_monitor.py --interval 2 --min-pct 0.2 --top 5

# Gate WebSocket 기반 모니터
python3 gate_ws_monitor.py --interval 2 --min-pct 0.2 --top 5
```
