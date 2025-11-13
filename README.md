# contango-hunter

## Scripts

- `contango_monitor.py`: REST 기반 전체 거래소(Upbit, Bithumb, Gate, Hyperliquid, OKX) 콘탱고 스캐너.
- `okx_ws_monitor.py`: OKX 무기한 선물 데이터를 WebSocket으로 실시간 스트리밍해 보다 빠른 콘탱고 감시를 수행.
- `gate_ws_monitor.py`: Gate.io USDT 무기한 선물을 WebSocket으로 모니터링.
- `hyperliquid_ws_monitor.py`: Hyperliquid perp 마켓을 WebSocket으로 모니터링.
- `upbit_ws_spot.py`: Upbit KRW 스팟 호가를 WebSocket으로 스트리밍해 USD 환산 가격을 출력.
- `bithumb_ws_spot.py`: Bithumb KRW 스팟 호가를 WebSocket으로 스트리밍.
- `ws_contango_engine.py`: Upbit/Bithumb 스팟과 OKX/Gate/Hyperliquid 선물 WebSocket을 통합해 하나의 콘탱고 엔진으로 출력.

## Usage

```bash
# REST 기반 모니터
python3 contango_monitor.py --interval 10 --min-pct 0.1 --top 10

# OKX WebSocket 기반 모니터
python3 okx_ws_monitor.py --interval 2 --min-pct 0.2 --top 5

# Gate WebSocket 기반 모니터
python3 gate_ws_monitor.py --interval 2 --min-pct 0.2 --top 5

# Hyperliquid WebSocket 기반 모니터
python3 hyperliquid_ws_monitor.py --interval 2 --min-pct 0.2 --top 5

# Upbit WebSocket 기반 KRW 스팟 스트림
python3 upbit_ws_spot.py --interval 2 --top 20

# Bithumb WebSocket 기반 KRW 스팟 스트림
python3 bithumb_ws_spot.py --interval 2 --top 20

# 통합 WebSocket 콘탱고 엔진
python3 ws_contango_engine.py --interval 2 --min-pct 0.2 --top 10 --futures okx,gate,hyper
```
