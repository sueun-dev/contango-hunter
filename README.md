# contango-hunter

## Scripts

- `contango_monitor.py`: REST 기반 전체 거래소(Upbit, Bithumb, Gate, Hyperliquid, OKX) 콘탱고 스캐너.
- WebSocket 실험용 스크립트들은 제거되었습니다. 현재는 `contango_monitor.py`만 유지합니다.

## Usage

```bash
# REST 기반 모니터
python3 contango_monitor.py --interval 10 --min-pct 0.1 --top 10 --clear
```
