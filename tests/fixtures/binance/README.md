# Binance golden-file fixtures

Genuinely captured, not hand-invented. Captured 2026-07-11 from the live
Binance production endpoints:

- `depth_diff_*.json`: raw frames from
  `wss://stream.binance.com:9443/stream?streams=btcusdt@depth@100ms`
  (combined-stream format, so each has a `stream` + `data` envelope).
- `depth_snapshot.json`: raw response from
  `GET https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=10`.

`depth_diff_malformed_*.json` files are hand-edited copies of a real
capture with a field corrupted/removed, since Binance's real feed won't
send malformed data on request — those specifically test our own error
handling, not real protocol behavior, and are labeled as such.

Sequence IDs are real Binance production update IDs at capture time —
not contiguous with any other run, don't assume cross-file chaining.
