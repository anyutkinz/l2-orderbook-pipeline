# OKX golden-file fixtures

Genuinely captured, not hand-invented. Captured 2026-07-12 from the live
OKX production endpoint `wss://ws.okx.com:8443/ws/v5/public`, subscribed
to `{"channel": "books", "instId": "BTC-USDT"}`.

- `subscribe_ack.json`: the `{"event": "subscribe", ...}` confirmation,
  the first message after sending the subscribe request.
- `books_snapshot.json`: the `action: "snapshot"` push -- always the
  first channel message after a successful subscribe, per OKX's
  protocol. 400 levels per side, as OKX actually sends. `prevSeqId` is
  `-1` on this message -- verified live, a sentinel (not a real
  predecessor), consistent with the snapshot becoming our `SnapshotEvent`
  directly (which has no `prev_id` field to consume it anyway).
- `books_update_normal.json`: an `action: "update"` push with real
  deletions (`qty="0"`), inserts, and updates on both sides in one
  message. Also the reference for the 4-element level shape
  `[price, qty, deprecated, numOrders]` -- confirmed live, a real
  divergence from Binance's 2-element `[price, qty]` shape (see
  DECISIONS.md M4 entry).
- `books_update_asks_only.json`: an update touching only the ask side
  (`bids` empty) -- real, not constructed, from the same capture session.
- `books_malformed_*.json`: hand-edited copies of a real capture with a
  field corrupted or removed. OKX won't send malformed data on request,
  so these specifically test our own error handling, not real protocol
  behavior.

Sequence IDs are real OKX production `seqId`/`prevSeqId` values at
capture time -- not contiguous with any other run, don't assume
cross-file chaining beyond what's noted above.
