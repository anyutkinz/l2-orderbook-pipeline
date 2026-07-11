# Decisions

Running log of non-obvious architectural choices, alternatives considered,
and why. Written for interview prep — every entry should be defensible
out loud.

## M0 — Project skeleton, config, logging, CI

### src-layout (`src/l2_pipeline/`) instead of a flat package at repo root

**Alternative:** flat layout (`l2_pipeline/` directly under repo root).

**Why:** with a flat layout, `pytest` can accidentally import the package
from the working directory even if it isn't installed, which masks packaging
bugs (missing `__init__.py`, bad `pyproject.toml` package config) until
someone tries to actually install and run the thing. src-layout forces an
editable install (`uv sync` → `pip install -e .` under the hood) before tests
can import anything, so packaging correctness is verified from commit 1
instead of discovered later.

### `book/`, `feeds/`, `sinks/` as separate top-level packages

**Why:** the spec requires the book engine to be pure (no I/O) so it's
unit-testable with synthetic message sequences. Putting it in its own
package with no dependency on `feeds/` or `sinks/` makes that constraint
mechanically checkable (an import of `aiohttp`/`websockets`/`pyarrow` inside
`book/` is a code-review red flag, not just a documented convention).

### Python pinned to 3.12, not the system 3.14

**Alternative:** develop against whatever Python is already installed
(3.14.6 on this machine).

**Why:** 3.12 is closer to what a production trading shop would actually
run (nobody runs bleeding-edge Python in prod), and has broader/safer wheel
availability for `pyarrow` and other C-extension-heavy dependencies today
than a Python version that shipped a few months ago. `pyproject.toml` pins
`requires-python = ">=3.12,<3.13"` so this is enforced by tooling, not just
documentation.

### `uv` for dependency management

**Alternative:** plain `pip` + `requirements.txt`, or Poetry.

**Why:** `uv` is fast, uses the standard `pyproject.toml` + PEP 735
dependency-groups format (no proprietary lockfile format like Poetry's),
and has become a de facto standard in the Python ecosystem quickly enough
that using it is itself a small positive signal about staying current with
tooling.

### CI wired up in M0 with no real logic yet

**Alternative:** defer CI until there's substantial code to test (e.g.
after M1's book engine).

**Why:** CI configuration issues (wrong Python version resolution, wrong
working directory, mismatched dependency groups) are much cheaper to debug
against an empty repo than against a repo with real logic in flight. Getting
"green from commit 1" also means every subsequent milestone is validated
automatically instead of retroactively.

### Config: YAML file parsed into frozen dataclasses, not a raw dict or Pydantic model

**Alternatives considered:**
- Raw `dict` passed around — rejected: no static typing, silent KeyErrors
  at arbitrary depth in the code instead of one validation point.
- Pydantic — rejected for now: it's a reasonable choice too, but the spec's
  "no heavy frameworks" preference and the fact that the validation surface
  here is small (a handful of fields, a couple of enums) means hand-rolled
  dataclass parsing with explicit error messages is simpler and has zero
  extra dependencies. Worth revisiting if the config grows a lot of
  conditional/nested structure later.

**Why frozen + slots:** config is loaded once at startup and never mutated;
`frozen=True` makes accidental mutation a `FrozenInstanceError` instead of a
silent bug, and `slots=True` is a free memory/attribute-typo win with no
downside for a value object like this.

**Validation strategy:** `load_config()` raises a single `ConfigError` with a
human-readable message for every failure mode (missing file, malformed YAML,
non-mapping root, missing required keys, empty exchange list, empty symbol
list, invalid enum value) rather than letting `KeyError`/`TypeError`/
`yaml.YAMLError` leak out. The goal: a broken config file should fail loudly
and specifically at startup, not three call frames deep at 2am.

### `overflow_policy` modeled as an enum now, real logic deferred to M5

**Why:** the backpressure policy for the book-engine → Parquet-sink queue
isn't implemented yet (that's M5, after profiling shows what actually
matters). But the config schema is fixed early because changing a config
*shape* later (e.g. going from a bare string to a structured field) would be
a breaking change for anyone with an existing config file. Using an
`enum.Enum` with one variant used (`drop_oldest`) and a second already
declared (`coalesce`) means M5 adds behavior, not schema.

### Logging: structured JSON via a custom `logging.Formatter`, not a third-party JSON logging library

**Alternative:** `python-json-logger` or similar.

**Why:** the actual requirement (structured logging of every resync/gap/
disconnect incident) only needs a formatter that emits one JSON object per
log line and lets call sites attach arbitrary structured fields via
`extra={"extra_fields": {...}}`. That's ~20 lines of stdlib `logging` code;
pulling in a dependency for it isn't justified yet. `format: "json" | other`
is config-driven so a human-readable formatter is available for local
dev/debugging without code changes.

### `app.py` in M0 only loads config + logging, no feed/book wiring

**Why:** M0 is scoped to "skeleton, config, logging, CI" — wiring in feed
clients or the book engine before they exist (M1+) would mean either dead
code or fake stubs pretending to do something they don't. `main()` today
proves the config-loading and logging plumbing works end-to-end; each later
milestone extends it with real behavior instead of replacing placeholders.

## M1 — Pure book engine

### `DiffEvent.prev_id` / `.final_id`, not Binance's raw `U` / `u`

**Alternative:** name the fields `U` and `u` directly, matching Binance's
docs.

**Why:** the book engine must be exchange-agnostic (per the top-level
spec), and it needs to validate OKX's `seqId`/`prevSeqId` chaining in M4
with the same state machine. `prev_id` = "checkpoint this event expects
the book to currently be at" and `final_id` = "checkpoint the book becomes
after applying it" is a contract both protocols can be translated into at
the feed-client boundary:
- Binance spot (`U`/`u`, no `pu`): `prev_id = U - 1`, `final_id = u`.
- Binance futures (has `pu`): `prev_id = pu` directly — no arithmetic,
  `pu` *is* this field. (Binance added `pu` for exactly this reason.)
- OKX (`seqId`/`prevSeqId`): `prev_id = prevSeqId`, `final_id = seqId`.

Raw `U`/`u` naming is kept only in the Binance feed client's parsing layer
(M3), for fidelity when cross-referencing raw message dumps while
debugging. The book engine itself never sees exchange-specific field names.

**Boundary condition re-derivation:** Binance's official first-event check
is `U <= lastUpdateId+1 <= u`. Substituting `prev_id = U - 1`:
`prev_id + 1 <= lastUpdateId + 1 <= u` → `prev_id <= lastUpdateId < final_id`.
This is the generalized boundary check the engine actually implements —
confirmed algebraically equivalent to Binance's formula, not an
approximation.

### `apply_levels()` split out from `apply_event()`

**Why:** `apply_event()` owns sequencing (does this event's `prev_id`
chain onto the book's current checkpoint?); `apply_levels()` owns nothing
but overwriting/deleting absolute price levels on one side of the ladder.
Keeping them separate makes the idempotency property testable directly:
applying the same absolute-value levels twice is a no-op the second time,
*by construction*, independent of sequencing. That property is exactly
what makes the snapshot-boundary straddle case safe (a REST snapshot may
already reflect part of a straddling event's effect — reapplying the full
event on top is harmless only because updates are absolute, not deltas).

Note this only works as a *separate* test of `apply_levels()` — running
the same `DiffEvent` through `apply_event()` twice in a row is expected to
fail the sequencing check the second time (its `prev_id` no longer equals
the post-first-application checkpoint), which is correct: real Binance/OKX
duplicate delivery isn't tolerated by the official protocols, and
shouldn't be silently absorbed by the engine.

### Bug caught during implementation: first survivor after a snapshot must bypass the strict equality check

While implementing `load_snapshot()`'s buffer replay, initially reused
`_apply_live()` (the strict `prev_id == last_applied_id` check) for *all*
surviving buffered events, including the first one. That's wrong: the
first survivor is validated by the *straddle* condition
(`prev_id <= last_update_id < final_id`), which allows `prev_id` to fall
strictly before the snapshot's checkpoint — that's the whole point of the
boundary case (test 6). Running it through the strict-equality check
would reject exactly the case it's supposed to accept.

Fix: the first survivor is applied unconditionally via `apply_levels()`
(already validated by the straddle check before this point), and only
survivors after it go through `_apply_live()`'s strict chaining check.
This is the off-by-one bug the milestone's test suite exists to catch —
and it did, on the first test run, before any hand-inspection.

### `ApplyResult` (typed return value), not a raised exception, for gap detection

**Why:** gap detection is a routine, expected condition in this domain —
network hiccups happen regularly — not a programmer error. A typed return
value forces exhaustive handling at call sites via mypy; an exception is
invisible in the type signature and encourages callers to forget to catch
it. Raised exceptions are reserved for genuine caller-contract violations:
`load_snapshot()` raises `ValueError` if called while `state is LIVE`,
because that's a feed-client bug (fetching a snapshot when one isn't
needed), not a protocol event.

### Buffer is never cleared on a rejected (stale) snapshot

**Why:** `load_snapshot()` returning `SNAPSHOT_STALE` leaves `self._buffer`
untouched — buffering keeps growing via ongoing `apply_event()` calls
while the feed client fetches a fresh snapshot and retries. Clearing the
buffer on a failed attempt would throw away events that a *later,
successful* snapshot might still need to bridge the gap (test 7 exercises
exactly this retry-and-recover path). Unbounded buffer growth under
repeated failures is an explicit open question, deferred to M3 where the
feed client can own a retry/backoff policy — the pure engine's job is
correctness given whatever it's handed, not operational timing decisions.

### Three states (`BUFFERING`, `RESYNCING`, `LIVE`), not four

**Why:** a gap detected in `LIVE` transitions *instantly* to `RESYNCING`
with the triggering event as the seed of a new buffer — there's no
observable intermediate "invalid" state to model separately. `BUFFERING`
(cold start, book has never been valid) and `RESYNCING` (book was valid,
lost it to a gap) are kept as distinct states despite identical internal
handling, because they're semantically different for the metrics/logging
this needs later (M6's resync counters need to know "recovering" from
"starting up").

### `Decimal`, not `float`, for price and quantity

**Why:** exchanges send price/quantity as JSON strings specifically to
avoid float precision issues on the wire. Parsing directly via
`Decimal(raw_str)` preserves the exact value; going through `float()`
first would throw that guarantee away for no benefit — an easy, avoidable
correctness gap in a project meant to demonstrate rigor.

### Addendum (M2): `BookEngine.full_book()`

Added after M1 shipped, to support the M2 differential-testing harness's
convergence checker, which needs to compare the engine's *entire* internal
ladder against a ground-truth oracle — `top_levels(n)` always sorts and
truncates to a configured depth, so using it with an arbitrarily large `n`
to fake a full dump would be an abuse of a method meant for "top N levels
to persist." `full_book()` returns defensive copies of the internal bid/ask
dicts, so callers can't mutate engine state through the returned objects.

## M2 — Deterministic simulation + fault injection

### Lineage: a mini deterministic-simulation-testing (DST) harness

The design borrows three specific patterns from FoundationDB's and
TigerBeetle's testing methodology, scoped down to fit a single-process,
synchronous, no-network M2 milestone:

1. **Seed-based determinism** — the entire run (market dynamics, fault
   timing, fault types) is driven by one integer seed; same seed
   reproduces bit-for-bit. This is what makes a failing run replayable
   from just the seed, not from a saved event log.
2. **Differential/model-based testing** — a ground-truth oracle (the
   market simulator's own internal ladder, updated unconditionally,
   never touched by fault injection) runs alongside the real
   `BookEngine`. Correctness is bit-exact convergence of the engine to
   the oracle, not a hand-picked set of expected outputs.
3. **Property-based exploration on top of the deterministic scenarios**
   (Hypothesis) — searches the space of seeds/fault-configs/step-counts
   for a combination the hand-written scenarios (S1-S8) didn't think of.

The full versions of these systems test entire distributed clusters
under simulated network partitions, disk corruption, and clock skew,
with a custom deterministic runtime replacing the OS scheduler. That's
out of scope here by design: M2 is single-process, fully synchronous, no
asyncio, no real network — determinism first, so every later milestone
(especially the real async feed clients in M3/M4) can be tested against
this harness without the harness itself needing to be debugged under
concurrency.

### Two independent RNG streams from one seed

If `MarketSimulator` and `FaultInjector` shared one `random.Random`
instance, adding or removing any random draw in one component would
silently shift what the other draws on the *same* seed — determinism
would hold in the narrow sense (same seed still reproduces *a* run) but
break in the useful sense (a harmless refactor changes what every
existing seed produces, making old bug reports unreproducible). Fix:
`derive_seed(seed, label)` hashes `f"{seed}:{label}"` through a throwaway
`random.Random` to produce an independent child seed per label; each
component takes an already-derived seed and constructs its own
`random.Random` internally — never a shared instance passed in from
outside. `build_simulation()` is the single place derivation happens.

### Fault precedence + shadowed-fault logging

Multiple faults can roll "true" on the same tick (e.g. `DROP_ONE` and
`DUPLICATE` both firing). Rather than let them combine into nonsensical
states, a fixed precedence applies, first match wins: active
window (`DISCONNECT`/`DROP_BURST` in progress) → `DISCONNECT` →
`DROP_BURST` → `DROP_ONE` → `DUPLICATE` → `REORDER` → deliver normally.

Every fault type is still rolled independently first (not short-circuit
evaluated), so a type that rolled true but lost to a higher-precedence
type gets logged as `shadowed=True` rather than silently disappearing.
Without this, a high-probability fault type could statistically starve a
lower-precedence one in a long run and nobody would notice — the S8
fault-storm test asserts every configured fault type actually *fired*
at least once, and the shadowed counts make starvation visible instead
of hidden if that assertion ever needs loosening.

One simplification: while an active window is suppressing delivery, no
other fault types are rolled *at all* that tick (not rolled-and-shadowed
— not rolled). Window suppression already determines delivery
unconditionally; computing "what would have happened instead" adds
noise, not information, for those ticks. Shadowed counts reflect ticks
where multiple ad hoc rolls genuinely competed, not ticks preempted by
an in-progress window.

### `@given`, not `RuleBasedStateMachine`

Hypothesis's stateful testing exists for when the tool needs to
*discover* an operation sequence by interleaving rule calls itself —
valuable when the sequence space isn't already captured by a compact
parameter set. Here it already is: `(seed, FaultConfig, num_steps)`
fully determines an entire run, faults included, by construction (that's
the seed-determinism property above). Modeling faults as Hypothesis
rules would mean reimplementing `FaultInjector`'s job a second time
inside the state machine's rule set, and would lose the "one seed
reproduces everything" property already built in. `@given` over a
composite strategy that draws seed + fault probabilities + step count,
running the whole deterministic simulation as one property-function
body, is the correctly-sized tool for a system that's already
seed-driven rather than one Hypothesis needs to drive itself.

### Bug Hypothesis actually found: simultaneous bid/ask inserts could cross

`MarketSimulator.step()` originally generated bid-side and ask-side
changes independently, both clamped against a `best_bid`/`best_ask`
snapshot taken *before either was applied*, then applied both at the
end. If both sides happened to insert in the same tick, each clamp was
correct in isolation (a new bid can't cross the *old* best ask; a new
ask can't cross the *old* best bid) but said nothing about whether the
two new prices crossed *each other* — and when the existing spread was
at least `2 * spread_min`, they could. H1 found this within its
configured `max_examples=200`, shrunk to seed 1615, an all-zero
`FaultConfig` (proving it was a pure market-generation bug, unrelated to
fault injection), 60 steps. Fix: apply each side's change immediately
after generating it, so a same-tick second insert clamps against the
*current* (already-updated) opposite price instead of a stale snapshot.
This is exactly the class of bug this harness exists to catch — order
of generation vs. order of application silently diverging — and it was
caught by the property layer, not the eight hand-written scenarios,
none of which happened to construct this specific interleaving.

## M3 — Live Binance feed client

### `BinanceFeedClient` is the production twin of `SimulatedFeedDriver`

Same control loop, same shape: GAP_DETECTED-or-cold-start triggers a
fetch-snapshot-and-load cycle with bounded retry on staleness.
`FaultInjector.poll()` is replaced by a real websocket stream;
`FaultInjector.request_snapshot()` is replaced by a real REST call.
`MarketSimulator` ≈ exchange internals, the injector ≈ the network,
`SimulatedFeedDriver`/`BinanceFeedClient` ≈ the feed client, `BookEngine`
≈ itself, completely unchanged between M2 and M3. The only things that
changed crossing from M2 to M3 are the two outer layers that touch the
outside world; the recovery *logic* was already fully proven by M2's
fault-storm and Hypothesis runs before a single real socket opened here.

### Two independent state machines, never merged

`ConnectionManager` (transport: DISCONNECTED/CONNECTING/CONNECTED/BACKOFF)
and `BookEngine`'s own state (BUFFERING/RESYNCING/LIVE) track genuinely
different things and must stay separate. `ConnectionManager` has zero
opinion about book validity; `BookEngine` has zero opinion about sockets.
The only coupling is one direction, explicit, and asymmetric: a new
connection *forces* `invalidate()` on the book (via the coupling rule
below), but a book resync never touches connection state. Merging them
into one state machine would create meaningless combined states (what
does "CONNECTED + BUFFERING" mean as a single enum value that
"CONNECTING + BUFFERING" doesn't already cover?) and couple two concerns
that change for different reasons, at different rates, for different
reasons entirely.

### Coupling rule: every reconnect forces `invalidate()`, not relying on chain-check gap detection

Added `BookEngine.invalidate(reason)` (M1 API extension, same pattern as
`full_book()`): forces `RESYNCING` (or leaves `BUFFERING` alone if never
LIVE yet) from any state, discarding book contents, checkpoint, and
buffer. Missed events during a WS outage are near-certain, and the
engine's own chain-check *would* probably catch it on the first event
after reconnect anyway — but "probably" isn't good enough for a coupling
this important. Defense in depth: don't rely on an emergent property
when an explicit one is one method call away.

### Backoff + full jitter (AWS lineage)

`delay = uniform(0, min(cap, base * 2**attempt))` is literally AWS's
"full jitter" formula from their well-known backoff blog post
(`base=0.5s, cap=30s` here). Rejected alternatives: no jitter (a fleet of
reconnecting clients would thunder-herd in lockstep against Binance after
any shared outage — not a concern at N=1 client, but a bad habit to
build); "equal jitter" (`cap/2 + uniform(0, min(cap, base*2**attempt)/2)`,
AWS's more conservative alternative) trades lower minimum delay for a
higher floor — full jitter's wider spread is the better fit for spreading
out reconnect load when doing this alone. `ConnectionManager` never
sleeps itself; it returns the delay for the caller to `await`, which is
what makes it testable with a fake clock and RNG (T1), no event loop
needed.

### Watchdog is message-based, not ping/pong-based

Rely on the `websockets` library's built-in ping/pong for protocol-level
keepalive (unchanged, out of our code entirely) — but that only proves
the *transport* is alive, not that Binance is actually sending us book
updates. A watchdog that only checked ping/pong would miss a connection
that's technically open but has silently stopped producing depth events
(the specific failure mode this exists to catch). Implemented as
`asyncio.wait_for(ws.recv(), timeout=watchdog_timeout)` around every
receive — if `BTCUSDT@depth@100ms` (nominally an update every ~100ms)
goes quiet for `watchdog_timeout` (default 10s), we declare the
connection dead ourselves rather than trusting the transport layer's
opinion of its own health.

### Token bucket sizing: weight verified live, not from memory

`GET /api/v3/depth` weight was checked directly against
developers.binance.com at implementation time (not recalled from
training data, which can be stale and the table has changed over
Binance's API history): 5 at `limit<=100`, 25/50/250 at the higher
tiers. We fetch at `limit=100` — 5x `book.depth_levels=20`, comfortable
margin, cheapest tier. Overall budget (confirmed): 6000
REQUEST_WEIGHT/minute per IP, reported via
`X-MBX-USED-WEIGHT-(intervalNum)(intervalLetter)` response headers,
logged at debug level on *every* snapshot fetch (not just 429/418) so
real consumption is observable during A1 even when nothing's going
wrong. `capacity=10, refill_rate=0.5/sec` sizing rationale is in
`ratelimit.py`'s module docstring, next to the verified numbers it's
derived from.

The general pattern here — token-bucket rate limiting in front of REST
calls, respecting `Retry-After` on 429/418, logging used-weight headers
for observability — is standard practice in production crypto feed
handlers (cryptofeed, NautilusTrader, and similar open-source exchange
connectivity libraries all implement some form of it). Citing that as
"this is an established pattern, not something invented here," not as a
claim of having read those codebases line-by-line in this session.

`TokenBucket` itself stays fully synchronous (no `asyncio.sleep` inside)
— it answers "how long" via `time_until_available()`, the caller in
`binance.py` does the actual `await asyncio.sleep(...)`. Same reasoning
as `ConnectionManager`: keeping the decision-making pure is what makes
T2 testable with a fake clock and zero event loop.

### Two separate retry loops, not one

`_fetch_snapshot()` has its own bounded retry loop for HTTP-level issues
(429/418, `Retry-After`-driven), separate from `_perform_resync()`'s
protocol-level retry loop (`SNAPSHOT_STALE`/`GAP_DETECTED`, capped at 20,
carried over from M2). Conflating them would mean a rate-limit backoff
retry burns down the same budget meant for protocol-level staleness
retries — two different failure classes with two different appropriate
retry budgets and backoff shapes, kept structurally separate rather than
sharing one counter that would silently mean different things depending
on which failure mode happened to fire first.

### `load_snapshot()` can return `GAP_DETECTED`, not just `SNAPSHOT_STALE` — caught before shipping in a test

First draft of `_perform_resync()` treated any non-`APPLIED` result as
"stale, retry" — but `load_snapshot()`'s buffer-replay loop can also
return `GAP_DETECTED` (the snapshot itself was accepted, but a *later*
buffered event failed to chain during replay). That's a genuinely
different cause than staleness, even though the recovery action (fetch a
fresh snapshot) happens to be identical either way. Caught while
hand-tracing the scenario T5 was about to encode — fixed to log the
correct incident type for each cause before the test could quietly bake
in the wrong label as "expected" behavior.

### Bug found empirically: cold-start double resync

First live run against real Binance showed two `RESYNC_COMPLETED`
incidents at startup instead of one. Root cause: the constructor
pre-set `_resync_needed` for "cold start needs an initial sync" — but
`run()`'s reconnect loop *already* calls `invalidate()` +
`resync_needed.set()` unconditionally on every connection including the
first. The pre-set let `_resync_worker` race ahead of the WS handshake:
it grabbed an empty buffer, fetched a snapshot, and completed a trivial
resync *before the websocket had even finished connecting* — which the
coupling rule then immediately discarded via `invalidate()` once the
real connection came up, forcing a second, real resync. Not incorrect
(no invariant violated, no bad state), but wasteful (an extra REST call
every cold start) and confusing in the logs. Fixed by removing the
redundant pre-set; `run()`'s existing per-connection trigger already
covers cold start correctly on its own.

### `events_buffered` in `RESYNC_COMPLETED` logs can undercount

It resets when `_perform_resync()` *starts*, but the reader loop and the
resync worker are separate coroutines — messages can buffer before the
resync task is even scheduled to run, and those don't get counted. It's
a debug-observability figure only; nothing in the apply/convergence logic
depends on its precision, and inflating its apparent precision would be
the kind of fake rigor this project explicitly rules out. Documented
plainly in the code rather than silently shipped as if exact.

### Windows signal handling: `add_signal_handler` with a `KeyboardInterrupt` fallback

`loop.add_signal_handler()` is POSIX-only — raises `NotImplementedError`
on Windows. `main()` tries it for both SIGINT and SIGTERM (works cleanly
on Linux, where this could plausibly run in CI or on a server) and falls
back to relying on `KeyboardInterrupt` propagating through the running
coroutine on Ctrl+C, caught by a `try/finally` around `run()`'s body that
unconditionally closes the websocket/HTTP client and logs final stats
regardless of which path triggered it.

Verified manually, not just reasoned about: git-bash/MSYS (this
project's default shell in the agent environment) does not appear to
attach a real Win32 console, so `GenerateConsoleCtrlEvent`-based Ctrl+C
simulation from that shell silently did nothing — confirmed down to a
minimal `asyncio.sleep()`-only repro with no project code involved, so
it's an environment property, not an app bug. Run manually from a real
PowerShell console instead: Ctrl+C correctly raised `KeyboardInterrupt`,
propagated through `CancelledError` into the `finally` block, ran
cleanup, and `main()` caught the re-raised `KeyboardInterrupt` cleanly
(exit code 0). See the M3 walkthrough for the literal output.

### Periodic heartbeat, separate from incident logging

Added after the live Ctrl+C run surfaced a real gap: steady-state
`apply_event()` calls are deliberately silent (counted, not logged) by
the event-driven incident-logging design, which means a long unattended
run gives no signal to distinguish "silently healthy" from "silently
stalled" without interrupting the process. `_heartbeat_worker()` is a
third persistent background task (alongside `_resync_worker`), logging
`messages_received` / `connection_state` / `book_state` at INFO level
every `heartbeat_interval_seconds` (default 30s, configurable). Doesn't
touch the incident-logging design — deliberately not tagged as an
`incident` in the structured log, since it isn't one; it's a liveness
pulse, checked in against `get_stats()`'s existing fields rather than
adding new bookkeeping.

### Guarding the "no lock needed" concurrency argument with a test, not just a comment

The reader loop and `_resync_worker` both call `BookEngine` methods
concurrently without a lock, safe only because no `BookEngine` method
contains an `await` — under asyncio's cooperative scheduling, each call
runs to completion with no yield point for the event loop to interleave
on. That argument used to live only in a comment, which nothing stops a
future change (M4's OKX logic landing in the same `book/` package,
for instance) from silently violating. `test_book_engine_has_no_async_methods`
(in M1's `test_book_engine.py`, since it protects an M1 invariant that
M3 merely depends on and documents) enforces it mechanically: it fails
loudly the moment any `BookEngine` method becomes a coroutine function.

