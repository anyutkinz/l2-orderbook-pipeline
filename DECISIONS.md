# Decisions

Running log of non-obvious architectural choices, alternatives considered,
and why. Written for interview prep: every entry should be defensible
out loud.

## M0: Project skeleton, config, logging, CI

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
- Raw `dict` passed around, rejected: no static typing, silent KeyErrors
  at arbitrary depth in the code instead of one validation point.
- Pydantic, rejected for now: it's a reasonable choice too, but the spec's
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

**Why:** M0 is scoped to "skeleton, config, logging, CI": wiring in feed
clients or the book engine before they exist (M1+) would mean either dead
code or fake stubs pretending to do something they don't. `main()` today
proves the config-loading and logging plumbing works end-to-end; each later
milestone extends it with real behavior instead of replacing placeholders.

## M1: Pure book engine

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
- Binance futures (has `pu`): `prev_id = pu` directly: no arithmetic,
  `pu` *is* this field. (Binance added `pu` for exactly this reason.)
- OKX (`seqId`/`prevSeqId`): `prev_id = prevSeqId`, `final_id = seqId`.

Raw `U`/`u` naming is kept only in the Binance feed client's parsing layer
(M3), for fidelity when cross-referencing raw message dumps while
debugging. The book engine itself never sees exchange-specific field names.

**Boundary condition re-derivation:** Binance's official first-event check
is `U <= lastUpdateId+1 <= u`. Substituting `prev_id = U - 1`:
`prev_id + 1 <= lastUpdateId + 1 <= u` → `prev_id <= lastUpdateId < final_id`.
This is the generalized boundary check the engine actually implements,
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
already reflect part of a straddling event's effect; reapplying the full
event on top is harmless only because updates are absolute, not deltas).

Note this only works as a *separate* test of `apply_levels()`: running
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
strictly before the snapshot's checkpoint. That's the whole point of the
boundary case (test 6). Running it through the strict-equality check
would reject exactly the case it's supposed to accept.

Fix: the first survivor is applied unconditionally via `apply_levels()`
(already validated by the straddle check before this point), and only
survivors after it go through `_apply_live()`'s strict chaining check.
This is the off-by-one bug the milestone's test suite exists to catch,
and it did, on the first test run, before any hand-inspection.

### `ApplyResult` (typed return value), not a raised exception, for gap detection

**Why:** gap detection is a routine, expected condition in this domain
(network hiccups happen regularly), not a programmer error. A typed return
value forces exhaustive handling at call sites via mypy; an exception is
invisible in the type signature and encourages callers to forget to catch
it. Raised exceptions are reserved for genuine caller-contract violations:
`load_snapshot()` raises `ValueError` if called while `state is LIVE`,
because that's a feed-client bug (fetching a snapshot when one isn't
needed), not a protocol event.

### Buffer is never cleared on a rejected (stale) snapshot

**Why:** `load_snapshot()` returning `SNAPSHOT_STALE` leaves `self._buffer`
untouched: buffering keeps growing via ongoing `apply_event()` calls
while the feed client fetches a fresh snapshot and retries. Clearing the
buffer on a failed attempt would throw away events that a *later,
successful* snapshot might still need to bridge the gap (test 7 exercises
exactly this retry-and-recover path). Unbounded buffer growth under
repeated failures is an explicit open question, deferred to M3 where the
feed client can own a retry/backoff policy. The pure engine's job is
correctness given whatever it's handed, not operational timing decisions.

### Three states (`BUFFERING`, `RESYNCING`, `LIVE`), not four

**Why:** a gap detected in `LIVE` transitions *instantly* to `RESYNCING`
with the triggering event as the seed of a new buffer. There's no
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
first would throw that guarantee away for no benefit: an easy, avoidable
correctness gap in a project meant to demonstrate rigor.

### Addendum (M2): `BookEngine.full_book()`

Added after M1 shipped, to support the M2 differential-testing harness's
convergence checker, which needs to compare the engine's *entire* internal
ladder against a ground-truth oracle. `top_levels(n)` always sorts and
truncates to a configured depth, so using it with an arbitrarily large `n`
to fake a full dump would be an abuse of a method meant for "top N levels
to persist." `full_book()` returns defensive copies of the internal bid/ask
dicts, so callers can't mutate engine state through the returned objects.

## M2: Deterministic simulation + fault injection

### Lineage: a mini deterministic-simulation-testing (DST) harness

The design borrows three specific patterns from FoundationDB's and
TigerBeetle's testing methodology, scoped down to fit a single-process,
synchronous, no-network M2 milestone:

1. **Seed-based determinism**: the entire run (market dynamics, fault
   timing, fault types) is driven by one integer seed; same seed
   reproduces bit-for-bit. This is what makes a failing run replayable
   from just the seed, not from a saved event log.
2. **Differential/model-based testing**: a ground-truth oracle (the
   market simulator's own internal ladder, updated unconditionally,
   never touched by fault injection) runs alongside the real
   `BookEngine`. Correctness is bit-exact convergence of the engine to
   the oracle, not a hand-picked set of expected outputs.
3. **Property-based exploration on top of the deterministic scenarios**
   (Hypothesis): searches the space of seeds/fault-configs/step-counts
   for a combination the hand-written scenarios (S1-S8) didn't think of.

The full versions of these systems test entire distributed clusters
under simulated network partitions, disk corruption, and clock skew,
with a custom deterministic runtime replacing the OS scheduler. That's
out of scope here by design: M2 is single-process, fully synchronous, no
asyncio, no real network. Determinism first, so every later milestone
(especially the real async feed clients in M3/M4) can be tested against
this harness without the harness itself needing to be debugged under
concurrency.

### Two independent RNG streams from one seed

If `MarketSimulator` and `FaultInjector` shared one `random.Random`
instance, adding or removing any random draw in one component would
silently shift what the other draws on the *same* seed. Determinism
would hold in the narrow sense (same seed still reproduces *a* run) but
break in the useful sense (a harmless refactor changes what every
existing seed produces, making old bug reports unreproducible). Fix:
`derive_seed(seed, label)` hashes `f"{seed}:{label}"` through a throwaway
`random.Random` to produce an independent child seed per label; each
component takes an already-derived seed and constructs its own
`random.Random` internally, never a shared instance passed in from
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
lower-precedence one in a long run and nobody would notice. The S8
fault-storm test asserts every configured fault type actually *fired*
at least once, and the shadowed counts make starvation visible instead
of hidden if that assertion ever needs loosening.

One simplification: while an active window is suppressing delivery, no
other fault types are rolled *at all* that tick (not rolled-and-shadowed,
not rolled). Window suppression already determines delivery
unconditionally; computing "what would have happened instead" adds
noise, not information, for those ticks. Shadowed counts reflect ticks
where multiple ad hoc rolls genuinely competed, not ticks preempted by
an in-progress window.

### `@given`, not `RuleBasedStateMachine`

Hypothesis's stateful testing exists for when the tool needs to
*discover* an operation sequence by interleaving rule calls itself,
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
two new prices crossed *each other*, and when the existing spread was
at least `2 * spread_min`, they could. H1 found this within its
configured `max_examples=200`, shrunk to seed 1615, an all-zero
`FaultConfig` (proving it was a pure market-generation bug, unrelated to
fault injection), 60 steps. Fix: apply each side's change immediately
after generating it, so a same-tick second insert clamps against the
*current* (already-updated) opposite price instead of a stale snapshot.
This is exactly the class of bug this harness exists to catch (order
of generation vs. order of application silently diverging), and it was
caught by the property layer, not the eight hand-written scenarios,
none of which happened to construct this specific interleaving.

## M3: Live Binance feed client

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
after reconnect anyway, but "probably" isn't good enough for a coupling
this important. Defense in depth: don't rely on an emergent property
when an explicit one is one method call away.

### Backoff + full jitter (AWS lineage)

`delay = uniform(0, min(cap, base * 2**attempt))` is literally AWS's
"full jitter" formula from their well-known backoff blog post
(`base=0.5s, cap=30s` here). Rejected alternatives: no jitter (a fleet of
reconnecting clients would thunder-herd in lockstep against Binance after
any shared outage, not a concern at N=1 client, but a bad habit to
build); "equal jitter" (`cap/2 + uniform(0, min(cap, base*2**attempt)/2)`,
AWS's more conservative alternative) trades lower minimum delay for a
higher floor; full jitter's wider spread is the better fit for spreading
out reconnect load when doing this alone. `ConnectionManager` never
sleeps itself; it returns the delay for the caller to `await`, which is
what makes it testable with a fake clock and RNG (T1), no event loop
needed.

### Watchdog is message-based, not ping/pong-based

Rely on the `websockets` library's built-in ping/pong for protocol-level
keepalive (unchanged, out of our code entirely), but that only proves
the *transport* is alive, not that Binance is actually sending us book
updates. A watchdog that only checked ping/pong would miss a connection
that's technically open but has silently stopped producing depth events
(the specific failure mode this exists to catch). Implemented as
`asyncio.wait_for(ws.recv(), timeout=watchdog_timeout)` around every
receive: if `BTCUSDT@depth@100ms` (nominally an update every ~100ms)
goes quiet for `watchdog_timeout` (default 10s), we declare the
connection dead ourselves rather than trusting the transport layer's
opinion of its own health.

### Token bucket sizing: weight verified live, not from memory

`GET /api/v3/depth` weight was checked directly against
developers.binance.com at implementation time (not recalled from
training data, which can be stale and the table has changed over
Binance's API history): 5 at `limit<=100`, 25/50/250 at the higher
tiers. We fetch at `limit=100`: 5x `book.depth_levels=20`, comfortable
margin, cheapest tier. Overall budget (confirmed): 6000
REQUEST_WEIGHT/minute per IP, reported via
`X-MBX-USED-WEIGHT-(intervalNum)(intervalLetter)` response headers,
logged at debug level on *every* snapshot fetch (not just 429/418) so
real consumption is observable during A1 even when nothing's going
wrong. `capacity=10, refill_rate=0.5/sec` sizing rationale is in
`ratelimit.py`'s module docstring, next to the verified numbers it's
derived from.

The general pattern here (token-bucket rate limiting in front of REST
calls, respecting `Retry-After` on 429/418, logging used-weight headers
for observability) is standard practice in production crypto feed
handlers (cryptofeed, NautilusTrader, and similar open-source exchange
connectivity libraries all implement some form of it). Citing that as
"this is an established pattern, not something invented here," not as a
claim of having read those codebases line-by-line in this session.

`TokenBucket` itself stays fully synchronous (no `asyncio.sleep` inside):
it answers "how long" via `time_until_available()`, the caller in
`binance.py` does the actual `await asyncio.sleep(...)`. Same reasoning
as `ConnectionManager`: keeping the decision-making pure is what makes
T2 testable with a fake clock and zero event loop.

### Two separate retry loops, not one

`_fetch_snapshot()` has its own bounded retry loop for HTTP-level issues
(429/418, `Retry-After`-driven), separate from `_perform_resync()`'s
protocol-level retry loop (`SNAPSHOT_STALE`/`GAP_DETECTED`, capped at 20,
carried over from M2). Conflating them would mean a rate-limit backoff
retry burns down the same budget meant for protocol-level staleness
retries: two different failure classes with two different appropriate
retry budgets and backoff shapes, kept structurally separate rather than
sharing one counter that would silently mean different things depending
on which failure mode happened to fire first.

### `load_snapshot()` can return `GAP_DETECTED`, not just `SNAPSHOT_STALE`: caught before shipping in a test

First draft of `_perform_resync()` treated any non-`APPLIED` result as
"stale, retry", but `load_snapshot()`'s buffer-replay loop can also
return `GAP_DETECTED` (the snapshot itself was accepted, but a *later*
buffered event failed to chain during replay). That's a genuinely
different cause than staleness, even though the recovery action (fetch a
fresh snapshot) happens to be identical either way. Caught while
hand-tracing the scenario T5 was about to encode, fixed to log the
correct incident type for each cause before the test could quietly bake
in the wrong label as "expected" behavior.

### Bug found empirically: cold-start double resync

First live run against real Binance showed two `RESYNC_COMPLETED`
incidents at startup instead of one. Root cause: the constructor
pre-set `_resync_needed` for "cold start needs an initial sync", but
`run()`'s reconnect loop *already* calls `invalidate()` +
`resync_needed.set()` unconditionally on every connection including the
first. The pre-set let `_resync_worker` race ahead of the WS handshake:
it grabbed an empty buffer, fetched a snapshot, and completed a trivial
resync *before the websocket had even finished connecting*, which the
coupling rule then immediately discarded via `invalidate()` once the
real connection came up, forcing a second, real resync. Not incorrect
(no invariant violated, no bad state), but wasteful (an extra REST call
every cold start) and confusing in the logs. Fixed by removing the
redundant pre-set; `run()`'s existing per-connection trigger already
covers cold start correctly on its own.

### `events_buffered` in `RESYNC_COMPLETED` logs can undercount

It resets when `_perform_resync()` *starts*, but the reader loop and the
resync worker are separate coroutines: messages can buffer before the
resync task is even scheduled to run, and those don't get counted. It's
a debug-observability figure only; nothing in the apply/convergence logic
depends on its precision, and inflating its apparent precision would be
the kind of fake rigor this project explicitly rules out. Documented
plainly in the code rather than silently shipped as if exact.

### Windows signal handling: `add_signal_handler` with a `KeyboardInterrupt` fallback

`loop.add_signal_handler()` is POSIX-only, raising `NotImplementedError`
on Windows. `main()` tries it for both SIGINT and SIGTERM (works cleanly
on Linux, where this could plausibly run in CI or on a server) and falls
back to relying on `KeyboardInterrupt` propagating through the running
coroutine on Ctrl+C, caught by a `try/finally` around `run()`'s body that
unconditionally closes the websocket/HTTP client and logs final stats
regardless of which path triggered it.

Verified manually, not just reasoned about: git-bash/MSYS (this
project's default shell in the agent environment) does not appear to
attach a real Win32 console, so `GenerateConsoleCtrlEvent`-based Ctrl+C
simulation from that shell silently did nothing: confirmed down to a
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
every `heartbeat_interval_seconds` (default 30s, configurable). It
doesn't touch the incident-logging design, and is deliberately not tagged as an
`incident` in the structured log, since it isn't one; it's a liveness
pulse, checked in against `get_stats()`'s existing fields rather than
adding new bookkeeping.

### Guarding the "no lock needed" concurrency argument with a test, not just a comment

The reader loop and `_resync_worker` both call `BookEngine` methods
concurrently without a lock, safe only because no `BookEngine` method
contains an `await`: under asyncio's cooperative scheduling, each call
runs to completion with no yield point for the event loop to interleave
on. That argument used to live only in a comment, which nothing stops a
future change (M4's OKX logic landing in the same `book/` package,
for instance) from silently violating. `test_book_engine_has_no_async_methods`
(in M1's `test_book_engine.py`, since it protects an M1 invariant that
M3 merely depends on and documents) enforces it mechanically: it fails
loudly the moment any `BookEngine` method becomes a coroutine function.

## M4: OKX feed client + normalization layer

### The architecture-validation property, stated and verified

If the exchange-agnostic design from M1-M3 is real, adding a second
exchange should touch zero lines of `book/`, `connection.py`, or
`ratelimit.py`: the entire diff should be a new protocol adapter plus a
thin normalization layer plus tests. Verified with `git diff --stat`
before every commit in this milestone, not just claimed: every commit
message in this section states the exact file set touched, and
`book/`, `connection.py`, `ratelimit.py` show empty diffs across all of
them. `test_book_engine_has_no_async_methods` (M1, still guarding the
concurrency argument M3 depends on) passes unmodified throughout.

### `transport.py` extraction as its own commit, before any OKX code

`WebSocketLike`/`WebSocketConnector` lived in `binance.py`, but they're
generic to any websocket-based feed client: OKX needs the exact same
protocols. Importing them from `binance.py` would be backwards coupling
(OKX depending on Binance's module for a concept that predates both).
Done as a pure move in its own commit, verified zero behavior change
(full suite green before and after, `git diff --stat` shows only a
lift-and-shift), specifically so the refactor is reviewable in isolation
from the feature work that depends on it: "refactor, then feature" as
two separate diffs a reviewer can evaluate independently.

`transport.py` did gain one thing later in this milestone:
`WebSocketLike.send()`. Binance's client never sends anything
post-connect (pure consumer of the stream), but OKX must send
subscribe/unsubscribe/ping, and a real websocket connection has
`.send()` regardless of whether Binance's client happens to use it, so
extending the *shared* protocol (rather than forking an OKX-only
variant) is the accurate model. Required adding a no-op `send()` to
Binance's own test fixture (`FakeWebSocket`) to keep satisfying the
widened protocol, caught immediately by mypy, not silently.

### Normalization: `InstrumentId` on `TimestampedEvent`

A canonical `(exchange, symbol)` pairing carried on every event, so
M5's sink sees one schema across feeds instead of inferring which
exchange an event came from. Deliberately thin: `symbol` stays in each
exchange's own native format (`"BTCUSDT"` vs `"BTC-USDT"`): no
cross-exchange "these are the same instrument" mapping here, that's
M5's job, not built early on spec. `binance.py` touched in exactly one
place (constructing `TimestampedEvent` with
`InstrumentId("binance", self._symbol)`), the one deliberate
exception to keeping M3 frozen, chosen over an `Optional[InstrumentId]`
field because a half-tagged event stream would defeat the point of a
normalization layer.

### Event-driven resync for OKX, not Binance's loop-driven retry

Binance's `_fetch_snapshot()` is request/response: await a REST call,
get a `SnapshotEvent` back directly, inside a `for attempt in range(...)`
loop in `_perform_resync()`. OKX's snapshot arrives asynchronously as a
normal channel push, seen by the *reader loop*, not by whatever sent
the resubscribe request: a nested retry loop that "awaits a snapshot"
doesn't fit that shape. **Request/response protocols get loop-driven
retries; push protocols get event-driven ones.** `_resync_worker`
becomes a thin trigger (wait on `resync_needed`, send one rate-limited
resubscribe, go back to waiting) instead of an owner of the retry
count; the reader loop owns applying whatever snapshot eventually
arrives and re-triggers the worker on failure. The outer shape stays
shared (`run()` owns `ConnectionManager` + a persistent resync worker +
an inline reader loop, identical incident vocabulary, identical
`get_stats()` contract); only the retry *orchestration* differs,
because that's what actually fits an async-push protocol.

**Retry-counter semantics, explicit and tested:**
`self._snapshot_retry_count` increments on every failed resync attempt
(`SNAPSHOT_STALE` or `GAP_DETECTED`-during-replay), resets to 0 on
`APPLIED` and on every new connection (right after `invalidate()`), and
forces a full reconnect once it reaches `snapshot_retry_limit`. The
double reset point (success *and* new-connection) is what guarantees no
leakage across episodes: a resync storm at hour 3 gets a full fresh
budget, not whatever was left over from an unrelated storm at hour 1.
`test_retry_counter_resets_on_success_and_does_not_leak_across_episodes`
proves this directly: episode 1 needs exactly 3 attempts (2 stale + 1
valid) to converge, episode 2 (a later, unrelated gap) needs its own
full 3 attempts to exhaust the limit; if the counter had leaked,
episode 2 would exhaust in fewer.

**Bug caught in review before it shipped:** `_request_resubscribe()`
originally re-read `self._current_ws` across two separate `await`
points (the unsubscribe send, then the subscribe send). A disconnect
landing between them (`run()`'s `finally` clearing `self._current_ws`
to `None`) would crash the second send with `AttributeError` on `None`,
silently killing `_resync_worker` for the rest of the process's
life, since nothing awaits its result. Fixed by capturing the reference
once into a local and suppressing mid-flight failures (harmless: `run()`'s
own reconnect handling is already covering the disconnect, and the next
connection gets a fresh snapshot for free via the normal subscribe flow
regardless).

**Bug caught by the tests failing, not by review:** 4 of the 6 new
client tests initially failed identically: every scenario that
`ws.enqueue()`d a message *after* `client.run()` was already consuming
never saw it; `engine.last_applied_id` stayed frozen at whatever the
cold-start snapshot set. Root cause was in the test fixture, not the
client: `FakeOKXWebSocket.recv()` did `await asyncio.sleep(3600)` when
its queue was empty, and Python's `asyncio.sleep` can't be woken early:
calling `enqueue()` while `recv()` was already suspended inside that
sleep just appended to a list nothing was watching. Binance's fixtures
never hit this because every scripted message was queued *before*
`client.run()` started; this was the first test that needed to inject a
message into an already-running client. Fixed by waiting on an
`asyncio.Event` instead of a fixed sleep, set by `enqueue()`, `send()`'s
queued responses, and `close()`. Worth keeping as a reminder that a
fake's own concurrency model needs the same scrutiny as production
code: a bug in the harness produces the exact same symptom as a bug
in the thing it's testing.

### Keepalive: two private `_receive_message()` methods, no shared `Protocol`

Binance's watchdog is one `asyncio.wait_for(ws.recv(), timeout=...)`
call. OKX's is a two-stage dance (silence → send text `"ping"` → a
second silence window → dead). Considered a shared `KeepaliveStrategy`
Protocol both clients would implement; rejected for two exchanges with
this little actually-shared logic: rule of three, don't abstract for
two cases. Each client owns a private method with the same name and
role, zero shared code, called from that client's own reader loop.
**Revisit this if a third exchange needs a third keepalive shape**:
that's the concrete trigger for extracting a real abstraction, not a
hypothetical future-proofing exercise now.

The `"pong"` check happens on the raw string, before any `json.loads`
call, so a pong can never surface as a false `MALFORMED_MESSAGE`, the same
reasoning as Binance's malformed-message handling, applied one layer
earlier here because OKX's liveness signal is itself a non-JSON payload.

### Rate limiting: two `TokenBucket` instances, `ratelimit.py` unmodified

Verified against live OKX docs at implementation time (checked
2026-07-12; changelog confirms the `checksum`-deprecation note is dated
2026-06-23, cited with that date rather than just "deprecated" since
API behavior notes like this can go stale): subscribe/unsubscribe/login
capped at 480/hour per connection; connection *attempts* capped at
3/sec per IP. Both figures independently confirmed via a live fetch
against `developers.okx.com`, not recalled from training data.

- **Resubscribe ops**: `capacity=40, refill=480/3600`. This
  connection's only consumer of the hourly budget beyond the one-time
  initial subscribe is resubscribe (unsubscribe+subscribe = 2 ops per
  attempt), so sizing directly against the documented limit needs no
  further fractioning.
- **Connection attempts**: `capacity=3, refill=3/sec`, matching the
  documented per-IP limit directly. Worth having despite
  `ConnectionManager`'s backoff already making rapid reconnects
  unlikely in practice: the documented limit is a hard ceiling, not a
  suggestion, and `base_seconds=0.5` alone doesn't guarantee staying
  under it in a pathological instant-fail-instant-retry sequence.

Both reuse the exact `TokenBucket` class from M3 with different
constructor arguments; no modification to `ratelimit.py` (confirmed by
`git diff --stat` showing it untouched across every M4 commit).

### Service notice routed through the standard reconnect path via a local exception

OKX pushes `{"event":"notice","code":"64008"}` ahead of planned
maintenance disconnects. Detected in the reader loop, logs
`OKX_SERVICE_NOTICE`, then raises a local `_ServiceNoticeReconnect` so
`run()`'s except chain routes it through the *same* `disconnected()` →
backoff → reconnect path as any other disconnect (mirrors how a
`TimeoutError` signals a watchdog trip), but skips logging a redundant
`WS_DISCONNECTED` for the same event, since the notice already explains
why. Kept the same jittered backoff rather than an immediate retry:
if OKX is notifying broadly ahead of planned maintenance, other clients
are likely reconnecting around the same moment too, and jitter still
helps even for a "graceful", self-initiated reconnect.

### OKX message-routing decision tree

```
raw text received
├─ raw == "pong"?                         -> liveness only, no JSON parse
├─ json.loads fails?                      -> MALFORMED_MESSAGE, skip
└─ parsed as JSON:
   ├─ has "event" key (control-plane)?
   │  ├─ event in {"subscribe","unsubscribe"} -> ack, DEBUG log
   │  ├─ event == "notice", code == "64008"    -> OKX_SERVICE_NOTICE, proactive reconnect
   │  ├─ event == "error"                      -> OKX_SUBSCRIBE_ERROR (WARNING)
   │  └─ unrecognized event                    -> MALFORMED_MESSAGE (defensive)
   └─ has "arg"/"action"/"data" (channel push)?
      ├─ action == "snapshot" -> parse_book_snapshot() -> engine.load_snapshot()
      ├─ action == "update"   -> parse_book_update()   -> engine.apply_event()
      └─ unrecognized/malformed -> MALFORMED_MESSAGE
```

### What live OKX verification found that the milestone spec didn't anticipate

Connected directly to `wss://ws.okx.com:8443/ws/v5/public` and captured
real `books`-channel traffic rather than writing a parser from docs/
memory (same standard as the Binance fixtures, provenance notes in
`tests/fixtures/okx/README.md`):

- **Not in the original spec**: OKX price levels are 4-element
  `[price, qty, deprecated, numOrders]`, not Binance's 2-element
  `[price, qty]`. A parser written from memory of the docs alone could
  easily assume the Binance shape and silently index the wrong field.
  Pinned with its own named regression test (U7), not just incidental
  coverage inside the normal-update parsing test.
- **Specifically verified per the spec's request**: `prevSeqId == -1`
  on the snapshot message, confirmed live as a sentinel, not a real
  predecessor. Confirmed irrelevant too: `SnapshotEvent` has no
  `prev_id` field, so the sentinel is never consumed regardless of its
  value.
- **Could not verify, said so rather than guessing**: "equal seqId on
  no-change pushes" for the incremental `books` channel specifically
  (found this documented for the snapshot-style `books5`/`bbo-tbt`
  channels, not `books`). Noted instead that the generic chaining check
  handles this correctly by construction even if it occurs: a message
  where `final_id == prev_id` still passes `prev_id == last_applied_id`
  and just re-sets the checkpoint to the same value, no special-casing
  needed either way.
- **Confirmed, not just trusted from the milestone spec**: the 480/hour
  and 3/sec rate limits, and the text `"ping"`/`"pong"` keepalive
  recipe, all independently re-verified against live docs rather than
  taken as given.

### Binance vs OKX: protocol differences and where each is absorbed

| Concern | Binance | OKX | Absorbed in |
|---|---|---|---|
| Snapshot delivery | Separate REST call | First WS push after subscribe | Feed client (`_fetch_snapshot` vs automatic) |
| Resync trigger | Direct REST re-fetch | Unsubscribe + resubscribe | Feed client (`_perform_resync` vs `_request_resubscribe`) |
| Resync orchestration | Loop-driven (`for` + retry count) | Event-driven (`resync_needed` + reader-loop-owned) | Feed client control flow |
| Sequence fields | `U`/`u`, `prev_id = U-1` | `seqId`/`prevSeqId`, direct mapping | Feed client parsing layer only |
| Sequence chaining semantics | Contiguous partition (`+1`) | Chain-by-equality (`prevSeqId == prior seqId`) | `BookEngine`'s generic `prev_id`/`final_id` contract absorbs both without knowing which |
| Price level shape | `[price, qty]` (2-element) | `[price, qty, deprecated, numOrders]` (4-element) | Feed client parsing layer only |
| Keepalive | Protocol-level ping/pong (library) + message-based watchdog | Application-level text `"ping"`/`"pong"`, two-stage | Feed client `_receive_message`, no shared abstraction |
| Rate limiting | REST weight budget (6000/min) | Connection (3/sec) + op budget (480/hour) | Two `TokenBucket` instances per client, same class |
| Integrity field | N/A | `checksum` deprecated (fixed to 0), `seqId`/`prevSeqId` is official | Never referenced in either parser |
| Planned-maintenance signal | None | `{"event":"notice","code":"64008"}` | OKX reader loop only, routed through standard reconnect |

Every row on the OKX side lives in `okx.py`; every row on the Binance
side lives in `binance.py` (or was already there since M3); `book/`,
`connection.py`, `ratelimit.py` appear in neither column, by design and
by `git diff --stat`.

## M5: Multi-feed concurrency + Parquet sink

### Row format: one self-contained top-N snapshot per Parquet row

Each `SnapshotRow` is a full top-`depth_levels` book snapshot (flat,
per-level interleaved `{side}_price_{i}`/`{side}_qty_{i}` columns), written
event-driven on every `ApplyStatus.APPLIED` result, not a delta row that
depends on the row before it. Lineage: Tardis.dev's `book_snapshot` format
and NautilusTrader's Parquet catalog both use this shape for the same
reason we need it here: a delta-row format is more compact, but losing
one delta row (queue overflow, a crash mid-batch) corrupts every row after
it until the next full resync, whereas losing a self-contained snapshot
row just reduces sampling density at that instant. Given `BoundedRowQueue`
already has a real drop policy (below), "a drop is safe by construction"
was worth the extra bytes per row.

`bids`/`asks` come from `engine.top_levels()` called with no explicit `n`:
it defaults to the depth the engine was already constructed with, not a
second, independently-specified depth at the sink layer. Thin sides pad
with **null, not zero**: a zero-quantity level looks like a real level
that happens to have no size (and `apply_levels()` in `book/engine.py`
already uses zero-qty as its own "delete this level" sentinel on the wire),
reusing it for "no level exists here" would make a thin book
indistinguishable from a deleted one downstream. `build_schema()` pads
explicitly with `None` (P1 asserts this directly, not just that the
present levels round-trip correctly).

### `decimal128(18, 8)` for price and quantity, not `float` or `string`

Matches the decimal precision actually observed on the wire (Binance and
OKX both send `qty` strings with 8 decimal places in the golden fixtures
captured back in M3/M4, e.g. `"0.11117000"`) rather than an arbitrarily
chosen scale. `float` was never a candidate: `Decimal` is already the
type used everywhere upstream (`PriceLevel`, `book/engine.py`), so writing
`float` to the sink would silently reintroduce the exact rounding-error
class the rest of the pipeline was built to avoid. `string` would preserve
precision too but forces every downstream reader (pandas, DuckDB, a
backtester) to parse before it can do arithmetic: `decimal128` is exact
*and* directly usable. Verified byte-exact via round-trip (P1): write a
table with realistic 8-decimal prices, read it back, assert the `Decimal`
values compare equal, not merely assumed to be lossless because pyarrow
documents it that way.

### Sink architecture: one `ParquetSink`, one `BoundedRowQueue`, `drop_oldest`

A single sink task, fed by one bounded `asyncio.Queue` shared by every
feed client. Feed clients call `BoundedRowQueue.put()`, which is fully
synchronous (never awaits, never touches disk), so a slow or stalled
sink can never backpressure the reader loop that's parsing live exchange
messages. On overflow it evicts the oldest queued row before enqueueing
the new one (`overflow_policy: drop_oldest`, the only variant actually
implemented, see below), incrementing a `rows_dropped` counter surfaced
in both `get_stats()` and the new pipeline-wide heartbeat. This is the
first real logic behind the `OverflowPolicy` enum M0 only stubbed out;
`COALESCE` stays declared-but-unimplemented, and `config.py` now rejects
it at load time (`test_coalesce_overflow_policy_raises_config_error`)
instead of silently behaving like `drop_oldest` if someone selects it.

P3 proves the backpressure guarantee two ways: `put()` on a full queue of
5 synchronous calls completes in well under the sanity bound with no
consumer ever draining (standing in for an arbitrarily slow/stalled
sink, no separate async consumer task is needed to prove this, since
`put()`'s latency is independent of `get()` by construction), and the
surviving rows after eviction are the newest 3, in FIFO order.

### The finalization gap: from "up to an hour" to "at most one checkpoint interval"

The original design only finalized `.tmp → final` (via `Path.replace()`)
at hour rotation or graceful shutdown. An ungraceful crash (`kill -9`,
power loss, OOM) landing mid-hour would leave the entire hour's data in an
orphaned, non-Hive-visible `.tmp` file: up to ~1 hour of buffered data
lost with no declared recovery path. Flagged explicitly rather than
shipped silently.

Chose **checkpoint-based finalization** (`_RotatingWriter` finalizes at
the hour boundary *or* every `checkpoint_interval_seconds`, 5 minutes by
default, whichever comes first, checked lazily on each incoming batch)
over the two alternatives considered:

- **Accept and document the full-hour gap**: simplest, but a 5-min-quant
  portfolio project with an undefended hour-long data-loss window is a
  worse interview conversation than the modest complexity of fixing it.
- **Finalize every batch**: the tightest possible bound (loses at most
  one `flush_interval_seconds`, i.e. a few seconds), but at the default
  `batch_size`/`flush_interval` this means a new Parquet file every few
  seconds per `(exchange, symbol)` pair, a file-count explosion that hurts
  every downstream reader (more file-open overhead, worse compression
  ratio per file, slower Hive-style glob reads) for a durability bound
  far tighter than this pipeline actually needs.

5 minutes is the deliberate middle: bounds worst-case loss to a number
small enough to defend, without fragmenting output into thousands of
tiny files during a multi-hour run. The check itself costs nothing extra:
it rides on `write_batch()`, which was already being called on every
flush; there's no separate proactive timer, since during genuine silence
there's nothing new at risk to checkpoint anyway. `Path.replace()`
(atomic cross-platform, unlike bare `os.rename` which raises on Windows
if the target exists) is the only thing that ever produces the final
name, so a crash can only ever leave an orphaned `.tmp`, never a
partially-written file masquerading as a finalized one.

`test_p2_ungraceful_crash_loses_at_most_one_checkpoint_interval` is the
test this decision demanded: write 3 batches with a fake clock advanced
past `checkpoint_interval_seconds` between writes 1 and 2, then never call
`close()` (simulating `kill -9`). Asserts exactly one finalized,
independently-readable `part-*.parquet` file exists (checkpoint 1's data)
and exactly one orphaned `.tmp` exists (checkpoint 2's data, at risk,
but bounded, and not falsely readable as a complete file since
`ParquetWriter.close()` was never called to write its footer).

### `FeedSupervisor`, not `asyncio.TaskGroup`

`asyncio.TaskGroup` cancels every sibling task the instant one raises,
exactly the opposite of what a multi-feed pipeline needs. One exchange's
websocket hiccuping should never take the other exchange's feed down with
it. `FeedSupervisor` gives each of the two task kinds it owns (feed tasks,
the one sink task) the failure semantics that actually fit it:

- **Feed failures are expected.** A crashed feed factory gets restarted
  with the same AWS full-jitter backoff formula used for reconnects
  (`full_jitter_delay`, extracted below), up to `max_restarts` within a
  rolling `restart_window_seconds` window. Exceeding the budget marks the
  feed `PERMANENTLY_FAILED` and leaves it dead; every *other* feed and
  the sink keep running, which is the entire point of not using
  `TaskGroup`. `restart_count` only increments on a crash that actually
  leads to a restart, not on the final crash that gives up, so
  `max_restarts=2` means exactly 2 restarts are attempted (3 total calls
  to the factory), matching the name literally (P4).
- **Sink failure is process-critical.** Parquet writes are the only
  reason this process exists, so the sink is never restarted: a crash
  there triggers `request_shutdown()` via the same `asyncio.Event` used
  for external shutdown requests (Ctrl+C, SIGTERM), which cancels every
  feed task and lets `ParquetSink.run()`'s `finally` block flush and
  finalize whatever writers are open, rather than silently continuing to
  drop rows into a queue nobody is draining (P5).

P4 covers both a feed that permanently fails (isolated from its healthy
sibling and the sink) and a feed that crashes once and then recovers
within its restart budget: the recovered feed's state settles back to
`RUNNING`, not stuck in `RESTARTING`.

### `full_jitter_delay` extracted from `ConnectionManager.disconnected()`

`ConnectionManager` already implemented `uniform(0, min(cap, base *
2**attempt))` inline for reconnect backoff. `FeedSupervisor` needed the
identical formula for restart backoff, but restart backoff has no
`ConnectionManager` instance to attach to: it's keyed by feed name, not
by a single connection's state machine. Pulled the formula out to a
standalone function (`full_jitter_delay(policy, attempt, rng) ->
float`) rather than duplicating the arithmetic a second time or giving
`FeedSupervisor` a fake `ConnectionManager` it doesn't otherwise need.
`ConnectionManager.disconnected()` now calls the extracted function;
`test_connection.py`'s full T1 suite (8 tests) passes completely
unmodified, confirming the extraction changed nothing observable.

### `ts_exchange_ms` and row emission: extracted at the call site, not inside the parsers

`TimestampedEvent` gained an optional `ts_exchange_ms: int | None` field.
Binance's `E` field and OKX's `ts` field are read where each reader loop
already has the raw parsed `dict` in hand (`_extract_ts_exchange_ms()` in
each client), specifically *not* folded into `parse_diff_event()` /
`parse_book_update()`: those functions' signatures, and every test that
already pins them, stay untouched (confirmed via `git diff` showing zero
change to either function body). OKX's resync path gets a real exchange
timestamp (the snapshot push's own `"ts"` field, threaded through
`_handle_resync_result`); Binance's REST snapshot response carries no
timestamp field at all, so its resync-completion row honestly records
`ts_exchange_ms=None` rather than reusing the local receive time under a
field name that implies it came from the exchange.

Building a `SnapshotRow` from engine state is identical logic for both
exchanges, not a protocol-specific divergence like the keepalive split
in M4, so it lives once, as `build_snapshot_row()` in `envelope.py`
(already the shared normalization boundary), called from both clients
after every `ApplyStatus.APPLIED` result, whether that result came from
`apply_event()` (steady-state diff) or `load_snapshot()` (resync
completing). Each client holds an optional `row_queue: BoundedRowQueue |
None` constructor parameter (`None` by default, so every existing test
that constructs a client without one is unaffected), and calls
`row_queue.put(row)` directly, matching the design's "feed clients
enqueue non-blockingly" decision literally rather than through an
intermediate generic callback.

### Test evidence

Full suite: 87 tests (74 at the end of M4 plus 13 new: `test_parquet_sink.py`
×6 covering P1/P2/P3, `test_supervisor.py` ×5 covering P4/P5, the P6
end-to-end simulation test, and one new config-validation test), `mypy
--strict` clean across 38 source files, `ruff` clean. P6
(`test_p6_simulation_row_matches_oracle_top_n`) is the strongest single
piece of evidence for the whole milestone: drives a *real* `BookEngine`
through the M2 harness under an active fault mix (drops, duplicates,
reorders, disconnects, delayed snapshots; `injector.log` is asserted
non-empty, so this isn't a fault-free run coasting through), builds a row
from the converged engine state, pushes it through a *real* `ParquetSink`
(not a mock: the actual async run loop, actual `pyarrow` write, actual
file finalize on shutdown), reads the file back, and compares every
price/qty column against the M2 oracle's own top-N. Nothing in that chain
is faked.

**Handed off, not automated**: A3 (a 30-60 minute live dual-feed run
against real Binance/OKX) is the user's own manual verification step;
this milestone's automated suite proves the pipeline's *logic*, not that
it survives real network conditions unattended for an hour, which no
unit test can honestly claim.

## M6: Prometheus metrics + Grafana dashboard

### Two latency concepts, never conflated

`l2_processing_latency_seconds` (Histogram) and `l2_feed_lag_seconds`
(Gauge) measure genuinely different things and are never allowed to blur
into each other, in code or in the dashboard:

- **Processing latency** is `monotonic_ns` at frame receipt (the existing
  `ts_local_ns` capture point) to `monotonic_ns` immediately after
  `apply_event()` returns `APPLIED`. One clock, no cross-machine
  comparison: this is an honestly precise measurement of how long this
  process took to fold an update into the book. It's what the README's
  p50/p95/p99 figures come from.
- **Feed lag** is wall-clock `time.time()` now minus the exchange's own
  event timestamp (Binance's `E`, OKX's `ts`). Two different clocks on two
  different machines: this number is only ever as good as clock sync
  between here and the exchange, which is unverified and unverifiable
  from this side. The Gauge's own `HELP` text says so verbatim
  ("approximate; compares exchange timestamp against local wall clock;
  subject to clock skew"), not just a code comment: the honesty has to
  survive all the way to whoever reads the dashboard, not just whoever
  reads the source.

The interview answer for "so what latency did you actually measure?" is
this distinction stated plainly: one number is a precise, single-clock
measurement of this process's own cost; the other is a cross-clock,
approximate operational signal (rising lag + flat processing latency
means the problem is upstream, not here), and the code and the exposed
metric names make it impossible to accidentally quote one as the other.

Both are observed at exactly one call site per client (the steady-state
diff-apply branch in `_reader_loop`/`_handle_update_push`) and
deliberately *not* at resync completion: a resync involves a network
round-trip to fetch a snapshot, an entirely different, much larger
latency shape that would pollute a histogram whose buckets are tuned for
in-process dict mutation. Fewer call sites also means fewer places that
could drift out of sync with each other over time.

### `PipelineCollector`: read `get_stats()` at scrape time, don't double-instrument

The single instrumentation rule for this milestone: **counters that
`get_stats()` already tracks get read at scrape time by a custom
`prometheus_client.registry.Collector`, never incremented a second time
by a `prometheus_client.Counter` sitting next to the existing one.**
Two increment sites for the same fact is exactly the kind of drift that
produces a dashboard quietly disagreeing with the logs during an
incident, and M5's `get_stats()` was already the established single
source of truth for every feed client, the sink, and the supervisor, so
respecting that boundary rather than re-deriving it from scratch was the
only defensible choice.

The only thing that *can't* be reconstructed after the fact is a
per-event histogram observation: there's no `get_stats()` counter that
tells you the shape of a latency distribution after the fact, only its
count. That's the one and only exception: `Histogram.observe()` and
`Gauge.set()` for the two latency metrics happen directly in the feed
clients' hot path, at the single call site described above, nowhere else.
Interview framing: instrumentation belongs at the scrape boundary by
default; it only moves into the hot path when the data genuinely can't
survive until scrape time, and even then it should cost as little as
possible there (see below).

`PipelineCollector` deliberately does **not** expose every key
`get_stats()` happens to carry: each metric it produces is named and
fixed (`l2_messages_received_total`, `l2_gaps_detected_total`,
`l2_ws_reconnects_total`, `l2_watchdog_trips_total`,
`l2_resyncs_completed_total`, `l2_feed_restarts_total`, plus the
unlabeled sink counters/gauge), matching exactly what the dashboard
needs. A generic "expose every counter key as a label value" passthrough
was considered and rejected: it would let cardinality grow silently every
time a future incident counter gets added to a feed client, which
contradicts the fixed-and-small cardinality promise below. The cost is
that a new incident type needing its own panel requires a
`metrics.py` change too, an acceptable, explicit tradeoff.

### Cardinality: `exchange`/`symbol` plus one closed enum, nothing dynamic

Every per-feed metric is labeled by exactly `exchange` and `symbol`,
both drawn from config, both fixed at process startup, never a
user-controlled or unbounded value. `l2_feed_state` adds one more label,
but its four possible values are `FeedSupervisor.FeedState`'s own closed
Python enum, not open-ended, using `StateSetMetricFamily`'s "one series
per possible state, 1 for current, 0 for the rest" pattern (the standard
Prometheus state-set idiom, e.g. `l2_feed_state{exchange="binance",
symbol="BTCUSDT", l2_feed_state="running"} 1`). M6-3 pins this literally:
build a real `PipelineCollector` plus the two hot-path metrics, observe
for exactly the two configured `(exchange, symbol)` pairs, then scan
*every* sample this collector and both metrics produce and assert the
full set of `(exchange, symbol)` pairs found equals exactly the
configured set, nothing more, nothing fewer.

### Histogram buckets: log-spaced, 50us-100ms, 1-2-5 per decade

```python
buckets=(0.00005, 0.0001, 0.0002, 0.0005, 0.001, 0.002, 0.005,
         0.01, 0.02, 0.05, 0.1)
```

Processing latency here is dominated by in-process dict mutation
(`apply_levels`) and event-loop scheduling overhead, not I/O: mass is
expected in the tens-of-microseconds range with a tail into low
milliseconds under GC pauses or scheduling jitter. Log spacing gives even
relative resolution across that span instead of concentrating precision
in one order of magnitude that may not even be where the interesting
behavior lives. `0.1` (100ms) is a deliberately generous "something is
badly wrong" ceiling; everything worse collapses into the automatic
`+Inf` bucket, which is fine, since the goal at that point is detecting a
bad tail exists, not characterizing its exact shape. Explicitly a first
guess: retunable once A4's live run shows the real distribution.

### Hot-path cost: `Histogram.observe()`/`Gauge.set()` are two float ops, not I/O

Both calls are pure in-process Python/C: `observe()` walks the fixed
11-bucket array (a linear scan, negligible at this size) and increments
a couple of atomic counters; `set()` is a single assignment under a lock
internal to the `Gauge`. Neither touches the network, the filesystem, or
`asyncio` in any way: no `await`, no scheduling, no lock shared with
anything else in the reader loop. At Binance/OKX's real message rates
(low hundreds/sec per feed, not millions), this is immeasurably small
next to the JSON parsing and dict mutation already happening on the same
code path. No benchmark was run to produce a number here (M7's job, not
M6's), but the *reason* it's negligible is structural (no I/O, no lock
contention, fixed small bucket count), not an assumption resting on a
number nobody measured.

### Grafana as code: one `docker compose up`, zero manual clicking

`ops/docker-compose.yml` runs Prometheus + Grafana only; the app itself
runs on the host, not in the compose stack, so Prometheus reaches it via
`host.docker.internal:9100` (the documented route from a container to a
host-published port on Docker Desktop). An `extra_hosts:
host.docker.internal:host-gateway` entry is included so the same compose
file also resolves that hostname on plain Docker Engine (e.g. Linux CI),
even though the primary target is Docker Desktop on Windows per the
user's environment: no host-networking mode, no Linux-only assumptions,
explicit port mappings only (`9090` Prometheus, `3000` Grafana).

Grafana's datasource (pinned `uid: prometheus`, pointed at
`http://prometheus:9090`, compose's own DNS resolves the service name)
and the dashboard JSON are both auto-provisioned from files under
`ops/grafana/provisioning/`, committed and versioned like any other
config: no manual "add a datasource" or "import a dashboard" click
required after `docker compose up`. The dashboard itself
(`ops/grafana/dashboards/l2-pipeline.json`) is one screen, six panels:
processing latency p50/p95/p99, feed lag, messages/sec, incidents/sec
(reconnects/gaps/resyncs/watchdog trips), sink health (rows
written/dropped per sec + queue depth), and a state-timeline panel for
per-feed supervisor state.

**Verified, not just written**: `docker compose config` (structure/mount/
port validation, doesn't need the daemon) passes cleanly; every YAML file
parses; the dashboard JSON parses. A genuine live `docker compose up`
boot was **not** performed in this session: Docker Desktop's daemon
wasn't running in this environment, and starting it unprompted was out of
scope. That live verification, plus the real-traffic screenshot, is
exactly what A4 is for.

### Test evidence

`tests/unit/test_metrics.py`, 4 tests (M6-1 through M6-4): Collector
correctness (fake stats providers with known counter values -> `
generate_latest()` output parsed back and checked sample-by-sample
against expected values and labels), Histogram observation (six synthetic
durations spanning below the smallest bucket through beyond the largest
-> exact bucket counts, `_sum`, `_count` all checked), the cardinality
guard described above, and a real ephemeral-port server spin-up/scrape/
clean-shutdown round trip (`port=0`, read back `server.server_port`,
`urllib.request.urlopen` the real HTTP response, parse it as Prometheus
text, then `shutdown()`/`server_close()`/`thread.join()` and assert the
thread actually exited). Full suite: 91 tests (87 at the end of M5 + 4
new), `mypy --strict` clean across 40 source files, `ruff` clean.

### Heartbeats and metrics stay both, on purpose

M5's per-connection heartbeat log line and the pipeline-wide heartbeat
log line are both untouched by this milestone, and metrics don't replace
either. They serve different audiences and different failure modes:
structured logs are for incident forensics after the fact (grep a
specific `WATCHDOG_TRIPPED` at a specific timestamp, reconstruct exactly
what a single connection did), metrics/dashboards are for noticing a
trend or an anomaly *while it's happening*, at a glance, without reading
log lines at all. Removing either in favor of the other would trade away
one of those two, for no benefit.

## M7: Benchmark report, stress test, README

### Bug found by hands-on verification: exchange/symbol as both a Hive partition key and an in-file column

This is the third real bug this project's own verification discipline has
caught (after M1's off-by-one, caught by its own test suite, and M2's
Hypothesis-found `delayed_snapshot_prob` edge case), and notably, the
first one caught specifically by *manual* live-data verification (the
user's own A3/A4 pandas check) rather than an automated test, which is
exactly why that manual step was kept in the process instead of trusting
92 passing tests alone.

**Symptom**: `pandas.read_parquet("./data")` against a real ~30-minute
dual-feed run failed with
`pyarrow.lib.ArrowTypeError: Unable to merge: Field exchange has
incompatible types: string vs dictionary<values=string, indices=int32,
ordered=0>`, not old-vs-new schema drift (the run postdated the
`ts_wall_ns`/`schema_version=2` fix), a real correctness bug in current
code.

**Root cause, confirmed by direct reproduction**, not guessed: `exchange`
and `symbol` were stored *twice*: once as Hive partition-directory
segments (`exchange=binance/symbol=BTCUSDT/...`, required for the
partitioned layout) and once more as plain-`string` columns inside every
Parquet file (`build_schema()`'s original `exchange`/`symbol` fields).
`pyarrow.parquet.read_table()` and `pandas.read_parquet()` both default to
`partitioning='hive'` (confirmed directly from their signatures, not
assumed), meaning they *always* reconstruct partition-key columns from
the directory path as `dictionary<values=string, indices=int32>`
columns. Reading more than one partition together therefore always tries
to merge two genuinely different representations of the same column name:
the in-file `string` version and the path-inferred `dictionary` version.
A single-partition read never exercises the merge, which is why this
wasn't caught by any of the earlier single-file-scoped tests (P1, P2, P6
before this fix all read back only one file at a time). Reproduced
directly with a two-line repro (write two small partitions, call
`pq.read_table()` on the parent directory) before touching any fix code,
and confirmed the *opposite* case (no in-file `exchange`/`symbol`
columns) reads back cleanly with the same default call.

**Fix**: `build_schema()` no longer includes `exchange`/`symbol` as
in-file columns at all: they exist only as Hive partition keys, which is
also standard Hive/Spark/Trino/DuckDB convention (never duplicate a
partition column inside the file); the original design's redundancy was
the actual non-standard choice, not the fix. `SnapshotRow` (the in-memory
dataclass) keeps both fields unchanged, since `ParquetSink`/
`_RotatingWriter` still need them to group rows and build each file's
partition path; only the *on-disk Arrow schema* dropped them.
`SCHEMA_VERSION` bumped `2 -> 3`.

**Regression test** (`test_p2_multi_partition_tree_reads_back_via_default_hive_reader`):
writes across two exchanges *and* two file rotations (both conditions are
required to reproduce it, confirmed empirically that a single partition
or a single file never triggers the merge), then calls the exact
previously-failing `pq.read_table(dir)` and asserts it succeeds with
correct data. P1 and P6 also updated: P1 now asserts `exchange`/`symbol`
are absent from the in-file schema (asserting the fix, not just avoiding
breakage); P6 now reads via the directory (exercising real Hive
reconstruction) instead of a single file.

**Data implication, stated directly**: schema_version=2 files (with the
in-file columns) and schema_version=3 files (without) cannot be safely
mixed in the same tree: combining them reintroduces essentially the same
class of merge inconsistency, just between two different in-file
representations across files instead of one in-file vs one path-inferred.
Any existing collected data must be purged before recollecting under the
fixed schema; there is no in-place migration path that's worth building
for a portfolio project's dev-machine data.

### Bug found by the stress test itself: `ts_local_ns` (monotonic) fed to Parquet date/hour partitioning

A second real bug, found by M7's own stress-replay smoke test before any
sweep numbers were trusted: `_hour_key_for()` interpreted
`SnapshotRow.ts_local_ns` as epoch nanoseconds, but `ts_local_ns` is
`time.monotonic_ns()` (M3's deliberate, correct choice for latency
measurement, immune to NTP adjustments, guaranteed non-decreasing), with
an undefined, non-epoch reference point. On this machine that reference
point sits near system boot, so every partition date/hour this pipeline
had ever produced (M5 onward, including any real Binance/OKX run)
was silently wrong (`date=1970-01-19` instead of the real date), with no
error raised, since a monotonic value reinterpreted as epoch time is
still a valid-looking number.

Confirmed directly (`datetime.fromtimestamp(time.monotonic_ns() / 1e9)`
prints a 1970 date on this machine) before touching any code. Fixed by
adding `ts_wall_ns` (`time.time_ns()`, captured back-to-back with the
existing `ts_local_ns` capture at every reader-loop receipt point and
resync-completion point) as a genuinely separate field on
`TimestampedEvent`/`SnapshotRow`, used exclusively for partitioning;
`ts_local_ns` keeps its original, correct, latency-only role and is never
again asked to behave like a timestamp. `SCHEMA_VERSION` bumped `1 -> 2`
for this (then `2 -> 3` for the bug above). Regression test
(`test_p2_partitioning_uses_ts_wall_ns_not_ts_local_ns`) pins the exact
failure mode: a monotonic-shaped, deliberately non-epoch `ts_local_ns`
alongside a real `ts_wall_ns`, asserting the output lands under the real
date and specifically not under `date=1970*`.

Both bugs share a root cause worth naming explicitly: a field being
technically well-typed (an `int`, a `str`) said nothing about whether it
was being used for the *purpose* its value actually represented. Neither
mypy nor the existing test suite could catch either one, because both
were semantically wrong in a way indistinguishable from correct at the
type level; only running the real code against real multi-partition,
real-clock conditions surfaced them. This is the concrete argument for
why A3/A4-style manual verification stays a required part of the process
even at 91+ passing tests, not a formality.

## M8: ParquetSink stall livelock (production incident)

### Symptom, observed live during a multi-hour unattended soak run

`queue_depth` pegged at `queue_maxsize` (10000) and stayed there;
`rows_written`/`batches_flushed` froze at the exact same values for the
rest of the run (hours); `rows_dropped` climbed at the same rate
`messages_received` climbed on both feeds. Both `FeedSupervisor`
`feed_states` stayed `"running"` the entire time, no `"sink crashed"`
log line ever appeared, and the process never shut down on its own. The
freeze began immediately after a long gap in the terminal log consistent
with the host machine suspending (a multi-hour jump in wall-clock
timestamps between consecutive log lines, followed by DNS resolution
failures typical of a network interface resuming from sleep).

### Root cause: `asyncio.wait_for(timeout<=0)` never runs the wrapped coroutine

`ParquetSink.run()`'s loop computed
`remaining = flush_interval - (clock() - last_flush)` and passed
`max(remaining, 0.0)` as `wait_for`'s timeout. `asyncio.wait_for` with a
timeout `<= 0` wraps the coroutine via `ensure_future()` (which always
defers actual execution to the next event-loop tick, never runs
synchronously inline) and, if it isn't already done, cancels it and
raises `TimeoutError` immediately, without ever letting it run, even if
the underlying queue already has rows waiting. Because the buffer was
still empty at that call (nothing had been appended yet), `last_flush`
was never updated afterward either, so on every subsequent loop
iteration `remaining` stayed negative forever: a permanent livelock. The
event loop itself was never blocked (each iteration still yields via
task creation/cancellation), which is why feed clients kept working
normally throughout, and no exception was ever raised, which is why
`FeedSupervisor._run_sink()`'s existing crash handler (log + trigger
`request_shutdown()`, see M5) never fired: that handler correctly
catches a sink that *raises*, but a sink that hangs without raising is
invisible to it.

Confirmed directly, not just theorized: a regression test
(`test_p7_consume_recovers_after_a_stall_instead_of_livelocking`)
reproduces the exact precondition, a large clock jump between `run()`
setting `last_flush` and `_consume()`'s first iteration with a row
already queued, and fails against the original code (verified by
temporarily reverting the fix and re-running the test before restoring
it) while passing against the fix.

### Fix: never hand `wait_for` a non-positive timeout

Whenever `remaining <= 0`, `_consume()` now flushes any buffered rows
immediately (buffer or not) and resets `last_flush` right there, before
computing a timeout, so the next `wait_for` call always gets a fresh,
strictly positive window and `queue.get()` is guaranteed an actual
chance to run. This closes the livelock at its exact mechanism rather
than working around a symptom.

### Defense in depth: a stall watchdog, independent of the fix above

Per the same reasoning as M3's message-based watchdog ("don't rely on an
emergent property when an explicit one is one method call away"),
`ParquetSink.run()` now runs `_consume()` and a new `_watchdog()` as
`asyncio.TaskGroup` siblings, sharing fate (either one failing tears
down `run()` as a unit). `_watchdog()` tracks `_last_progress_at`
(updated whenever a row is actually pulled off the queue or a batch is
flushed) and raises if the queue is non-empty and no progress has
happened for `stall_timeout_seconds` (default 60s). A merely-idle,
empty queue is never treated as a stall, only a backed-up one with
nothing draining it.

`TaskGroup` (not `FeedSupervisor`) is the right tool here specifically
because `_consume()` and its own watchdog *should* share fate, the
opposite of M5's reasoning for why `FeedSupervisor` deliberately avoids
`TaskGroup` at the top level (there, independent feeds must NOT share
fate). Same library feature, applied at the scope its semantics actually
fit.

Because the watchdog raises rather than silently logging, the exception
propagates out of `run()` to `FeedSupervisor._run_sink()`'s *existing*
`except Exception:` handler unchanged, no changes needed there: a sink
hang is exactly the process-critical failure that path already exists to
catch, it simply never fired for a hang that raised nothing before now.

**Ruled out, not just assumed**: a swallowed exception (`_run_sink`'s
exception handling was re-read and is correct: it logs and triggers
shutdown for anything raised; nothing was ever raised, confirmed by the
total absence of a `"sink crashed"` log line across the entire multi-hour
freeze) and a disk-space edge case (the run's own recorded free space at
investigation time was low but nonzero, and the freeze's exact
onset, correlated with the sleep/resume gap and matching the livelock
mechanism byte-for-byte, is sufficient explanation without invoking disk
pressure).

**Known residual limitation, stated plainly**: `_watchdog()` is a
cooperative asyncio task on the same event loop as `_consume()`. If a
future change made `_flush()` (specifically `pq.ParquetWriter.write_table()`
or `Path.replace()`) block synchronously on a genuinely stalled disk
instead of returning promptly, that would freeze the entire event loop,
including the watchdog itself, since single-threaded asyncio cannot
preempt a synchronous call. The watchdog added here catches this
incident's actual failure mode (a livelock that keeps yielding to the
loop) and any future regression of the same shape, but a truly blocking
I/O hang would need `_flush()` moved onto a separate thread via
`loop.run_in_executor()` to remain detectable, which is real added
complexity deliberately not taken on without evidence it is needed.

### Test evidence

`tests/unit/test_parquet_sink.py`, 4 new tests (P7): the livelock
reproduction above; the watchdog raising on a genuine stall; the
watchdog *not* raising on legitimate idle emptiness (guarding against
false-positive shutdowns); and `run()` propagating a child task's
failure end-to-end via `TaskGroup` (verified with a monkeypatched
watchdog for a fast, deterministic trigger, independent of the timing-
sensitive livelock reproduction). Full suite: 97 tests (93 before this
milestone + 4 new), `mypy --strict` clean, `ruff` clean.

## M9: Binance reconnect wedge + unreachable supervisor escalation (production incident)

### Symptom, observed live during the same class of multi-hour soak run as M8

A second host suspend/resume gap (`soak_20260714_115958.log`, incident
onset 2026-07-15T03:46:52), this time hitting both feeds with identical
`gaierror: getaddrinfo failed` disconnects within the same few seconds.
OKX recovered on its own once real connectivity returned (`WS_RECONNECTED`
at 04:37:35, `messages_received` climbing steadily afterward). Binance
never recovered for the rest of the log, a span of 93 minutes and
counting at the point the log was captured: `connection_state` cycled
`connecting` <-> `backoff` on every ~30s heartbeat, `messages_received`
frozen at the exact same value throughout, no further `WS_RECONNECTED`
ever logged. Both `feed_states.binance` stayed `"running"` and
`feed_restart_counts.binance` stayed `0` the entire time -- the pipeline
heartbeat gave no indication anything was wrong, and `rows_written`/
`batches_flushed` kept climbing (OKX alone was enough to keep those
counters moving), which would have made the outage easy to miss glancing
only at the top-line heartbeat.

### Root cause 1: FeedSupervisor's restart path was structurally unreachable

`BinanceFeedClient.run()` and `OKXFeedClient.run()` each own a `while
True:` loop whose `except TimeoutError` / `except Exception` handlers
catch *every* connection failure internally and retry forever via
`ConnectionManager`'s backoff -- by design, per M3/M4 (an isolated
feed hiccup shouldn't need supervisor involvement). But this means
`run()` never raises for an ordinary reconnect failure, no matter how
many in a row. `FeedSupervisor._run_feed` (M5) can only restart a feed
when its factory *raises* -- so a feed stuck retrying indefinitely inside
its own loop was, and always had been, invisible to the one piece of code
whose entire job is noticing a feed that isn't recovering.

**Compounding, found in the same pass**: even if something had made
`run()` raise, the restart would have been broken. `app.py` registered
`client.run` -- a bound method of one already-constructed client -- as
the supervisor's "factory". `_run_feed`'s restart calls `entry.factory()`
again, which would replay `run()` on that *same* instance whose `finally`
block had already closed its owned httpx client and cancelled its
background tasks. A real restart would have crash-looped immediately,
burning through `max_restarts` and landing the feed in
`PERMANENTLY_FAILED` -- a strictly worse outcome than the silent wedge it
was meant to fix.

### Root cause 2: a second, independent asymmetry between the two clients

`OKXFeedClient._request_resubscribe()` wraps its sends in
`contextlib.suppress(Exception)`, with an explicit comment (M4) warning
why: an unguarded exception there would silently kill `_resync_worker`
for the rest of the process's life, since nothing awaits that
fire-and-forget task's result. `BinanceFeedClient._fetch_snapshot()` had
no equivalent guard around its `httpx` call -- a network-level failure
fetching the REST snapshot (the same disconnect class hitting the WS
side) would propagate unhandled through `_perform_resync()` straight out
of `_resync_worker()`, killing it silently. Confirmed, not just
theorized: reverting the fix and reproducing with a fake HTTP client that
fails its first three calls then succeeds, the un-caught `OSError`
resurfaces through `run()`'s own cleanup (`await resync_task` re-raising
the dead task's exception) instead of the resync ever completing.

`outage_duration_seconds` was also confirmed wrong by the same incident:
OKX logged `21.2` for a `WS_RECONNECTED` whose real gap (from the first
disconnect to the eventual reconnect) was ~50 minutes.
`ConnectionManager.disconnected()` overwrote `_disconnected_at` on
*every* call, not just the first disconnect of an episode -- a real
outage is a disconnect followed by however many failed retries (each
itself calling `disconnected()` again) before a connect finally
succeeds, so the field was measuring only the last retry's gap.

**Ruled out, not assumed**: whether Binance's WS handshake itself was
failing for an external reason (an exchange-side throttle triggered by
the reconnect burst, a longer-lived DNS condition specific to that
hostname) couldn't be established from the log alone, and this review
doesn't claim to have pinned that down -- it's plausible, and Binance's
client, unlike OKX's, has no per-attempt connection-rate limiter (OKX's
`_connection_bucket`, added in M4 against OKX's documented 3/sec-per-IP
limit) to prevent hammering reconnects during exactly this kind of burst.
That asymmetry is noted as a contributing factor, not fixed here, since
retrofitting a new rate limit changes behavior for every reconnect, not
just this failure mode -- out of scope for this pass. What *is* fixed,
and doesn't depend on ever identifying the external trigger: a feed that
can't reconnect must eventually escalate rather than retry forever
silently, and a forced restart must actually get a clean, working
instance when it does.

### Fix: an escalation budget in ConnectionManager's consumers, a real factory in app.py

Both feed clients now take a `max_consecutive_reconnect_failures`
parameter (default 10, `DEFAULT_MAX_CONSECUTIVE_RECONNECT_FAILURES` in
`connection.py`, shared rather than duplicated since it's the same policy
question for both). After every `disconnected()` call, `run()` checks
`ConnectionManager.attempt` (already tracked, already reset to 0 the
moment a message gets through on a live connection -- so this can never
fire for isolated disconnects sprinkled across an otherwise-healthy run,
only a genuine unbroken run of failures) and raises a new
`ReconnectBudgetExhausted` once the budget is exhausted, logging
`RECONNECT_BUDGET_EXHAUSTED`. This is the one exception deliberately
*not* swallowed by run()'s own except chain -- it propagates out of
`run()` entirely, finally giving `FeedSupervisor._run_feed`'s existing
restart-with-backoff logic (unchanged, unreachable until now) something
to catch.

`app.py` now builds each feed via a real factory (`_make_feed_factory`):
every call, including restarts, constructs a brand new client instance --
fresh `ConnectionManager`, fresh httpx client, fresh everything -- rather
than replaying `run()` on the one that just raised. `PipelineCollector`
needs a stats source that survives this: `FeedRegistration` now holds a
small `_CurrentFeedClient` indirection cell (updated by the factory on
every construction) instead of a fixed client reference, so metrics keep
reading the *live* instance across a restart instead of a dead one's
frozen counters.

`BinanceFeedClient._fetch_snapshot()` now catches exceptions around the
`httpx` call the same way it already handled 429/418: log
`SNAPSHOT_FETCH_NETWORK_ERROR`, count it, and retry within the existing
`http_retry_limit` loop -- a network failure degrades to the existing
`HttpRetryLimitExceeded` -> `SNAPSHOT_FETCH_FAILED` path (which
`_perform_resync` already handles by returning cleanly) instead of
crashing `_resync_worker`.

`ConnectionManager.disconnected()` now only sets `_disconnected_at` the
first time it fires after a connection, leaving it alone on every
subsequent retry within the same outage -- `outage_duration_seconds` now
spans the whole episode.

### Test evidence

Every regression test here was confirmed against the *original* code
first (same rigor as M8): `test_connection.py`'s new
`test_outage_duration_spans_the_whole_episode_not_just_the_last_retry`
fails pre-fix (asserts the full ~3000s span, gets the last retry's 21.2s)
and passes after. `test_reconnect_escalation.py` (new file) has three
tests -- Binance and OKX each raising `ReconnectBudgetExhausted` after a
scripted run of always-failing connects, and an end-to-end
`FeedSupervisor` + real `BinanceFeedClient` test proving the *fresh*
post-restart instance (not the wedged original) is what actually
recovers -- all three verified to fail (either a collection error, since
`ReconnectBudgetExhausted` didn't exist yet, or a real `TimeoutError`
from `run()` never raising) with the escalation checks reverted, and pass
with them restored.
`test_binance_control_loop.py`'s new
`test_t9_network_error_on_snapshot_fetch_does_not_kill_resync_worker`
scripts three network failures then a working response and asserts the
resync still completes; reverted to the original unguarded `_fetch_snapshot`,
it fails with the dead resync task's `OSError` resurfacing through
`run()`'s own cleanup, exactly matching the failure mode this fix closes.
Full suite: 102 tests (97 before this milestone + 5 new), `mypy --strict`
clean, `ruff` clean.

