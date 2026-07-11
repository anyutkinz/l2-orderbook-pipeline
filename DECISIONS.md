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

