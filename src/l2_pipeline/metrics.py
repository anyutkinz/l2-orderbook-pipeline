from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from http.server import HTTPServer
from threading import Thread
from typing import Any, Protocol

from prometheus_client import CollectorRegistry, Gauge, Histogram, start_http_server
from prometheus_client.core import (
    CounterMetricFamily,
    GaugeMetricFamily,
    Metric,
    StateSetMetricFamily,
)
from prometheus_client.registry import Collector

from l2_pipeline.supervisor import FeedState, FeedSupervisor

# Log-spaced, 1-2-5 per decade, 50us -> 100ms: latency here is dominated by
# in-process dict mutation (apply_levels) and event-loop overhead, not I/O,
# so mass is expected in the tens-of-us range with a tail into low-ms under
# GC pauses or scheduling jitter. 100ms is a deliberately generous "something
# is badly wrong" ceiling -- everything worse collapses into +Inf, which is
# fine since the goal is detecting a bad tail, not characterizing it. First
# guess, retunable after the A4 live run.
PROCESSING_LATENCY_BUCKETS = (
    0.00005,
    0.0001,
    0.0002,
    0.0005,
    0.001,
    0.002,
    0.005,
    0.01,
    0.02,
    0.05,
    0.1,
)

_FEED_COUNTER_SPECS = (
    ("l2_messages_received_total", "messages_received", "Messages received, per feed."),
    ("l2_gaps_detected_total", "gap_detected", "Sequence gaps detected, per feed."),
    ("l2_ws_reconnects_total", "ws_reconnected", "Websocket (re)connections, per feed."),
    ("l2_watchdog_trips_total", "watchdog_tripped", "Watchdog timeouts, per feed."),
    ("l2_resyncs_completed_total", "resync_completed", "Successful resyncs, per feed."),
)


class FeedStatsProvider(Protocol):
    """Matches BinanceFeedClient/OKXFeedClient's public shape. metrics.py
    doesn't import either client -- same decoupling as app.py's own
    _FeedClient Protocol.
    """

    def get_stats(self) -> dict[str, Any]: ...


class SinkStatsProvider(Protocol):
    def get_stats(self) -> dict[str, Any]: ...


@dataclass(frozen=True, slots=True)
class FeedRegistration:
    exchange: str
    symbol: str
    provider: FeedStatsProvider


def build_processing_latency_histogram(registry: CollectorRegistry) -> Histogram:
    return Histogram(
        "l2_processing_latency_seconds",
        "Time from frame receipt (ts_local capture) to an APPLIED diff-apply "
        "result, on one monotonic clock -- honestly precise, no cross-clock "
        "skew. Only observed for steady-state diff applies, not resync "
        "completions (a different, network-bound latency shape that would "
        "pollute this histogram's buckets).",
        labelnames=("exchange", "symbol"),
        buckets=PROCESSING_LATENCY_BUCKETS,
        registry=registry,
    )


def build_feed_lag_gauge(registry: CollectorRegistry) -> Gauge:
    return Gauge(
        "l2_feed_lag_seconds",
        "Approximate; compares exchange timestamp against local wall clock; subject to clock skew.",
        labelnames=("exchange", "symbol"),
        registry=registry,
    )


class PipelineCollector(Collector):
    """Reads get_stats() from every feed client, the sink, and the
    supervisor at scrape time -- zero duplicated increment sites, M5's
    counters stay the single source of truth. Deliberately does not expose
    every key get_stats() happens to carry: each metric here is named and
    fixed, matching exactly what the dashboard needs, so cardinality can't
    grow just because a future incident counter gets added to a client.
    """

    def __init__(
        self,
        feeds: list[FeedRegistration],
        sink: SinkStatsProvider,
        supervisor: FeedSupervisor,
    ) -> None:
        self._feeds = feeds
        self._sink = sink
        self._supervisor = supervisor

    def collect(self) -> Iterable[Metric]:
        yield from self._feed_counters()
        yield self._feed_state()
        yield from self._sink_metrics()

    def _feed_counters(self) -> Iterable[Metric]:
        for metric_name, counter_key, help_text in _FEED_COUNTER_SPECS:
            family = CounterMetricFamily(metric_name, help_text, labels=("exchange", "symbol"))
            for feed in self._feeds:
                counters = feed.provider.get_stats().get("counters", {})
                family.add_metric([feed.exchange, feed.symbol], counters.get(counter_key, 0))
            yield family

        restarts = CounterMetricFamily(
            "l2_feed_restarts_total", "Restarts performed, per feed.", labels=("exchange", "symbol")
        )
        for feed in self._feeds:
            restarts.add_metric(
                [feed.exchange, feed.symbol], self._supervisor.restart_count(feed.exchange)
            )
        yield restarts

    def _feed_state(self) -> Metric:
        family = StateSetMetricFamily(
            "l2_feed_state",
            "Current supervisor-tracked state, per feed.",
            labels=("exchange", "symbol"),
        )
        for feed in self._feeds:
            current = self._supervisor.feed_state(feed.exchange)
            family.add_metric(
                [feed.exchange, feed.symbol],
                {state.value: state is current for state in FeedState},
            )
        return family

    def _sink_metrics(self) -> Iterable[Metric]:
        stats = self._sink.get_stats()
        counters = stats.get("counters", {})

        written = CounterMetricFamily("l2_sink_rows_written_total", "Rows written by the sink.")
        written.add_metric([], counters.get("rows_written", 0))
        yield written

        dropped = CounterMetricFamily(
            "l2_sink_rows_dropped_total", "Rows dropped by the bounded queue on overflow."
        )
        dropped.add_metric([], stats.get("rows_dropped", 0))
        yield dropped

        depth = GaugeMetricFamily(
            "l2_sink_queue_depth", "Current depth of the single shared row queue."
        )
        depth.add_metric([], stats.get("queue_depth", 0))
        yield depth


def start_metrics_server(port: int, registry: CollectorRegistry) -> tuple[HTTPServer, Thread]:
    """Runs prometheus_client's own WSGI server in a daemon thread -- never
    touches the asyncio event loop. Returns (server, thread) so callers
    (and tests, via port=0 for an ephemeral bind) can shut it down cleanly:
    server.shutdown(); server.server_close(); thread.join().
    """
    return start_http_server(port, registry=registry)
