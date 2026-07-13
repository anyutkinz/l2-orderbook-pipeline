from __future__ import annotations

import urllib.request
from typing import Any

import pytest
from prometheus_client import CollectorRegistry, generate_latest
from prometheus_client.parser import text_string_to_metric_families

from l2_pipeline.feeds.connection import BackoffPolicy
from l2_pipeline.metrics import (
    FeedRegistration,
    PipelineCollector,
    build_feed_lag_gauge,
    build_processing_latency_histogram,
    start_metrics_server,
)
from l2_pipeline.supervisor import FeedSupervisor


class _FakeRng:
    def uniform(self, a: float, b: float) -> float:
        return a


class _FakeFeedProvider:
    def __init__(self, counters: dict[str, int]) -> None:
        self._counters = counters

    def get_stats(self) -> dict[str, Any]:
        return {"counters": dict(self._counters)}


class _FakeSink:
    def __init__(self, rows_written: int, rows_dropped: int, queue_depth: int) -> None:
        self._rows_written = rows_written
        self._rows_dropped = rows_dropped
        self._queue_depth = queue_depth

    def get_stats(self) -> dict[str, Any]:
        return {
            "counters": {"rows_written": self._rows_written, "batches_flushed": 3},
            "rows_dropped": self._rows_dropped,
            "queue_depth": self._queue_depth,
        }


async def _noop() -> None:
    return None


def _build_supervisor(exchanges: list[str]) -> FeedSupervisor:
    supervisor = FeedSupervisor(BackoffPolicy(), _FakeRng())
    for name in exchanges:
        supervisor.add_feed(name, _noop)
    supervisor.set_sink(_noop)
    return supervisor


def _parsed_samples(
    registry: CollectorRegistry,
) -> dict[tuple[str, tuple[tuple[str, str], ...]], float]:
    text = generate_latest(registry).decode()
    samples: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
    for family in text_string_to_metric_families(text):
        for sample in family.samples:
            samples[(sample.name, tuple(sorted(sample.labels.items())))] = sample.value
    return samples


# M6-1: Collector correctness
def test_m6_1_collector_exposes_expected_samples_with_correct_labels() -> None:
    binance_provider = _FakeFeedProvider(
        {
            "messages_received": 42,
            "gap_detected": 2,
            "ws_reconnected": 1,
            "watchdog_tripped": 0,
            "resync_completed": 1,
        }
    )
    okx_provider = _FakeFeedProvider(
        {
            "messages_received": 17,
            "gap_detected": 0,
            "ws_reconnected": 3,
            "watchdog_tripped": 1,
            "resync_completed": 2,
        }
    )
    sink = _FakeSink(rows_written=100, rows_dropped=5, queue_depth=12)
    supervisor = _build_supervisor(["binance", "okx"])

    registry = CollectorRegistry()
    registry.register(
        PipelineCollector(
            feeds=[
                FeedRegistration("binance", "BTCUSDT", binance_provider),
                FeedRegistration("okx", "BTC-USDT", okx_provider),
            ],
            sink=sink,
            supervisor=supervisor,
        )
    )

    samples = _parsed_samples(registry)

    assert (
        samples[("l2_messages_received_total", (("exchange", "binance"), ("symbol", "BTCUSDT")))]
        == 42
    )
    assert (
        samples[("l2_messages_received_total", (("exchange", "okx"), ("symbol", "BTC-USDT")))] == 17
    )
    assert (
        samples[("l2_gaps_detected_total", (("exchange", "binance"), ("symbol", "BTCUSDT")))] == 2
    )
    assert samples[("l2_ws_reconnects_total", (("exchange", "okx"), ("symbol", "BTC-USDT")))] == 3
    assert samples[("l2_watchdog_trips_total", (("exchange", "okx"), ("symbol", "BTC-USDT")))] == 1
    assert (
        samples[("l2_resyncs_completed_total", (("exchange", "binance"), ("symbol", "BTCUSDT")))]
        == 1
    )
    # never restarted -- supervisor.add_feed() was called but run() never was
    assert (
        samples[("l2_feed_restarts_total", (("exchange", "binance"), ("symbol", "BTCUSDT")))] == 0
    )

    assert samples[("l2_sink_rows_written_total", ())] == 100
    assert samples[("l2_sink_rows_dropped_total", ())] == 5
    assert samples[("l2_sink_queue_depth", ())] == 12

    running = (("exchange", "binance"), ("l2_feed_state", "running"), ("symbol", "BTCUSDT"))
    restarting = (("exchange", "binance"), ("l2_feed_state", "restarting"), ("symbol", "BTCUSDT"))
    assert samples[("l2_feed_state", running)] == 1.0
    assert samples[("l2_feed_state", restarting)] == 0.0


# M6-2: Histogram observation
def test_m6_2_histogram_bucket_counts_and_sum_match_expectations() -> None:
    registry = CollectorRegistry()
    hist = build_processing_latency_histogram(registry)
    # spans below the smallest bucket through beyond the largest real bucket
    durations = [0.00003, 0.00007, 0.0003, 0.003, 0.03, 0.2]
    for d in durations:
        hist.labels(exchange="binance", symbol="BTCUSDT").observe(d)

    samples = _parsed_samples(registry)
    labels = (("exchange", "binance"), ("symbol", "BTCUSDT"))

    assert samples[("l2_processing_latency_seconds_count", labels)] == len(durations)
    assert samples[("l2_processing_latency_seconds_sum", labels)] == pytest.approx(sum(durations))

    def bucket(le: str) -> float:
        key = tuple(sorted((*labels, ("le", le))))
        return samples[("l2_processing_latency_seconds_bucket", key)]

    assert bucket("0.0001") == 2  # 0.00003, 0.00007
    assert bucket("0.001") == 3  # + 0.0003
    assert bucket("0.1") == 5  # everything except 0.2
    assert bucket("+Inf") == len(durations)


# M6-3: Cardinality guard
def test_m6_3_cardinality_matches_exactly_the_configured_exchange_symbol_pairs() -> None:
    configured_pairs = {("binance", "BTCUSDT"), ("okx", "BTC-USDT")}

    registry = CollectorRegistry()
    hist = build_processing_latency_histogram(registry)
    gauge = build_feed_lag_gauge(registry)
    for exchange, symbol in configured_pairs:
        hist.labels(exchange=exchange, symbol=symbol).observe(0.001)
        gauge.labels(exchange=exchange, symbol=symbol).set(0.5)

    sink = _FakeSink(rows_written=1, rows_dropped=0, queue_depth=0)
    supervisor = _build_supervisor(["binance", "okx"])
    registry.register(
        PipelineCollector(
            feeds=[
                FeedRegistration("binance", "BTCUSDT", _FakeFeedProvider({"messages_received": 1})),
                FeedRegistration("okx", "BTC-USDT", _FakeFeedProvider({"messages_received": 1})),
            ],
            sink=sink,
            supervisor=supervisor,
        )
    )

    text = generate_latest(registry).decode()
    observed_pairs: set[tuple[str, str]] = set()
    for family in text_string_to_metric_families(text):
        for sample in family.samples:
            if "exchange" in sample.labels and "symbol" in sample.labels:
                observed_pairs.add((sample.labels["exchange"], sample.labels["symbol"]))

    assert observed_pairs == configured_pairs


# M6-4: metrics endpoint
def test_m6_4_metrics_endpoint_serves_parseable_prometheus_text() -> None:
    registry = CollectorRegistry()
    hist = build_processing_latency_histogram(registry)
    hist.labels(exchange="binance", symbol="BTCUSDT").observe(0.001)

    server, thread = start_metrics_server(0, registry)  # port=0 -> OS-assigned ephemeral port
    try:
        port = server.server_port
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as response:
            body = response.read().decode()

        families = list(text_string_to_metric_families(body))
        assert any(family.name == "l2_processing_latency_seconds" for family in families)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive()
