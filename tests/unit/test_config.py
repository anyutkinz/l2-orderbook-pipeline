from __future__ import annotations

from pathlib import Path

import pytest

from l2_pipeline.config import ConfigError, OverflowPolicy, load_config

VALID_YAML = """
exchanges:
  - name: binance
    symbols: ["BTCUSDT"]
    ws_url: "wss://stream.binance.com:9443/ws"
    rest_snapshot_url: "https://api.binance.com/api/v3/depth"
  - name: okx
    symbols: ["BTC-USDT"]
    ws_url: "wss://ws.okx.com:8443/ws/v5/public"

book:
  depth_levels: 20

output:
  parquet_dir: "./data"
  queue_maxsize: 5000
  overflow_policy: "drop_oldest"

metrics:
  prometheus_port: 9100

logging:
  level: "INFO"
  format: "json"
"""


def _write(tmp_path: Path, content: str) -> Path:
    config_file = tmp_path / "config.yaml"
    config_file.write_text(content)
    return config_file


def test_load_valid_config(tmp_path: Path) -> None:
    config = load_config(_write(tmp_path, VALID_YAML))

    assert [e.name for e in config.exchanges] == ["binance", "okx"]
    assert config.exchanges[0].symbols == ["BTCUSDT"]
    assert config.exchanges[1].rest_snapshot_url is None
    assert config.book.depth_levels == 20
    assert config.output.overflow_policy is OverflowPolicy.DROP_OLDEST


def test_missing_file_raises_config_error(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "missing.yaml")


def test_malformed_yaml_raises_config_error(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="malformed YAML"):
        load_config(_write(tmp_path, "exchanges: [unclosed"))


def test_no_exchanges_raises_config_error(tmp_path: Path) -> None:
    content = """
exchanges: []
book: {}
output:
  parquet_dir: "./data"
metrics: {}
logging: {}
"""
    with pytest.raises(ConfigError, match="at least one exchange"):
        load_config(_write(tmp_path, content))


def test_exchange_with_no_symbols_raises_config_error(tmp_path: Path) -> None:
    content = """
exchanges:
  - name: binance
    symbols: []
    ws_url: "wss://stream.binance.com:9443/ws"
book: {}
output:
  parquet_dir: "./data"
metrics: {}
logging: {}
"""
    with pytest.raises(ConfigError, match="no symbols"):
        load_config(_write(tmp_path, content))


def test_invalid_overflow_policy_raises_config_error(tmp_path: Path) -> None:
    content = """
exchanges:
  - name: binance
    symbols: ["BTCUSDT"]
    ws_url: "wss://stream.binance.com:9443/ws"
book: {}
output:
  parquet_dir: "./data"
  overflow_policy: "explode"
metrics: {}
logging: {}
"""
    with pytest.raises(ConfigError, match="invalid overflow_policy"):
        load_config(_write(tmp_path, content))


def test_coalesce_overflow_policy_raises_config_error(tmp_path: Path) -> None:
    """coalesce is a declared-but-unimplemented placeholder (M0 decision) --
    BoundedRowQueue (M5) only implements drop_oldest, so selecting it must
    fail loudly at config load rather than silently behaving like
    drop_oldest at runtime.
    """
    content = """
exchanges:
  - name: binance
    symbols: ["BTCUSDT"]
    ws_url: "wss://stream.binance.com:9443/ws"
book: {}
output:
  parquet_dir: "./data"
  overflow_policy: "coalesce"
metrics: {}
logging: {}
"""
    with pytest.raises(ConfigError, match="not yet implemented"):
        load_config(_write(tmp_path, content))


def test_non_mapping_root_raises_config_error(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="must be a mapping"):
        load_config(_write(tmp_path, "- just\n- a\n- list\n"))
