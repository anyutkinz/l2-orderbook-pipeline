from __future__ import annotations

import enum
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


class ConfigError(Exception):
    """Raised when a config file is missing, malformed, or structurally invalid."""


class OverflowPolicy(enum.Enum):
    DROP_OLDEST = "drop_oldest"
    COALESCE = "coalesce"


@dataclass(frozen=True, slots=True)
class ExchangeConfig:
    name: str
    symbols: list[str]
    ws_url: str
    rest_snapshot_url: str | None = None


@dataclass(frozen=True, slots=True)
class BookConfig:
    depth_levels: int = 20


@dataclass(frozen=True, slots=True)
class OutputConfig:
    parquet_dir: Path
    queue_maxsize: int = 10_000
    overflow_policy: OverflowPolicy = OverflowPolicy.DROP_OLDEST


@dataclass(frozen=True, slots=True)
class MetricsConfig:
    prometheus_port: int = 9100


@dataclass(frozen=True, slots=True)
class LoggingConfig:
    level: str = "INFO"
    format: str = "json"


@dataclass(frozen=True, slots=True)
class AppConfig:
    exchanges: list[ExchangeConfig]
    book: BookConfig
    output: OutputConfig
    metrics: MetricsConfig
    logging: LoggingConfig


def load_config(path: Path) -> AppConfig:
    try:
        text = path.read_text()
    except FileNotFoundError as exc:
        raise ConfigError(f"config file not found: {path}") from exc

    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ConfigError(f"malformed YAML in {path}: {exc}") from exc

    if not isinstance(raw, dict):
        raise ConfigError(f"config root must be a mapping, got {type(raw).__name__}")

    try:
        exchanges = [_parse_exchange(e) for e in raw["exchanges"]]
        book = BookConfig(**raw.get("book", {}))
        output = _parse_output(raw["output"])
        metrics = MetricsConfig(**raw.get("metrics", {}))
        logging_cfg = LoggingConfig(**raw.get("logging", {}))
    except (KeyError, TypeError) as exc:
        raise ConfigError(f"invalid config structure: {exc}") from exc

    if not exchanges:
        raise ConfigError("at least one exchange must be configured")
    for exch in exchanges:
        if not exch.symbols:
            raise ConfigError(f"exchange {exch.name!r} has no symbols configured")
    if book.depth_levels <= 0:
        raise ConfigError("book.depth_levels must be positive")

    return AppConfig(
        exchanges=exchanges,
        book=book,
        output=output,
        metrics=metrics,
        logging=logging_cfg,
    )


def _parse_exchange(raw: dict[str, Any]) -> ExchangeConfig:
    return ExchangeConfig(
        name=raw["name"],
        symbols=list(raw["symbols"]),
        ws_url=raw["ws_url"],
        rest_snapshot_url=raw.get("rest_snapshot_url"),
    )


def _parse_output(raw: dict[str, Any]) -> OutputConfig:
    policy_str = raw.get("overflow_policy", OverflowPolicy.DROP_OLDEST.value)
    try:
        policy = OverflowPolicy(policy_str)
    except ValueError as exc:
        valid = ", ".join(p.value for p in OverflowPolicy)
        raise ConfigError(
            f"invalid overflow_policy {policy_str!r}, must be one of: {valid}"
        ) from exc
    return OutputConfig(
        parquet_dir=Path(raw["parquet_dir"]),
        queue_maxsize=raw.get("queue_maxsize", 10_000),
        overflow_policy=policy,
    )
