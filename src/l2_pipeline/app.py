from __future__ import annotations

import argparse
import logging
from pathlib import Path

from l2_pipeline.config import load_config
from l2_pipeline.logging_setup import configure_logging

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="L2 order book ingestion pipeline")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/default.yaml"),
        help="Path to config YAML",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    configure_logging(config.logging.level, config.logging.format)
    logger.info(
        "config loaded",
        extra={"extra_fields": {"exchanges": [e.name for e in config.exchanges]}},
    )


if __name__ == "__main__":
    main()
