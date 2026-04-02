from __future__ import annotations

import logging


def configure_logging() -> None:
    """Configure consistent application logging."""
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=logging.INFO,
    )
