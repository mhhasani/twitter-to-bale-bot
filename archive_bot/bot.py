from __future__ import annotations

import logging

from app.application import ArchiveBotApp
from app.logging_setup import configure_logging

logger = logging.getLogger(__name__)


def main() -> None:
    configure_logging()

    try:
        app = ArchiveBotApp()
        app.run()
    except KeyboardInterrupt:
        logger.info("⛔ Bot stopped by user")
    except Exception as exc:
        logger.error("❌ Bot startup failed: %s", exc)
        raise


if __name__ == "__main__":
    main()
