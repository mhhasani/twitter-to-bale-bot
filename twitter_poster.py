"""
توییتر → بله (با تصویر)
هر چند دقیقه تایملاین توییتر رو می‌کشه و توییت‌های جدید فارسی رو
به صورت عکس (اسکرین‌شات واقعی توییتر) همزمان به کانال بله می‌فرسته.

راه‌اندازی اولیه:
  python twitter_poster.py --setup

اجرای عادی:
  python twitter_poster.py
"""

import asyncio
import json
import logging
import os
import sys
from pathlib import Path

import httpx
from dotenv import load_dotenv

from tweet_image import screenshot_timeline

load_dotenv()

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ─── تنظیمات ────────────────────────────────────────────────────────────────
BOT_TOKEN  = os.getenv("BALE_BOT_TOKEN")
CHANNEL_ID = os.getenv("TWITTER_CHANNEL_ID")

COOKIES_FILE   = Path("twitter_cookies.json")
SEEN_FILE      = Path("twitter_seen.json")
FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL", "300"))

BALE_API = f"https://tapi.bale.ai/bot{BOT_TOKEN}"
# ─────────────────────────────────────────────────────────────────────────────


def _load_seen() -> set:
    if SEEN_FILE.exists():
        return set(json.loads(SEEN_FILE.read_text()))
    return set()


def _save_seen(seen: set):
    SEEN_FILE.write_text(json.dumps(list(seen)[-5000:]))


async def _send_photo_to_bale(img_bytes: bytes) -> bool:
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                r = await client.post(
                    f"{BALE_API}/sendPhoto",
                    data={"chat_id": CHANNEL_ID},
                    files={"photo": ("tweet.png", img_bytes, "image/png")},
                )
            if r.status_code == 200:
                return True
            logger.warning(f"خطای بله (تلاش {attempt+1}): {r.status_code} {r.text[:200]}")
            if r.status_code < 500:
                return False  # خطای کلاینت، retry نکن
            await asyncio.sleep(2)
        except Exception as e:
            logger.warning(f"خطای شبکه (تلاش {attempt+1}): {e}")
            await asyncio.sleep(2)
    return False


async def fetch_and_post(seen: set) -> set:
    logger.info("لود تایملاین و اسکرین‌شات...")

    batches: list[list[tuple[str, bytes]]] = await asyncio.get_event_loop().run_in_executor(
        None, screenshot_timeline, seen
    )

    if not batches:
        logger.info("توییت جدید فارسی یافت نشد.")
        return seen

    for i, batch in enumerate(batches, 1):
        logger.info(f"ارسال دسته {i}/{len(batches)} ({len(batch)} توییت)...")
        tasks = [_send_photo_to_bale(png) for _, png in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for (tid, _), ok in zip(batch, results):
            if ok is True:
                seen.add(tid)
                logger.info(f"✅ {tid}")
            else:
                logger.warning(f"❌ {tid}: {ok}")
        _save_seen(seen)

    return seen


async def run():
    if not BOT_TOKEN:
        logger.error("BALE_BOT_TOKEN تنظیم نشده!")
        sys.exit(1)
    if not CHANNEL_ID:
        logger.error("TWITTER_CHANNEL_ID تنظیم نشده! آن را در .env تنظیم کن.")
        sys.exit(1)
    if not COOKIES_FILE.exists():
        logger.error(f"فایل کوکی {COOKIES_FILE} وجود ندارد.")
        sys.exit(1)

    logger.info("✅ شروع ربات")

    seen = _load_seen()
    logger.info(f"تعداد توییت‌های دیده‌شده: {len(seen)}")

    seen = await fetch_and_post(seen)
    logger.info(f"دور اول تمام. بعدی در {FETCH_INTERVAL} ثانیه.")

    while True:
        await asyncio.sleep(FETCH_INTERVAL)
        logger.info("فچ تایملاین...")
        seen = await fetch_and_post(seen)


if __name__ == "__main__":
    asyncio.run(run())
