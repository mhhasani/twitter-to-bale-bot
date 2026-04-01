"""
اسکرین‌شات توییت‌های فارسی از تایملاین با Selenium
- یه driver مشترک (راه‌اندازی یه‌بار)
- یه بار لود تایملاین → dedup و فیلتر فارسی از DOM
- هر scroll یه batch جداگانه برمی‌گردونه
"""

import io
import json
import random
import re
import threading
import time
from pathlib import Path

from PIL import Image
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

COOKIES_FILE = Path(__file__).parent / "twitter_cookies.json"
SEARCH_URL = "https://x.com/search?q=lang%3Afa%20min_faves%3A750%20-filter%3Areplies&src=typed_query&f=live"

_driver: webdriver.Chrome | None = None
_lock = threading.Lock()


def _build_driver() -> webdriver.Chrome:
    opts = Options()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=700,2000")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--hide-scrollbars")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--log-level=3")
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)

    # مخفی کردن webdriver flag از JavaScript
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"},
    )

    driver.get("https://twitter.com")
    time.sleep(2)

    raw = json.loads(COOKIES_FILE.read_text())
    cookies = raw if isinstance(raw, list) else [{"name": k, "value": v} for k, v in raw.items()]
    for c in cookies:
        try:
            driver.add_cookie(
                {
                    "name": c["name"],
                    "value": c["value"],
                    "domain": c.get("domain", ".twitter.com"),
                }
            )
        except Exception:
            pass

    driver.get(SEARCH_URL)
    time.sleep(3)
    driver.execute_script("localStorage.setItem('night_mode', '2')")

    print(f"[driver] URL بعد از init: {driver.current_url}")
    return driver


def _get_driver() -> webdriver.Chrome:
    global _driver
    if _driver is not None:
        try:
            _ = _driver.current_url
            return _driver
        except Exception:
            try:
                _driver.quit()
            except Exception:
                pass
            _driver = None
    _driver = _build_driver()
    return _driver


def _tweet_id_from_article(article) -> str | None:
    try:
        links = article.find_elements(By.CSS_SELECTOR, "a[href*='/status/']")
        for link in links:
            href = link.get_attribute("href") or ""
            if "/status/" in href:
                tid = href.split("/status/")[1].split("/")[0].split("?")[0]
                if tid.isdigit():
                    return tid
    except Exception:
        pass
    return None


def _has_persian(text: str) -> bool:
    return bool(re.search(r"[\u0600-\u06FF]", text))


def _wait_for_images(driver, article, timeout: float = 8.0) -> None:
    """صبر کن تا همه img داخل article لود بشن (video نادیده گرفته میشه)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        all_loaded = driver.execute_script("""
            var imgs = arguments[0].querySelectorAll('img');
            for (var i = 0; i < imgs.length; i++) {
                var img = imgs[i];
                if (!img.complete || img.naturalWidth === 0) return false;
            }
            return true;
        """, article)
        if all_loaded:
            return
        time.sleep(0.3)
    print("  [warn] تایم‌اوت لود تصاویر")


def screenshot_timeline(seen: set) -> list[list[tuple[str, bytes]]]:
    """
    تایملاین رو لود می‌کنه، توییت‌های فارسی که در seen نیستن رو اسکرین‌شات می‌گیره.
    هر scroll یه batch جداگانه‌ست.
    Returns: list of batches, هر batch یه list از (tweet_id, png_bytes)
    """
    batches: list[list[tuple[str, bytes]]] = []

    with _lock:
        driver = _get_driver()
        print("[timeline] لود صفحه جستجوی فارسی ...")
        driver.get(SEARCH_URL)
        print(f"[timeline] URL: {driver.current_url}")

        # صبر برای لود کامل نتایج جستجو (حداکثر ۶۰ ثانیه)
        try:
            WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "article[data-testid='tweet']"))
            )
            print("[timeline] article پیدا شد ✓")
        except Exception:
            print(f"[timeline] خطا - article پیدا نشد | title: {driver.title}")
            return batches

        time.sleep(5)  # صبر اضافه برای لود رسانه‌ها

        processed = set()
        for scroll_i in range(2):
            batch: list[tuple[str, bytes]] = []

            # فاز ۱: فقط tid‌ها رو collect کن (قبل از هر scrollIntoView)
            articles = driver.find_elements(By.CSS_SELECTOR, "article[data-testid='tweet']")
            print(f"[scroll {scroll_i + 1}] {len(articles)} article در DOM")
            new_tids = []
            for article in articles:
                try:
                    tid = _tweet_id_from_article(article)
                except StaleElementReferenceException:
                    continue
                if not tid or tid in seen or tid in processed:
                    continue
                processed.add(tid)
                new_tids.append(tid)
            print(f"  {len(new_tids)} توییت جدید برای بررسی")

            # فاز ۲: هر tid رو fresh از DOM بخون و پردازش کن
            for tid in new_tids:
                # re-fetch article از DOM
                try:
                    links = driver.find_elements(By.CSS_SELECTOR, f"a[href*='/status/{tid}']")
                    article = None
                    for link in links:
                        try:
                            article = link.find_element(By.XPATH, "./ancestor::article[@data-testid='tweet']")
                            break
                        except Exception:
                            continue
                    if not article:
                        print(f"  skip (article گم شد): {tid}")
                        continue
                except Exception:
                    continue

                try:
                    text_els = article.find_elements(By.CSS_SELECTOR, "[data-testid='tweetText']")
                    text = text_els[0].text if text_els else ""
                    if not _has_persian(text):
                        print(f"  skip (انگلیسی): {tid}")
                        continue
                except StaleElementReferenceException:
                    continue
                except Exception:
                    continue

                # کلیک روی "show more" اگه وجود داشت
                try:
                    show_more = article.find_elements(By.CSS_SELECTOR, "[data-testid='tweet-text-show-more-link']")
                    if show_more:
                        driver.execute_script("arguments[0].click()", show_more[0])
                        time.sleep(0.6)
                except Exception:
                    pass

                try:
                    rect = driver.execute_script(
                        """
                        var art = arguments[0];
                        art.scrollIntoView({block:'center', behavior:'instant'});
                        var ar = art.getBoundingClientRect();
                        var vh = window.innerHeight;
                        var cx = ar.left + ar.width / 2;

                        // cropTop: از بالا پایین بیا تا اولین نقطه داخل article
                        var cropTop = ar.top;
                        var el = document.elementFromPoint(cx, ar.top + 2);
                        var iter = 0;
                        while (el && !art.contains(el) && iter < 80) {
                            cropTop = el.getBoundingClientRect().bottom + 1;
                            el = document.elementFromPoint(cx, cropTop + 2);
                            iter++;
                        }
                        // حداقل 115px از بالا — اطمینان از skip کامل header+tabs
                        cropTop = Math.max(cropTop, 115);
                        cropTop = Math.min(cropTop, ar.bottom - 30);

                        // cropBottom: از action bar (like/retweet) بخون
                        var actionBar = art.querySelector('[role="group"]');
                        var cropBottom = actionBar
                            ? Math.min(actionBar.getBoundingClientRect().bottom + 6, vh)
                            : Math.min(ar.bottom, vh);

                        return {
                            cropTop: cropTop,
                            cropBottom: cropBottom,
                            left: Math.max(0, ar.left),
                            width: ar.width
                        };
                    """,
                        article,
                    )
                    _wait_for_images(driver, article)

                    vp_png = driver.get_screenshot_as_png()
                    img = Image.open(io.BytesIO(vp_png))
                    dpr = img.width / driver.execute_script("return window.innerWidth")
                    x = int(rect["left"] * dpr)
                    y = int(rect["cropTop"] * dpr)
                    w = int(rect["width"] * dpr)
                    h = int((rect["cropBottom"] - rect["cropTop"]) * dpr)
                    if h <= 10:
                        print(f"  skip (h too small): {tid}")
                        continue
                    cropped = img.crop((x, y, x + w, y + h))
                    buf = io.BytesIO()
                    cropped.save(buf, format="PNG")
                    batch.append((tid, buf.getvalue()))
                    print(f"  screenshot: {tid} | top={rect['cropTop']:.0f} bot={rect['cropBottom']:.0f}")
                except StaleElementReferenceException:
                    print(f"  skip (stale): {tid}")
                except Exception as e:
                    print(f"  خطا screenshot {tid}: {e}")

            if batch:
                batches.append(batch)
                print(f"  → batch {len(batches)}: {len(batch)} توییت")

            print(f"[scroll {scroll_i + 1}] scroll پایین...")
            # اسکرول انسانی: چند قدم کوچیک با تاخیر تصادفی
            steps = random.randint(4, 7)
            for _ in range(steps):
                delta = random.randint(200, 400)
                driver.execute_script(f"window.scrollBy(0, {delta})")
                time.sleep(random.uniform(0.3, 0.9))
            time.sleep(random.uniform(12, 18))

        total = sum(len(b) for b in batches)
        print(f"جمع: {total} توییت در {len(batches)} دسته")

    return batches
