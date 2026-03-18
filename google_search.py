"""
ماژول جستجو: DDGS + fetch محتوا + Pollinations AI
"""

import requests
from bs4 import BeautifulSoup
from ddgs import DDGS

_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


def _fetch_text(url: str, max_chars: int = 1000) -> str:
    """متن اصلی یه صفحه رو استخراج می‌کنه"""
    try:
        r = requests.get(url, headers=_HEADERS, timeout=6)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup(["script", "style", "nav", "header", "footer", "aside"]):
            tag.decompose()
        paragraphs = [p.get_text(strip=True) for p in soup.find_all("p") if len(p.get_text(strip=True)) > 60]
        text = " ".join(paragraphs)
        return text[:max_chars] if text else ""
    except Exception:
        return ""


def _ai_summarize(query: str, context: str) -> str:
    """با Pollinations AI خلاصه می‌کنه"""
    prompt = f"""سوال کاربر: {query}

متن استخراج‌شده از نتایج جستجو:
{context}

لطفاً یه خلاصه مفید، روان و کامل به فارسی بنویس. اطلاعات مهم رو حفظ کن و در آخر لینک‌های منابع رو بذار."""

    try:
        r = requests.post(
            "https://text.pollinations.ai/openai",
            json={"model": "openai", "messages": [{"role": "user", "content": prompt}]},
            timeout=30,
        )
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"]
    except Exception:
        return ""


def quick_search(query: str, num_results: int = 5) -> str:
    # جستجوی وب
    try:
        raw = list(DDGS().text(query, max_results=num_results))
    except Exception as e:
        return f"❌ خطا در جستجو: {e}"

    if not raw:
        return f"❌ نتیجه‌ای برای '{query}' پیدا نشد."

    # fetch محتوای هر صفحه
    sources = []
    for r in raw:
        snippet = r.get("body", "").strip()
        if len(snippet) < 100:
            snippet = _fetch_text(r["href"], max_chars=1000)
        if not snippet:
            snippet = r.get("body", "")
        sources.append({"title": r["title"], "url": r["href"], "text": snippet})

    # ساخت context (حداکثر 3000 کاراکتر کل)
    context_parts = []
    total = 0
    for i, s in enumerate(sources, 1):
        part = f"منبع {i}: {s['title']}\nلینک: {s['url']}\n{s['text']}"
        if total + len(part) > 3000:
            break
        context_parts.append(part)
        total += len(part)
    context = "\n\n".join(context_parts)

    # خلاصه‌سازی با AI
    summary = _ai_summarize(query, context)

    if summary:
        return summary

    # fallback: نتایج خام
    lines = [f"🔍 *{query}*\n"]
    for i, s in enumerate(sources, 1):
        lines.append(f"{i}. *{s['title']}*")
        lines.append(f"📝 {s['text'][:300]}")
        lines.append(f"🔗 {s['url']}\n")
    return "\n".join(lines)
