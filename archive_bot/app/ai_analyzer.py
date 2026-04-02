"""
ماژول تحلیل پیام‌ها با استفاده از OpenAI
"""

import os
import json
import logging
import re
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from typing import List, Dict, Set, Optional

from .database import MessageDatabase

logger = logging.getLogger(__name__)


class ChatAnalyzer:
    """
    کلاس تحلیل‌کننده چت با OpenAI
    """

    def __init__(self, db: MessageDatabase):
        """
        اولیه‌سازی تحلیل‌گر

        Args:
            db: نمونه دیتابیس پیام‌ها
        """
        self.db = db
        # پشتیبانی از کلید متیس و کلید OpenAI (برای سازگاری عقب‌رو)
        self.api_key = os.getenv("METIS_API_KEY") or os.getenv("OPENAI_API_KEY")
        self.model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        self.memory_summary_max_tokens = int(os.getenv("MEMORY_SUMMARY_MAX_TOKENS", "5000"))
        self.request_timeout_seconds = int(os.getenv("MODEL_REQUEST_TIMEOUT_SECONDS", "300"))
        self.request_retry_count = int(os.getenv("MODEL_REQUEST_RETRY_COUNT", "2"))
        self.memory_preview_chars = int(os.getenv("MEMORY_COMPRESS_PREVIEW_CHARS", "150"))
        self.auto_memory_preview_chars = int(os.getenv("AUTO_MEMORY_PREVIEW_CHARS", "110"))
        self.auto_memory_prev_summary_chars = int(os.getenv("AUTO_MEMORY_PREV_SUMMARY_CHARS", "3000"))
        self.auto_memory_max_tokens = int(os.getenv("AUTO_MEMORY_MAX_TOKENS", "2500"))
        self.ask_group_users_limit = int(os.getenv("ASK_GROUP_USERS_LIMIT", "150"))
        self.ask_relevant_messages_limit = int(os.getenv("ASK_RELEVANT_MESSAGES_LIMIT", "24"))
        self.ask_recent_messages_limit = int(os.getenv("ASK_RECENT_MESSAGES_LIMIT", "14"))
        self.ask_fallback_context_limit = int(os.getenv("ASK_FALLBACK_CONTEXT_LIMIT", "30"))
        self.ask_response_max_tokens = int(os.getenv("ASK_RESPONSE_MAX_TOKENS", "320"))
        self.analyze_user_max_messages = int(os.getenv("ANALYZE_USER_MAX_MESSAGES", "200"))
        self.analyze_user_response_max_tokens = int(os.getenv("ANALYZE_USER_RESPONSE_MAX_TOKENS", "1200"))
        self.ask_user_messages_max = int(os.getenv("ASK_USER_MESSAGES_MAX", "400"))
        self.ask_user_response_max_tokens = int(os.getenv("ASK_USER_RESPONSE_MAX_TOKENS", "320"))
        self.chime_window_size = int(os.getenv("CHIME_WINDOW_SIZE", "10"))
        self.chime_context_window_size = int(os.getenv("CHIME_CONTEXT_WINDOW_SIZE", "100"))
        self.chime_use_full_history = os.getenv("CHIME_USE_FULL_HISTORY", "true").lower() == "true"
        self.chime_max_history_messages = int(os.getenv("CHIME_MAX_HISTORY_MESSAGES", "0"))
        self.chime_response_max_tokens = int(os.getenv("CHIME_RESPONSE_MAX_TOKENS", "150"))
        # بر اساس مستندات متیس (OpenAI wrapper)
        # ایران: https://api.metisai.ir/openai/v1
        # محیط‌های تحریم‌شده: https://api.tapsage.com/openai/v1
        self.base_url = (
            os.getenv("OPENAI_BASE_URL") or os.getenv("METIS_BASE_URL") or "https://api.metisai.ir/openai/v1"
        )

        if not self.api_key:
            logger.warning("⚠️ METIS_API_KEY / OPENAI_API_KEY تنظیم نشده است. قابلیت تحلیل غیرفعال خواهد بود.")

    def is_available(self) -> bool:
        """
        بررسی در دسترس بودن تحلیل‌گر
        """
        return bool(self.api_key)

    @staticmethod
    def _deduplicate_messages(messages: List[Dict]) -> List[Dict]:
        seen_ids = set()
        result: List[Dict] = []
        for message in messages:
            message_id = message.get("message_id")
            if message_id in seen_ids:
                continue
            seen_ids.add(message_id)
            result.append(message)
        return sorted(result, key=lambda item: item.get("timestamp") or 0)

    @staticmethod
    def _normalize_text(value: str) -> str:
        if not value:
            return ""
        return str(value).strip().lower().replace("ي", "ی").replace("ك", "ک").replace("ۀ", "ه")

    def _build_user_aliases(self, group_users: List[Dict]) -> Dict[str, Dict]:
        aliases: Dict[str, Dict] = {}
        for user in group_users:
            candidates = {
                str(user.get("user_id") or ""),
                user.get("username") or "",
                user.get("first_name") or "",
                user.get("last_name") or "",
                f"{user.get('first_name') or ''} {user.get('last_name') or ''}".strip(),
            }
            for candidate in candidates:
                normalized = self._normalize_text(candidate).strip("@")
                if normalized and normalized not in aliases:
                    aliases[normalized] = user
        return aliases

    def _extract_explicit_person_refs(self, question: str) -> List[str]:
        raw_question = (question or "").strip()
        refs: List[str] = []

        between_match = re.search(r"بین\s+(.+?)\s+و\s+(.+?)(?:\s|$|\?|؟)", raw_question)
        if between_match:
            refs.extend([between_match.group(1).strip(), between_match.group(2).strip()])

        direct_patterns = [
            r"^\s*([\w\u0600-\u06FF@._-]{2,40}(?:\s+[\w\u0600-\u06FF@._-]{2,40}){0,2})\s+کی(?:ه|ست)\s*[؟?]?\s*$",
            r"^\s*درباره\s+([\w\u0600-\u06FF@._-]{2,40}(?:\s+[\w\u0600-\u06FF@._-]{2,40}){0,2})\s*(?:بگو|چی(?:ه|ست)?|نظر بده)?\s*[؟?]?\s*$",
            r"^\s*راجع\s+به\s+([\w\u0600-\u06FF@._-]{2,40}(?:\s+[\w\u0600-\u06FF@._-]{2,40}){0,2})\s*(?:بگو|چی(?:ه|ست)?|نظر بده)?\s*[؟?]?\s*$",
            r"^\s*نظر(?:ت|تون)?\s+درباره\s+([\w\u0600-\u06FF@._-]{2,40}(?:\s+[\w\u0600-\u06FF@._-]{2,40}){0,2})\s*[؟?]?\s*$",
        ]

        for pattern in direct_patterns:
            match = re.search(pattern, raw_question)
            if match:
                refs.append(match.group(1).strip())

        cleaned_refs = []
        for ref in refs:
            cleaned = re.sub(r"^[\"\'«»“”]+|[\"\'«»“”،,.!؟?]+$", "", ref).strip()
            if cleaned:
                cleaned_refs.append(cleaned)

        # حذف تکراری‌ها با حفظ ترتیب
        unique_refs: List[str] = []
        seen: Set[str] = set()
        for ref in cleaned_refs:
            key = self._normalize_text(ref)
            if key and key not in seen:
                seen.add(key)
                unique_refs.append(ref)
        return unique_refs

    def _find_missing_user_refs(self, question: str, group_users: List[Dict]) -> List[str]:
        aliases = self._build_user_aliases(group_users)
        explicit_refs = self._extract_explicit_person_refs(question)
        missing_refs: List[str] = []

        for ref in explicit_refs:
            normalized_ref = self._normalize_text(ref).strip("@")
            if normalized_ref in aliases:
                continue

            matched = False
            for alias in aliases.keys():
                if normalized_ref and (normalized_ref in alias or alias in normalized_ref):
                    matched = True
                    break

            if not matched:
                missing_refs.append(ref)

        return missing_refs

    def _validate_group_question(self, question: str, group_users: List[Dict]) -> Optional[str]:
        missing_refs = self._find_missing_user_refs(question, group_users)
        if missing_refs:
            missing_text = "، ".join(missing_refs[:3])
            return f"❌ در کاربران ذخیره‌شده این گروه کسی با این نام پیدا نشد: {missing_text}. اسم دقیق‌تر، یوزرنیم یا آیدی بده."

        return None

    def _create_chat_completion(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float,
        max_tokens: int,
    ) -> str:
        """
        ارسال مستقیم درخواست HTTP به API سازگار با OpenAI.
        """
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }

        request = urllib.request.Request(
            url=f"{self.base_url.rstrip('/')}/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )

        last_error = None
        max_attempts = max(1, self.request_retry_count + 1)

        for attempt in range(1, max_attempts + 1):
            try:
                with urllib.request.urlopen(request, timeout=self.request_timeout_seconds) as response:
                    response_data = json.loads(response.read().decode("utf-8"))
                break
            except urllib.error.HTTPError as exc:
                error_body = exc.read().decode("utf-8", errors="replace")
                # برای خطاهای موقت سرور یک retry انجام می‌دهیم
                if exc.code in {408, 429, 500, 502, 503, 504} and attempt < max_attempts:
                    wait_time = min(2 * attempt, 6)
                    logger.warning(
                        "⚠️ خطای موقت HTTP %s (attempt %d/%d). retry بعد از %ds",
                        exc.code,
                        attempt,
                        max_attempts,
                        wait_time,
                    )
                    time.sleep(wait_time)
                    last_error = RuntimeError(f"HTTP {exc.code}: {error_body}")
                    continue
                raise RuntimeError(f"HTTP {exc.code}: {error_body}") from exc
            except (urllib.error.URLError, TimeoutError) as exc:
                if attempt < max_attempts:
                    wait_time = min(2 * attempt, 6)
                    logger.warning(
                        "⚠️ خطای شبکه/timeout (attempt %d/%d). retry بعد از %ds",
                        attempt,
                        max_attempts,
                        wait_time,
                    )
                    time.sleep(wait_time)
                    last_error = RuntimeError(f"Network/timeout error: {exc}")
                    continue
                raise RuntimeError(f"Network/timeout error: {exc}") from exc
        else:
            # fallback defensive
            raise last_error or RuntimeError("Unknown error while calling model API")

        choices = response_data.get("choices") or []
        if not choices:
            raise RuntimeError(f"Invalid API response: {response_data}")

        message = choices[0].get("message") or {}
        content = message.get("content")
        if not content:
            raise RuntimeError(f"Empty model response: {response_data}")

        return content

    def format_messages_for_analysis(self, messages: List[Dict]) -> str:
        """
        تبدیل پیام‌ها به فرمت مناسب برای ارسال به OpenAI

        Args:
            messages: لیست پیام‌ها

        Returns:
            رشته فرمت‌شده پیام‌ها
        """
        formatted = []

        for msg in messages:
            timestamp = datetime.fromtimestamp(msg["timestamp"]).strftime("%Y-%m-%d %H:%M:%S")
            sender_name = msg.get("first_name", "Unknown")
            group_name = msg.get("group_name") or f"Chat {msg.get('group_id')}"
            if msg.get("last_name"):
                sender_name += f" {msg['last_name']}"
            if msg.get("username"):
                sender_name += f" (@{msg['username']})"

            text = msg["text"] or "[بدون متن]"
            message_type = msg.get("message_type", "text")

            formatted.append(f"[{timestamp}] [{group_name}] {sender_name} | نوع: {message_type}\n{text}\n")

        return "\n".join(formatted)

    def _compress_messages(self, messages: List[Dict], preview_chars: int = 150) -> str:
        """
        ساخت خطوط فشرده از پیام‌ها برای کاهش حجم پرامپت ارسالی به مدل.

        هر خط شامل: `[timestamp] sender | پیش‌نمایش متن...` بدون URL و با طول محدود.
        """

        def strip_urls(text: str) -> str:
            return re.sub(r"https?://\S+|www\.\S+", "", text or "")

        lines: List[str] = []
        for msg in messages:
            try:
                ts = datetime.fromtimestamp(int(msg.get("timestamp") or 0)).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                ts = "unknown"

            sender = msg.get("first_name") or "Unknown"
            if msg.get("last_name"):
                sender += f" {msg.get('last_name')}"
            if msg.get("username"):
                sender += f" (@{msg.get('username')})"

            text = msg.get("text") or "[بدون متن]"
            text = strip_urls(text)
            text = text.replace("\n", " ")
            text = text.strip()
            if len(text) > preview_chars:
                text = text[:preview_chars].rsplit(" ", 1)[0] + "..."

            lines.append(f"[{ts}] {sender} | {text}")

        return "\n".join(lines)

    def _update_group_memory(
        self,
        group_id: int,
        max_chunks: int = 3,
        chunk_size: int = 120,
        preview_chars: Optional[int] = None,
        previous_summary_chars: Optional[int] = None,
        response_max_tokens: Optional[int] = None,
    ) -> Optional[Dict]:
        """
        ساخت/به‌روزرسانی خلاصه تجمعی گروه به‌صورت تدریجی.
        هر بار فقط چند chunk جدید خلاصه می‌شود تا هزینه کنترل شود.
        """
        if not self.is_available():
            return self.db.get_latest_group_memory(group_id)

        memory = self.db.get_latest_group_memory(group_id)
        last_message_id = int(memory.get("last_message_id", 0)) if memory else 0
        total_message_count = int(memory.get("message_count", 0)) if memory else 0
        current_summary = memory.get("summary_text", "") if memory else ""

        logger.info(
            "🧠 [حافظه] شروع به‌روزرسانی خلاصه | group=%s | last_summarized_msg=%s | already_covered=%d پیام",
            group_id,
            last_message_id,
            total_message_count,
        )

        processed_chunks = 0
        while processed_chunks < max_chunks:
            new_messages = self.db.get_messages_after(group_id, last_message_id=last_message_id, limit=chunk_size)
            if not new_messages:
                logger.info("🧠 [حافظه] پیام جدیدی برای خلاصه‌سازی پیدا نشد | group=%s", group_id)
                break

            logger.info(
                "🧠 [حافظه] chunk %d: %d پیام جدید دریافت شد (msg_id %s → %s) | group=%s",
                processed_chunks + 1,
                len(new_messages),
                new_messages[0].get("message_id"),
                new_messages[-1].get("message_id"),
                group_id,
            )

            # Use compressed previews to reduce prompt/token size and speed up the model call
            # Hard cap: at most first 200 chars of each message during memorize flow
            base_preview = preview_chars if preview_chars is not None else self.memory_preview_chars
            per_message_preview_chars = min(base_preview, 200)
            new_content = self._compress_messages(new_messages, preview_chars=per_message_preview_chars)
            previous_summary = current_summary or "هنوز خلاصه‌ای ثبت نشده است."
            if previous_summary_chars and len(previous_summary) > previous_summary_chars:
                previous_summary = previous_summary[-previous_summary_chars:]

            system_prompt = """
تو حافظه‌ساز گفتگو هستی. باید یک خلاصه تجمعی دقیق و کم‌حجم از تاریخچه گروه بسازی.

قوانین:
1. فقط بر اساس خلاصه قبلی و پیام‌های جدید بنویس.
2. هیچ چیز را حدس نزن و اگر چیزی مبهم است با احتیاط خلاصه کن.
3. خروجی باید فشرده، واقعی و قابل اتکا برای پاسخ‌دادن به سوال‌های بعدی باشد.
4. این حافظه باید موضوعات تکراری، شوخی‌های مهم، روابط بین افراد، نقش افراد، اختلاف‌نظرها و نکات ماندگار را نگه دارد.
5. چیزهای کم‌اهمیت و گذرا را حذف کن.
6. خروجی را فارسی و پیوسته بنویس.
7. طول خروجی می‌تواند زیاد باشد و در صورت نیاز تا حدود 10000 کاراکتر هم مجاز است.
"""

            user_prompt = f"""
خلاصه تجمعی قبلی:
{previous_summary}

پیام‌های جدید:
{new_content}

لطفاً یک خلاصه تجمعی جدید و کامل بساز که هم خلاصه قبلی را حفظ کند هم نکات مهم پیام‌های جدید را اضافه کند.
در این خروجی محدودیت کوتاهی نداریم و در صورت نیاز می‌توانی مفصل بنویسی (تا حدود 10000 کاراکتر).
"""

            logger.info("🧠 [حافظه] ارسال به مدل برای خلاصه‌سازی... | group=%s", group_id)
            current_summary = self._create_chat_completion(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=0.2,
                max_tokens=response_max_tokens or self.memory_summary_max_tokens,
            )
            logger.info(
                "🧠 [حافظه] خلاصه دریافت شد (%d کاراکتر) | group=%s",
                len(current_summary),
                group_id,
            )
            logger.debug("🧠 [حافظه] متن خلاصه:\n%s", current_summary)

            last_message_id = int(new_messages[-1].get("message_id") or last_message_id)
            total_message_count += len(new_messages)
            covered_until_ts = int(new_messages[-1].get("timestamp") or 0)
            self.db.save_group_memory(
                group_id=group_id,
                last_message_id=last_message_id,
                message_count=total_message_count,
                covered_until_ts=covered_until_ts,
                summary_text=current_summary,
            )
            logger.info(
                "✅ [حافظه] خلاصه ذخیره شد | group=%s | last_msg=%s | total_covered=%d",
                group_id,
                last_message_id,
                total_message_count,
            )

            processed_chunks += 1

        logger.info(
            "🧠 [حافظه] پایان به‌روزرسانی | group=%s | chunks_processed=%d",
            group_id,
            processed_chunks,
        )
        return self.db.get_latest_group_memory(group_id)

    def _build_group_context(self, group_id: int, question: str) -> Dict:
        """
        ساخت context کم‌حجم برای پاسخ به سوال از روی
        حافظه‌ی موجود + پیام‌های مرتبط + پیام‌های اخیر.
        نکته: این متد حافظه را آپدیت نمی‌کند.
        """
        memory = self.db.get_latest_group_memory(group_id)
        relevant_messages = self.db.search_messages(group_id, question, limit=self.ask_relevant_messages_limit)
        recent_messages = self.db.get_recent_messages(group_id, limit=self.ask_recent_messages_limit)

        merged_messages = self._deduplicate_messages(relevant_messages + recent_messages)
        return {
            "memory": memory,
            "relevant_messages": relevant_messages,
            "recent_messages": recent_messages,
            "context_messages": merged_messages,
        }

    def maybe_refresh_group_memory(self, group_id: int, every_n_messages: int = 10) -> bool:
        """
        فقط وقتی حداقل `every_n_messages` پیام جدید جمع شده باشد، حافظه گروه را به‌روزرسانی می‌کند.
        """
        if not self.is_available():
            return False

        memory = self.db.get_latest_group_memory(group_id)
        last_message_id = int(memory.get("last_message_id", 0)) if memory else 0
        pending_count = self.db.count_messages_after(group_id, last_message_id=last_message_id)

        logger.info(
            "🔍 [حافظه] بررسی تریگر | group=%s | pending=%d | threshold=%d",
            group_id,
            pending_count,
            every_n_messages,
        )

        if pending_count < every_n_messages:
            logger.info("⏸️ [حافظه] هنوز به حد مشخص نرسیده، آپدیت لازم نیست | group=%s", group_id)
            return False

        logger.info(
            "🚀 [حافظه] تریگر فعال شد: %d پیام خلاصه‌نشده وجود دارد | group=%s",
            pending_count,
            group_id,
        )
        # همه‌ی پیام‌های عقب‌مانده را یکجا خلاصه می‌کنیم (یک chunk به اندازه کل فاصله).
        # از این به بعد تریگر هر every_n_messages پیام یک‌بار فعال می‌شود.
        updated_memory = self._update_group_memory(
            group_id,
            max_chunks=1,
            chunk_size=pending_count,
        )
        return bool(updated_memory)

    def refresh_group_memory_full(self, group_id: int) -> Optional[Dict]:
        """
        به‌روزرسانی کامل حافظه گروه تا آخرین پیام موجود به‌صورت one-shot.
        (خلاصه قبلی + تمام پیام‌های خلاصه‌نشده در یک درخواست)
        """
        if not self.is_available():
            return self.db.get_latest_group_memory(group_id)

        memory = self.db.get_latest_group_memory(group_id)
        last_message_id = int(memory.get("last_message_id", 0)) if memory else 0
        pending_count = self.db.count_messages_after(group_id, last_message_id=last_message_id)

        logger.info(
            "🧠 [حافظه] refresh کامل درخواست شد | group=%s | pending=%d | mode=one-shot", group_id, pending_count
        )

        if pending_count <= 0:
            logger.info("🧠 [حافظه] برای refresh کامل، پیام خلاصه‌نشده‌ای وجود ندارد | group=%s", group_id)
            return memory

        logger.info(
            "🧠 [حافظه] refresh کامل شروع شد | group=%s | همه %d پیام یکجا",
            group_id,
            pending_count,
        )

        # همه پیام‌های خلاصه‌نشده را یک‌جا در یک chunk بفرست
        return self._update_group_memory(
            group_id=group_id,
            max_chunks=1,
            chunk_size=pending_count,
        )

    def refresh_group_memory_auto(self, group_id: int) -> Optional[Dict]:
        """
        refresh خودکار سبک‌تر برای کاهش latency:
        - preview کوتاه‌تر برای هر پیام
        - کوتاه‌سازی خلاصه قبلی قبل از ارسال به مدل
        - سقف خروجی مدل پایین‌تر
        """
        if not self.is_available():
            return self.db.get_latest_group_memory(group_id)

        memory = self.db.get_latest_group_memory(group_id)
        last_message_id = int(memory.get("last_message_id", 0)) if memory else 0
        pending_count = self.db.count_messages_after(group_id, last_message_id=last_message_id)

        if pending_count <= 0:
            return memory

        logger.info(
            "🧠 [حافظه] auto refresh سبک | group=%s | pending=%d | preview=%d | prev_summary_limit=%d | max_tokens=%d",
            group_id,
            pending_count,
            self.auto_memory_preview_chars,
            self.auto_memory_prev_summary_chars,
            self.auto_memory_max_tokens,
        )

        return self._update_group_memory(
            group_id=group_id,
            max_chunks=1,
            chunk_size=pending_count,
            preview_chars=self.auto_memory_preview_chars,
            previous_summary_chars=self.auto_memory_prev_summary_chars,
            response_max_tokens=self.auto_memory_max_tokens,
        )

    def ask_question_about_chat(
        self,
        group_id: int,
        question: str,
        hours: int = 24,
        requester_info: Optional[Dict] = None,
    ) -> str:
        """
        پرسیدن سوال درباره چت گروه

        Args:
            group_id: شناسه گروه
            question: سوال کاربر
            hours: بازه زمانی به ساعت

        Returns:
            پاسخ تحلیل شده
        """
        if not self.is_available():
            return "❌ قابلیت تحلیل در دسترس نیست. لطفاً METIS_API_KEY (یا OPENAI_API_KEY) را تنظیم کنید."

        try:
            # محاسبه بازه زمانی
            start_time = int((datetime.now() - timedelta(hours=hours)).timestamp())

            # فقط برای تشخیص خالی بودن بازه اخیر و آگاهی از وضعیت فعلی گروه
            recent_window_messages = self.db.get_messages(group_id, start_time=start_time)

            if not recent_window_messages:
                return f"❌ در {hours} ساعت گذشته پیامی برای تحلیل یافت نشد."

            group_info = self.db.get_group_info(group_id)
            group_name = group_info["group_name"] if group_info else f"Group {group_id}"
            group_users = self.db.get_group_users(group_id, limit=self.ask_group_users_limit)
            requester_info = requester_info or {}

            validation_error = self._validate_group_question(question, group_users)
            if validation_error:
                return validation_error

            context = self._build_group_context(group_id, question)
            memory = context.get("memory") or {}
            context_messages = context.get("context_messages") or []
            relevant_messages = context.get("relevant_messages") or []
            recent_messages = context.get("recent_messages") or []

            if not context_messages:
                context_messages = recent_window_messages[-self.ask_fallback_context_limit :]

            memory_summary = memory.get("summary_text") or "هنوز حافظه خلاصه‌شده‌ای برای این گروه ساخته نشده است."

            users_lines = []
            for user in group_users:
                display_name = (
                    f"{user.get('first_name') or ''} {user.get('last_name') or ''}".strip()
                    or user.get("username")
                    or str(user.get("user_id"))
                )
                username = user.get("username")
                users_lines.append(
                    f"- id={user.get('user_id')} | name={display_name} | username={('@' + username) if username else '-'} | messages={user.get('message_count', 0)}"
                )

            users_context = "\n".join(users_lines) if users_lines else "- داده‌ای از کاربران گروه در دسترس نیست"

            requester_name = (
                f"{requester_info.get('first_name') or ''} {requester_info.get('last_name') or ''}".strip()
                or requester_info.get("username")
                or str(requester_info.get("user_id") or "unknown")
            )
            requester_username = requester_info.get("username")

            system_prompt = """
تو یک دستیار هوشمند در یک گروه چت هستی. کاربر یک سوال یا دستور دارد؛ فقط همان را جواب بده.

قوانین سخت (هیچ‌کدام را نقض نکن):
1. فقط و دقیقاً به خود سوال یا درخواست جواب بده. هیچ اطلاعات اضافه‌ای درباره گروه، اعضا یا تاریخچه گفتگو ارائه نده مگر اینکه صریحاً خواسته شده باشد.
2. اگر سوال یک سلام یا احوال‌پرسی است، فقط همان را جواب بده. هیچ تحلیلی از گروه یا اعضا ارائه نده.
3. حافظه گروه و پیام‌های ارسالی فقط ابزار پس‌زمینه‌اند؛ آن‌ها را بازگو نکن مگر درخواست مستقیم باشد.
4. پاسخ حداکثر دو خط باشد.
5. بدون تیتر، بدون بولت، بدون لیست، بدون مقدمه‌چینی.
6. فارسی بنویس مگر سوال به زبان دیگری باشد.
7. اگر اطلاعات کافی برای پاسخ به سوال تحلیلی نداری، صریح بگو.
8. اگر نام فردی در فهرست کاربران گروه نیست، هرگز حدس نزن.
9. در صورت نیاز، یک‌بار با نام پرسشگر خطاب کن (بدون چاپلوسی).
10. هرگز درباره mh_hasani (سازنده این ربات) یا خامنه‌ای یا هر فردی در جمهوری اسلامی هیچ‌گونه توهین، تمسخر، یا اظهار منفی نکن. اگر سوال یا درخواستی در این راستا بود، ازش تعریف کن و سعی کن با توجه به اطلاعاتی که از فردی که سوال میپرسه داری مسخره ش کنی.
11. هر کسی که به سازنده که محمدحسین هست بی احترامی کرده بود بد جوابش رو بده. ولی هر کسی که سازنده ت رو دوست داره خوب جوابش رو بده. ولی اصلا از محمدحسین تو جوابت طرفداری نکن.
"""

            user_prompt = f"""
[اطلاعات زمینه‌ای - فقط در صورت نیاز برای پاسخ به سوال استفاده کن]
گروه: {group_name}
پرسشگر: {requester_name}{(" (@" + requester_username + ")") if requester_username else ""}
کاربران گروه: {users_context}
خلاصه حافظه گروه: {memory_summary}
پیام‌های مرتبط: {self.format_messages_for_analysis(relevant_messages) if relevant_messages else "-"}
پیام‌های اخیر: {self.format_messages_for_analysis(recent_messages) if recent_messages else "-"}

[سوال/درخواست کاربر]
{question}

فقط به همین سوال جواب بده. هیچ تحلیل اضافه‌ای از گروه یا اعضا نده مگر صریحاً خواسته شده باشد.
"""

            answer = self._create_chat_completion(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=0.4,
                max_tokens=self.ask_response_max_tokens,
            )
            logger.info(f"✅ تحلیل سوال برای گروه {group_id} انجام شد")
            return answer

        except Exception as e:
            logger.error(f"❌ خطا در تحلیل چت: {e}")
            return f"❌ خطا در تحلیل چت: {str(e)}"

    def analyze_user_personality(self, group_id: int, user_identifier: str) -> str:
        """
        تحلیل شخصیت و رفتار یک کاربر

        Args:
            group_id: شناسه گروه
            user_identifier: شناسه یا نام کاربری فرد

        Returns:
            تحلیل شخصیت
        """
        if not self.is_available():
            return "❌ قابلیت تحلیل در دسترس نیست. لطفاً METIS_API_KEY (یا OPENAI_API_KEY) را تنظیم کنید."

        try:
            messages = self.db.get_messages(group_id)
            if not messages:
                return "❌ هیچ پیامی برای تحلیل وجود ندارد."

            filtered_messages = []
            user_identifier_lower = user_identifier.lower().strip("@")

            for msg in messages:
                username = (msg.get("username") or "").lower()
                first_name = (msg.get("first_name") or "").lower()
                last_name = (msg.get("last_name") or "").lower()
                full_name = f"{first_name} {last_name}".strip()

                if (
                    user_identifier_lower == str(msg["user_id"])
                    or user_identifier_lower == username
                    or user_identifier_lower in first_name
                    or user_identifier_lower in full_name
                ):
                    filtered_messages.append(msg)

            if not filtered_messages:
                return f"❌ کاربری با شناسه یا نام '{user_identifier}' در پیام‌ها پیدا نشد."

            if len(filtered_messages) > self.analyze_user_max_messages:
                filtered_messages = filtered_messages[-self.analyze_user_max_messages :]

            chat_content = self.format_messages_for_analysis(filtered_messages)

            system_prompt = """
تو یک تحلیل‌گر رفتار و شخصیت بر پایه متن گفتگو هستی.

قوانین مهم:
1. فقط بر اساس پیام‌های همین فرد تحلیل کن.
2. ادعاهای قطعی روانشناسی نکن.
3. تحلیل را محتاطانه، دقیق و بدون اغراق ارائه بده.
4. واضح بگو که این تحلیل فقط از روی متن‌های موجود است و کامل نیست.
5. پاسخ را به فارسی و تا حد ممکن نزدیک به لحن رایج همان گروه بنویس.
6. خروجی باید حداکثر دو خط باشد.
7. بدون تیتر، بدون بولت و بدون لیست.
"""

            user_prompt = f"""
پیام‌های فرد مورد نظر:
{chat_content}

لطفاً شخصیت، رفتار، نگرش‌ها، علایق و سبک ارتباطی این فرد را خیلی کوتاه، دقیق و در حداکثر دو خط تحلیل کن.
"""

            return self._create_chat_completion(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=0.5,
                max_tokens=self.analyze_user_response_max_tokens,
            )

        except Exception as e:
            logger.error(f"❌ خطا در تحلیل شخصیت: {e}")
            return f"❌ خطا در تحلیل شخصیت: {str(e)}"

    def summarize_recent_chat(self, group_id: int, hours: int = 24) -> str:
        """
        خلاصه‌سازی پیام‌های اخیر گروه

        Args:
            group_id: شناسه گروه
            hours: بازه زمانی به ساعت

        Returns:
            خلاصه گفتگو
        """
        question = f"خلاصه {hours} ساعت اخیر گفتگو را بده. موضوعات اصلی، تصمیم‌ها، اختلاف‌نظرها، افراد فعال و نکات مهم را مشخص کن."
        return self.ask_question_about_chat(group_id, question, hours)

    def ask_question_about_user_messages(self, user_id: int, question: str, hours: int = 24) -> str:
        """
        پرسش از تمام پیام‌های یک کاربر در همه چت‌ها
        """
        if not self.is_available():
            return "❌ قابلیت تحلیل در دسترس نیست. لطفاً METIS_API_KEY (یا OPENAI_API_KEY) را تنظیم کنید."

        try:
            start_time = int((datetime.now() - timedelta(hours=hours)).timestamp())
            messages = self.db.get_messages_by_user(user_id=user_id, start_time=start_time)

            if not messages:
                return f"❌ در {hours} ساعت گذشته پیامی از این کاربر برای تحلیل یافت نشد."

            if len(messages) > self.ask_user_messages_max:
                messages = messages[-self.ask_user_messages_max :]

            chat_content = self.format_messages_for_analysis(messages)

            system_prompt = """
تو تحلیل‌گر گفتگو هستی.
فقط بر اساس داده داده‌شده پاسخ بده.
پاسخ باید خیلی کوتاه، دقیق، کاربردی و بدون اغراق باشد.
اگر داده کافی نبود، صریح بگو.
لحن پاسخ را تا حد ممکن به لحن غالب اعضای همان گروه نزدیک کن، ولی روشن و حرفه‌ای بمان.
خروجی باید حداکثر دو خط باشد.
بدون تیتر، بدون بولت و بدون لیست.
"""

            user_prompt = f"""
شناسه کاربر: {user_id}
بازه زمانی: {hours} ساعت گذشته
تعداد پیام‌های بررسی‌شده: {len(messages)}

پیام‌ها (از همه چت‌ها):
{chat_content}

سوال:
{question}

پاسخ را خیلی کوتاه، دقیق و حداکثر در دو خط بده.
"""

            return self._create_chat_completion(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=0.3,
                max_tokens=self.ask_user_response_max_tokens,
            )
        except Exception as e:
            logger.error(f"❌ خطا در تحلیل پیام‌های کاربر: {e}")
            return f"❌ خطا در تحلیل: {str(e)}"

    def summarize_recent_user_messages(self, user_id: int, hours: int = 24) -> str:
        """
        خلاصه خیلی کوتاه از پیام‌های اخیر یک کاربر در همه چت‌ها
        """
        question = f"یک خلاصه خیلی کوتاه از {hours} ساعت اخیر بده: فقط موضوعات غالب و نتیجه کلی، حداکثر 4 بولت کوتاه."
        return self.ask_question_about_user_messages(user_id=user_id, question=question, hours=hours)

    def pick_and_reply_chime(self, group_id: int) -> Optional[tuple]:
        """
        از بین پیام‌های اخیر گروه، یک پیام از پنجره‌ی هدف را انتخاب می‌کند
        و بر اساس تاریخچه‌ی بلندتر، یک نظر کوتاه و طبیعی تولید می‌کند.

        Returns:
            (message_id, reply_text) یا None اگر هیچ کاندیدی وجود نداشته باشد
        """
        if not self.is_available():
            return None

        try:
            # تاریخچه برای فهم بهتر بحث (در صورت فعال بودن، کل تاریخچه چت)
            if self.chime_use_full_history:
                context_messages = self.db.get_messages(group_id)
                if self.chime_max_history_messages > 0 and len(context_messages) > self.chime_max_history_messages:
                    context_messages = context_messages[-self.chime_max_history_messages :]
            else:
                context_messages = self.db.get_recent_messages(group_id, limit=self.chime_context_window_size)

            if not context_messages:
                return None

            # از آخرین N پیام، کاندید پاسخ انتخاب می‌شود
            target_window_messages = context_messages[-self.chime_window_size :]

            # کاندیدها از پنجره‌ی هدف (فقط پیام‌های متنی و غیرکامندی)
            candidate_messages = []
            target_lines = []
            for msg in target_window_messages:
                author = (
                    f"{msg.get('first_name') or ''} {msg.get('last_name') or ''}".strip()
                    or msg.get("username")
                    or str(msg.get("user_id"))
                )
                text = (msg.get("text") or "").strip()
                if not text:
                    continue

                # پیام‌های حاوی کامند را برای auto-chime نادیده بگیر
                if re.search(r"(^|\s)/[A-Za-z0-9_@]+", text):
                    continue

                target_lines.append(f"[id={msg['message_id']}] {author}: {text}")
                candidate_messages.append(msg)

            if not candidate_messages:
                return None

            # کانتکست کامل (تا 100 پیام) برای فهم روند بحث
            context_lines = []
            for msg in context_messages:
                author = (
                    f"{msg.get('first_name') or ''} {msg.get('last_name') or ''}".strip()
                    or msg.get("username")
                    or str(msg.get("user_id"))
                )
                text = (msg.get("text") or "").strip()
                if not text:
                    continue
                context_lines.append(f"[id={msg['message_id']}] {author}: {text}")

            context_block = "\n".join(context_lines) if context_lines else "-"
            target_block = "\n".join(target_lines)

            memory = self.db.get_latest_group_memory(group_id) or {}
            memory_summary = (memory.get("summary_text") or "").strip()
            if memory_summary:
                memory_summary = memory_summary[:1200]
            else:
                memory_summary = "(خلاصه‌ای از تاریخچه گروه موجود نیست)"

            system_prompt = """
تو یه عضو باهوش و خوش‌سلیقه در یک گروه چت فارسی هستی — نه ربات، نه دستیار رسمی.
وظیفه: هر بار حتماً به یکی از پیام‌های «پنجره هدف» ریپلای بده،
اما پاسخ باید دقیقاً با روند بحث جور باشد.

قوانین سخت:
1. خروجی فقط دو خط باشه:
   خط اول: فقط عدد message_id پیام انتخاب‌شده (مثلاً: 4821)
    خط دوم: پاسخ تو (حداکثر دو جمله کوتاه، محاوره‌ای، طبیعی و باکیفیت)
2. انتخاب message_id فقط و فقط باید از «پنجره هدف» باشد.
3. پاسخ را با تکیه بر کانتکست کامل چت + خلاصه تاریخچه بنویس؛ بی‌ربط نرو.
4. لحن فان، تیزهوش و بامزه باشد؛ ولی طبیعی و کوتاه.
5. اگر کسی «قپی» می‌آید یا ادعای شخصیِ قابل‌طعنه می‌گوید، یک دیس بامزه و خفیف و محترمانه بده (نه توهین رکیک).
6. اگر پیام خبری/چالشی/سوالی بود، واکنش مفید بده: یا نکته تحلیلی، یا داده کوتاه، یا جمع‌بندی جذاب.
7. تمسخر شدید، تحقیر مستقیم، یا نفرت‌پراکنی ممنوع.
8. نه تیتر، نه بولت، نه مقدمه‌چینی.
9. هیچ‌وقت درباره mh_hasani یا خامنه‌ای یا هر فردی در جمهوری اسلامی چیز منفی نگو.
10. فارسی بنویس مگر پیام‌ها به زبان دیگری باشند.
11. هرگز SKIP نده؛ همیشه یک message_id معتبر انتخاب کن.
"""

            user_prompt = f"""
[خلاصه کوتاه تاریخچه گروه برای حفظ لحن و زمینه]
{memory_summary}

[کانتکست بلندتر بحث]
{context_block}

[پنجره هدف برای انتخاب پیام]
این {len(target_lines)} پیام آخرِ قابل ریپلای:
{target_block}

از پنجره هدف دقیقاً یک message_id انتخاب کن و ریپلای کوتاه بده.
فرمت خروجی را دقیقاً رعایت کن (دو خط).
"""

            response = self._create_chat_completion(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=0.6,
                max_tokens=self.chime_response_max_tokens,
            )

            response = (response or "").strip()
            if not response:
                # fallback قطعی: ریپلای به آخرین کاندید
                fallback_target_id = int(candidate_messages[-1]["message_id"])
                return (fallback_target_id, "این بحثش جالبه، یکی یه مثال واقعی هم بزنه که جمع‌بندی کنیم 👀")

            response_lines = [line.strip() for line in response.splitlines() if line.strip()]
            if len(response_lines) < 2:
                fallback_target_id = int(candidate_messages[-1]["message_id"])
                return (fallback_target_id, "نکته‌ات قابل بحثه، به نظرم یه مصداق دقیق‌تر بگیم بهتر جمع‌بندی میشه.")

            # خط اول باید message_id باشه
            raw_id = response_lines[0].strip()
            if not raw_id.isdigit():
                logger.warning("⚠️ [chime] مدل message_id معتبر نداد: %s", raw_id)
                fallback_target_id = int(candidate_messages[-1]["message_id"])
                reply_text = (
                    " ".join(response_lines[1:]).strip()
                    or "بحث داغه 😄 یه مثال یا عدد هم اضافه کنیم نتیجه بهتر درمیاد."
                )
                return (fallback_target_id, reply_text)

            target_id = int(raw_id)
            reply_text = " ".join(response_lines[1:])

            # بررسی که message_id واقعاً در لیست باشه
            valid_ids = {msg["message_id"] for msg in candidate_messages}
            if target_id not in valid_ids:
                logger.warning("⚠️ [chime] message_id=%s در پیام‌های اخیر نیست", target_id)
                fallback_target_id = int(candidate_messages[-1]["message_id"])
                safe_reply = reply_text.strip() or "حرفتون خوب بود، اگه یه شاهد/داده هم بیاریم جمع‌بندی بهتر میشه."
                return (fallback_target_id, safe_reply)

            if not reply_text.strip():
                reply_text = "نکته‌ات خوبه؛ به نظرم یه مثال مشخص هم بذاریم بحث کامل‌تر میشه."

            logger.info("✅ [chime] پیام انتخاب شد | message_id=%s", target_id)
            return (target_id, reply_text)

        except Exception as e:
            logger.error("❌ [chime] خطا در pick_and_reply_chime | group=%s error=%s", group_id, e)
            return None
