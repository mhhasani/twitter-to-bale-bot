"""
ربات آرشیو و تحلیل گفتگوهای بله
این ربات پیام‌های گروه را در SQLite ذخیره می‌کند و امکان تحلیل با OpenAI را می‌دهد.
"""

import os
import logging
import threading
from datetime import datetime
from dotenv import load_dotenv
from bale import Bot, Message
from database import MessageDatabase
from ai_analyzer import ChatAnalyzer

# بارگذاری متغیرهای محیطی
load_dotenv()

# تنظیمات لاگ
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# دریافت توکن ربات
BOT_TOKEN = os.getenv("BALE_BOT_TOKEN")
COMMANDS_ENABLED = os.getenv("ENABLE_BOT_COMMANDS", "false").lower() == "true"
ALLOWED_COMMAND_USER_ID = int(os.getenv("ALLOWED_COMMAND_USER_ID", "1412990760"))
AUTO_MEMORIZE_ENABLED = os.getenv("AUTO_MEMORIZE_ENABLED", "true").lower() == "true"
AUTO_MEMORIZE_THRESHOLD = int(os.getenv("AUTO_MEMORIZE_THRESHOLD", "50"))
SUMMARY_DEFAULT_HOURS = int(os.getenv("SUMMARY_DEFAULT_HOURS", "24"))
SUMMARY_MIN_HOURS = int(os.getenv("SUMMARY_MIN_HOURS", "1"))
SUMMARY_MAX_HOURS = int(os.getenv("SUMMARY_MAX_HOURS", "168"))
ASK_DEFAULT_HOURS = int(os.getenv("ASK_DEFAULT_HOURS", "72"))
ASK_MAX_QUESTION_CHARS = int(os.getenv("ASK_MAX_QUESTION_CHARS", "500"))
EXPORT_DEFAULT_COUNT = int(os.getenv("EXPORT_DEFAULT_COUNT", "20"))
EXPORT_MIN_COUNT = int(os.getenv("EXPORT_MIN_COUNT", "1"))
EXPORT_MAX_COUNT = int(os.getenv("EXPORT_MAX_COUNT", "100"))

if not BOT_TOKEN:
    raise ValueError("❌ توکن ربات یافت نشد! لطفاً فایل .env را بررسی کنید.")

# ساخت نمونه ربات و سرویس‌ها
bot = Bot(token=BOT_TOKEN)
db = MessageDatabase(db_path=os.getenv("SQLITE_DB_PATH", "messages.db"))
analyzer = ChatAnalyzer(db) if COMMANDS_ENABLED else None
AUTO_MEMORIZE_IN_PROGRESS = set()
AUTO_MEMORIZE_LOCK = threading.Lock()


def safe_get_attr(obj, attr_name, default=None):
    """
    دریافت امن خصوصیت از آبجکت
    """
    try:
        return getattr(obj, attr_name, default)
    except Exception:
        return default


def detect_message_type(message: Message) -> str:
    """
    تشخیص نوع پیام
    """
    if safe_get_attr(message, "text"):
        return "text"
    if safe_get_attr(message, "photo"):
        return "photo"
    if safe_get_attr(message, "video"):
        return "video"
    if safe_get_attr(message, "document"):
        return "document"
    if safe_get_attr(message, "audio"):
        return "audio"
    if safe_get_attr(message, "voice"):
        return "voice"
    if safe_get_attr(message, "sticker"):
        return "sticker"
    if safe_get_attr(message, "location"):
        return "location"
    if safe_get_attr(message, "contact"):
        return "contact"
    return "unknown"


def get_message_text_content(message: Message) -> str:
    """
    استخراج محتوای متنی قابل ذخیره از پیام.
    فقط متن اصلی یا کپشن برگردانده می‌شود.
    """
    return (safe_get_attr(message, "text") or safe_get_attr(message, "caption") or "").strip()


def should_store_message(message: Message) -> bool:
    """
    فقط پیام‌هایی را ذخیره می‌کنیم که متن یا کپشن داشته باشند.
    """
    return bool(get_message_text_content(message))


def extract_metadata(message: Message) -> dict:
    """
    استخراج متادیتای کامل پیام
    """
    author = safe_get_attr(message, "author")
    chat = safe_get_attr(message, "chat")
    sender_chat = safe_get_attr(message, "sender_chat")
    reply_to = safe_get_attr(message, "reply_to_message")
    forward_from = safe_get_attr(message, "forward_from")
    forward_from_chat = safe_get_attr(message, "forward_from_chat")

    entities = safe_get_attr(message, "entities") or []
    caption_entities = safe_get_attr(message, "caption_entities") or []

    def extract_entity_items(entity_list):
        result = []
        for ent in entity_list:
            result.append(
                {
                    "type": safe_get_attr(ent, "type"),
                    "offset": safe_get_attr(ent, "offset"),
                    "length": safe_get_attr(ent, "length"),
                    "url": safe_get_attr(ent, "url"),
                }
            )
        return result

    metadata = {
        "chat": {
            "id": safe_get_attr(chat, "id"),
            "type": safe_get_attr(chat, "type"),
            "title": safe_get_attr(chat, "title"),
            "username": safe_get_attr(chat, "username"),
        },
        "author": {
            "user_id": safe_get_attr(author, "user_id"),
            "username": safe_get_attr(author, "username"),
            "first_name": safe_get_attr(author, "first_name"),
            "last_name": safe_get_attr(author, "last_name"),
            "is_bot": safe_get_attr(author, "is_bot"),
        },
        "message": {
            "message_id": safe_get_attr(message, "message_id"),
            "date": safe_get_attr(message, "date"),
            "text": safe_get_attr(message, "text"),
            "caption": safe_get_attr(message, "caption"),
            "is_reply": reply_to is not None,
            "reply_to_message_id": safe_get_attr(reply_to, "message_id"),
            "is_forwarded": bool(
                forward_from is not None
                or forward_from_chat is not None
                or safe_get_attr(message, "forward_date") is not None
            ),
            "forward_from_user_id": safe_get_attr(forward_from, "user_id"),
            "forward_from_username": safe_get_attr(forward_from, "username"),
            "forward_from_chat_id": safe_get_attr(forward_from_chat, "id"),
            "forward_from_chat_title": safe_get_attr(forward_from_chat, "title"),
            "forward_date": safe_get_attr(message, "forward_date"),
            "forward_from_message_id": safe_get_attr(message, "forward_from_message_id"),
            "is_edited": safe_get_attr(message, "edit_date") is not None,
            "edit_date": safe_get_attr(message, "edit_date"),
            "media_group_id": safe_get_attr(message, "media_group_id"),
            "sender_chat_id": safe_get_attr(sender_chat, "id"),
            "sender_chat_title": safe_get_attr(sender_chat, "title"),
            "has_entities": len(entities) > 0,
            "has_caption_entities": len(caption_entities) > 0,
            "has_reply_markup": safe_get_attr(message, "reply_markup") is not None,
            "entities": extract_entity_items(entities),
            "caption_entities": extract_entity_items(caption_entities),
            "reply_to": {
                "message_id": safe_get_attr(reply_to, "message_id"),
                "date": safe_get_attr(reply_to, "date"),
                "from_user_id": safe_get_attr(safe_get_attr(reply_to, "author"), "user_id"),
                "from_username": safe_get_attr(safe_get_attr(reply_to, "author"), "username"),
                "text": safe_get_attr(reply_to, "text"),
                "caption": safe_get_attr(reply_to, "caption"),
            }
            if reply_to is not None
            else None,
        },
    }

    for field_name in [
        "photo",
        "video",
        "document",
        "audio",
        "voice",
        "sticker",
        "location",
        "contact",
        "entities",
        "caption_entities",
    ]:
        value = safe_get_attr(message, field_name)
        if value is not None:
            try:
                metadata[field_name] = str(value)
            except Exception:
                metadata[field_name] = "[unserializable]"

    return metadata


def is_group_chat(message: Message) -> bool:
    """
    بررسی اینکه پیام مربوط به گروه یا کانال است
    """
    chat = safe_get_attr(message, "chat")
    chat_type = safe_get_attr(chat, "type")
    return chat_type in {"group", "supergroup", "channel"}


def can_execute_commands(message: Message) -> bool:
    """
    فقط یک کاربر مجاز بتواند command اجرا کند.
    """
    author = safe_get_attr(message, "author")
    user_id = safe_get_attr(author, "user_id")
    return str(user_id) == str(ALLOWED_COMMAND_USER_ID)


def resolve_target_group_id(message: Message):
    """
    تعیین گروه هدف برای اجرای دستورها.
    اگر دستور در گروه زده شود همان گروه استفاده می‌شود.
    اگر در PV زده شود، آخرین گروهی که پیام ذخیره‌شده دارد انتخاب می‌شود.
    """
    if is_group_chat(message):
        return safe_get_attr(safe_get_attr(message, "chat"), "id")

    groups = db.get_all_groups()
    for group in groups:
        gid = group.get("group_id")
        if gid is None:
            continue
        stats = db.get_group_stats(gid)
        if stats and stats.get("total_messages", 0) > 0:
            return gid

    return None


async def store_message(message: Message):
    """
    ذخیره پیام در دیتابیس فقط در صورتی که محتوای متنی داشته باشد.
    """
    try:
        chat = safe_get_attr(message, "chat")
        author = safe_get_attr(message, "author")

        if not chat or not author or not should_store_message(message):
            return False

        group_id = safe_get_attr(chat, "id")
        group_name = safe_get_attr(chat, "title") or safe_get_attr(chat, "username") or f"Chat {group_id}"

        user_id = safe_get_attr(author, "user_id")
        username = safe_get_attr(author, "username")
        first_name = safe_get_attr(author, "first_name")
        last_name = safe_get_attr(author, "last_name")
        is_bot = bool(safe_get_attr(author, "is_bot", False))

        message_id = safe_get_attr(message, "message_id")
        raw_timestamp = safe_get_attr(message, "date")
        if isinstance(raw_timestamp, datetime):
            timestamp = int(raw_timestamp.timestamp())
        elif isinstance(raw_timestamp, (int, float)):
            timestamp = int(raw_timestamp)
        elif isinstance(raw_timestamp, str) and raw_timestamp.isdigit():
            timestamp = int(raw_timestamp)
        else:
            timestamp = int(datetime.now().timestamp())
        message_type = detect_message_type(message)
        text = get_message_text_content(message)
        metadata = extract_metadata(message)

        db.add_group(group_id=group_id, group_name=group_name)
        db.add_user(
            user_id=user_id,
            username=username,
            first_name=first_name,
            last_name=last_name,
            is_bot=is_bot,
        )
        saved = db.add_message(
            message_id=message_id,
            group_id=group_id,
            user_id=user_id,
            text=text,
            timestamp=timestamp,
            message_type=message_type,
            metadata=metadata,
        )

        return saved

    except Exception as e:
        logger.error(f"❌ خطا در ذخیره پیام: {e}")
        return False


def maybe_auto_memorize_group(group_id: int):
    """
    بررسی عقب‌افتادگی حافظه و اجرای خودکار summarize در صورت عبور از آستانه.
    """
    if not AUTO_MEMORIZE_ENABLED or not analyzer:
        return

    with AUTO_MEMORIZE_LOCK:
        if group_id in AUTO_MEMORIZE_IN_PROGRESS:
            logger.info("⏳ [auto-memorize] قبلاً در حال اجراست | group=%s", group_id)
            return

    try:
        existing_memory = analyzer.db.get_latest_group_memory(group_id)
        last_msg_id = int((existing_memory or {}).get("last_message_id", 0))
        pending = analyzer.db.count_messages_after(group_id, last_message_id=last_msg_id)

        logger.info(
            "🔎 [auto-memorize] بررسی عقب‌افتادگی | group=%s pending=%d threshold=%d",
            group_id,
            pending,
            AUTO_MEMORIZE_THRESHOLD,
        )

        if pending < AUTO_MEMORIZE_THRESHOLD:
            return

        with AUTO_MEMORIZE_LOCK:
            AUTO_MEMORIZE_IN_PROGRESS.add(group_id)
        logger.info(
            "🚀 [auto-memorize] فعال شد | group=%s pending=%d mode=one-shot",
            group_id,
            pending,
        )

        def _run_auto_memorize(gid: int):
            try:
                analyzer.refresh_group_memory_auto(gid)
                logger.info("✅ [auto-memorize] تکمیل شد | group=%s", gid)
            except Exception as inner_e:
                logger.error("❌ [auto-memorize] خطا در اجرای پس‌زمینه | group=%s error=%s", gid, inner_e)
            finally:
                with AUTO_MEMORIZE_LOCK:
                    AUTO_MEMORIZE_IN_PROGRESS.discard(gid)

        threading.Thread(target=_run_auto_memorize, args=(group_id,), daemon=True).start()

    except Exception as e:
        logger.error("❌ [auto-memorize] خطا | group=%s error=%s", group_id, e)
        with AUTO_MEMORIZE_LOCK:
            AUTO_MEMORIZE_IN_PROGRESS.discard(group_id)


def build_help_message() -> str:
    return (
        "🤖 ربات آرشیو و تحلیل گروه فعال است.\n\n"
        "دستورها:\n"
        "/start - معرفی ربات\n"
        "/help - راهنما\n"
        "/stats - آمار گروه فعلی\n"
        "/summary [hours] - خلاصه گفتگوهای اخیر\n"
        "/ask [سوال] - پرسش آزاد از داده‌های گروه\n"
        "/analyze [user] - تحلیل رفتار و شخصیت یک فرد\n"
        "/export_recent [count] - نمایش آخرین پیام‌های ذخیره شده\n"
        "/memorize - ساخت/به‌روزرسانی خلاصه تجمعی گروه\n\n"
        "نکته: فقط پیام‌هایی که متن یا کپشن داشته باشند ذخیره می‌شوند تا مصرف داده پایین بماند."
    )


async def handle_command(message: Message, text: str) -> bool:
    """
    مدیریت دستورهای ربات
    """
    lower_text = text.strip()
    requester_id = safe_get_attr(safe_get_attr(message, "author"), "user_id")
    group_id = resolve_target_group_id(message)

    if group_id is None and not lower_text.startswith("/start") and not lower_text.startswith("/help"):
        await message.reply("📭 هنوز هیچ پیام گروهی ذخیره نشده تا این دستور اجرا شود.")
        return True

    if lower_text.startswith("/start"):
        await message.reply(
            "👋 سلام! من ربات آرشیو و تحلیل گفتگو هستم.\n"
            "تمام پیام‌های این گروه را ذخیره می‌کنم تا بعداً بتوانید از آن‌ها سؤال بپرسید.\n\n" + build_help_message()
        )
        return True

    if lower_text.startswith("/help"):
        await message.reply(build_help_message())
        return True

    if lower_text.startswith("/stats"):
        stats = db.get_group_stats(group_id)
        if not stats or not stats.get("total_messages"):
            await message.reply("📭 هنوز داده‌ای برای این گروه ذخیره نشده است.")
            return True

        top_users = stats.get("top_users", [])[:5]
        top_users_text = (
            "\n".join(
                f"- {user.get('first_name') or user.get('username') or user.get('user_id')}: {user.get('message_count')} پیام"
                for user in top_users
            )
            or "- داده‌ای نیست"
        )

        response = (
            f"📊 آمار گروه\n\n"
            f"- تعداد کل پیام‌ها: {stats.get('total_messages', 0)}\n"
            f"- تعداد کاربران فعال: {stats.get('active_users', 0)}\n"
            f"- فعال‌ترین کاربران:\n{top_users_text}"
        )
        await message.reply(response)
        return True

    if lower_text.startswith("/summary"):
        parts = lower_text.split(maxsplit=1)
        hours = SUMMARY_DEFAULT_HOURS
        if len(parts) > 1:
            try:
                hours = max(SUMMARY_MIN_HOURS, min(SUMMARY_MAX_HOURS, int(parts[1].strip())))
            except ValueError:
                await message.reply("❌ فرمت درست: /summary 24")
                return True

        loading = await message.reply("🧠 در حال خلاصه‌سازی گفتگوها...")
        result = analyzer.summarize_recent_user_messages(requester_id, hours)
        try:
            await loading.delete()
        except Exception:
            pass
        await message.reply(result)
        return True

    if lower_text.startswith("/ask"):
        if group_id is None:
            await message.reply("📭 هنوز هیچ پیام گروهی ذخیره نشده تا تحلیل انجام شود.")
            return True

        parts = lower_text.split(maxsplit=1)
        if len(parts) < 2 or not parts[1].strip():
            await message.reply("❌ فرمت درست: /ask سوال شما")
            return True

        question = parts[1].strip()
        if len(question) > ASK_MAX_QUESTION_CHARS:
            await message.reply(f"❌ متن سوال طولانی است. حداکثر {ASK_MAX_QUESTION_CHARS} کاراکتر مجاز است.")
            return True

        author = safe_get_attr(message, "author")
        requester_info = {
            "user_id": safe_get_attr(author, "user_id"),
            "username": safe_get_attr(author, "username"),
            "first_name": safe_get_attr(author, "first_name"),
            "last_name": safe_get_attr(author, "last_name"),
        }

        loading = await message.reply("🧠 در حال تحلیل گفتگو و پاسخ به سؤال...")
        result = analyzer.ask_question_about_chat(
            group_id,
            question,
            hours=ASK_DEFAULT_HOURS,
            requester_info=requester_info,
        )
        try:
            await loading.delete()
        except Exception:
            pass
        await message.reply(result)
        return True

    if lower_text.startswith("/analyze"):
        parts = lower_text.split(maxsplit=1)
        if len(parts) < 2 or not parts[1].strip():
            await message.reply("❌ فرمت درست: /analyze username_or_name")
            return True

        loading = await message.reply("🧠 در حال تحلیل رفتار و شخصیت فرد...")
        result = analyzer.analyze_user_personality(group_id, parts[1].strip())
        try:
            await loading.delete()
        except Exception:
            pass
        await message.reply(result)
        return True

    if lower_text.startswith("/export_recent"):
        parts = lower_text.split(maxsplit=1)
        count = EXPORT_DEFAULT_COUNT
        if len(parts) > 1:
            try:
                count = max(EXPORT_MIN_COUNT, min(EXPORT_MAX_COUNT, int(parts[1].strip())))
            except ValueError:
                await message.reply("❌ فرمت درست: /export_recent 20")
                return True

        messages = db.get_messages(group_id, limit=count)
        if not messages:
            await message.reply("📭 پیامی برای نمایش وجود ندارد.")
            return True

        lines = []
        for msg in messages[-count:]:
            sender = msg.get("first_name") or msg.get("username") or str(msg.get("user_id"))
            ts = datetime.fromtimestamp(msg["timestamp"]).strftime("%m-%d %H:%M")
            content = (msg.get("text") or "[بدون متن]").replace("\n", " ")[:120]
            lines.append(f"[{ts}] {sender}: {content}")

        await message.reply("📝 آخرین پیام‌ها:\n\n" + "\n".join(lines))
        return True

    if lower_text.startswith("/memorize"):
        if not can_execute_commands(message):
            await message.reply("⛔ فقط کاربر مجاز می‌تواند /memorize را اجرا کند.")
            return True

        if group_id is None:
            await message.reply("📭 هنوز هیچ پیام گروهی ذخیره نشده تا حافظه ساخته شود.")
            return True

        loading = await message.reply("🧠 در حال خواندن و خلاصه‌سازی پیام‌های گروه...")
        try:
            existing_memory = analyzer.db.get_latest_group_memory(group_id)
            last_msg_id = int((existing_memory or {}).get("last_message_id", 0))
            pending = analyzer.db.count_messages_after(group_id, last_message_id=last_msg_id)
            if pending <= 0:
                try:
                    await loading.delete()
                except Exception:
                    pass
                await message.reply("📭 پیام جدیدی برای خلاصه‌سازی وجود نداشت.")
                return True

            logger.info(
                "🧠 [/memorize] شروع خلاصه‌سازی کامل | group=%s pending=%d (یکجا)",
                group_id,
                pending,
            )

            analyzer.refresh_group_memory_full(group_id)
            try:
                await loading.delete()
            except Exception:
                pass

            await message.reply("✅ حافظه آپدیت شد.")
        except Exception as e:
            try:
                await loading.delete()
            except Exception:
                pass
            logger.error("❌ خطا در /memorize | group=%s error=%s", group_id, e)
            await message.reply(f"❌ خطا در خلاصه‌سازی: {e}")
        return True

    return False


@bot.event
async def on_message(message: Message):
    """
    مدیریت پیام‌های دریافتی و ذخیره فقط پیام‌های متن‌دار
    """
    try:
        was_stored = await store_message(message)

        text = safe_get_attr(message, "text") or ""
        is_ask_command = text.strip().startswith("/ask")
        if COMMANDS_ENABLED and text.startswith("/") and (can_execute_commands(message) or is_ask_command):
            handled = await handle_command(message, text)
            if handled:
                logger.info("✅ دستور پردازش شد")
                return
        elif text.startswith("/"):
            logger.info(
                "⏸️ اجرای دستور نادیده گرفته شد (غیرفعال یا کاربر غیرمجاز) | user=%s message=%s",
                safe_get_attr(safe_get_attr(message, "author"), "user_id"),
                safe_get_attr(message, "message_id"),
            )

        if is_group_chat(message):
            if was_stored:
                group_id = safe_get_attr(safe_get_attr(message, "chat"), "id")
                maybe_auto_memorize_group(group_id)
                logger.info(
                    "💾 پیام متن‌دار ذخیره شد | group=%s user=%s message=%s",
                    safe_get_attr(safe_get_attr(message, "chat"), "id"),
                    safe_get_attr(safe_get_attr(message, "author"), "user_id"),
                    safe_get_attr(message, "message_id"),
                )
            elif should_store_message(message):
                logger.warning(
                    "⚠️ پیام متن‌دار دریافت شد اما ذخیره نشد | group=%s user=%s message=%s",
                    safe_get_attr(safe_get_attr(message, "chat"), "id"),
                    safe_get_attr(safe_get_attr(message, "author"), "user_id"),
                    safe_get_attr(message, "message_id"),
                )
            else:
                logger.info(
                    "⏭️ پیام بدون متن نادیده گرفته شد | group=%s user=%s message=%s",
                    safe_get_attr(safe_get_attr(message, "chat"), "id"),
                    safe_get_attr(safe_get_attr(message, "author"), "user_id"),
                    safe_get_attr(message, "message_id"),
                )
            return

        # await message.reply(
        #     "👋 من برای آرشیو و تحلیل گفتگوهای گروهی ساخته شده‌ام.\n"
        #     "لطفاً من را به گروه اضافه کنید و دسترسی لازم بدهید.\n\n" + build_help_message()
        # )

    except Exception as e:
        logger.error(f"خطا در پردازش پیام: {e}")
        try:
            await message.reply("❌ متأسفانه خطایی رخ داد. لطفاً دوباره تلاش کنید.")
        except Exception:
            pass


@bot.event
async def on_callback(callback_query):
    """
    مدیریت callback queryها (در صورت نیاز در آینده)
    """
    await callback_query.answer("✅ دریافت شد")


def main():
    """
    تابع اصلی برای اجرای ربات
    """
    try:
        logger.info("🤖 ربات در حال راه‌اندازی...")
        logger.info(f"🔑 استفاده از توکن: {BOT_TOKEN[:10]}...")

        # اجرای ربات
        bot.run()

    except KeyboardInterrupt:
        logger.info("⛔ ربات توسط کاربر متوقف شد")
    except Exception as e:
        logger.error(f"❌ خطا در اجرای ربات: {e}")
        raise


if __name__ == "__main__":
    main()
