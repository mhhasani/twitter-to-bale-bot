from __future__ import annotations

import asyncio
import logging
import threading
from datetime import datetime
from typing import Any, Optional

from bale import Bot, Message
from dotenv import load_dotenv

from .ai_analyzer import ChatAnalyzer
from .config import AppConfig
from .database import MessageDatabase
from .message_utils import (
    detect_message_type,
    extract_metadata,
    get_message_text_content,
    is_group_chat,
    normalize_timestamp,
    safe_get_attr,
    should_store_message,
)

logger = logging.getLogger(__name__)


class ArchiveBotApp:
    """Main bot application with clean orchestration and preserved behavior."""

    def __init__(self) -> None:
        load_dotenv()
        self.config = AppConfig.from_env()

        self.bot = Bot(token=self.config.bot_token)
        self.db = MessageDatabase(db_path=self.config.sqlite_db_path)
        self.analyzer = ChatAnalyzer(self.db) if self.config.commands_enabled else None

        self._auto_memorize_in_progress: set[int] = set()
        self._auto_memorize_lock = threading.Lock()

        self._register_events()

    def _register_events(self) -> None:
        @self.bot.event
        async def on_message(message: Message):
            await self.on_message(message)

        @self.bot.event
        async def on_callback(callback_query):
            await self.on_callback(callback_query)

    def can_execute_commands(self, message: Message) -> bool:
        """Allow command execution only for configured user."""
        author = safe_get_attr(message, "author")
        user_id = safe_get_attr(author, "user_id")
        return str(user_id) == str(self.config.allowed_command_user_id)

    def resolve_target_group_id(self, message: Message) -> Optional[int]:
        """Resolve target group for commands, including private chat fallback."""
        if is_group_chat(message):
            return safe_get_attr(safe_get_attr(message, "chat"), "id")

        groups = self.db.get_all_groups()
        for group in groups:
            group_id = group.get("group_id")
            if group_id is None:
                continue

            stats = self.db.get_group_stats(group_id)
            if stats and stats.get("total_messages", 0) > 0:
                return group_id

        return None

    async def store_message(self, message: Message) -> bool:
        """Store only text/caption messages with full metadata."""
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
            timestamp = normalize_timestamp(safe_get_attr(message, "date"))
            message_type = detect_message_type(message)
            text = get_message_text_content(message)
            metadata = extract_metadata(message)

            self.db.add_group(group_id=group_id, group_name=group_name)
            self.db.add_user(
                user_id=user_id,
                username=username,
                first_name=first_name,
                last_name=last_name,
                is_bot=is_bot,
            )
            return self.db.add_message(
                message_id=message_id,
                group_id=group_id,
                user_id=user_id,
                text=text,
                timestamp=timestamp,
                message_type=message_type,
                metadata=metadata,
            )
        except Exception as exc:
            logger.error("❌ Error storing message: %s", exc)
            return False

    def maybe_auto_memorize_group(self, group_id: int) -> None:
        """Trigger incremental memory refresh when pending threshold is reached."""
        if not self.config.auto_memorize_enabled or not self.analyzer:
            return

        with self._auto_memorize_lock:
            if group_id in self._auto_memorize_in_progress:
                logger.info("⏳ [auto-memorize] already running | group=%s", group_id)
                return

        try:
            existing_memory = self.analyzer.db.get_latest_group_memory(group_id)
            last_msg_id = int((existing_memory or {}).get("last_message_id", 0))
            pending = self.analyzer.db.count_messages_after(group_id, last_message_id=last_msg_id)

            logger.info(
                "🔎 [auto-memorize] pending check | group=%s pending=%d threshold=%d",
                group_id,
                pending,
                self.config.auto_memorize_threshold,
            )

            if pending < self.config.auto_memorize_threshold:
                return

            with self._auto_memorize_lock:
                self._auto_memorize_in_progress.add(group_id)

            logger.info("🚀 [auto-memorize] started | group=%s pending=%d mode=one-shot", group_id, pending)

            def _run_auto_memorize(target_group_id: int) -> None:
                try:
                    self.analyzer.refresh_group_memory_auto(target_group_id)
                    logger.info("✅ [auto-memorize] completed | group=%s", target_group_id)
                except Exception as inner_exc:
                    logger.error(
                        "❌ [auto-memorize] background error | group=%s error=%s",
                        target_group_id,
                        inner_exc,
                    )
                finally:
                    with self._auto_memorize_lock:
                        self._auto_memorize_in_progress.discard(target_group_id)

            threading.Thread(target=_run_auto_memorize, args=(group_id,), daemon=True).start()
        except Exception as exc:
            logger.error("❌ [auto-memorize] failed | group=%s error=%s", group_id, exc)
            with self._auto_memorize_lock:
                self._auto_memorize_in_progress.discard(group_id)

    async def maybe_auto_chime_group(self, group_id: int) -> None:
        """Trigger auto-chime every N stored messages."""
        if not self.analyzer or not self.analyzer.is_available():
            return

        should_trigger = self.db.mark_chime_message_and_should_trigger(group_id, self.config.auto_chime_every_n)
        if not should_trigger:
            return

        logger.info("💬 [auto-chime] check started | group=%s", group_id)

        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, lambda: self.analyzer.pick_and_reply_chime(group_id))

            if not result:
                logger.info("💬 [auto-chime] model skipped | group=%s", group_id)
                return

            target_message_id, reply_text = result
            await self.bot.send_message(group_id, reply_text, reply_to_message_id=target_message_id)
            logger.info("✅ [auto-chime] reply sent | group=%s target_msg=%s", group_id, target_message_id)
        except Exception as exc:
            logger.error("❌ [auto-chime] failed | group=%s error=%s", group_id, exc)

    def build_help_message(self) -> str:
        return (
            "🤖 Group archive and analysis bot is active.\n\n"
            "Commands:\n"
            "/start - Bot intro\n"
            "/help - Help\n"
            "/stats - Current group stats\n"
            "/summary [hours] - Summarize recent conversations\n"
            "/ask [question] - Ask free-form questions about group data\n"
            "/analyze [user] - Analyze a user's behavior/persona\n"
            "/export_recent [count] - Show latest stored messages\n"
            "/memorize - Build/update cumulative group memory\n\n"
            "Note: only messages with text/caption are stored to reduce data usage."
        )

    async def handle_command(self, message: Message, text: str) -> bool:
        """Handle supported bot commands."""
        lower_text = text.strip()
        requester_id = safe_get_attr(safe_get_attr(message, "author"), "user_id")
        group_id = self.resolve_target_group_id(message)

        if group_id is None and not lower_text.startswith("/start") and not lower_text.startswith("/help"):
            await message.reply("📭 No stored group messages found yet for this command.")
            return True

        if lower_text.startswith("/start"):
            await message.reply(
                "👋 Hi! I am a conversation archive and analysis bot.\n"
                "I store this group's messages so you can query them later.\n\n" + self.build_help_message()
            )
            return True

        if lower_text.startswith("/help"):
            await message.reply(self.build_help_message())
            return True

        if lower_text.startswith("/stats"):
            stats = self.db.get_group_stats(group_id)
            if not stats or not stats.get("total_messages"):
                await message.reply("📭 No data stored for this group yet.")
                return True

            top_users = stats.get("top_users", [])[:5]
            top_users_text = (
                "\n".join(
                    f"- {user.get('first_name') or user.get('username') or user.get('user_id')}: {user.get('message_count')} messages"
                    for user in top_users
                )
                or "- No data"
            )

            response = (
                f"📊 Group stats\n\n"
                f"- Total messages: {stats.get('total_messages', 0)}\n"
                f"- Active users: {stats.get('active_users', 0)}\n"
                f"- Most active users:\n{top_users_text}"
            )
            await message.reply(response)
            return True

        if lower_text.startswith("/summary"):
            parts = lower_text.split(maxsplit=1)
            hours = self.config.summary_default_hours
            if len(parts) > 1:
                try:
                    hours = max(
                        self.config.summary_min_hours, min(self.config.summary_max_hours, int(parts[1].strip()))
                    )
                except ValueError:
                    await message.reply("❌ Invalid format. Use: /summary 24")
                    return True

            loading = await message.reply("🧠 Summarizing recent conversations...")
            result = self.analyzer.summarize_recent_user_messages(requester_id, hours)
            try:
                await loading.delete()
            except Exception:
                pass
            await message.reply(result)
            return True

        if lower_text.startswith("/ask"):
            if group_id is None:
                await message.reply("📭 No stored group messages available for analysis.")
                return True

            parts = lower_text.split(maxsplit=1)
            if len(parts) < 2 or not parts[1].strip():
                await message.reply("❌ Invalid format. Use: /ask your question")
                return True

            question = parts[1].strip()
            if len(question) > self.config.ask_max_question_chars:
                await message.reply(
                    f"❌ Question is too long. Max {self.config.ask_max_question_chars} characters allowed."
                )
                return True

            author = safe_get_attr(message, "author")
            requester_info = {
                "user_id": safe_get_attr(author, "user_id"),
                "username": safe_get_attr(author, "username"),
                "first_name": safe_get_attr(author, "first_name"),
                "last_name": safe_get_attr(author, "last_name"),
            }

            loading = await message.reply("🧠 Analyzing chat context and preparing answer...")
            result = self.analyzer.ask_question_about_chat(
                group_id,
                question,
                hours=self.config.ask_default_hours,
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
                await message.reply("❌ Invalid format. Use: /analyze username_or_name")
                return True

            loading = await message.reply("🧠 Analyzing user behavior and persona...")
            result = self.analyzer.analyze_user_personality(group_id, parts[1].strip())
            try:
                await loading.delete()
            except Exception:
                pass
            await message.reply(result)
            return True

        if lower_text.startswith("/export_recent"):
            parts = lower_text.split(maxsplit=1)
            count = self.config.export_default_count
            if len(parts) > 1:
                try:
                    count = max(self.config.export_min_count, min(self.config.export_max_count, int(parts[1].strip())))
                except ValueError:
                    await message.reply("❌ Invalid format. Use: /export_recent 20")
                    return True

            messages = self.db.get_messages(group_id, limit=count)
            if not messages:
                await message.reply("📭 No messages available to display.")
                return True

            lines = []
            for item in messages[-count:]:
                sender = item.get("first_name") or item.get("username") or str(item.get("user_id"))
                ts = datetime.fromtimestamp(item["timestamp"]).strftime("%m-%d %H:%M")
                content = (item.get("text") or "[no text]").replace("\n", " ")[:120]
                lines.append(f"[{ts}] {sender}: {content}")

            await message.reply("📝 Latest messages:\n\n" + "\n".join(lines))
            return True

        if lower_text.startswith("/memorize"):
            if not self.can_execute_commands(message):
                await message.reply("⛔ Only the authorized user can run /memorize.")
                return True

            if group_id is None:
                await message.reply("📭 No stored group messages available to build memory.")
                return True

            loading = await message.reply("🧠 Reading and summarizing group messages...")
            try:
                existing_memory = self.analyzer.db.get_latest_group_memory(group_id)
                last_msg_id = int((existing_memory or {}).get("last_message_id", 0))
                pending = self.analyzer.db.count_messages_after(group_id, last_message_id=last_msg_id)
                if pending <= 0:
                    try:
                        await loading.delete()
                    except Exception:
                        pass
                    await message.reply("📭 No new messages found for summarization.")
                    return True

                logger.info("🧠 [/memorize] full refresh started | group=%s pending=%d", group_id, pending)
                self.analyzer.refresh_group_memory_full(group_id)

                try:
                    await loading.delete()
                except Exception:
                    pass
                await message.reply("✅ Memory updated.")
            except Exception as exc:
                try:
                    await loading.delete()
                except Exception:
                    pass
                logger.error("❌ /memorize failed | group=%s error=%s", group_id, exc)
                await message.reply(f"❌ Summarization failed: {exc}")
            return True

        return False

    async def on_message(self, message: Message) -> None:
        """Process incoming messages and keep the same command/storage behavior."""
        try:
            was_stored = await self.store_message(message)

            text = safe_get_attr(message, "text") or ""
            is_ask_command = text.strip().startswith("/ask")

            if (
                self.config.commands_enabled
                and text.startswith("/")
                and (self.can_execute_commands(message) or is_ask_command)
            ):
                handled = await self.handle_command(message, text)
                if handled:
                    logger.info("✅ command processed")
                    return
            elif text.startswith("/"):
                logger.info(
                    "⏸️ command ignored (disabled or unauthorized) | user=%s message=%s",
                    safe_get_attr(safe_get_attr(message, "author"), "user_id"),
                    safe_get_attr(message, "message_id"),
                )

            if is_group_chat(message):
                if was_stored:
                    group_id = safe_get_attr(safe_get_attr(message, "chat"), "id")
                    self.maybe_auto_memorize_group(group_id)

                    is_command_message = text.strip().startswith("/")
                    if self.config.auto_chime_enabled and not is_command_message:
                        try:
                            asyncio.create_task(self.maybe_auto_chime_group(group_id))
                        except Exception:
                            logger.exception("❌ auto-chime scheduling failed")

                    logger.info(
                        "💾 stored text message | group=%s user=%s message=%s",
                        safe_get_attr(safe_get_attr(message, "chat"), "id"),
                        safe_get_attr(safe_get_attr(message, "author"), "user_id"),
                        safe_get_attr(message, "message_id"),
                    )
                elif should_store_message(message):
                    logger.warning(
                        "⚠️ text message received but not stored | group=%s user=%s message=%s",
                        safe_get_attr(safe_get_attr(message, "chat"), "id"),
                        safe_get_attr(safe_get_attr(message, "author"), "user_id"),
                        safe_get_attr(message, "message_id"),
                    )
                else:
                    logger.info(
                        "⏭️ non-text message skipped | group=%s user=%s message=%s",
                        safe_get_attr(safe_get_attr(message, "chat"), "id"),
                        safe_get_attr(safe_get_attr(message, "author"), "user_id"),
                        safe_get_attr(message, "message_id"),
                    )
                return
        except Exception as exc:
            logger.error("❌ message processing failed: %s", exc)
            try:
                await message.reply("❌ An error occurred. Please try again.")
            except Exception:
                pass

    async def on_callback(self, callback_query: Any) -> None:
        """Handle callback queries for future extension."""
        await callback_query.answer("✅ Received")

    def run(self) -> None:
        logger.info("🤖 Bot is starting...")
        logger.info("🔑 Token prefix: %s...", self.config.bot_token[:10])
        self.bot.run()
