from __future__ import annotations

import asyncio
import logging
import re
import threading
from datetime import datetime
from typing import Any, Optional

from bale import Bot, Message
from dotenv import load_dotenv

from .ai_analyzer import ChatAnalyzer
from .config import AppConfig
from .database import MessageDatabase
from .message_utils import (
    contains_ask_like_keyword,
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

    def _persist_bot_message(
        self,
        sent_message: Message,
        fallback_group_id: Optional[int] = None,
        bot_source: str = "ask",
    ) -> None:
        """Persist an outgoing bot message to dedicated storage for future reply context."""
        try:
            if not sent_message:
                return

            chat = safe_get_attr(sent_message, "chat")
            reply_to = safe_get_attr(sent_message, "reply_to_message")

            message_id = safe_get_attr(sent_message, "message_id")
            group_id = safe_get_attr(chat, "id") or fallback_group_id
            text = get_message_text_content(sent_message)
            timestamp = normalize_timestamp(safe_get_attr(sent_message, "date"))
            reply_to_message_id = safe_get_attr(reply_to, "message_id")

            if not message_id or group_id is None:
                return

            metadata = {
                "chat_title": safe_get_attr(chat, "title"),
                "chat_type": safe_get_attr(chat, "type"),
                "bot_author_id": safe_get_attr(safe_get_attr(sent_message, "author"), "user_id"),
                "bot_author_username": safe_get_attr(safe_get_attr(sent_message, "author"), "username"),
                "reply_to_message_id": reply_to_message_id,
            }

            bot_author = safe_get_attr(sent_message, "author")
            bot_user_id = safe_get_attr(bot_author, "user_id")
            bot_username = safe_get_attr(bot_author, "username")
            bot_first_name = safe_get_attr(bot_author, "first_name") or "پری"
            bot_last_name = safe_get_attr(bot_author, "last_name")

            if bot_user_id:
                self.db.add_user(
                    user_id=bot_user_id,
                    username=bot_username,
                    first_name=bot_first_name,
                    last_name=bot_last_name,
                    is_bot=True,
                )

            stored = self.db.add_message(
                message_id=message_id,
                group_id=group_id,
                user_id=int(bot_user_id or 0),
                text=text,
                timestamp=timestamp,
                message_type=f"bot_{bot_source}",
                metadata=metadata,
                is_bot_message=True,
            )

            if stored:
                self.maybe_auto_memorize_group(int(group_id))
        except Exception as exc:
            logger.error("❌ Failed to persist bot message: %s", exc)

    async def _reply_and_store(
        self,
        message: Message,
        text: str,
        *,
        store: bool = True,
        bot_source: str = "ask",
    ) -> Optional[Message]:
        """Send reply through Bale Message API and optionally persist it as bot message."""
        sent = await message.reply(text)
        if store:
            self._persist_bot_message(
                sent,
                fallback_group_id=safe_get_attr(safe_get_attr(message, "chat"), "id"),
                bot_source=bot_source,
            )
        return sent

    async def _send_and_store(
        self,
        chat_id: int,
        text: str,
        *,
        reply_to_message_id: Optional[int] = None,
        store: bool = True,
        bot_source: str = "ask",
    ) -> Optional[Message]:
        """Send a message via bot API and optionally persist it as bot message."""
        sent = await self.bot.send_message(chat_id, text, reply_to_message_id=reply_to_message_id)
        if store:
            self._persist_bot_message(sent, fallback_group_id=chat_id, bot_source=bot_source)
        return sent

    @staticmethod
    def _normalize_reply_punctuation(text: str) -> str:
        """Keep reply text plain by removing punctuation styles the user doesn't want."""
        value = (text or "").strip()
        if not value:
            return value

        value = value.replace("،", " ").replace(",", " ")
        value = value.replace("!", " ").replace("！", " ")
        value = re.sub(r"\s+", " ", value).strip()
        return value

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
            await self._send_and_store(
                group_id, reply_text, reply_to_message_id=target_message_id, store=True, bot_source="chime"
            )
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

    async def _handle_ask_flow(
        self,
        message: Message,
        question: str,
        show_loading: bool = True,
        length_check_text: Optional[str] = None,
    ) -> bool:
        """Run the same analysis flow used by /ask command."""
        group_id = self.resolve_target_group_id(message)
        if group_id is None:
            await self._reply_and_store(message, "📭 No stored group messages available for analysis.", store=False)
            return True

        normalized_question = (question or "").strip()
        if not normalized_question:
            await self._reply_and_store(message, "❌ Invalid format. Use: /ask your question", store=False)
            return True

        text_for_length_check = (length_check_text if length_check_text is not None else normalized_question).strip()
        if len(text_for_length_check) > self.config.ask_max_question_chars:
            await self._reply_and_store(
                message,
                f"❌ Question is too long. Max {self.config.ask_max_question_chars} characters allowed.",
                store=False,
            )
            return True

        author = safe_get_attr(message, "author")
        requester_info = {
            "user_id": safe_get_attr(author, "user_id"),
            "username": safe_get_attr(author, "username"),
            "first_name": safe_get_attr(author, "first_name"),
            "last_name": safe_get_attr(author, "last_name"),
        }

        loading = None
        if show_loading:
            loading = await self._reply_and_store(
                message, "🧠 Analyzing chat context and preparing answer...", store=False
            )
        result = self.analyzer.ask_question_about_chat(
            group_id,
            normalized_question,
            hours=self.config.ask_default_hours,
            requester_info=requester_info,
        )
        if loading is not None:
            try:
                await loading.delete()
            except Exception:
                pass
        normalized_result = self._normalize_reply_punctuation(result)
        await self._reply_and_store(message, normalized_result, store=True)
        return True

    async def _handle_reply_to_bot_message(self, message: Message) -> bool:
        """Handle user replies to previously sent bot messages with contextual response."""
        if not self.config.commands_enabled or not self.analyzer or not self.analyzer.is_available():
            return False

        if not is_group_chat(message):
            return False

        reply_to = safe_get_attr(message, "reply_to_message")
        reply_to_message_id = safe_get_attr(reply_to, "message_id")
        if not reply_to_message_id:
            return False

        group_id = safe_get_attr(safe_get_attr(message, "chat"), "id")
        if group_id is None:
            return False

        bot_message = self.db.get_message_by_id(group_id=group_id, message_id=reply_to_message_id)
        if not bot_message or not bool(bot_message.get("is_bot_message")):
            return False

        user_text = get_message_text_content(message)
        if not user_text:
            return False

        chain_depth = max(1, int(self.config.reply_chain_max_depth))
        chain_messages = self.db.get_reply_chain_context(
            group_id=group_id,
            start_message_id=reply_to_message_id,
            max_depth=chain_depth,
        )

        def _format_chain_item(item: dict) -> str:
            sender = (
                "پری"
                if item.get("is_bot_message")
                else (
                    f"{item.get('first_name') or ''} {item.get('last_name') or ''}".strip()
                    or item.get("username")
                    or str(item.get("user_id"))
                )
            )
            body = (item.get("text") or "[بدون متن]").strip()
            return f"- {sender}: {body}"

        chain_context = "\n".join(_format_chain_item(item) for item in chain_messages) if chain_messages else "-"

        bot_text = (bot_message.get("text") or "").strip()
        contextual_question = (
            "کاربر در حال ریپلای به پیام قبلی ربات است. "
            f"زنجیره ریپلای تا {chain_depth} پیام قبل:\n{chain_context}\n"
            f"پیام قبلی ربات: {bot_text or '[بدون متن]'}\n"
            f"پیام جدید کاربر: {user_text}\n"
            "با توجه به پیام قبلی ربات و پیام جدید کاربر، پاسخ کوتاه و مستقیم بده."
        )

        handled = await self._handle_ask_flow(
            message,
            contextual_question,
            show_loading=False,
            length_check_text=user_text,
        )
        if handled:
            logger.info("✅ reply-to-bot flow processed | group=%s reply_to=%s", group_id, reply_to_message_id)
        return handled

    async def handle_command(self, message: Message, text: str) -> bool:
        """Handle supported bot commands."""
        lower_text = text.strip()
        requester_id = safe_get_attr(safe_get_attr(message, "author"), "user_id")
        group_id = self.resolve_target_group_id(message)

        if group_id is None and not lower_text.startswith("/start") and not lower_text.startswith("/help"):
            await self._reply_and_store(message, "📭 No stored group messages found yet for this command.", store=False)
            return True

        if lower_text.startswith("/start"):
            await self._reply_and_store(
                message,
                "👋 Hi! I am a conversation archive and analysis bot.\n"
                "I store this group's messages so you can query them later.\n\n" + self.build_help_message(),
                store=False,
            )
            return True

        if lower_text.startswith("/help"):
            await self._reply_and_store(message, self.build_help_message(), store=False)
            return True

        if lower_text.startswith("/stats"):
            stats = self.db.get_group_stats(group_id)
            if not stats or not stats.get("total_messages"):
                await self._reply_and_store(message, "📭 No data stored for this group yet.", store=False)
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
            await self._reply_and_store(message, response, store=False)
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
                    await self._reply_and_store(message, "❌ Invalid format. Use: /summary 24", store=False)
                    return True

            loading = await self._reply_and_store(message, "🧠 Summarizing recent conversations...", store=False)
            result = self.analyzer.summarize_recent_user_messages(requester_id, hours)
            try:
                await loading.delete()
            except Exception:
                pass
            await self._reply_and_store(message, result, store=False)
            return True

        if lower_text.startswith("/ask"):
            parts = lower_text.split(maxsplit=1)
            if len(parts) < 2 or not parts[1].strip():
                await self._reply_and_store(message, "❌ Invalid format. Use: /ask your question", store=False)
                return True

            return await self._handle_ask_flow(message, parts[1], show_loading=True)

        if lower_text.startswith("/analyze"):
            parts = lower_text.split(maxsplit=1)
            if len(parts) < 2 or not parts[1].strip():
                await self._reply_and_store(message, "❌ Invalid format. Use: /analyze username_or_name", store=False)
                return True

            loading = await self._reply_and_store(message, "🧠 Analyzing user behavior and persona...", store=False)
            result = self.analyzer.analyze_user_personality(group_id, parts[1].strip())
            try:
                await loading.delete()
            except Exception:
                pass
            await self._reply_and_store(message, result)
            return True

        if lower_text.startswith("/export_recent"):
            parts = lower_text.split(maxsplit=1)
            count = self.config.export_default_count
            if len(parts) > 1:
                try:
                    count = max(self.config.export_min_count, min(self.config.export_max_count, int(parts[1].strip())))
                except ValueError:
                    await self._reply_and_store(message, "❌ Invalid format. Use: /export_recent 20", store=False)
                    return True

            messages = self.db.get_messages(group_id, limit=count)
            if not messages:
                await self._reply_and_store(message, "📭 No messages available to display.", store=False)
                return True

            lines = []
            for item in messages[-count:]:
                sender = item.get("first_name") or item.get("username") or str(item.get("user_id"))
                ts = datetime.fromtimestamp(item["timestamp"]).strftime("%m-%d %H:%M")
                content = (item.get("text") or "[no text]").replace("\n", " ")[:120]
                lines.append(f"[{ts}] {sender}: {content}")

            await self._reply_and_store(message, "📝 Latest messages:\n\n" + "\n".join(lines), store=False)
            return True

        if lower_text.startswith("/memorize"):
            if not self.can_execute_commands(message):
                await self._reply_and_store(message, "⛔ Only the authorized user can run /memorize.", store=False)
                return True

            if group_id is None:
                await self._reply_and_store(
                    message, "📭 No stored group messages available to build memory.", store=False
                )
                return True

            loading = await self._reply_and_store(message, "🧠 Reading and summarizing group messages...", store=False)
            try:
                existing_memory = self.analyzer.db.get_latest_group_memory(group_id)
                last_msg_id = int((existing_memory or {}).get("last_message_id", 0))
                pending = self.analyzer.db.count_messages_after(group_id, last_message_id=last_msg_id)
                if pending <= 0:
                    try:
                        await loading.delete()
                    except Exception:
                        pass
                    await self._reply_and_store(message, "📭 No new messages found for summarization.", store=False)
                    return True

                logger.info("🧠 [/memorize] full refresh started | group=%s pending=%d", group_id, pending)
                self.analyzer.refresh_group_memory_full(group_id)

                try:
                    await loading.delete()
                except Exception:
                    pass
                await self._reply_and_store(message, "✅ Memory updated.", store=False)
            except Exception as exc:
                try:
                    await loading.delete()
                except Exception:
                    pass
                logger.error("❌ /memorize failed | group=%s error=%s", group_id, exc)
                await self._reply_and_store(message, f"❌ Summarization failed: {exc}", store=False)
            return True

        return False

    async def on_message(self, message: Message) -> None:
        """Process incoming messages and keep the same command/storage behavior."""
        try:
            was_stored = await self.store_message(message)

            text = safe_get_attr(message, "text") or ""
            is_ask_command = text.strip().startswith("/ask")

            # Always run auto-memorize for group messages regardless of what follows.
            if is_group_chat(message) and was_stored:
                group_id = safe_get_attr(safe_get_attr(message, "chat"), "id")
                self.maybe_auto_memorize_group(group_id)

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

            if await self._handle_reply_to_bot_message(message):
                return

            # Feature: treat messages containing configured keywords as /ask-style request.
            if (
                self.config.commands_enabled
                and contains_ask_like_keyword(text)
                and self.analyzer
                and self.analyzer.is_available()
            ):
                handled_keyword_ask = await self._handle_ask_flow(message, text, show_loading=False)
                if handled_keyword_ask:
                    logger.info("✅ ask-like keyword flow processed")
                    return

            if is_group_chat(message):
                if was_stored:
                    group_id = safe_get_attr(safe_get_attr(message, "chat"), "id")

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
                await self._reply_and_store(message, "❌ An error occurred. Please try again.")
            except Exception:
                pass

    async def on_callback(self, callback_query: Any) -> None:
        """Handle callback queries for future extension."""
        await callback_query.answer("✅ Received")

    def run(self) -> None:
        logger.info("🤖 Bot is starting...")
        logger.info("🔑 Token prefix: %s...", self.config.bot_token[:10])
        self.bot.run()
