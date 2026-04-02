from __future__ import annotations

import json
import logging
import sqlite3
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence

logger = logging.getLogger(__name__)


class MessageDatabase:
    """SQLite-backed storage for groups, users, messages, and summaries."""

    _TIMESTAMP_EXPR = (
        "CASE WHEN typeof(m.timestamp)='text' "
        "THEN CAST(strftime('%s', m.timestamp) AS INTEGER) "
        "ELSE CAST(m.timestamp AS INTEGER) END"
    )
    _DEFAULT_BOT_USER_ID = -100

    def __init__(self, db_path: str = "messages.db"):
        self.db_path = db_path
        self.init_database()

    def _connect(self, row_factory: Any = None) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        if row_factory is not None:
            connection.row_factory = row_factory
        return connection

    def _execute_write(self, query: str, params: Sequence[Any] = ()) -> None:
        with self._connect() as connection:
            connection.execute(query, params)
            connection.commit()

    def _fetch_all(
        self,
        query: str,
        params: Sequence[Any] = (),
        *,
        decode_metadata: bool = False,
    ) -> List[Dict]:
        with self._connect(sqlite3.Row) as connection:
            rows = connection.execute(query, params).fetchall()
        return self._rows_to_dicts(rows, decode_metadata=decode_metadata)

    def _fetch_one(
        self,
        query: str,
        params: Sequence[Any] = (),
        *,
        decode_metadata: bool = False,
    ) -> Optional[Dict]:
        with self._connect(sqlite3.Row) as connection:
            row = connection.execute(query, params).fetchone()
        if not row:
            return None
        return self._row_to_dict(row, decode_metadata=decode_metadata)

    def _row_to_dict(self, row: sqlite3.Row, *, decode_metadata: bool = False) -> Dict:
        item = dict(row)
        if decode_metadata and item.get("metadata"):
            item["metadata"] = json.loads(item["metadata"])
        return item

    def _rows_to_dicts(self, rows: Iterable[sqlite3.Row], *, decode_metadata: bool = False) -> List[Dict]:
        return [self._row_to_dict(row, decode_metadata=decode_metadata) for row in rows]

    def init_database(self):
        """Create tables and indexes if they do not already exist."""
        table_statements = [
            """
            CREATE TABLE IF NOT EXISTS groups (
                group_id INTEGER PRIMARY KEY,
                group_name TEXT NOT NULL,
                description TEXT,
                members_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                is_bot BOOLEAN DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS messages (
                message_id INTEGER PRIMARY KEY,
                group_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                text TEXT,
                message_type TEXT DEFAULT 'text',
                is_bot_message INTEGER NOT NULL DEFAULT 0,
                timestamp INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                metadata TEXT,
                FOREIGN KEY (group_id) REFERENCES groups(group_id),
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS group_memory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id INTEGER NOT NULL,
                last_message_id INTEGER NOT NULL,
                message_count INTEGER NOT NULL DEFAULT 0,
                covered_until_ts INTEGER,
                summary_text TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (group_id) REFERENCES groups(group_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS group_chime_state (
                group_id INTEGER PRIMARY KEY,
                pending_count INTEGER NOT NULL DEFAULT 0,
                last_triggered_at TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (group_id) REFERENCES groups(group_id)
            )
            """,
        ]

        index_statements = [
            "CREATE INDEX IF NOT EXISTS idx_messages_group_time ON messages(group_id, timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_messages_user ON messages(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_messages_group_bot ON messages(group_id, is_bot_message, timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_group_memory_group_id ON group_memory(group_id, updated_at)",
            "CREATE INDEX IF NOT EXISTS idx_group_chime_state_updated ON group_chime_state(updated_at)",
        ]

        try:
            with self._connect() as connection:
                cursor = connection.cursor()
                for statement in table_statements:
                    cursor.execute(statement)

                # Backward-compatible migration for existing databases.
                cursor.execute("PRAGMA table_info(messages)")
                message_columns = {row[1] for row in cursor.fetchall()}
                if "is_bot_message" not in message_columns:
                    cursor.execute("ALTER TABLE messages ADD COLUMN is_bot_message INTEGER NOT NULL DEFAULT 0")

                self._migrate_legacy_bot_messages(cursor)

                for statement in index_statements:
                    cursor.execute(statement)

                connection.commit()
            logger.info("✅ Database initialized/updated successfully")
        except Exception as exc:
            logger.error("❌ Error initializing database: %s", exc)
            raise

    def _migrate_legacy_bot_messages(self, cursor: sqlite3.Cursor) -> None:
        """Move rows from legacy bot_messages table into messages and drop old table."""
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='bot_messages' LIMIT 1")
        if not cursor.fetchone():
            return

        logger.info("🔄 Legacy bot_messages table detected. Starting migration...")

        cursor.execute(
            """
            INSERT OR IGNORE INTO users (user_id, username, first_name, last_name, is_bot)
            VALUES (?, ?, ?, ?, 1)
            """,
            (self._DEFAULT_BOT_USER_ID, "pari_bot", "پری", None),
        )

        cursor.execute("PRAGMA table_info(bot_messages)")
        legacy_columns = {row[1] for row in cursor.fetchall()}
        has_reply_to = "reply_to_message_id" in legacy_columns
        has_metadata = "metadata" in legacy_columns

        select_fields = ["message_id", "group_id", "text", "timestamp"]
        select_fields.append("reply_to_message_id" if has_reply_to else "NULL AS reply_to_message_id")
        select_fields.append("metadata" if has_metadata else "NULL AS metadata")

        cursor.execute(
            f"""
            SELECT {", ".join(select_fields)}
            FROM bot_messages
            ORDER BY timestamp ASC, message_id ASC
            """
        )
        legacy_rows = cursor.fetchall()
        migrated_count = 0

        for message_id, group_id, text, timestamp, reply_to_message_id, metadata in legacy_rows:
            parsed_metadata: Dict[str, Any] = {}
            if metadata:
                try:
                    parsed_metadata = json.loads(metadata)
                except Exception:
                    parsed_metadata = {}

            if reply_to_message_id and "reply_to_message_id" not in parsed_metadata:
                parsed_metadata["reply_to_message_id"] = reply_to_message_id

            bot_user_id = parsed_metadata.get("bot_author_id") or self._DEFAULT_BOT_USER_ID
            try:
                bot_user_id = int(bot_user_id)
            except Exception:
                bot_user_id = self._DEFAULT_BOT_USER_ID

            bot_username = parsed_metadata.get("bot_author_username")

            cursor.execute(
                """
                INSERT OR IGNORE INTO users (user_id, username, first_name, last_name, is_bot)
                VALUES (?, ?, ?, ?, 1)
                """,
                (bot_user_id, bot_username, "پری", None),
            )

            cursor.execute(
                """
                INSERT OR IGNORE INTO messages
                (message_id, group_id, user_id, text, message_type, is_bot_message, timestamp, metadata)
                VALUES (?, ?, ?, ?, ?, 1, ?, ?)
                """,
                (
                    int(message_id),
                    int(group_id),
                    int(bot_user_id),
                    text,
                    "text",
                    int(timestamp),
                    json.dumps(parsed_metadata, ensure_ascii=False) if parsed_metadata else None,
                ),
            )
            migrated_count += cursor.rowcount or 0

        cursor.execute("DROP TABLE IF EXISTS bot_messages")
        logger.info(
            "✅ Legacy bot_messages migrated: %s row(s) moved, table dropped",
            migrated_count,
        )

    def _make_json_safe(self, value):
        """Recursively convert values into JSON-serializable structures."""
        if isinstance(value, dict):
            return {str(key): self._make_json_safe(item) for key, item in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [self._make_json_safe(item) for item in value]
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        return str(value)

    def add_group(self, group_id: int, group_name: str, description: str = None, members_count: int = 0):
        """Insert or replace a group record."""
        try:
            self._execute_write(
                """
                INSERT OR REPLACE INTO groups
                (group_id, group_name, description, members_count, last_updated)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (group_id, group_name, description, members_count),
            )
            logger.info("✅ Group '%s' (ID: %s) saved", group_name, group_id)
        except Exception as exc:
            logger.error("❌ Error adding group: %s", exc)

    def add_user(
        self,
        user_id: int,
        username: str = None,
        first_name: str = None,
        last_name: str = None,
        is_bot: bool = False,
    ):
        """Insert or replace a user record."""
        try:
            self._execute_write(
                """
                INSERT OR REPLACE INTO users
                (user_id, username, first_name, last_name, is_bot)
                VALUES (?, ?, ?, ?, ?)
                """,
                (user_id, username, first_name, last_name, is_bot),
            )
        except Exception as exc:
            logger.error("❌ Error adding user: %s", exc)

    def add_message(
        self,
        message_id: int,
        group_id: int,
        user_id: int,
        text: str,
        timestamp: int,
        message_type: str = "text",
        metadata: Dict = None,
        is_bot_message: bool = False,
    ) -> bool:
        """Insert or replace a message record."""
        try:
            metadata_json = json.dumps(self._make_json_safe(metadata), ensure_ascii=False) if metadata else None
            self._execute_write(
                """
                INSERT OR REPLACE INTO messages
                (message_id, group_id, user_id, text, message_type, is_bot_message, timestamp, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    message_id,
                    group_id,
                    user_id,
                    text,
                    message_type,
                    1 if is_bot_message else 0,
                    timestamp,
                    metadata_json,
                ),
            )
            logger.info("✅ Message #%s from group %s saved", message_id, group_id)
            return True
        except Exception as exc:
            logger.error("❌ Error saving message: %s", exc)
            return False

    def get_messages(
        self,
        group_id: int,
        limit: int = None,
        start_time: int = None,
        end_time: int = None,
    ) -> List[Dict]:
        """Return messages for a group with optional time filtering."""
        try:
            query = f"""
                SELECT
                    m.message_id, m.group_id, m.user_id, m.text,
                    m.message_type,
                    m.is_bot_message,
                    {self._TIMESTAMP_EXPR} AS timestamp,
                    m.metadata,
                    u.username, u.first_name, u.last_name
                FROM messages m
                JOIN users u ON m.user_id = u.user_id
                WHERE m.group_id = ?
            """
            params: List[Any] = [group_id]

            if start_time:
                query += f" AND {self._TIMESTAMP_EXPR} >= ?"
                params.append(start_time)

            if end_time:
                query += f" AND {self._TIMESTAMP_EXPR} <= ?"
                params.append(end_time)

            query += " ORDER BY m.timestamp ASC"

            if limit:
                query += f" LIMIT {limit}"

            return self._fetch_all(query, params, decode_metadata=True)
        except Exception as exc:
            logger.error("❌ Error fetching messages: %s", exc)
            return []

    def get_user_messages(self, group_id: int, user_id: int) -> List[Dict]:
        """Return all messages for a user inside a specific group."""
        try:
            return self._fetch_all(
                """
                SELECT
                    m.message_id, m.group_id, m.user_id, m.text,
                    m.message_type, m.is_bot_message, m.timestamp, m.metadata,
                    u.username, u.first_name, u.last_name
                FROM messages m
                JOIN users u ON m.user_id = u.user_id
                WHERE m.group_id = ? AND m.user_id = ?
                ORDER BY m.timestamp ASC
                """,
                (group_id, user_id),
            )
        except Exception as exc:
            logger.error("❌ Error fetching user messages: %s", exc)
            return []

    def get_messages_by_user(
        self,
        user_id: int,
        limit: int = None,
        start_time: int = None,
        end_time: int = None,
    ) -> List[Dict]:
        """Return messages for a user across all chats."""
        try:
            query = f"""
                SELECT
                    m.message_id,
                    m.group_id,
                    g.group_name,
                    m.user_id,
                    m.text,
                    m.message_type,
                    m.is_bot_message,
                    {self._TIMESTAMP_EXPR} AS timestamp,
                    m.metadata,
                    u.username,
                    u.first_name,
                    u.last_name
                FROM messages m
                JOIN users u ON m.user_id = u.user_id
                LEFT JOIN groups g ON m.group_id = g.group_id
                WHERE m.user_id = ?
            """
            params: List[Any] = [user_id]

            if start_time:
                query += f" AND {self._TIMESTAMP_EXPR} >= ?"
                params.append(start_time)

            if end_time:
                query += f" AND {self._TIMESTAMP_EXPR} <= ?"
                params.append(end_time)

            query += f" ORDER BY {self._TIMESTAMP_EXPR} ASC"

            if limit:
                query += f" LIMIT {limit}"

            return self._fetch_all(query, params, decode_metadata=True)
        except Exception as exc:
            logger.error("❌ Error fetching messages by user: %s", exc)
            return []

    def get_group_stats(self, group_id: int) -> Dict:
        """Return aggregate stats for a group."""
        try:
            with self._connect() as connection:
                cursor = connection.cursor()

                cursor.execute("SELECT COUNT(*) FROM messages WHERE group_id = ?", (group_id,))
                total_messages = cursor.fetchone()[0]

                cursor.execute(
                    "SELECT COUNT(DISTINCT user_id) FROM messages WHERE group_id = ? AND is_bot_message = 0",
                    (group_id,),
                )
                active_users = cursor.fetchone()[0]

                cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM messages WHERE group_id = ?", (group_id,))
                min_time, max_time = cursor.fetchone()

                cursor.execute(
                    """
                    SELECT u.user_id, u.username, u.first_name, COUNT(m.message_id) as message_count
                    FROM messages m
                    JOIN users u ON m.user_id = u.user_id
                    WHERE m.group_id = ? AND m.is_bot_message = 0
                    GROUP BY u.user_id
                    ORDER BY message_count DESC
                    LIMIT 10
                    """,
                    (group_id,),
                )
                top_users = [
                    dict(zip(["user_id", "username", "first_name", "message_count"], row)) for row in cursor.fetchall()
                ]

            return {
                "group_id": group_id,
                "total_messages": total_messages,
                "active_users": active_users,
                "first_message_time": min_time,
                "last_message_time": max_time,
                "top_users": top_users,
            }
        except Exception as exc:
            logger.error("❌ Error fetching group stats: %s", exc)
            return {}

    def get_group_users(self, group_id: int, limit: int = 200) -> List[Dict]:
        """Return users in a group with message counts and last activity."""
        try:
            query = f"""
                SELECT
                    u.user_id,
                    u.username,
                    u.first_name,
                    u.last_name,
                    u.is_bot,
                    COUNT(m.message_id) AS message_count,
                    MAX({self._TIMESTAMP_EXPR}) AS last_activity_ts
                FROM messages m
                JOIN users u ON m.user_id = u.user_id
                WHERE m.group_id = ? AND m.is_bot_message = 0
                GROUP BY u.user_id, u.username, u.first_name, u.last_name, u.is_bot
                ORDER BY message_count DESC
                LIMIT ?
            """
            return self._fetch_all(query, (group_id, limit))
        except Exception as exc:
            logger.error("❌ Error fetching group users: %s", exc)
            return []

    def get_group_info(self, group_id: int) -> Optional[Dict]:
        """Return group metadata if it exists."""
        try:
            return self._fetch_one("SELECT * FROM groups WHERE group_id = ?", (group_id,))
        except Exception as exc:
            logger.error("❌ Error fetching group info: %s", exc)
            return None

    def get_recent_messages(self, group_id: int, limit: int = 20) -> List[Dict]:
        """Return the latest messages for a group in ascending order."""
        try:
            query = f"""
                SELECT * FROM (
                    SELECT
                        m.message_id, m.group_id, m.user_id, m.text,
                        m.message_type,
                        m.is_bot_message,
                        {self._TIMESTAMP_EXPR} AS timestamp,
                        m.metadata,
                        u.username, u.first_name, u.last_name
                    FROM messages m
                    JOIN users u ON m.user_id = u.user_id
                    WHERE m.group_id = ?
                    ORDER BY {self._TIMESTAMP_EXPR} DESC
                    LIMIT ?
                ) recent_messages
                ORDER BY timestamp ASC
            """
            return self._fetch_all(query, (group_id, limit), decode_metadata=True)
        except Exception as exc:
            logger.error("❌ Error fetching recent messages: %s", exc)
            return []

    def get_latest_group_memory(self, group_id: int) -> Optional[Dict]:
        """Return the latest cumulative memory entry for a group."""
        try:
            return self._fetch_one(
                """
                SELECT *
                FROM group_memory
                WHERE group_id = ?
                ORDER BY updated_at DESC, id DESC
                LIMIT 1
                """,
                (group_id,),
            )
        except Exception as exc:
            logger.error("❌ Error fetching group memory: %s", exc)
            return None

    def save_group_memory(
        self,
        group_id: int,
        last_message_id: int,
        message_count: int,
        covered_until_ts: int,
        summary_text: str,
    ) -> bool:
        """Persist a new cumulative summary for a group."""
        try:
            self._execute_write(
                """
                INSERT INTO group_memory
                (group_id, last_message_id, message_count, covered_until_ts, summary_text, updated_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (group_id, last_message_id, message_count, covered_until_ts, summary_text),
            )
            return True
        except Exception as exc:
            logger.error("❌ Error saving group memory: %s", exc)
            return False

    def get_messages_after(self, group_id: int, last_message_id: int = 0, limit: int = 120) -> List[Dict]:
        """Return messages newer than the last summarized message."""
        try:
            query = f"""
                SELECT
                    m.message_id, m.group_id, m.user_id, m.text,
                    m.message_type,
                    m.is_bot_message,
                    {self._TIMESTAMP_EXPR} AS timestamp,
                    m.metadata,
                    u.username, u.first_name, u.last_name
                FROM messages m
                JOIN users u ON m.user_id = u.user_id
                                WHERE m.group_id = ? AND m.message_id > ?
                ORDER BY m.message_id ASC
                LIMIT ?
            """
            return self._fetch_all(query, (group_id, last_message_id, limit), decode_metadata=True)
        except Exception as exc:
            logger.error("❌ Error fetching new group messages: %s", exc)
            return []

    def count_messages_after(self, group_id: int, last_message_id: int = 0) -> int:
        """Count unsummarized messages for a group."""
        try:
            with self._connect() as connection:
                count = connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM messages
                                        WHERE group_id = ? AND message_id > ?
                    """,
                    (group_id, last_message_id),
                ).fetchone()[0]
            return int(count or 0)
        except Exception as exc:
            logger.error("❌ Error counting unsummarized messages: %s", exc)
            return 0

    def mark_chime_message_and_should_trigger(self, group_id: int, every_n: int = 10) -> bool:
        """Increment auto-chime counter and decide whether it should trigger."""
        try:
            threshold = max(1, int(every_n or 1))

            with self._connect() as connection:
                cursor = connection.cursor()
                cursor.execute("BEGIN IMMEDIATE")
                cursor.execute(
                    """
                    INSERT INTO group_chime_state (group_id, pending_count, updated_at)
                    VALUES (?, 0, CURRENT_TIMESTAMP)
                    ON CONFLICT(group_id) DO NOTHING
                    """,
                    (group_id,),
                )

                cursor.execute("SELECT pending_count FROM group_chime_state WHERE group_id = ?", (group_id,))
                row = cursor.fetchone()
                current_pending = int((row[0] if row else 0) or 0)
                next_pending = current_pending + 1

                should_trigger = next_pending >= threshold
                if should_trigger:
                    cursor.execute(
                        """
                        UPDATE group_chime_state
                        SET pending_count = 0,
                            last_triggered_at = CURRENT_TIMESTAMP,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE group_id = ?
                        """,
                        (group_id,),
                    )
                else:
                    cursor.execute(
                        """
                        UPDATE group_chime_state
                        SET pending_count = ?,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE group_id = ?
                        """,
                        (next_pending, group_id),
                    )

                connection.commit()
            return should_trigger
        except Exception as exc:
            logger.error("❌ Error updating auto-chime state: %s", exc)
            return False

    def search_messages(self, group_id: int, query_text: str, limit: int = 25) -> List[Dict]:
        """Search related messages using a simple SQLite relevance score."""
        try:
            terms = [term.strip() for term in query_text.split() if len(term.strip()) >= 3]
            if not terms:
                return self.get_recent_messages(group_id, limit=limit)

            score_parts: List[str] = []
            params: List[Any] = []

            for term in terms[:8]:
                score_parts.append("CASE WHEN lower(m.text) LIKE lower(?) THEN 1 ELSE 0 END")
                params.append(f"%{term}%")

            score_expr = " + ".join(score_parts) if score_parts else "0"
            query = f"""
                SELECT
                    m.message_id, m.group_id, m.user_id, m.text,
                    m.message_type,
                    m.is_bot_message,
                    {self._TIMESTAMP_EXPR} AS timestamp,
                    m.metadata,
                    u.username, u.first_name, u.last_name,
                    ({score_expr}) AS relevance_score
                FROM messages m
                JOIN users u ON m.user_id = u.user_id
                WHERE m.group_id = ?
                  AND m.text IS NOT NULL
                  AND trim(m.text) <> ''
                ORDER BY relevance_score DESC, {self._TIMESTAMP_EXPR} DESC
                LIMIT ?
            """

            messages = []
            for item in self._fetch_all(query, [*params, group_id, limit], decode_metadata=True):
                if item.get("relevance_score", 0) > 0:
                    messages.append(item)

            return list(reversed(messages)) if messages else []
        except Exception as exc:
            logger.error("❌ Error searching related messages: %s", exc)
            return []

    @staticmethod
    def _extract_reply_to_message_id(message: Dict) -> Optional[int]:
        metadata = message.get("metadata") or {}
        if isinstance(metadata, dict):
            direct_reply_to = metadata.get("reply_to_message_id")
            if direct_reply_to:
                return int(direct_reply_to)

            nested = metadata.get("message")
            if isinstance(nested, dict) and nested.get("reply_to_message_id"):
                return int(nested.get("reply_to_message_id"))

        return None

    def get_message_by_id(self, group_id: int, message_id: int) -> Optional[Dict]:
        """Fetch one message by id from the merged messages table."""
        try:
            query = f"""
                SELECT
                    m.message_id, m.group_id, m.user_id, m.text,
                    m.message_type, m.is_bot_message,
                    {self._TIMESTAMP_EXPR} AS timestamp,
                    m.metadata,
                    u.username, u.first_name, u.last_name
                FROM messages m
                LEFT JOIN users u ON m.user_id = u.user_id
                WHERE m.group_id = ? AND m.message_id = ?
                LIMIT 1
            """
            return self._fetch_one(query, (group_id, message_id), decode_metadata=True)
        except Exception as exc:
            logger.error("❌ Error fetching message by id: %s", exc)
            return None

    def get_reply_chain_context(self, group_id: int, start_message_id: int, max_depth: int = 5) -> List[Dict]:
        """Get up to max_depth previous reply-chain messages including the start message."""
        chain: List[Dict] = []
        current_message_id = int(start_message_id)
        depth = 0

        while current_message_id and depth < max_depth:
            message = self.get_message_by_id(group_id, current_message_id)
            if not message:
                break

            chain.append(message)
            current_message_id = self._extract_reply_to_message_id(message) or 0
            depth += 1

        chain.reverse()
        return chain

    def get_all_groups(self) -> List[Dict]:
        """Return all groups ordered by latest update."""
        try:
            return self._fetch_all("SELECT * FROM groups ORDER BY last_updated DESC")
        except Exception as exc:
            logger.error("❌ Error fetching groups: %s", exc)
            return []

    def clear_messages(self, group_id: int = None):
        """Delete messages for one group or for all groups."""
        try:
            if group_id:
                self._execute_write("DELETE FROM messages WHERE group_id = ?", (group_id,))
                logger.info("✅ All messages for group %s were deleted", group_id)
            else:
                self._execute_write("DELETE FROM messages")
                logger.info("✅ All messages were deleted")
        except Exception as exc:
            logger.error("❌ Error deleting messages: %s", exc)
