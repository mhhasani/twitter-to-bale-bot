"""
مدیریت دیتابیس SQLite برای ذخیره پیام‌های گروه‌ها
"""

import sqlite3
import json
from datetime import date, datetime
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)


class MessageDatabase:
    """
    کلاس مدیریت دیتابیس برای ذخیره پیام‌های گروه‌ها
    """

    def __init__(self, db_path: str = "messages.db"):
        """
        اولیه‌سازی دیتابیس

        Args:
            db_path: مسیر فایل دیتابیس SQLite
        """
        self.db_path = db_path
        self.init_database()

    def init_database(self):
        """
        ایجاد جداول دیتابیس اگر وجود ندارند
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # جدول گروه‌ها
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS groups (
                    group_id INTEGER PRIMARY KEY,
                    group_name TEXT NOT NULL,
                    description TEXT,
                    members_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # جدول کاربران
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    is_bot BOOLEAN DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # جدول پیام‌ها
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    message_id INTEGER PRIMARY KEY,
                    group_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    text TEXT,
                    message_type TEXT DEFAULT 'text',
                    timestamp INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    metadata TEXT,
                    FOREIGN KEY (group_id) REFERENCES groups(group_id),
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            """)

            cursor.execute("""
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
            """)

            # ایجاد ایندکس‌ها برای بهتری جستجو
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_group_time ON messages(group_id, timestamp)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_user ON messages(user_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_group_memory_group_id ON group_memory(group_id, updated_at)")

            conn.commit()
            conn.close()
            logger.info("✅ دیتابیس با موفقیت ایجاد/بروزرسانی شد")

        except Exception as e:
            logger.error(f"❌ خطا در ایجاد دیتابیس: {e}")
            raise

    def _make_json_safe(self, value):
        """
        تبدیل بازگشتی داده‌ها به فرم قابل ذخیره در JSON.
        """
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
        """
        اضافه کردن گروه جدید یا بروزرسانی موجود

        Args:
            group_id: شناسه گروه
            group_name: نام گروه
            description: توضیحات گروه
            members_count: تعداد اعضا
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute(
                """
                INSERT OR REPLACE INTO groups 
                (group_id, group_name, description, members_count, last_updated)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
                (group_id, group_name, description, members_count),
            )

            conn.commit()
            conn.close()
            logger.info(f"✅ گروه '{group_name}' (ID: {group_id}) ذخیره شد")

        except Exception as e:
            logger.error(f"❌ خطا در اضافه کردن گروه: {e}")

    def add_user(
        self, user_id: int, username: str = None, first_name: str = None, last_name: str = None, is_bot: bool = False
    ):
        """
        اضافه کردن کاربر جدید یا بروزرسانی موجود

        Args:
            user_id: شناسه کاربر
            username: نام کاربری
            first_name: نام کوچک
            last_name: نام خانوادگی
            is_bot: آیا کاربر یک بات است
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute(
                """
                INSERT OR REPLACE INTO users 
                (user_id, username, first_name, last_name, is_bot)
                VALUES (?, ?, ?, ?, ?)
            """,
                (user_id, username, first_name, last_name, is_bot),
            )

            conn.commit()
            conn.close()

        except Exception as e:
            logger.error(f"❌ خطا در اضافه کردن کاربر: {e}")

    def add_message(
        self,
        message_id: int,
        group_id: int,
        user_id: int,
        text: str,
        timestamp: int,
        message_type: str = "text",
        metadata: Dict = None,
    ) -> bool:
        """
        ذخیره پیام جدید

        Args:
            message_id: شناسه پیام
            group_id: شناسه گروه
            user_id: شناسه کاربر فرستنده
            text: متن پیام
            timestamp: زمان ارسال (Unix timestamp)
            message_type: نوع پیام (text, photo, video, etc.)
            metadata: متادیتای اضافی
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            metadata_json = json.dumps(self._make_json_safe(metadata), ensure_ascii=False) if metadata else None

            cursor.execute(
                """
                INSERT OR REPLACE INTO messages 
                (message_id, group_id, user_id, text, message_type, timestamp, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (message_id, group_id, user_id, text, message_type, timestamp, metadata_json),
            )

            conn.commit()
            conn.close()
            logger.info(f"✅ پیام #{message_id} از گروه {group_id} ذخیره شد")
            return True

        except Exception as e:
            logger.error(f"❌ خطا در ذخیره پیام: {e}")
            return False

    def get_messages(
        self, group_id: int, limit: int = None, start_time: int = None, end_time: int = None
    ) -> List[Dict]:
        """
        دریافت پیام‌های گروه

        Args:
            group_id: شناسه گروه
            limit: حداکثر تعداد پیام‌ها
            start_time: زمان شروع (Unix timestamp)
            end_time: زمان پایان (Unix timestamp)

        Returns:
            لیست پیام‌ها با اطلاعات کامل
        """
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            ts_expr = "CASE WHEN typeof(m.timestamp)='text' THEN CAST(strftime('%s', m.timestamp) AS INTEGER) ELSE CAST(m.timestamp AS INTEGER) END"

            query = """
                SELECT 
                    m.message_id, m.group_id, m.user_id, m.text, 
                    m.message_type,
                    {ts_expr} AS timestamp,
                    m.metadata,
                    u.username, u.first_name, u.last_name
                FROM messages m
                JOIN users u ON m.user_id = u.user_id
                WHERE m.group_id = ?
            """.format(ts_expr=ts_expr)
            params = [group_id]

            if start_time:
                query += f" AND {ts_expr} >= ?"
                params.append(start_time)

            if end_time:
                query += f" AND {ts_expr} <= ?"
                params.append(end_time)

            query += " ORDER BY m.timestamp ASC"

            if limit:
                query += f" LIMIT {limit}"

            cursor.execute(query, params)
            rows = cursor.fetchall()

            messages = []
            for row in rows:
                msg_dict = dict(row)
                if msg_dict["metadata"]:
                    msg_dict["metadata"] = json.loads(msg_dict["metadata"])
                messages.append(msg_dict)

            conn.close()
            return messages

        except Exception as e:
            logger.error(f"❌ خطا در دریافت پیام‌ها: {e}")
            return []

    def get_user_messages(self, group_id: int, user_id: int) -> List[Dict]:
        """
        دریافت تمام پیام‌های یک کاربر در گروه

        Args:
            group_id: شناسه گروه
            user_id: شناسه کاربر

        Returns:
            لیست پیام‌های کاربر
        """
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT 
                    m.message_id, m.group_id, m.user_id, m.text, 
                    m.message_type, m.timestamp, m.metadata,
                    u.username, u.first_name, u.last_name
                FROM messages m
                JOIN users u ON m.user_id = u.user_id
                WHERE m.group_id = ? AND m.user_id = ?
                ORDER BY m.timestamp ASC
            """,
                (group_id, user_id),
            )

            rows = cursor.fetchall()
            messages = [dict(row) for row in rows]
            conn.close()
            return messages

        except Exception as e:
            logger.error(f"❌ خطا در دریافت پیام‌های کاربر: {e}")
            return []

    def get_messages_by_user(
        self, user_id: int, limit: int = None, start_time: int = None, end_time: int = None
    ) -> List[Dict]:
        """
        دریافت پیام‌های یک کاربر از تمام چت‌ها

        Args:
            user_id: شناسه کاربر
            limit: حداکثر تعداد پیام‌ها
            start_time: زمان شروع (Unix timestamp)
            end_time: زمان پایان (Unix timestamp)

        Returns:
            لیست پیام‌های کاربر در تمام چت‌ها
        """
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            ts_expr = "CASE WHEN typeof(m.timestamp)='text' THEN CAST(strftime('%s', m.timestamp) AS INTEGER) ELSE CAST(m.timestamp AS INTEGER) END"

            query = """
                SELECT
                    m.message_id,
                    m.group_id,
                    g.group_name,
                    m.user_id,
                    m.text,
                    m.message_type,
                    {ts_expr} AS timestamp,
                    m.metadata,
                    u.username,
                    u.first_name,
                    u.last_name
                FROM messages m
                JOIN users u ON m.user_id = u.user_id
                LEFT JOIN groups g ON m.group_id = g.group_id
                WHERE m.user_id = ?
            """.format(ts_expr=ts_expr)
            params = [user_id]

            if start_time:
                query += f" AND {ts_expr} >= ?"
                params.append(start_time)

            if end_time:
                query += f" AND {ts_expr} <= ?"
                params.append(end_time)

            query += f" ORDER BY {ts_expr} ASC"

            if limit:
                query += f" LIMIT {limit}"

            cursor.execute(query, params)
            rows = cursor.fetchall()

            messages = []
            for row in rows:
                msg_dict = dict(row)
                if msg_dict["metadata"]:
                    msg_dict["metadata"] = json.loads(msg_dict["metadata"])
                messages.append(msg_dict)

            conn.close()
            return messages

        except Exception as e:
            logger.error(f"❌ خطا در دریافت پیام‌های کاربر در همه چت‌ها: {e}")
            return []

    def get_group_stats(self, group_id: int) -> Dict:
        """
        دریافت آمار گروه

        Args:
            group_id: شناسه گروه

        Returns:
            دیکشنری شامل آمارهای گروه
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # تعداد کل پیام‌ها
            cursor.execute("SELECT COUNT(*) FROM messages WHERE group_id = ?", (group_id,))
            total_messages = cursor.fetchone()[0]

            # تعداد کاربران فعال
            cursor.execute(
                """
                SELECT COUNT(DISTINCT user_id) FROM messages WHERE group_id = ?
            """,
                (group_id,),
            )
            active_users = cursor.fetchone()[0]

            # اولین و آخرین پیام
            cursor.execute(
                """
                SELECT MIN(timestamp), MAX(timestamp) FROM messages WHERE group_id = ?
            """,
                (group_id,),
            )
            min_time, max_time = cursor.fetchone()

            # پرفعال‌ترین کاربران
            cursor.execute(
                """
                SELECT u.user_id, u.username, u.first_name, COUNT(m.message_id) as message_count
                FROM messages m
                JOIN users u ON m.user_id = u.user_id
                WHERE m.group_id = ?
                GROUP BY u.user_id
                ORDER BY message_count DESC
                LIMIT 10
            """,
                (group_id,),
            )
            top_users = [
                dict(zip(["user_id", "username", "first_name", "message_count"], row)) for row in cursor.fetchall()
            ]

            conn.close()

            return {
                "group_id": group_id,
                "total_messages": total_messages,
                "active_users": active_users,
                "first_message_time": min_time,
                "last_message_time": max_time,
                "top_users": top_users,
            }

        except Exception as e:
            logger.error(f"❌ خطا در دریافت آمار گروه: {e}")
            return {}

    def get_group_users(self, group_id: int, limit: int = 200) -> List[Dict]:
        """
        دریافت لیست کاربران یک گروه به همراه آمار فعالیت

        Args:
            group_id: شناسه گروه
            limit: حداکثر تعداد کاربر

        Returns:
            لیست کاربران گروه با تعداد پیام و آخرین زمان فعالیت
        """
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            ts_expr = "CASE WHEN typeof(m.timestamp)='text' THEN CAST(strftime('%s', m.timestamp) AS INTEGER) ELSE CAST(m.timestamp AS INTEGER) END"

            query = f"""
                SELECT
                    u.user_id,
                    u.username,
                    u.first_name,
                    u.last_name,
                    u.is_bot,
                    COUNT(m.message_id) AS message_count,
                    MAX({ts_expr}) AS last_activity_ts
                FROM messages m
                JOIN users u ON m.user_id = u.user_id
                WHERE m.group_id = ?
                GROUP BY u.user_id, u.username, u.first_name, u.last_name, u.is_bot
                ORDER BY message_count DESC
                LIMIT ?
            """

            cursor.execute(query, (group_id, limit))
            rows = cursor.fetchall()
            conn.close()

            return [dict(row) for row in rows]

        except Exception as e:
            logger.error(f"❌ خطا در دریافت کاربران گروه: {e}")
            return []

    def get_group_info(self, group_id: int) -> Optional[Dict]:
        """
        دریافت اطلاعات گروه

        Args:
            group_id: شناسه گروه

        Returns:
            دیکشنری شامل اطلاعات گروه
        """
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute("SELECT * FROM groups WHERE group_id = ?", (group_id,))
            row = cursor.fetchone()
            conn.close()

            if row:
                return dict(row)
            return None

        except Exception as e:
            logger.error(f"❌ خطا در دریافت اطلاعات گروه: {e}")
            return None

    def get_recent_messages(self, group_id: int, limit: int = 20) -> List[Dict]:
        """
        دریافت آخرین پیام‌های یک گروه
        """
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            ts_expr = "CASE WHEN typeof(m.timestamp)='text' THEN CAST(strftime('%s', m.timestamp) AS INTEGER) ELSE CAST(m.timestamp AS INTEGER) END"

            query = f"""
                SELECT * FROM (
                    SELECT
                        m.message_id, m.group_id, m.user_id, m.text,
                        m.message_type,
                        {ts_expr} AS timestamp,
                        m.metadata,
                        u.username, u.first_name, u.last_name
                    FROM messages m
                    JOIN users u ON m.user_id = u.user_id
                    WHERE m.group_id = ?
                    ORDER BY {ts_expr} DESC
                    LIMIT ?
                ) recent_messages
                ORDER BY timestamp ASC
            """

            cursor.execute(query, (group_id, limit))
            rows = cursor.fetchall()
            messages = []
            for row in rows:
                msg_dict = dict(row)
                if msg_dict["metadata"]:
                    msg_dict["metadata"] = json.loads(msg_dict["metadata"])
                messages.append(msg_dict)

            conn.close()
            return messages

        except Exception as e:
            logger.error(f"❌ خطا در دریافت آخرین پیام‌های گروه: {e}")
            return []

    def get_latest_group_memory(self, group_id: int) -> Optional[Dict]:
        """
        دریافت آخرین خلاصه تجمعی گروه
        """
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT *
                FROM group_memory
                WHERE group_id = ?
                ORDER BY updated_at DESC, id DESC
                LIMIT 1
                """,
                (group_id,),
            )

            row = cursor.fetchone()
            conn.close()
            return dict(row) if row else None

        except Exception as e:
            logger.error(f"❌ خطا در دریافت حافظه گروه: {e}")
            return None

    def save_group_memory(
        self,
        group_id: int,
        last_message_id: int,
        message_count: int,
        covered_until_ts: int,
        summary_text: str,
    ) -> bool:
        """
        ذخیره خلاصه تجمعی گروه
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute(
                """
                INSERT INTO group_memory
                (group_id, last_message_id, message_count, covered_until_ts, summary_text, updated_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (group_id, last_message_id, message_count, covered_until_ts, summary_text),
            )

            conn.commit()
            conn.close()
            return True

        except Exception as e:
            logger.error(f"❌ خطا در ذخیره حافظه گروه: {e}")
            return False

    def get_messages_after(self, group_id: int, last_message_id: int = 0, limit: int = 120) -> List[Dict]:
        """
        دریافت پیام‌های جدیدتر از آخرین پیامِ خلاصه‌شده
        """
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            ts_expr = "CASE WHEN typeof(m.timestamp)='text' THEN CAST(strftime('%s', m.timestamp) AS INTEGER) ELSE CAST(m.timestamp AS INTEGER) END"

            cursor.execute(
                f"""
                SELECT
                    m.message_id, m.group_id, m.user_id, m.text,
                    m.message_type,
                    {ts_expr} AS timestamp,
                    m.metadata,
                    u.username, u.first_name, u.last_name
                FROM messages m
                JOIN users u ON m.user_id = u.user_id
                WHERE m.group_id = ? AND m.message_id > ?
                ORDER BY m.message_id ASC
                LIMIT ?
                """,
                (group_id, last_message_id, limit),
            )

            rows = cursor.fetchall()
            messages = []
            for row in rows:
                msg_dict = dict(row)
                if msg_dict["metadata"]:
                    msg_dict["metadata"] = json.loads(msg_dict["metadata"])
                messages.append(msg_dict)

            conn.close()
            return messages

        except Exception as e:
            logger.error(f"❌ خطا در دریافت پیام‌های جدید گروه: {e}")
            return []

    def count_messages_after(self, group_id: int, last_message_id: int = 0) -> int:
        """
        شمارش تعداد پیام‌های خلاصه‌نشده‌ی گروه
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM messages
                WHERE group_id = ? AND message_id > ?
                """,
                (group_id, last_message_id),
            )

            count = cursor.fetchone()[0]
            conn.close()
            return int(count or 0)

        except Exception as e:
            logger.error(f"❌ خطا در شمارش پیام‌های خلاصه‌نشده: {e}")
            return 0

    def search_messages(self, group_id: int, query_text: str, limit: int = 25) -> List[Dict]:
        """
        بازیابی پیام‌های مرتبط با سؤال با امتیازدهی ساده روی SQLite.
        """
        try:
            terms = [term.strip() for term in query_text.split() if len(term.strip()) >= 3]
            if not terms:
                return self.get_recent_messages(group_id, limit=limit)

            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            ts_expr = "CASE WHEN typeof(m.timestamp)='text' THEN CAST(strftime('%s', m.timestamp) AS INTEGER) ELSE CAST(m.timestamp AS INTEGER) END"
            score_parts = []
            params = []

            for term in terms[:8]:
                score_parts.append("CASE WHEN lower(m.text) LIKE lower(?) THEN 1 ELSE 0 END")
                params.append(f"%{term}%")

            score_expr = " + ".join(score_parts) if score_parts else "0"

            query = f"""
                SELECT
                    m.message_id, m.group_id, m.user_id, m.text,
                    m.message_type,
                    {ts_expr} AS timestamp,
                    m.metadata,
                    u.username, u.first_name, u.last_name,
                    ({score_expr}) AS relevance_score
                FROM messages m
                JOIN users u ON m.user_id = u.user_id
                WHERE m.group_id = ?
                  AND m.text IS NOT NULL
                  AND trim(m.text) <> ''
                ORDER BY relevance_score DESC, {ts_expr} DESC
                LIMIT ?
            """

            cursor.execute(query, [*params, group_id, limit])
            rows = cursor.fetchall()

            messages = []
            for row in rows:
                msg_dict = dict(row)
                if msg_dict["metadata"]:
                    msg_dict["metadata"] = json.loads(msg_dict["metadata"])
                if msg_dict.get("relevance_score", 0) > 0:
                    messages.append(msg_dict)

            conn.close()
            return list(reversed(messages)) if messages else []

        except Exception as e:
            logger.error(f"❌ خطا در جستجوی پیام‌های مرتبط: {e}")
            return []

    def get_all_groups(self) -> List[Dict]:
        """
        دریافت تمام گروه‌ها

        Returns:
            لیست تمام گروه‌ها
        """
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute("SELECT * FROM groups ORDER BY last_updated DESC")
            rows = cursor.fetchall()
            conn.close()

            return [dict(row) for row in rows]

        except Exception as e:
            logger.error(f"❌ خطا در دریافت گروه‌ها: {e}")
            return []

    def clear_messages(self, group_id: int = None):
        """
        حذف پیام‌ها

        Args:
            group_id: شناسه گروه (اگر None باشد، تمام پیام‌ها حذف می‌شوند)
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            if group_id:
                cursor.execute("DELETE FROM messages WHERE group_id = ?", (group_id,))
                logger.info(f"✅ تمام پیام‌های گروه {group_id} حذف شدند")
            else:
                cursor.execute("DELETE FROM messages")
                logger.info("✅ تمام پیام‌ها حذف شدند")

            conn.commit()
            conn.close()

        except Exception as e:
            logger.error(f"❌ خطا در حذف پیام‌ها: {e}")
