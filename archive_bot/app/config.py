from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class AppConfig:
    """Runtime configuration loaded from environment variables."""

    bot_token: str
    sqlite_db_path: str
    commands_enabled: bool
    allowed_command_user_id: int
    auto_memorize_enabled: bool
    auto_memorize_threshold: int
    summary_default_hours: int
    summary_min_hours: int
    summary_max_hours: int
    ask_default_hours: int
    ask_max_question_chars: int
    export_default_count: int
    export_min_count: int
    export_max_count: int
    auto_chime_enabled: bool
    auto_chime_every_n: int

    @classmethod
    def from_env(cls) -> "AppConfig":
        bot_token = os.getenv("BALE_BOT_TOKEN")
        if not bot_token:
            raise ValueError("❌ Bot token was not found. Please check your .env file.")

        raw_db_path = os.getenv("SQLITE_DB_PATH", "messages.db")
        db_path = Path(raw_db_path)
        if not db_path.is_absolute():
            project_root = Path(__file__).resolve().parents[1]
            db_path = project_root / db_path

        return cls(
            bot_token=bot_token,
            sqlite_db_path=str(db_path),
            commands_enabled=os.getenv("ENABLE_BOT_COMMANDS", "false").lower() == "true",
            allowed_command_user_id=int(os.getenv("ALLOWED_COMMAND_USER_ID", "1412990760")),
            auto_memorize_enabled=os.getenv("AUTO_MEMORIZE_ENABLED", "true").lower() == "true",
            auto_memorize_threshold=int(os.getenv("AUTO_MEMORIZE_THRESHOLD", "50")),
            summary_default_hours=int(os.getenv("SUMMARY_DEFAULT_HOURS", "24")),
            summary_min_hours=int(os.getenv("SUMMARY_MIN_HOURS", "1")),
            summary_max_hours=int(os.getenv("SUMMARY_MAX_HOURS", "168")),
            ask_default_hours=int(os.getenv("ASK_DEFAULT_HOURS", "72")),
            ask_max_question_chars=int(os.getenv("ASK_MAX_QUESTION_CHARS", "500")),
            export_default_count=int(os.getenv("EXPORT_DEFAULT_COUNT", "20")),
            export_min_count=int(os.getenv("EXPORT_MIN_COUNT", "1")),
            export_max_count=int(os.getenv("EXPORT_MAX_COUNT", "100")),
            auto_chime_enabled=os.getenv("AUTO_CHIME_ENABLED", "false").lower() == "true",
            auto_chime_every_n=int(os.getenv("AUTO_CHIME_EVERY_N", "10")),
        )
