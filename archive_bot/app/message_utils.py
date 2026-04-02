from __future__ import annotations

from datetime import datetime
from typing import Any

from bale import Message

# Messages containing any of these keywords are treated like /ask.
ASK_LIKE_TRIGGER_KEYWORDS = ["پری", "هی ربات", "هی گروک", "@parijoonbot"]


def contains_ask_like_keyword(text: str) -> bool:
    value = (text or "").strip()
    if not value:
        return False
    return any(keyword in value for keyword in ASK_LIKE_TRIGGER_KEYWORDS)


def safe_get_attr(obj: Any, attr_name: str, default: Any = None) -> Any:
    """Safely read an attribute from an object."""
    try:
        return getattr(obj, attr_name, default)
    except Exception:
        return default


def detect_message_type(message: Message) -> str:
    """Detect message type using known Bale fields."""
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
    """Get normalized text or caption content for storage."""
    return (safe_get_attr(message, "text") or safe_get_attr(message, "caption") or "").strip()


def should_store_message(message: Message) -> bool:
    """Store only messages that contain text content or caption."""
    return bool(get_message_text_content(message))


def is_group_chat(message: Message) -> bool:
    """Return True if the message belongs to a group-like chat."""
    chat = safe_get_attr(message, "chat")
    chat_type = safe_get_attr(chat, "type")
    return chat_type in {"group", "supergroup", "channel"}


def extract_metadata(message: Message) -> dict:
    """Extract rich metadata from Bale message."""
    author = safe_get_attr(message, "author")
    chat = safe_get_attr(message, "chat")
    sender_chat = safe_get_attr(message, "sender_chat")
    reply_to = safe_get_attr(message, "reply_to_message")
    forward_from = safe_get_attr(message, "forward_from")
    forward_from_chat = safe_get_attr(message, "forward_from_chat")

    entities = safe_get_attr(message, "entities") or []
    caption_entities = safe_get_attr(message, "caption_entities") or []

    def extract_entity_items(entity_list: list) -> list[dict]:
        result: list[dict] = []
        for entity in entity_list:
            result.append(
                {
                    "type": safe_get_attr(entity, "type"),
                    "offset": safe_get_attr(entity, "offset"),
                    "length": safe_get_attr(entity, "length"),
                    "url": safe_get_attr(entity, "url"),
                }
            )
        return result

    metadata: dict = {
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


def normalize_timestamp(raw_timestamp: Any) -> int:
    """Convert various timestamp formats to Unix timestamp (seconds)."""
    if isinstance(raw_timestamp, datetime):
        return int(raw_timestamp.timestamp())
    if isinstance(raw_timestamp, (int, float)):
        return int(raw_timestamp)
    if isinstance(raw_timestamp, str) and raw_timestamp.isdigit():
        return int(raw_timestamp)
    return int(datetime.now().timestamp())
