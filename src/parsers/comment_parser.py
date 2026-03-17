from __future__ import annotations

import re

from telethon import TelegramClient

from src.models import LeadUser

POST_LINK_RE = re.compile(r"https?://t\.me/(?P<username>[A-Za-z0-9_]+)/(?P<msg_id>\d+)")


async def parse_comment_authors(client: TelegramClient, post_link: str, limit: int = 200) -> list[LeadUser]:
    """Парсить унікальних авторів коментарів під постом каналу: https://t.me/<channel>/<post_id>."""
    match = POST_LINK_RE.match(post_link.strip())
    if not match:
        raise ValueError("Невірний формат посилання. Очікується https://t.me/<channel>/<post_id>")

    channel = await client.get_entity(match.group("username"))
    message_id = int(match.group("msg_id"))

    users: dict[int, LeadUser] = {}
    async for comment in client.iter_messages(channel, reply_to=message_id, limit=limit):
        author = await comment.get_sender()
        if not author or not getattr(author, "id", None):
            continue

        users[author.id] = LeadUser(
            telegram_id=author.id,
            username=getattr(author, "username", None),
            first_name=getattr(author, "first_name", None),
            last_name=getattr(author, "last_name", None),
            source="comments",
        )

    return list(users.values())
