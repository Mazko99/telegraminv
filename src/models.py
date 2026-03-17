from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class LeadUser:
    telegram_id: int | None = None
    username: str | None = None
    phone: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    source: str = "unknown"
