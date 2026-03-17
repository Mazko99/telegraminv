from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.models import LeadUser

# Канонічні поля, які використовує код
CANONICAL_FIELDS = {
    "telegram_id",
    "username",
    "phone",
    "first_name",
    "last_name",
}

# Мапа колонок (ua/ru/en + типові варіанти) -> канонічна назва
COLUMN_ALIASES: dict[str, str] = {
    # id
    "id": "telegram_id",
    "telegram_id": "telegram_id",
    "telegram id": "telegram_id",
    "айди": "telegram_id",

    # username
    "username": "username",
    "@username": "username",
    "user": "username",
    "логин": "username",
    "нік": "username",

    # phone
    "phone": "phone",
    "phone_number": "phone",
    "номер телефона": "phone",
    "номер телефону": "phone",
    "телефон": "phone",

    # first_name
    "first_name": "first_name",
    "firstname": "first_name",
    "name": "first_name",
    "имя": "first_name",
    "імя": "first_name",
    "iмя": "first_name",

    # last_name
    "last_name": "last_name",
    "lastname": "last_name",
    "surname": "last_name",
    "фамилия": "last_name",
    "прізвище": "last_name",
}


def load_users_from_excel(path: str | Path) -> list[LeadUser]:
    df = pd.read_excel(path)
    normalized = _normalize_columns(df)

    if not ({"phone", "username"} & set(normalized.columns)):
        raise ValueError(
            "Excel має містити хоча б одну колонку для контакту: phone/номер телефона або username/@username"
        )

    users: list[LeadUser] = []
    for row in normalized.to_dict(orient="records"):
        username = _normalize_username(_to_str(row.get("username")))
        users.append(
            LeadUser(
                telegram_id=_to_int(row.get("telegram_id")),
                username=username,
                phone=_normalize_phone(_to_str(row.get("phone"))),
                first_name=_to_str(row.get("first_name")),
                last_name=_to_str(row.get("last_name")),
                source="excel",
            )
        )

    # відкидаємо порожні рядки без username і phone
    return [u for u in users if u.username or u.phone]


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map: dict[str, str] = {}

    for col in df.columns:
        key = _normalize_header(col)
        canonical = COLUMN_ALIASES.get(key)
        if canonical:
            rename_map[col] = canonical

    normalized = df.rename(columns=rename_map)

    # Якщо частина колонок невідома — це не помилка: просто ігноруємо зайве
    keep_cols = [c for c in normalized.columns if c in CANONICAL_FIELDS]
    if not keep_cols:
        raise ValueError(
            "Не вдалося розпізнати жодної колонки. Дозволені: id/ID, username/@username, phone/номер телефона, имя/first_name, фамилия/last_name"
        )

    return normalized[keep_cols]


def _normalize_header(value: object) -> str:
    text = str(value).strip().lower()
    text = " ".join(text.split())
    return text


def _to_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none"}:
        return None
    return text


def _to_int(value: object) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none"}:
        return None
    return int(float(text))


def _normalize_username(username: str | None) -> str | None:
    if not username:
        return None
    username = username.strip()
    if username.startswith("https://t.me/"):
        username = username.removeprefix("https://t.me/")
    if username.startswith("@"):
        username = username[1:]
    return username or None


def _normalize_phone(phone: str | None) -> str | None:
    if not phone:
        return None
    return phone.replace(" ", "")
