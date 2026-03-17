from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


load_dotenv()


@dataclass(slots=True)
class Settings:
    bot_token: str
    tg_api_id: int
    tg_api_hash: str
    bot_admins: set[int]
    db_path: Path
    sessions_dir: Path

    @classmethod
    def from_env(cls) -> "Settings":
        admins_raw = os.getenv("BOT_ADMINS", "").strip()
        admins = {
            int(chunk.strip())
            for chunk in admins_raw.split(",")
            if chunk.strip().isdigit()
        }
        db_path = Path(os.getenv("DB_PATH", "data/app.db"))
        sessions_dir = Path(os.getenv("SESSIONS_DIR", "data/sessions"))
        sessions_dir.mkdir(parents=True, exist_ok=True)
        db_path.parent.mkdir(parents=True, exist_ok=True)

        return cls(
            bot_token=os.environ["BOT_TOKEN"],
            tg_api_id=int(os.environ["TG_API_ID"]),
            tg_api_hash=os.environ["TG_API_HASH"],
            bot_admins=admins,
            db_path=db_path,
            sessions_dir=sessions_dir,
        )
