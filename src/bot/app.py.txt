from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from pathlib import Path

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message

from src.config import Settings
from src.models import LeadUser
from src.parsers.comment_parser import parse_comment_authors
from src.services.excel_loader import load_users_from_excel
from src.services.runtime_store import RuntimeStore, StoredAccount
from src.services.telegram_user_client import TelegramUserClient


class AuthFlow(StatesGroup):
    phone = State()
    code = State()
    password = State()


@dataclass(slots=True)
class UserPipelineConfig:
    excel_path: str | None = None
    source_post_link: str | None = None
    target_group_link: str | None = None
    active_phone: str | None = None


class InviteLoopManager:
    def __init__(self, settings: Settings, store: RuntimeStore, bot: Bot) -> None:
        self.settings = settings
        self.store = store
        self.bot = bot
        self.task: asyncio.Task | None = None
        self.stop_event = asyncio.Event()
        self.chat_id: int | None = None
        self.excel_path: str | None = None
        self.group_link: str | None = None

        self.parallel_accounts = 20
        self.chunk_size = 141
        self.cooldown_seconds = 4 * 60 * 60
        self.index_lock = asyncio.Lock()

    @property
    def running(self) -> bool:
        return self.task is not None and not self.task.done()

    async def start(self, chat_id: int, excel_path: str, group_link: str) -> str:
        if self.running:
            return "⚠️ Цикл вже запущено. Зупиніть /stop_invite перед новим запуском."

        self.chat_id = chat_id
        self.excel_path = excel_path
        self.group_link = group_link
        self.stop_event.clear()
        self.task = asyncio.create_task(self._run())
        return "✅ Фоновий цикл інвайту запущено (20 потоків, cooldown 4h)."

    async def stop(self) -> str:
        if not self.running:
            return "⚠️ Цикл зараз не запущено."

        self.stop_event.set()
        assert self.task is not None
        await self.task
        self.task = None
        return "🛑 Цикл інвайту зупинено."

    async def _run(self) -> None:
        assert self.chat_id is not None and self.excel_path is not None and self.group_link is not None

        users = load_users_from_excel(self.excel_path)
        if not users:
            await self.bot.send_message(self.chat_id, "❌ Excel порожній або не містить валідних користувачів.")
            return

        await self.bot.send_message(
            self.chat_id,
            f"🚀 Старт циклу. Користувачів у файлі: {len(users)}. Початковий index: {self.store.get_index()}"
        )

        while not self.stop_event.is_set():
            accounts = self.store.get_available_accounts(self.parallel_accounts, self.cooldown_seconds)
            if not accounts:
                wait_sec = self.store.get_next_ready_in_seconds(self.cooldown_seconds)
                await self.bot.send_message(
                    self.chat_id,
                    f"⏳ Немає доступних акаунтів. Наступне вікно приблизно через {wait_sec // 60} хв."
                )
                await asyncio.sleep(min(max(wait_sec, 60), 300))
                continue

            assigned = self._assign_chunks(users, accounts)
            tasks = [self._process_account(account, chunk, start_idx, len(users)) for account, chunk, start_idx in assigned]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            success_accounts = 0
            for result in results:
                if isinstance(result, Exception):
                    await self.bot.send_message(self.chat_id, f"❌ Помилка потоку: {result}")
                    continue
                success_accounts += 1
                await self.bot.send_message(self.chat_id, result)

            await self.bot.send_message(
                self.chat_id,
                f"✅ Раунд завершено: акаунтів оброблено={success_accounts}/{len(accounts)}. Поточний index={self.store.get_index()}"
            )

    def _assign_chunks(
        self,
        users: list[LeadUser],
        accounts: list[StoredAccount],
    ) -> list[tuple[StoredAccount, list[LeadUser], int]]:
        start = self.store.get_index()
        n = len(users)
        out: list[tuple[StoredAccount, list[LeadUser], int]] = []

        cursor = start
        for account in accounts:
            chunk_start = cursor
            chunk: list[LeadUser] = []
            for _ in range(self.chunk_size):
                chunk.append(users[cursor])
                cursor = (cursor + 1) % n
            out.append((account, chunk, chunk_start))

        return out

    async def _process_account(self, account: StoredAccount, users_chunk: list[LeadUser], start_idx: int, total_users: int) -> str:
        client = TelegramUserClient(self.settings.tg_api_id, self.settings.tg_api_hash, Path(account.session_file))
        await client.connect()
        try:
            await client.ensure_authorized()
            c_ok, c_fail, refs = await client.add_to_contacts(users_chunk, dry_run=False, progress=None)
            i_ok, i_fail = await client.invite_to_group(refs, self.group_link or "", dry_run=False, progress=None)
            self.store.mark_account_used(account.phone)
            async with self.index_lock:
                self.store.advance_index(self.chunk_size, total_users)
        finally:
            await client.disconnect()

        return (
            f"📊 {account.phone} [start={start_idx}]: контакти успішно={c_ok}, контакти fail={c_fail}; "
            f"інвайти успішно={i_ok}, інвайти fail={i_fail}"
        )


settings = Settings.from_env()
bot = Bot(settings.bot_token)
dp = Dispatcher()
user_cfg: dict[int, UserPipelineConfig] = {}
store = RuntimeStore(settings.db_path)
invite_loop = InviteLoopManager(settings, store, bot)


def _cfg(user_id: int) -> UserPipelineConfig:
    if user_id not in user_cfg:
        user_cfg[user_id] = UserPipelineConfig()
    return user_cfg[user_id]


def _normalize_phone(phone: str) -> str:
    cleaned = re.sub(r"[^\d+]", "", phone.strip())
    if cleaned.startswith("00"):
        cleaned = f"+{cleaned[2:]}"
    if not cleaned.startswith("+"):
        cleaned = f"+{cleaned}"
    return cleaned


def _phone_key(phone: str) -> str:
    return re.sub(r"\D", "", phone)


def _session_path(user_id: int, phone: str | None) -> Path:
    if phone:
        return settings.sessions_dir / f"user_{user_id}_{_phone_key(phone)}.session"
    return settings.sessions_dir / f"user_{user_id}.session"


def _client(user_id: int, phone: str | None) -> TelegramUserClient:
    return TelegramUserClient(settings.tg_api_id, settings.tg_api_hash, _session_path(user_id, phone))


def _parse_mode(text: str | None) -> bool | None:
    parts = (text or "").split(maxsplit=1)
    if len(parts) < 2:
        return None
    mode = parts[1].strip().lower()
    if mode == "real":
        return True
    if mode == "dry":
        return False
    return None


async def _get_active_client(message: Message) -> TelegramUserClient | None:
    cfg = _cfg(message.from_user.id)
    if not cfg.active_phone:
        await message.answer("❌ Немає active_phone. Спочатку виконайте /auth")
        return None

    client = _client(message.from_user.id, cfg.active_phone)
    await client.connect()
    try:
        await client.ensure_authorized()
        return client
    except Exception as exc:
        await client.disconnect()
        await message.answer(f"❌ Помилка сесії: {exc}")
        return None


@dp.message(Command("start"))
async def start(message: Message) -> None:
    await message.answer(
        "Команди:\n"
        "/auth - авторизація акаунта і збереження сесії\n"
        "/set_excel <path>\n"
        "/set_target <group_link>\n"
        "/status - статус + кількість збережених акаунтів\n"
        "/invite real - запускає безкінечний цикл (20 акаунтів паралельно)\n"
        "/stop_invite - зупинити цикл\n"
        "/import_excel [dry|real] - одноразове додавання контактів\n"
        "/parse_comments [dry|real] - одноразово з коментарів"
    )


@dp.message(Command("status"))
async def status(message: Message) -> None:
    cfg = _cfg(message.from_user.id)
    accounts = store.list_accounts()
    await message.answer(
        "Поточні параметри:\n"
        f"excel_path: {cfg.excel_path or '-'}\n"
        f"source_post_link: {cfg.source_post_link or '-'}\n"
        f"target_group_link: {cfg.target_group_link or '-'}\n"
        f"active_phone: {cfg.active_phone or '-'}\n"
        f"saved_accounts: {len(accounts)}\n"
        f"invite_loop_running: {'yes' if invite_loop.running else 'no'}\n"
        f"excel_index: {store.get_index()}"
    )


@dp.message(Command("set_excel"))
async def set_excel(message: Message) -> None:
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Приклад: /set_excel data/leads.xlsx")
        return

    path = parts[1].strip()
    if not Path(path).exists():
        await message.answer(f"Файл не знайдено: {path}")
        return

    _cfg(message.from_user.id).excel_path = path
    await message.answer(f"✅ Excel шлях збережено: {path}")


@dp.message(Command("set_source"))
async def set_source(message: Message) -> None:
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Приклад: /set_source https://t.me/mychannel/123")
        return
    _cfg(message.from_user.id).source_post_link = parts[1].strip()
    await message.answer("✅ Джерело для парсингу коментарів збережено.")


@dp.message(Command("set_target"))
async def set_target(message: Message) -> None:
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Приклад: /set_target https://t.me/mygroup")
        return
    _cfg(message.from_user.id).target_group_link = parts[1].strip()
    await message.answer("✅ Цільову групу збережено.")


@dp.message(Command("auth"))
async def auth_start(message: Message, state: FSMContext) -> None:
    await state.set_state(AuthFlow.phone)
    await message.answer("Введіть номер телефону у форматі +380...")


@dp.message(AuthFlow.phone)
async def auth_phone(message: Message, state: FSMContext) -> None:
    phone = _normalize_phone(message.text or "")
    session_path = _session_path(message.from_user.id, phone)

    client = TelegramUserClient(settings.tg_api_id, settings.tg_api_hash, session_path)
    await client.connect()
    try:
        if await client.has_authorized_session():
            await state.clear()
            _cfg(message.from_user.id).active_phone = phone
            store.upsert_account(phone, str(session_path))
            await message.answer(f"✅ Цей акаунт вже авторизований: {phone}")
            return

        phone_code_hash = await client.begin_login(phone)
    except Exception as exc:
        await message.answer(f"❌ Не вдалося надіслати код: {exc}")
        await message.answer("Порада: зачекайте 1-2 хв, перевірте номер у форматі +380..., потім /auth ще раз.")
        return
    finally:
        await client.disconnect()

    await state.update_data(phone=phone, phone_code_hash=phone_code_hash)
    await state.set_state(AuthFlow.code)
    await message.answer("Код надіслано. Введіть код з Telegram:")


@dp.message(AuthFlow.code)
async def auth_code(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    phone = data["phone"]

    client = _client(message.from_user.id, phone)
    await client.connect()
    try:
        ok = await client.complete_login(
            phone=phone,
            code=(message.text or "").strip(),
            phone_code_hash=data["phone_code_hash"],
        )
    except Exception as exc:
        await message.answer(f"❌ Помилка коду: {exc}")
        return
    finally:
        await client.disconnect()

    if ok:
        await state.clear()
        _cfg(message.from_user.id).active_phone = phone
        store.upsert_account(phone, str(_session_path(message.from_user.id, phone)))
        await message.answer(f"✅ Авторизація завершена. Активний акаунт: {phone}")
        return

    await state.set_state(AuthFlow.password)
    await message.answer("На акаунті увімкнено 2FA. Введіть пароль (cloud password):")


@dp.message(AuthFlow.password)
async def auth_password(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    phone = data["phone"]

    client = _client(message.from_user.id, phone)
    await client.connect()
    try:
        ok = await client.complete_password_login((message.text or "").strip())
    finally:
        await client.disconnect()

    if not ok:
        await message.answer("❌ Невірний 2FA пароль. Спробуйте ще раз або /auth спочатку.")
        return

    await state.clear()
    _cfg(message.from_user.id).active_phone = phone
    store.upsert_account(phone, str(_session_path(message.from_user.id, phone)))
    await message.answer(f"✅ 2FA успішний. Активний акаунт: {phone}")


@dp.message(Command("stop_invite"))
async def stop_invite(message: Message) -> None:
    text = await invite_loop.stop()
    await message.answer(text)


@dp.message(Command("import_excel"))
async def import_excel(message: Message) -> None:
    cfg = _cfg(message.from_user.id)
    if not cfg.excel_path:
        await message.answer("Спочатку задайте Excel: /set_excel data/leads.xlsx")
        return

    real_mode = _parse_mode(message.text)
    if real_mode is None:
        await message.answer("Вкажіть режим: /import_excel dry або /import_excel real")
        return
    dry_run = not real_mode

    try:
        users = load_users_from_excel(cfg.excel_path)
    except Exception as exc:
        await message.answer(f"❌ Помилка читання Excel: {exc}")
        return

    client = await _get_active_client(message)
    if not client:
        return

    try:
        added, failed, _ = await client.add_to_contacts(users, dry_run=dry_run, progress=message.answer)
    finally:
        await client.disconnect()

    await message.answer(f"🏁 Імпорт контактів: успішно={added}, fail={failed}, mode={'REAL' if real_mode else 'DRY'}")


@dp.message(Command("parse_comments"))
async def parse_comments(message: Message) -> None:
    cfg = _cfg(message.from_user.id)
    if not cfg.source_post_link:
        await message.answer("Спочатку задайте source пост: /set_source https://t.me/channel/123")
        return

    real_mode = _parse_mode(message.text)
    if real_mode is None:
        await message.answer("Вкажіть режим: /parse_comments dry або /parse_comments real")
        return
    dry_run = not real_mode

    client = await _get_active_client(message)
    if not client:
        return

    try:
        parsed_users = await parse_comment_authors(client.client, cfg.source_post_link, limit=300)
        added, failed, _ = await client.add_to_contacts(parsed_users, dry_run=dry_run, progress=message.answer)
    finally:
        await client.disconnect()

    await message.answer(f"🏁 Коментарі -> контакти: успішно={added}, fail={failed}, mode={'REAL' if real_mode else 'DRY'}")


@dp.message(Command("invite"))
async def invite(message: Message) -> None:
    cfg = _cfg(message.from_user.id)
    if not cfg.target_group_link:
        await message.answer("Спочатку задайте target групу: /set_target https://t.me/mygroup")
        return
    if not cfg.excel_path:
        await message.answer("Для invite потрібен Excel: /set_excel data/leads.xlsx")
        return

    real_mode = _parse_mode(message.text)
    if real_mode is None:
        await message.answer("Вкажіть режим: /invite dry або /invite real")
        return

    if real_mode:
        text = await invite_loop.start(message.chat.id, cfg.excel_path, cfg.target_group_link)
        await message.answer(text)
        return

    # dry: одиночний прогін через активний акаунт
    users = load_users_from_excel(cfg.excel_path)
    client = await _get_active_client(message)
    if not client:
        return
    try:
        c_ok, c_fail, refs = await client.add_to_contacts(users[:141], dry_run=True, progress=message.answer)
        i_ok, i_fail = await client.invite_to_group(refs, cfg.target_group_link, dry_run=True, progress=message.answer)
    finally:
        await client.disconnect()

    await message.answer(f"🏁 DRY single-run: contacts={c_ok}/{c_fail}, invites={i_ok}/{i_fail}")


async def main() -> None:
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
