from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Awaitable, Callable

from telethon import TelegramClient
from telethon.errors import FloodWaitError, SessionPasswordNeededError
from telethon.errors.rpcerrorlist import (
    AuthKeyUnregisteredError,
    AuthRestartError,
    PasswordHashInvalidError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    PhoneNumberFloodError,
    PhoneNumberInvalidError,
)
from telethon.tl.functions.channels import InviteToChannelRequest
from telethon.tl.functions.contacts import AddContactRequest, ImportContactsRequest
from telethon.tl.functions.messages import AddChatUserRequest
from telethon.tl.types import Chat, InputPhoneContact

from src.models import LeadUser

ProgressCb = Callable[[str], Awaitable[None]]


class TelegramUserClient:
    def __init__(self, api_id: int, api_hash: str, session_file: Path) -> None:
        self.client = TelegramClient(str(session_file), api_id, api_hash)

    async def connect(self) -> None:
        await self.client.connect()

    async def disconnect(self) -> None:
        await self.client.disconnect()

    async def ensure_authorized(self) -> None:
        try:
            me = await self.client.get_me()
        except AuthKeyUnregisteredError as exc:
            raise RuntimeError("Сесія зламана або застаріла. Виконайте /auth заново.") from exc

        if me is None:
            raise RuntimeError("Сесія не авторизована. Спочатку виконайте /auth.")


    async def has_authorized_session(self) -> bool:
        try:
            return await self.client.is_user_authorized()
        except Exception:
            return False

    async def begin_login(self, phone: str) -> str:
        for attempt in range(1, 4):
            try:
                sent = await self.client.send_code_request(phone)
                return sent.phone_code_hash
            except AuthRestartError:
                if attempt >= 3:
                    raise RuntimeError("Telegram просить перезапустити авторизацію. Спробуйте /auth ще раз через 1-2 хв.")
                await asyncio.sleep(2)
            except FloodWaitError as exc:
                raise RuntimeError(f"Ліміт запитів коду. Повторіть через {exc.seconds} сек.") from exc
            except PhoneNumberFloodError as exc:
                raise RuntimeError("Надто багато запитів коду на цей номер. Спробуйте пізніше.") from exc
            except PhoneNumberInvalidError as exc:
                raise RuntimeError("Некоректний номер телефону. Перевірте формат +380...") from exc

        raise RuntimeError("Не вдалося ініціювати авторизацію.")

    async def complete_login(self, phone: str, code: str, phone_code_hash: str) -> bool:
        try:
            await self.client.sign_in(phone=phone, code=code, phone_code_hash=phone_code_hash)
            return True
        except SessionPasswordNeededError:
            return False
        except PhoneCodeInvalidError as exc:
            raise RuntimeError("Невірний код підтвердження.") from exc
        except PhoneCodeExpiredError as exc:
            raise RuntimeError("Код протерміновано. Запустіть /auth і отримайте новий код.") from exc

    async def complete_password_login(self, password: str) -> bool:
        try:
            await self.client.sign_in(password=password)
            return True
        except PasswordHashInvalidError:
            return False

    async def add_to_contacts(
        self,
        users: list[LeadUser],
        dry_run: bool = False,
        progress: ProgressCb | None = None,
    ) -> tuple[int, int, list[str]]:
        success = 0
        failed = 0
        refs: list[str] = []

        for idx, user in enumerate(users, start=1):
            label = user.phone or (f"@{user.username}" if user.username else f"id:{user.telegram_id}")
            if dry_run:
                if user.username:
                    refs.append(user.username if user.username.startswith("@") else f"@{user.username}")
                elif user.phone:
                    refs.append(user.phone)
                success += 1
                if progress:
                    await progress(f"[dry-run] Контакт {idx}/{len(users)}: {label}")
                continue

            try:
                if user.phone:
                    contact = InputPhoneContact(
                        client_id=idx,
                        phone=user.phone,
                        first_name=user.first_name or "Unknown",
                        last_name=user.last_name or "",
                    )
                    result = await self.client(ImportContactsRequest([contact]))
                    if result.imported:
                        success += 1
                        refs.append(user.phone)
                        if progress:
                            await progress(f"✅ Додано контакт {idx}/{len(users)}: {label}")
                    else:
                        failed += 1
                        if progress:
                            await progress(f"⚠️ Контакт не додано {idx}/{len(users)}: {label} (Telegram не імпортував)")
                elif user.username:
                    username_ref = user.username if user.username.startswith("@") else f"@{user.username}"
                    entity = await self.client.get_entity(username_ref)
                    await self.client(
                        AddContactRequest(
                            id=entity,
                            first_name=user.first_name or getattr(entity, "first_name", "Unknown") or "Unknown",
                            last_name=user.last_name or getattr(entity, "last_name", "") or "",
                            phone="",
                            add_phone_privacy_exception=False,
                        )
                    )
                    refs.append(username_ref)
                    success += 1
                    if progress:
                        await progress(f"✅ Додано контакт {idx}/{len(users)}: {username_ref}")
                else:
                    failed += 1
                    if progress:
                        await progress(f"⚠️ Пропуск контакту {idx}/{len(users)}: немає phone/username")

                await asyncio.sleep(2)
            except FloodWaitError as e:
                failed += 1
                if progress:
                    await progress(f"⏳ FloodWait {e.seconds}s на контакті {idx}, чекаю...")
                await asyncio.sleep(e.seconds + 1)
            except Exception as exc:
                failed += 1
                if progress:
                    await progress(f"❌ Не вдалося додати {label}: {exc}")

        return success, failed, refs

    async def invite_to_group(
        self,
        refs: list[str],
        group_link: str,
        dry_run: bool = False,
        progress: ProgressCb | None = None,
    ) -> tuple[int, int]:
        invited = 0
        failed = 0
        target = None
        if not dry_run:
            target = await self.client.get_entity(group_link)

        for idx, ref in enumerate(refs, start=1):
            if dry_run:
                invited += 1
                if progress:
                    await progress(f"[dry-run] Інвайт {idx}/{len(refs)}: {ref}")
                continue

            try:
                user = await self.client.get_entity(ref)
                if isinstance(target, Chat):
                    await self.client(AddChatUserRequest(chat_id=target.id, user_id=user, fwd_limit=10))
                else:
                    await self.client(InviteToChannelRequest(channel=target, users=[user]))
                invited += 1
                if progress:
                    await progress(f"✅ Запрошено {idx}/{len(refs)}: {ref}")
                await asyncio.sleep(4)
            except FloodWaitError as e:
                failed += 1
                if progress:
                    await progress(f"⏳ FloodWait {e.seconds}s на інвайті {idx}, чекаю...")
                await asyncio.sleep(e.seconds + 1)
            except Exception as exc:
                failed += 1
                if progress:
                    await progress(f"❌ Інвайт не вдався ({ref}): {exc}")

        return invited, failed
