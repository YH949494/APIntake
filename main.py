"""
Telegram Intake → Approve → Schedule → Publish bot.

Environment variables:
- BOT_TOKEN (required)
- INTAKE_CHAT_ID (required)
- TARGET_CHANNEL_ID (required)
- ADMIN_USER_IDS (optional, comma-separated; if empty, fallback to intake group admins)
- TZ (optional, default "Asia/Kuala_Lumpur")
- DEFAULT_WINDOWS (optional JSON string of [{"name":"today_night","start":"20:30","end":"23:30"}, ...])
- MAX_POSTS_PER_DAY (optional int, default 2)
- MIN_GAP_MINUTES (optional int, default 180)
- DB_PATH (optional, default "./queue.db")

Run:
  python main.py
"""

import asyncio
import json
import logging 
import os
import random
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, date, time, timedelta
from typing import Any, Iterable, Optional
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ChatType
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger(__name__)

STATUS_PENDING = "PENDING"
STATUS_APPROVED = "APPROVED"
STATUS_REJECTED = "REJECTED"
STATUS_POSTED = "POSTED"

STATE_EDIT_CAPTION = "edit_caption"
STATE_CUSTOM_TIME = "custom_time"

DEFAULT_WINDOWS = [
    {"name": "today_night", "start": "20:30", "end": "23:30"},
    {"name": "tomorrow_morning", "start": "09:00", "end": "11:30"},
]


@dataclass
class Config:
    bot_token: str
    intake_chat_id: int
    target_channel_id: int
    admin_user_ids: set[int]
    tz: ZoneInfo
    windows: list[dict[str, str]]
    max_posts_per_day: int
    min_gap_minutes: int
    db_path: str


def load_config() -> Config:
    bot_token = os.getenv("BOT_TOKEN", "").strip()
    intake_chat_id = int(os.getenv("INTAKE_CHAT_ID", "0"))
    target_channel_id = int(os.getenv("TARGET_CHANNEL_ID", "0"))
    admin_user_ids_raw = os.getenv("ADMIN_USER_IDS", "").strip()
    admin_user_ids = {int(v.strip()) for v in admin_user_ids_raw.split(",") if v.strip()}
    tz_name = os.getenv("TZ", "Asia/Kuala_Lumpur")
    windows_raw = os.getenv("DEFAULT_WINDOWS", "")
    max_posts_per_day = int(os.getenv("MAX_POSTS_PER_DAY", "2"))
    min_gap_minutes = int(os.getenv("MIN_GAP_MINUTES", "180"))
    db_path = os.getenv("DB_PATH", "./queue.db")

    if not bot_token:
        raise RuntimeError("Missing BOT_TOKEN env var")
    if not intake_chat_id:
        raise RuntimeError("Missing INTAKE_CHAT_ID env var")
    if not target_channel_id:
        raise RuntimeError("Missing TARGET_CHANNEL_ID env var")

    if windows_raw:
        windows = parse_windows(windows_raw)
    else:
        windows = DEFAULT_WINDOWS

    return Config(
        bot_token=bot_token,
        intake_chat_id=intake_chat_id,
        target_channel_id=target_channel_id,
        admin_user_ids=admin_user_ids,
        tz=ZoneInfo(tz_name),
        windows=windows,
        max_posts_per_day=max_posts_per_day,
        min_gap_minutes=min_gap_minutes,
        db_path=db_path,
    )


class Database:
    def __init__(self, path: str) -> None:
        self._path = path
        self._lock = asyncio.Lock()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path)
        conn.row_factory = sqlite3.Row
        return conn

    async def init(self) -> None:
        async with self._lock:
            conn = self._connect()
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS items (
                        item_id TEXT PRIMARY KEY,
                        intake_chat_id INTEGER,
                        intake_message_id INTEGER,
                        intake_from_user_id INTEGER,
                        media_type TEXT,
                        file_id TEXT,
                        text_content TEXT,
                        caption TEXT,
                        status TEXT,
                        approval_message_id INTEGER,
                        scheduled_at TEXT,
                        posted_at TEXT,
                        target_message_id INTEGER,
                        created_at TEXT,
                        updated_at TEXT
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS states (
                        user_id INTEGER PRIMARY KEY,
                        state TEXT,
                        item_id TEXT,
                        created_at TEXT
                    )
                    """
                )
                conn.commit()
            finally:
                conn.close()

    async def execute(self, sql: str, params: Iterable[Any] = ()) -> None:
        async with self._lock:
            conn = self._connect()
            try:
                conn.execute(sql, params)
                conn.commit()
            finally:
                conn.close()

    async def fetchone(self, sql: str, params: Iterable[Any] = ()) -> Optional[sqlite3.Row]:
        async with self._lock:
            conn = self._connect()
            try:
                cur = conn.execute(sql, params)
                row = cur.fetchone()
                return row
            finally:
                conn.close()

    async def fetchall(self, sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
        async with self._lock:
            conn = self._connect()
            try:
                cur = conn.execute(sql, params)
                rows = cur.fetchall()
                return rows
            finally:
                conn.close()

    async def claim_for_post(self, item_id: str) -> bool:
        async with self._lock:
            conn = self._connect()
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    UPDATE items
                    SET target_message_id = 0, updated_at = ?
                    WHERE item_id = ? AND status = ? AND target_message_id IS NULL
                    """,
                    (now_iso(), item_id, STATUS_APPROVED),
                )
                conn.commit()
                return cur.rowcount == 1
            finally:
                conn.close()


def now_tz(tz: ZoneInfo) -> datetime:
    return datetime.now(tz)


def now_iso(tz: ZoneInfo | None = None) -> str:
    current = now_tz(tz or ZoneInfo("UTC"))
    return current.isoformat()


def parse_windows(raw: str) -> list[dict[str, str]]:
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("DEFAULT_WINDOWS must be valid JSON") from exc
    if not isinstance(data, list):
        raise RuntimeError("DEFAULT_WINDOWS must be a list")
    windows: list[dict[str, str]] = []
    for entry in data:
        if not isinstance(entry, dict):
            raise RuntimeError("DEFAULT_WINDOWS entries must be objects")
        if not all(key in entry for key in ("name", "start", "end")):
            raise RuntimeError("DEFAULT_WINDOWS entries require name/start/end")
        windows.append({"name": entry["name"], "start": entry["start"], "end": entry["end"]})
    return windows


async def is_admin(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> bool:
    config: Config = context.bot_data["config"]

    # If whitelist is provided, use it.
    if config.admin_user_ids:
        return user_id in config.admin_user_ids

    # Otherwise fallback: must be admin/creator in the intake group.
    try:
        member = await context.bot.get_chat_member(config.intake_chat_id, user_id)
        return member.status in ("creator", "administrator")
    except Exception:
        return False


def parse_time(value: str) -> time:
    hour, minute = value.split(":")
    return time(int(hour), int(minute))


def window_bounds(target_date: date, window: dict[str, str], tz: ZoneInfo) -> tuple[datetime, datetime]:
    start_time = parse_time(window["start"])
    end_time = parse_time(window["end"])
    start_dt = datetime.combine(target_date, start_time, tzinfo=tz)
    end_dt = datetime.combine(target_date, end_time, tzinfo=tz)
    return start_dt, end_dt


def slot_ok(candidate: datetime, existing: list[datetime], min_gap: int) -> bool:
    for scheduled in existing:
        gap = abs((candidate - scheduled).total_seconds()) / 60
        if gap < min_gap:
            return False
    return True


def pick_next_slot(
    tz: ZoneInfo,
    windows: list[dict[str, str]],
    scheduled_times: list[datetime],
    max_posts_per_day: int,
    min_gap_minutes: int,
    now: datetime,
) -> Optional[datetime]:
    for offset in range(0, 7):
        target_date = (now + timedelta(days=offset)).date()
        day_times = [value for value in scheduled_times if value.date() == target_date]
        if len(day_times) >= max_posts_per_day:
            continue
        for window in windows:
            if offset == 0 and window["name"] == "tomorrow_morning":
                continue
            if offset == 1 and window["name"] == "today_night":
                continue
            start_dt, end_dt = window_bounds(target_date, window, tz)
            if end_dt <= now:
                continue
            for _ in range(12):
                minutes = int((end_dt - start_dt).total_seconds() // 60)
                if minutes <= 0:
                    break
                candidate = start_dt + timedelta(minutes=random.randint(0, minutes))
                if candidate <= now:
                    continue
                if slot_ok(candidate, day_times, min_gap_minutes):
                    return candidate
            fallback = start_dt
            while fallback <= end_dt:
                if fallback > now and slot_ok(fallback, day_times, min_gap_minutes):
                    return fallback
                fallback += timedelta(minutes=15)
    return None


def format_dt(value: Optional[str], tz: ZoneInfo) -> str:
    if not value:
        return "(unscheduled)"
    parsed = datetime.fromisoformat(value)
    return parsed.astimezone(tz).strftime("%Y-%m-%d %H:%M")


def build_approval_card(item: sqlite3.Row, tz: ZoneInfo, suggested: Optional[datetime]) -> str:
    caption = item["caption"] or item["text_content"] or "(none)"
    suggested_text = suggested.strftime("%Y-%m-%d %H:%M") if suggested else "(unavailable)"
    scheduled_text = format_dt(item["scheduled_at"], tz)
    return (
        f"Item ID: {item['item_id']}\n"
        f"Media: {item['media_type']}\n"
        f"Caption: {caption}\n"
        f"Suggested: {suggested_text}\n"
        f"Scheduled: {scheduled_text}\n"
        f"Status: {item['status']}"
    )


def approval_keyboard(item_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Approve & Schedule", callback_data=f"APPROVE_NEXT:{item_id}")],
            [InlineKeyboardButton("🕒 Approve (pick time)", callback_data=f"APPROVE_PICK:{item_id}")],
            [InlineKeyboardButton("✏️ Edit caption", callback_data=f"EDIT_CAPTION:{item_id}")],
            [InlineKeyboardButton("❌ Reject", callback_data=f"REJECT:{item_id}")],
        ]
    )


def pick_keyboard(item_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Next slot", callback_data=f"PICK_NEXT:{item_id}")],
            [InlineKeyboardButton("Tonight window", callback_data=f"PICK_TONIGHT:{item_id}")],
            [InlineKeyboardButton("Tomorrow morning", callback_data=f"PICK_TOMORROW:{item_id}")],
            [InlineKeyboardButton("Custom time", callback_data=f"PICK_CUSTOM:{item_id}")],
        ]
    )


def safe_caption(text: str) -> str:
    text = text.strip()
    if len(text) > 200:
        return text[:200]
    return text


def read_media(message) -> tuple[str, Optional[str], Optional[str], Optional[str]]:
    if message.photo:
        return "photo", message.photo[-1].file_id, None, message.caption
    if message.video:
        return "video", message.video.file_id, None, message.caption
    if message.animation:
        return "animation", message.animation.file_id, None, message.caption
    if message.document:
        return "document", message.document.file_id, None, message.caption
    if message.text:
        return "text", None, message.text, None
    return "unsupported", None, None, None


async def on_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat and update.effective_chat.type == ChatType.PRIVATE:
        await update.message.reply_text(
            "This bot only works in the intake group. Use /help in the group for instructions."
        )


async def on_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    config: Config = context.bot_data["config"]
    if update.effective_chat.id != config.intake_chat_id:
        return
    if not update.effective_user or not await is_admin(context, update.effective_user.id):
        return
    await update.message.reply_text(
        "Admins: post or forward media into the intake group. Use the buttons to approve, edit, or reject."
    )


async def on_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    config: Config = context.bot_data["config"]
    db: Database = context.bot_data["db"]
    if update.effective_chat.id != config.intake_chat_id:
        return
    if not update.effective_user or not await is_admin(context, update.effective_user.id):
        return
    since = (now_tz(config.tz) - timedelta(days=7)).isoformat()
    rows = await db.fetchall(
        """
        SELECT status, COUNT(*) AS count
        FROM items
        WHERE created_at >= ?
        GROUP BY status
        """,
        (since,),
    )
    counts = {row["status"]: row["count"] for row in rows}
    text = (
        "Last 7 days:\n"
        f"Pending: {counts.get(STATUS_PENDING, 0)}\n"
        f"Approved: {counts.get(STATUS_APPROVED, 0)}\n"
        f"Rejected: {counts.get(STATUS_REJECTED, 0)}\n"
        f"Posted: {counts.get(STATUS_POSTED, 0)}"
    )
    await update.message.reply_text(text)


async def on_queue(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    config: Config = context.bot_data["config"]
    db: Database = context.bot_data["db"]
    if update.effective_chat.id != config.intake_chat_id:
        return
    if not update.effective_user or not await is_admin(context, update.effective_user.id):
        return
    rows = await db.fetchall(
        """
        SELECT item_id, media_type, scheduled_at
        FROM items
        WHERE status = ? AND scheduled_at IS NOT NULL
        ORDER BY scheduled_at ASC
        LIMIT 5
        """,
        (STATUS_APPROVED,),
    )
    if not rows:
        await update.message.reply_text("Queue is empty.")
        return
    lines = [
        f"{format_dt(row['scheduled_at'], config.tz)} | {row['item_id']} | {row['media_type']}"
        for row in rows
    ]
    await update.message.reply_text("Next scheduled:\n" + "\n".join(lines))


async def on_intake_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    config: Config = context.bot_data["config"]
    db: Database = context.bot_data["db"]
    message = update.effective_message
    if not message or not update.effective_chat:
        return
    if update.effective_chat.id != config.intake_chat_id:
        return
    if not message.from_user or not await is_admin(context, message.from_user.id):
        return
    if message.text:
        state_row = await db.fetchone(
            "SELECT 1 FROM states WHERE user_id = ?", (message.from_user.id,)
        )
        if state_row:
            return
    media_type, file_id, text_content, caption = read_media(message)
    if media_type == "unsupported":
        return

    item_id = uuid.uuid4().hex
    created_at = now_tz(config.tz).isoformat()
    await db.execute(
        """
        INSERT INTO items (
            item_id, intake_chat_id, intake_message_id, intake_from_user_id, media_type,
            file_id, text_content, caption, status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            item_id,
            update.effective_chat.id,
            message.message_id,
            message.from_user.id,
            media_type,
            file_id,
            text_content,
            caption,
            STATUS_PENDING,
            created_at,
            created_at,
        ),
    )

    scheduled_times = await db.fetchall(
        "SELECT scheduled_at FROM items WHERE status = ? AND scheduled_at IS NOT NULL",
        (STATUS_APPROVED,),
    )
    scheduled_list = [datetime.fromisoformat(row["scheduled_at"]) for row in scheduled_times]
    suggestion = pick_next_slot(
        config.tz,
        config.windows,
        scheduled_list,
        config.max_posts_per_day,
        config.min_gap_minutes,
        now_tz(config.tz),
    )

    item = await db.fetchone("SELECT * FROM items WHERE item_id = ?", (item_id,))
    card = build_approval_card(item, config.tz, suggestion)
    reply = await message.reply_text(card, reply_markup=approval_keyboard(item_id))
    await db.execute(
        "UPDATE items SET approval_message_id = ?, updated_at = ? WHERE item_id = ?",
        (reply.message_id, now_tz(config.tz).isoformat(), item_id),
    )
    LOGGER.info("action=intake_received item_id=%s chat_id=%s", item_id, update.effective_chat.id)


async def update_approval_card(
    application: Application,
    item_id: str,
    remove_keyboard: bool = False,
) -> None:
    config: Config = application.bot_data["config"]
    db: Database = application.bot_data["db"]
    item = await db.fetchone("SELECT * FROM items WHERE item_id = ?", (item_id,))
    if not item:
        return
    scheduled_times = await db.fetchall(
        "SELECT scheduled_at FROM items WHERE status = ? AND scheduled_at IS NOT NULL",
        (STATUS_APPROVED,),
    )
    scheduled_list = [datetime.fromisoformat(row["scheduled_at"]) for row in scheduled_times]
    suggestion = pick_next_slot(
        config.tz,
        config.windows,
        scheduled_list,
        config.max_posts_per_day,
        config.min_gap_minutes,
        now_tz(config.tz),
    )
    card = build_approval_card(item, config.tz, suggestion)
    keyboard = None if remove_keyboard else approval_keyboard(item_id)
    try:
        await application.bot.edit_message_text(
            chat_id=item["intake_chat_id"],
            message_id=item["approval_message_id"],
            text=card,
            reply_markup=keyboard,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("action=update_card_failed item_id=%s error=%s", item_id, exc)


async def handle_approve_next(context: ContextTypes.DEFAULT_TYPE, item_id: str) -> None:
    config: Config = context.bot_data["config"]
    db: Database = context.bot_data["db"]
    now = now_tz(config.tz)
    scheduled_times = await db.fetchall(
        "SELECT scheduled_at FROM items WHERE status = ? AND scheduled_at IS NOT NULL",
        (STATUS_APPROVED,),
    )
    scheduled_list = [datetime.fromisoformat(row["scheduled_at"]) for row in scheduled_times]
    slot = pick_next_slot(
        config.tz,
        config.windows,
        scheduled_list,
        config.max_posts_per_day,
        config.min_gap_minutes,
        now,
    )
    if not slot:
        return
    await db.execute(
        """
        UPDATE items
        SET status = ?, scheduled_at = ?, updated_at = ?
        WHERE item_id = ?
        """,
        (STATUS_APPROVED, slot.isoformat(), now.isoformat(), item_id),
    )
    LOGGER.info("action=approved_next item_id=%s scheduled_at=%s", item_id, slot.isoformat())


async def handle_schedule_with_window(
    context: ContextTypes.DEFAULT_TYPE,
    item_id: str,
    window_name: str,
    offset_days: int,
) -> Optional[datetime]:
    config: Config = context.bot_data["config"]
    db: Database = context.bot_data["db"]
    window = next((item for item in config.windows if item["name"] == window_name), None)
    if not window:
        return None
    now = now_tz(config.tz)
    target_date = (now + timedelta(days=offset_days)).date()
    start_dt, end_dt = window_bounds(target_date, window, config.tz)
    scheduled_times = await db.fetchall(
        "SELECT scheduled_at FROM items WHERE status = ? AND scheduled_at IS NOT NULL",
        (STATUS_APPROVED,),
    )
    scheduled_list = [datetime.fromisoformat(row["scheduled_at"]) for row in scheduled_times]
    day_count = sum(1 for value in scheduled_list if value.date() == target_date)
    if day_count >= config.max_posts_per_day:
        return None
    for _ in range(20):
        minutes = int((end_dt - start_dt).total_seconds() // 60)
        if minutes <= 0:
            break
        candidate = start_dt + timedelta(minutes=random.randint(0, minutes))
        if candidate <= now:
            continue
        if slot_ok(candidate, scheduled_list, config.min_gap_minutes):
            await db.execute(
                """
                UPDATE items
                SET status = ?, scheduled_at = ?, updated_at = ?
                WHERE item_id = ?
                """,
                (STATUS_APPROVED, candidate.isoformat(), now.isoformat(), item_id),
            )
            return candidate
    return None


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    config: Config = context.bot_data["config"]
    db: Database = context.bot_data["db"]

    query = update.callback_query
    if not query:
        return

    await query.answer()

    # 🔒 HARD GUARDS (ADD THESE HERE)
    if not query.message or query.message.chat_id != config.intake_chat_id:
        return

    if not query.from_user or not await is_admin(context, query.from_user.id):
        return
      
    data = query.data or ""
    if ":" not in data:
        return
    action, item_id = data.split(":", 1)
    if action == "APPROVE_NEXT":
        await handle_approve_next(context, item_id)
        await update_approval_card(context.application, item_id)
    elif action == "APPROVE_PICK":
        await query.edit_message_reply_markup(reply_markup=pick_keyboard(item_id))
    elif action == "EDIT_CAPTION":
        await db.execute(
            "REPLACE INTO states (user_id, state, item_id, created_at) VALUES (?, ?, ?, ?)",
            (query.from_user.id, STATE_EDIT_CAPTION, item_id, now_tz(config.tz).isoformat()),
        )
        await query.message.reply_text("Send the new caption (max 200 chars).")
    elif action == "REJECT":
        await db.execute(
            """
            UPDATE items
            SET status = ?, scheduled_at = NULL, updated_at = ?
            WHERE item_id = ?
            """,
            (STATUS_REJECTED, now_tz(config.tz).isoformat(), item_id),
        )
        await update_approval_card(context.application, item_id, remove_keyboard=True)
    elif action == "PICK_NEXT":
        await handle_approve_next(context, item_id)
        await update_approval_card(context.application, item_id)
    elif action == "PICK_TONIGHT":
        slot = await handle_schedule_with_window(context, item_id, "today_night", 0)
        if slot:
            await update_approval_card(context.application, item_id)
    elif action == "PICK_TOMORROW":
        slot = await handle_schedule_with_window(context, item_id, "tomorrow_morning", 1)
        if slot:
            await update_approval_card(context.application, item_id)
    elif action == "PICK_CUSTOM":
        await db.execute(
            "REPLACE INTO states (user_id, state, item_id, created_at) VALUES (?, ?, ?, ?)",
            (query.from_user.id, STATE_CUSTOM_TIME, item_id, now_tz(config.tz).isoformat()),
        )
        await query.message.reply_text("Send custom time: YYYY-MM-DD HH:MM in TZ.")
    else:
        return


async def on_state_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    config: Config = context.bot_data["config"]
    db: Database = context.bot_data["db"]
    message = update.effective_message
    if not message or not message.from_user or not message.text:
        return
    if update.effective_chat.id != config.intake_chat_id:
        return
    if not await is_admin(context, message.from_user.id):
        return

    state_row = await db.fetchone("SELECT * FROM states WHERE user_id = ?", (message.from_user.id,))
    if not state_row:
        return

    state = state_row["state"]
    item_id = state_row["item_id"]
    await db.execute("DELETE FROM states WHERE user_id = ?", (message.from_user.id,))

    if state == STATE_EDIT_CAPTION:
        new_caption = safe_caption(message.text)
        await db.execute(
            """
            UPDATE items
            SET caption = ?, text_content = CASE WHEN media_type = 'text' THEN ? ELSE text_content END, updated_at = ?
            WHERE item_id = ?
            """,
            (new_caption, new_caption, now_tz(config.tz).isoformat(), item_id),
        )
        await update_approval_card(context.application, item_id)
        return

    if state == STATE_CUSTOM_TIME:
        try:
            parsed = datetime.strptime(message.text.strip(), "%Y-%m-%d %H:%M")
            scheduled = parsed.replace(tzinfo=config.tz)
        except ValueError:
            await message.reply_text("Invalid format. Please use YYYY-MM-DD HH:MM.")
            return
        if scheduled <= now_tz(config.tz):
            await message.reply_text("Time must be in the future.")
            return
        scheduled_times = await db.fetchall(
            "SELECT scheduled_at FROM items WHERE status = ? AND scheduled_at IS NOT NULL",
            (STATUS_APPROVED,),
        )
        scheduled_list = [datetime.fromisoformat(row["scheduled_at"]) for row in scheduled_times]
        day_count = sum(1 for value in scheduled_list if value.date() == scheduled.date())
        if day_count >= config.max_posts_per_day:
            await message.reply_text("That day already reached max posts.")
            return
        if not slot_ok(scheduled, scheduled_list, config.min_gap_minutes):
            await message.reply_text("That time violates the minimum gap.")
            return
        await db.execute(
            """
            UPDATE items
            SET status = ?, scheduled_at = ?, updated_at = ?
            WHERE item_id = ?
            """,
            (STATUS_APPROVED, scheduled.isoformat(), now_tz(config.tz).isoformat(), item_id),
        )
        await update_approval_card(context.application, item_id)


async def publish_due_items(app: Application, config: Config, db: Database) -> None:
    now = now_tz(config.tz)
    rows = await db.fetchall(
        """
        SELECT * FROM items
        WHERE status = ? AND scheduled_at IS NOT NULL
        ORDER BY scheduled_at ASC
        LIMIT 10
        """,
        (STATUS_APPROVED,),
    )
    for row in rows:
        scheduled = datetime.fromisoformat(row["scheduled_at"])
        if scheduled > now:
            continue
        item_id = row["item_id"]
        claimed = await db.claim_for_post(item_id)
        if not claimed:
            continue
        try:
            if row["media_type"] == "photo":
                sent = await app.bot.send_photo(
                    chat_id=config.target_channel_id,
                    photo=row["file_id"],
                    caption=row["caption"],
                )
            elif row["media_type"] == "video":
                sent = await app.bot.send_video(
                    chat_id=config.target_channel_id,
                    video=row["file_id"],
                    caption=row["caption"],
                )
            elif row["media_type"] == "animation":
                sent = await app.bot.send_animation(
                    chat_id=config.target_channel_id,
                    animation=row["file_id"],
                    caption=row["caption"],
                )
            elif row["media_type"] == "document":
                sent = await app.bot.send_document(
                    chat_id=config.target_channel_id,
                    document=row["file_id"],
                    caption=row["caption"],
                )
            else:
                sent = await app.bot.send_message(
                    chat_id=config.target_channel_id,
                    text=row["text_content"] or row["caption"] or "",
                )
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("action=publish_failed item_id=%s error=%s", item_id, exc)
            await db.execute(
                "UPDATE items SET target_message_id = NULL, updated_at = ? WHERE item_id = ?",
                (now_tz(config.tz).isoformat(), item_id),
            )
            continue

        await db.execute(
            """
            UPDATE items
            SET status = ?, posted_at = ?, target_message_id = ?, updated_at = ?
            WHERE item_id = ?
            """,
            (
                STATUS_POSTED,
                now.isoformat(),
                sent.message_id,
                now.isoformat(),
                item_id,
            ),
        )
        await update_approval_card(app, item_id, remove_keyboard=True)
        LOGGER.info("action=posted item_id=%s target_message_id=%s", item_id, sent.message_id)


async def scheduler_loop(application: Application) -> None:
    config: Config = application.bot_data["config"]
    db: Database = application.bot_data["db"]
    await publish_due_items(application, config, db)


async def on_shutdown(app: Application) -> None:
    scheduler: AsyncIOScheduler = app.bot_data.get("scheduler")
    if scheduler:
        scheduler.shutdown(wait=False)


def main() -> None:
    config = load_config()
    db = Database(config.db_path)

    application = Application.builder().token(config.bot_token).build()
    application.bot_data["config"] = config
    application.bot_data["db"] = db

    application.add_handler(CommandHandler("start", on_start))
    application.add_handler(CommandHandler("help", on_help))
    application.add_handler(CommandHandler("status", on_status))
    application.add_handler(CommandHandler("queue", on_queue))
    application.add_handler(CallbackQueryHandler(handle_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_state_text))
    application.add_handler(
        MessageHandler(
            filters.Chat(config.intake_chat_id) & ~filters.COMMAND,
            on_intake_message,
        )
    )

    scheduler = AsyncIOScheduler(timezone=config.tz)

    scheduler.add_job(
        scheduler_loop,
        trigger="interval",
        minutes=5,
        args=[application],
        coalesce=True,
        max_instances=1,
        misfire_grace_time=120,   # 可选：允许最多延迟2分钟后仍执行一次
)
    
application.bot_data["scheduler"] = scheduler


    async def post_init(app: Application) -> None:
        await db.init()
        scheduler.start()

    application.post_init = post_init
    application.post_shutdown = on_shutdown

    LOGGER.info("action=bot_start intake_chat_id=%s", config.intake_chat_id)
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
