import asyncio
import uuid
import math
import csv
import logging
import os
from typing import Optional, List, Tuple, Dict, Any
from datetime import datetime

import aiosqlite
import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ContentType
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
    PreCheckoutQuery,
    InputFile,
)
from aiogram.filters import Command

BOT_TOKEN = "8205190372:AAH9UcrhAgr--y245eR4AzLYhh74i3rzjy8"
MAIN_ADMIN_ID = 7418079991
DB_PATH = "bot_users.db"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BANNER_FILE = os.path.join(BASE_DIR, "banner.png")
PRIVACY_URL = "https://teletype.in/@glinomeas/politika"
YOOMONEY_ACCESS_TOKEN = "923830691E6C9FDB57B4DDAA4EE0FA6D1410B9540055D79DB4DB0503B8D9EE20"
YOOMONEY_RECEIVER = "4100118173380375"
YOOMONEY_OP_HISTORY = "https://yoomoney.ru/api/operation-history"
YOOMONEY_ACCOUNT_INFO = "https://yoomoney.ru/api/account-info"
YOOMONEY_QUICKPAY_BASE = "https://yoomoney.ru/quickpay/confirm.xml"
POLL_INTERVAL = 25
DEFAULT_EXCHANGE_RATE = 85.0
EXCHANGE_API_URL = "https://api.exchangerate.host/latest?base=USD&symbols=RUB"
STARS_PER_USD = 70
REFERRAL_BONUS_RUB = 5.0
MIN_TOPUP_RUB = 100.0
BASE_USD_PER_MONTH = 3.0
BASE_USD_YEAR = 25.0
DEFAULT_MAX_STARS = 1000000
DEFAULT_MIN_STARS = 300
EMPTY_ACCOUNT_PRICE_RUB = 99.0
PROXY_PRICE_RUB = 49.0
PROXY_COUNTRIES = [
    "Россия",
    "Украина",
    "США",
    "Казахстан",
    "Англия",
    "Германия",
    "Канада",
    "Нидерланды",
    "Великобритания",
    "Австрия",
]
PROXY_TECH_DESCRIPTION = "IPv4 Socks5"
CHANNEL_USERNAME = "@glino_premka"
CHANNEL_INVITE_LINK = "https://t.me/glino_premka"
SUPPORT_URL = "https://t.me/glino_premka_helper"
STARS_GIFT_USERNAME = "glinomeas"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("shop")
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

DB_CONN: Optional[aiosqlite.Connection] = None
DB_LOCK = asyncio.Lock()
HTTP_SESSION: Optional[aiohttp.ClientSession] = None


async def init_db():
    global DB_CONN
    DB_CONN = await aiosqlite.connect(DB_PATH)
    await DB_CONN.execute(
        "CREATE TABLE IF NOT EXISTS users(user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT)"
    )
    await DB_CONN.execute("CREATE TABLE IF NOT EXISTS admins(user_id INTEGER PRIMARY KEY)")
    await DB_CONN.execute(
        "CREATE TABLE IF NOT EXISTS bans(user_id INTEGER PRIMARY KEY, reason TEXT)"
    )
    await DB_CONN.execute(
        "CREATE TABLE IF NOT EXISTS profiles(user_id INTEGER PRIMARY KEY, real_balance REAL DEFAULT 0, bonus_balance REAL DEFAULT 0, referrer_id INTEGER DEFAULT NULL, referrals_count INTEGER DEFAULT 0, subscribed INTEGER DEFAULT 0)"
    )
    await DB_CONN.execute(
        "CREATE TABLE IF NOT EXISTS promocodes(code TEXT PRIMARY KEY, amount REAL NOT NULL, activations INTEGER NOT NULL)"
    )
    await DB_CONN.execute(
        "CREATE TABLE IF NOT EXISTS promocode_uses(code TEXT NOT NULL, user_id INTEGER NOT NULL, PRIMARY KEY(code, user_id))"
    )
    await DB_CONN.execute(
        "CREATE TABLE IF NOT EXISTS referrals(invited_id INTEGER PRIMARY KEY, referrer_id INTEGER, rewarded INTEGER DEFAULT 0)"
    )
    await DB_CONN.execute("CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT)")
    await DB_CONN.execute(
        "CREATE TABLE IF NOT EXISTS orders(order_id TEXT PRIMARY KEY, user_id INTEGER NOT NULL, item_type TEXT NOT NULL, item_id TEXT, months INTEGER DEFAULT 0, qty INTEGER DEFAULT 0, price_rub REAL DEFAULT 0, price_usd REAL DEFAULT 0, price_stars INTEGER DEFAULT 0, method TEXT, payment_ref TEXT, paid INTEGER DEFAULT 0, created_at TEXT DEFAULT CURRENT_TIMESTAMP)"
    )
    await DB_CONN.execute(
        "CREATE TABLE IF NOT EXISTS topups(order_id TEXT PRIMARY KEY, user_id INTEGER NOT NULL, amount_rub REAL NOT NULL, payment_ref TEXT, paid INTEGER DEFAULT 0, created_at TEXT DEFAULT CURRENT_TIMESTAMP)"
    )
    await DB_CONN.execute(
        "CREATE TABLE IF NOT EXISTS discounts(id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, percent REAL)"
    )
    await DB_CONN.commit()
    await migrate_db()
    async with DB_LOCK:
        defaults = [
            ("show_discount", "1"),
            ("max_stars", str(DEFAULT_MAX_STARS)),
            ("min_stars", str(DEFAULT_MIN_STARS)),
            ("exchange_rate", str(DEFAULT_EXCHANGE_RATE)),
            ("auto_rate", "0"),
        ]
        for k, v in defaults:
            cur = await DB_CONN.execute(
                "SELECT 1 FROM settings WHERE key = ? LIMIT 1", (k,)
            )
            if await cur.fetchone() is None:
                await DB_CONN.execute("INSERT INTO settings(key, value) VALUES(?,?)", (k, v))
        await DB_CONN.commit()


async def migrate_db():
    async def existing_columns(table: str) -> List[str]:
        cur = await DB_CONN.execute(f"PRAGMA table_info('{table}')")
        rows = await cur.fetchall()
        return [r[1] for r in rows]

    async def table_exists(table: str) -> bool:
        cur = await DB_CONN.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (table,)
        )
        return (await cur.fetchone()) is not None

    async def add_col(table: str, col_sql: str):
        try:
            await DB_CONN.execute(f"ALTER TABLE {table} ADD COLUMN {col_sql}")
            await DB_CONN.commit()
        except Exception:
            pass

    if await table_exists("profiles"):
        cols = await existing_columns("profiles")
        for name, ddl in {
            "real_balance": "real_balance REAL DEFAULT 0",
            "bonus_balance": "bonus_balance REAL DEFAULT 0",
            "referrer_id": "referrer_id INTEGER DEFAULT NULL",
            "referrals_count": "referrals_count INTEGER DEFAULT 0",
            "subscribed": "subscribed INTEGER DEFAULT 0",
        }.items():
            if name not in cols:
                await add_col("profiles", ddl)
    if await table_exists("orders"):
        cols = await existing_columns("orders")
        for name, ddl in {
            "item_type": "item_type TEXT",
            "item_id": "item_id TEXT",
            "months": "months INTEGER DEFAULT 0",
            "qty": "qty INTEGER DEFAULT 0",
            "price_rub": "price_rub REAL DEFAULT 0",
            "price_usd": "price_usd REAL DEFAULT 0",
            "price_stars": "price_stars INTEGER DEFAULT 0",
            "method": "method TEXT",
            "payment_ref": "payment_ref TEXT",
            "paid": "paid INTEGER DEFAULT 0",
            "created_at": "created_at TEXT DEFAULT CURRENT_TIMESTAMP",
        }.items():
            if name not in cols:
                await add_col("orders", ddl)
    if await table_exists("topups"):
        cols = await existing_columns("topups")
        for name, ddl in {
            "payment_ref": "payment_ref TEXT",
            "paid": "paid INTEGER DEFAULT 0",
            "created_at": "created_at TEXT DEFAULT CURRENT_TIMESTAMP",
        }.items():
            if name not in cols:
                await add_col("topups", ddl)
    if await table_exists("discounts"):
        cols = await existing_columns("discounts")
        if "id" not in cols:
            await DB_CONN.execute(
                "CREATE TABLE IF NOT EXISTS discounts__new(id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, percent REAL)"
            )
            try:
                await DB_CONN.execute(
                    "INSERT INTO discounts__new(title, percent) SELECT title, percent FROM discounts"
                )
            except Exception:
                pass
            await DB_CONN.execute("DROP TABLE discounts")
            await DB_CONN.execute("ALTER TABLE discounts__new RENAME TO discounts")
            await DB_CONN.commit()
    if await table_exists("bans"):
        cols = await existing_columns("bans")
        if "reason" not in cols:
            await add_col("bans", "reason TEXT")


async def init_http():
    global HTTP_SESSION
    if HTTP_SESSION is None:
        HTTP_SESSION = aiohttp.ClientSession()


async def add_user(user_id: int, username: Optional[str], first_name: Optional[str]):
    async with DB_LOCK:
        await DB_CONN.execute(
            "INSERT OR REPLACE INTO users(user_id, username, first_name) VALUES(?,?,?)",
            (user_id, username, first_name),
        )
        await DB_CONN.execute(
            "INSERT OR IGNORE INTO profiles(user_id) VALUES(?)", (user_id,)
        )
        await DB_CONN.commit()


async def list_all_user_ids() -> List[int]:
    async with DB_LOCK:
        cur = await DB_CONN.execute("SELECT user_id FROM users")
        return [r[0] for r in await cur.fetchall()]


async def get_username_display(user_id: int) -> str:
    async with DB_LOCK:
        cur = await DB_CONN.execute(
            "SELECT username, first_name FROM users WHERE user_id = ? LIMIT 1",
            (user_id,),
        )
        row = await cur.fetchone()
    uname = ""
    if row:
        username, first_name = row[0], row[1]
        if username:
            uname = f"@{username}"
        elif first_name:
            uname = first_name
    return uname or str(user_id)


async def is_admin(user_id: int) -> bool:
    if user_id == MAIN_ADMIN_ID:
        return True
    async with DB_LOCK:
        cur = await DB_CONN.execute(
            "SELECT 1 FROM admins WHERE user_id = ? LIMIT 1", (user_id,)
        )
        return (await cur.fetchone()) is not None


async def add_admin_db(user_id: int):
    async with DB_LOCK:
        await DB_CONN.execute(
            "INSERT OR REPLACE INTO admins(user_id) VALUES(?)", (user_id,)
        )
        await DB_CONN.commit()


async def remove_admin_db(user_id: int):
    async with DB_LOCK:
        await DB_CONN.execute("DELETE FROM admins WHERE user_id = ?", (user_id,))
        await DB_CONN.commit()


async def list_admins_db() -> List[int]:
    async with DB_LOCK:
        cur = await DB_CONN.execute("SELECT user_id FROM admins")
        return [r[0] for r in await cur.fetchall()]


async def get_all_admin_ids() -> List[int]:
    ids = [MAIN_ADMIN_ID]
    try:
        ids += await list_admins_db()
    except Exception:
        pass
    uniq: List[int] = []
    for i in ids:
        if i not in uniq:
            uniq.append(i)
    return uniq


async def notify_admins_paid(user_id: int, order_id: str, amount_rub: float, method: str):
    admins = await get_all_admin_ids()
    uname = await get_username_display(user_id)
    dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    text = (
        "🔔 <b>Оплата получена</b>\n\n"
        f"Покупатель: {uname} (id {user_id})\n"
        f"Сумма: <b>{amount_rub:.2f}₽</b>\n"
        f"Способ: <b>{method}</b>\n"
        f"Заказ: <code>{order_id}</code>\n"
        f"Дата: {dt}"
    )
    for aid in admins:
        try:
            await bot.send_message(aid, text)
        except Exception:
            pass


async def get_ban_reason(user_id: int) -> Optional[str]:
    async with DB_LOCK:
        cur = await DB_CONN.execute(
            "SELECT reason FROM bans WHERE user_id = ? LIMIT 1", (user_id,)
        )
        row = await cur.fetchone()
    return row[0] if row else None


async def is_banned(user_id: int) -> bool:
    return (await get_ban_reason(user_id)) is not None


async def ban_user_db(user_id: int, reason: str):
    async with DB_LOCK:
        await DB_CONN.execute(
            "INSERT OR REPLACE INTO bans(user_id, reason) VALUES(?,?)", (user_id, reason)
        )
        await DB_CONN.commit()


async def unban_user_db(user_id: int):
    async with DB_LOCK:
        await DB_CONN.execute("DELETE FROM bans WHERE user_id = ?", (user_id,))
    await DB_CONN.commit()


async def get_profile(user_id: int) -> Dict[str, Any]:
    async with DB_LOCK:
        cur = await DB_CONN.execute(
            "SELECT user_id, real_balance, bonus_balance, referrer_id, referrals_count, subscribed FROM profiles WHERE user_id = ? LIMIT 1",
            (user_id,),
        )
        row = await cur.fetchone()
    if not row:
        return {
            "user_id": user_id,
            "real_balance": 0.0,
            "bonus_balance": 0.0,
            "referrer_id": None,
            "referrals_count": 0,
            "subscribed": 0,
        }
    return {
        "user_id": row[0],
        "real_balance": float(row[1] or 0.0),
        "bonus_balance": float(row[2] or 0.0),
        "referrer_id": row[3],
        "referrals_count": int(row[4] or 0),
        "subscribed": int(row[5] or 0),
    }


async def add_real_balance(user_id: int, amount: float):
    async with DB_LOCK:
        await DB_CONN.execute(
            "UPDATE profiles SET real_balance = real_balance + ? WHERE user_id = ?",
            (float(amount), user_id),
        )
        await DB_CONN.commit()


async def deduct_real_balance(user_id: int, amount: float) -> bool:
    prof = await get_profile(user_id)
    if prof["real_balance"] < amount:
        return False
    async with DB_LOCK:
        await DB_CONN.execute(
            "UPDATE profiles SET real_balance = real_balance - ? WHERE user_id = ?",
            (float(amount), user_id),
        )
        await DB_CONN.commit()
    return True


async def set_money_balance(user_id: int, new_value: float):
    async with DB_LOCK:
        await DB_CONN.execute(
            "UPDATE profiles SET real_balance = ? WHERE user_id = ?",
            (float(new_value), user_id),
        )
        await DB_CONN.commit()


async def admin_add_money(user_id: int, amount: float, reason: str = ""):
    await add_real_balance(user_id, amount)
    newb = (await get_profile(user_id))["real_balance"]
    txt = (
        f"💰 Баланс пополнен на <b>{amount:.2f}₽</b>. "
        f"Текущий баланс: <b>{newb:.2f}₽</b>."
    )
    if reason:
        txt += f"\nПричина: {reason}"
    try:
        await bot.send_message(user_id, txt)
    except Exception:
        pass


async def admin_sub_money(user_id: int, amount: float, reason: str = "") -> float:
    prof = await get_profile(user_id)
    deduct = min(float(amount), prof["real_balance"])
    if deduct > 0:
        await deduct_real_balance(user_id, deduct)
    newb = (await get_profile(user_id))["real_balance"]
    txt = (
        f"💸 С вашего баланса списано <b>{deduct:.2f}₽</b>. "
        f"Текущий баланс: <b>{newb:.2f}₽</b>."
    )
    if reason:
        txt += f"\nПричина: {reason}"
    try:
        await bot.send_message(user_id, txt)
    except Exception:
        pass
    return float(deduct)


async def admin_zero_money(user_id: int, reason: str = "") -> float:
    prof = await get_profile(user_id)
    await set_money_balance(user_id, 0.0)
    txt = "🧹 Ваш баланс обнулён."
    if reason:
        txt += f"\nПричина: {reason}"
    try:
        await bot.send_message(user_id, txt)
    except Exception:
        pass
    return float(prof["real_balance"])


async def set_profile_referrer(invited_id: int, referrer_id: int):
    async with DB_LOCK:
        await DB_CONN.execute(
            "UPDATE profiles SET referrer_id = ? WHERE user_id = ? AND referrer_id IS NULL",
            (referrer_id, invited_id),
        )
        await DB_CONN.execute(
            "INSERT OR IGNORE INTO referrals(invited_id, referrer_id, rewarded) VALUES(?,?,0)",
            (invited_id, referrer_id),
        )
        await DB_CONN.commit()


async def is_referral_rewarded(invited_id: int) -> bool:
    async with DB_LOCK:
        cur = await DB_CONN.execute(
            "SELECT rewarded FROM referrals WHERE invited_id = ? LIMIT 1",
            (invited_id,),
        )
        row = await cur.fetchone()
    return bool(row and int(row[0]) == 1)


async def mark_referral_rewarded(invited_id: int):
    async with DB_LOCK:
        await DB_CONN.execute(
            "UPDATE referrals SET rewarded = 1 WHERE invited_id = ?", (invited_id,)
        )
        cur = await DB_CONN.execute(
            "SELECT referrer_id FROM referrals WHERE invited_id = ? LIMIT 1",
            (invited_id,),
        )
        row = await cur.fetchone()
        if row and row[0]:
            await DB_CONN.execute(
                "UPDATE profiles SET referrals_count = referrals_count + 1 WHERE user_id = ?",
                (row[0],),
            )
        await DB_CONN.commit()


async def get_referral_stats(user_id: int) -> Tuple[int, float]:
    prof = await get_profile(user_id)
    refs_done = prof["referrals_count"]
    invited_bonus_count = 0
    async with DB_LOCK:
        cur = await DB_CONN.execute(
            "SELECT rewarded FROM referrals WHERE invited_id = ? LIMIT 1",
            (user_id,),
        )
        row = await cur.fetchone()
    if row and int(row[0] or 0) == 1:
        invited_bonus_count = 1
    total_earned = (refs_done + invited_bonus_count) * REFERRAL_BONUS_RUB
    return refs_done, float(total_earned)


async def award_referral_if_needed(
    invited_id: int, amount: float = REFERRAL_BONUS_RUB
) -> bool:
    try:
        prof = await get_profile(invited_id)
        ref = prof["referrer_id"]
        if not ref or await is_referral_rewarded(invited_id):
            return False
        await add_real_balance(invited_id, amount)
        await add_real_balance(ref, amount)
        await mark_referral_rewarded(invited_id)
        try:
            await bot.send_message(
                invited_id,
                "🎉 <b>Реферальный бонус!</b>\n\n"
                f"Вам начислено <b>{amount:.0f}₽</b> за подписку по реферальной ссылке. Спасибо, что с нами! 🌟",
            )
        except Exception:
            pass
        try:
            await bot.send_message(
                ref,
                "🫂 <b>Новый реферал!</b>\n\n"
                f"Вашему балансу добавлено <b>{amount:.0f}₽</b> за приглашённого друга. Продолжайте приглашать! 💸",
            )
        except Exception:
            pass
        return True
    except Exception as e:
        logger.exception("award_referral_if_needed: %s", e)
        return False


async def create_promocode(code: str, amount: float, activations: int):
    code = (code or "").strip().upper()
    async with DB_LOCK:
        await DB_CONN.execute(
            "INSERT OR REPLACE INTO promocodes(code, amount, activations) VALUES(?,?,?)",
            (code, float(amount), int(activations)),
        )
        await DB_CONN.commit()


async def delete_promocode(code: str) -> bool:
    code = (code or "").strip().upper()
    async with DB_LOCK:
        cur = await DB_CONN.execute("DELETE FROM promocodes WHERE code = ?", (code,))
        await DB_CONN.commit()
        return cur.rowcount > 0


async def get_promocode(code: str) -> Optional[Dict[str, Any]]:
    code = (code or "").strip().upper()
    async with DB_LOCK:
        cur = await DB_CONN.execute(
            "SELECT code, amount, activations FROM promocodes WHERE code = ? LIMIT 1",
            (code,),
        )
        row = await cur.fetchone()
    if not row:
        return None
    return {"code": row[0], "amount": float(row[1]), "activations": int(row[2])}


async def use_promocode(code: str, user_id: int) -> Optional[float]:
    code = (code or "").strip().upper()
    if not code:
        return None

    amount: Optional[float] = None

    async with DB_LOCK:
        cur = await DB_CONN.execute(
            "SELECT amount, activations FROM promocodes WHERE code = ? LIMIT 1",
            (code,),
        )
        row = await cur.fetchone()
        if not row:
            return None
        amount, activations = float(row[0]), int(row[1])
        if activations <= 0:
            return None

        cur = await DB_CONN.execute(
            "SELECT 1 FROM promocode_uses WHERE code = ? AND user_id = ? LIMIT 1",
            (code, user_id),
        )
        if await cur.fetchone():
            return None

        await DB_CONN.execute(
            "INSERT INTO promocode_uses(code, user_id) VALUES(?,?)",
            (code, user_id),
        )
        await DB_CONN.execute(
            "UPDATE promocodes SET activations = activations - 1 WHERE code = ?",
            (code,),
        )
        await DB_CONN.commit()

    if amount is not None:
        await add_real_balance(user_id, amount)
        return amount

    return None


async def create_discount(title: str, percent: float):
    async with DB_LOCK:
        await DB_CONN.execute(
            "INSERT INTO discounts(title, percent) VALUES(?,?)",
            (title, float(percent)),
        )
        await DB_CONN.commit()


async def delete_discount(id_or_title: str) -> bool:
    async with DB_LOCK:
        if id_or_title.isdigit():
            cur = await DB_CONN.execute(
                "DELETE FROM discounts WHERE id = ?", (int(id_or_title),)
            )
        else:
            cur = await DB_CONN.execute(
                "DELETE FROM discounts WHERE title = ?", (id_or_title,)
            )
        await DB_CONN.commit()
        return cur.rowcount > 0


async def list_discounts() -> List[Tuple[int, str, float]]:
    try:
        async with DB_LOCK:
            cur = await DB_CONN.execute(
                "SELECT id, title, percent FROM discounts ORDER BY id DESC"
            )
            return await cur.fetchall()
    except Exception:
        async with DB_LOCK:
            cur = await DB_CONN.execute(
                "SELECT rowid AS id, title, percent FROM discounts ORDER BY rowid DESC"
            )
            return await cur.fetchall()


async def get_setting(key: str, default: Optional[str] = None) -> Optional[str]:
    async with DB_LOCK:
        cur = await DB_CONN.execute(
            "SELECT value FROM settings WHERE key = ? LIMIT 1", (key,)
        )
        row = await cur.fetchone()
    return row[0] if row else default


async def set_setting(key: str, value: str):
    async with DB_LOCK:
        await DB_CONN.execute(
            "INSERT OR REPLACE INTO settings(key, value) VALUES(?,?)", (key, value)
        )
        await DB_CONN.commit()


DISCOUNT_ITEM_KEYS = {
    "premium": "Telegram Premium",
    "stars": "Покупка Звёзд",
    "empty": "Пустой аккаунт",
    "proxy": "Proxy/VPN",
}


async def get_item_discount(item_key: str) -> Tuple[str, float]:
    mode = await get_setting(f"discount_{item_key}_mode", "none")
    value_str = await get_setting(f"discount_{item_key}_value", "0")
    try:
        value = float(value_str or 0.0)
    except Exception:
        value = 0.0
    return mode, value


async def set_item_discount(item_key: str, mode: str, value: float):
    await set_setting(f"discount_{item_key}_mode", mode)
    await set_setting(f"discount_{item_key}_value", str(float(value)))


async def save_order(
    order_id: str,
    user_id: int,
    item_type: str,
    item_id: str = "",
    months: int = 0,
    qty: int = 0,
    price_rub: float = 0.0,
    price_usd: float = 0.0,
    price_stars: int = 0,
    method: str = "",
    payment_ref: Optional[str] = None,
):
    async with DB_LOCK:
        await DB_CONN.execute(
            "INSERT OR REPLACE INTO orders(order_id, user_id, item_type, item_id, months, qty, price_rub, price_usd, price_stars, method, payment_ref, paid) VALUES(?,?,?,?,?,?,?,?,?,?,?,0)",
            (
                order_id,
                user_id,
                item_type,
                item_id,
                months,
                qty,
                float(price_rub),
                float(price_usd),
                int(price_stars),
                method,
                payment_ref,
            ),
        )
        await DB_CONN.commit()


async def mark_order_paid(order_id: str) -> bool:
    async with DB_LOCK:
        cur = await DB_CONN.execute(
            "UPDATE orders SET paid = 1 WHERE order_id = ?", (order_id,)
        )
        await DB_CONN.commit()
        return cur.rowcount > 0


async def get_order(order_id: str) -> Optional[Tuple]:
    async with DB_LOCK:
        cur = await DB_CONN.execute(
            "SELECT order_id, user_id, item_type, item_id, months, qty, price_rub, price_usd, price_stars, method, payment_ref, paid, created_at FROM orders WHERE order_id = ? LIMIT 1",
            (order_id,),
        )
        return await cur.fetchone()


async def list_orders(limit: int = 200) -> List[Tuple]:
    async with DB_LOCK:
        cur = await DB_CONN.execute(
            "SELECT order_id, user_id, item_type, item_id, months, qty, price_rub, price_usd, price_stars, method, payment_ref, paid, created_at FROM orders ORDER BY created_at DESC LIMIT ?",
            (limit,),
        )
        return await cur.fetchall()


async def get_unpaid_topups(limit: int = 120) -> List[Tuple[str, int, float]]:
    async with DB_LOCK:
        cur = await DB_CONN.execute(
            "SELECT order_id, user_id, price_rub FROM orders WHERE item_type = 'topup' AND paid = 0 ORDER BY created_at DESC LIMIT ?",
            (limit,),
        )
        return await cur.fetchall()


async def export_users_csv(path: str = "users_export.csv") -> str:
    async with DB_LOCK:
        cur = await DB_CONN.execute("SELECT user_id, username, first_name FROM users")
        rows = await cur.fetchall()
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["user_id", "username", "first_name"])
        for r in rows:
            w.writerow(r)
    return path


def usd_to_stars(usd: float) -> int:
    return int(math.floor(usd * STARS_PER_USD))


async def fetch_exchange_rate_from_api() -> Optional[float]:
    await init_http()
    try:
        async with HTTP_SESSION.get(EXCHANGE_API_URL, timeout=10) as resp:
            if resp.status == 200:
                j = await resp.json()
                rate = (j.get("rates") or {}).get("RUB")
                if rate:
                    return float(rate)
    except Exception:
        logger.exception("fetch_exchange_rate_from_api failed")
    return None


async def get_exchange_rate_value() -> float:
    auto = (await get_setting("auto_rate", "0")) == "1"
    if auto:
        r = await fetch_exchange_rate_from_api()
        if r:
            try:
                await set_setting("exchange_rate", str(r))
            except Exception:
                pass
            return float(r)
    s = await get_setting("exchange_rate", str(DEFAULT_EXCHANGE_RATE))
    try:
        return float(s)
    except Exception:
        return DEFAULT_EXCHANGE_RATE


def quickpay_link(receiver: str, amount_rub: float, label: str, payment_type: str = "AC") -> str:
    from urllib.parse import urlencode

    params = {
        "receiver": receiver,
        "quickpay-form": "shop",
        "sum": f"{amount_rub:.2f}",
        "paymentType": payment_type,
        "label": label,
    }
    return YOOMONEY_QUICKPAY_BASE + "?" + urlencode(params)


async def yoomoney_check_label(label: str) -> bool:
    if not YOOMONEY_ACCESS_TOKEN:
        return False
    await init_http()
    headers = {"Authorization": f"Bearer {YOOMONEY_ACCESS_TOKEN}"}
    data = {"label": label, "records": 50, "type": "deposition"}
    try:
        async with HTTP_SESSION.post(
            YOOMONEY_OP_HISTORY, data=data, headers=headers, timeout=15
        ) as resp:
            if resp.status != 200:
                return False
            j = await resp.json()
            for op in j.get("operations", []):
                lab = op.get("label") or op.get("comment") or op.get("title")
                status = (op.get("status") or op.get("state") or "").lower()
                if str(lab) == str(label) and status in (
                    "success",
                    "completed",
                    "done",
                ):
                    return True
    except Exception:
        logger.exception("yoomoney_check_label failed")
    return False


async def get_payment_status(payment_ref: str) -> Dict[str, Any]:
    try:
        ok = await yoomoney_check_label(payment_ref)
        return {"paid": bool(ok), "backend": "yoomoney"}
    except Exception:
        return {"paid": False}


async def stars_needed_for_rub(amount_rub: float) -> int:
    rate = await get_exchange_rate_value()
    usd = amount_rub / rate if rate > 0 else amount_rub / DEFAULT_EXCHANGE_RATE
    base_stars = usd_to_stars(usd)
    need = math.ceil(base_stars * 1.15)
    return max(need, 1)


async def premium_price_rub(months: int) -> float:
    if months == 1:
        usd = BASE_USD_PER_MONTH
    elif months == 12:
        usd = BASE_USD_YEAR
    else:
        usd = BASE_USD_PER_MONTH * months
    rate = await get_exchange_rate_value()
    rub = math.ceil(usd * rate * 100) / 100.0
    return rub


BTN_TELEGRAM = "📦 Telegram"
BTN_PROXY = "🛰 Proxy/VPN"
BTN_FREE = "💠 Бесплатная Премка"
BTN_PROFILE = "👤 Профиль"
BTN_PROMO = "🎫 Промокод"
BTN_SUPPORT = "🆘 Поддержка"
BTN_PRIVACY = "🔒 Политика конфиденциальности"
BTN_REF_SYSTEM = "🫂 Реферальная система"
BTN_BACK = "🔙 Назад"
BTN_PREMIUM = "💎 Telegram Premium"
BTN_STARS = "⭐ Купить Звёзды"
BTN_EMPTY = f"🆕 Пустой Аккаунт - {EMPTY_ACCOUNT_PRICE_RUB:.0f}₽"
BTN_TOPUP = "💳 Пополнить Баланс"
BTN_REF = "👥 Рефералы"
BTN_PAY_YM = "🔗 Оплатить YooMoney"
BTN_PAY_BAL = "🧾 Оплатить С Баланса"
BTN_PAY_STARS = "⭐ Оплатить Звёздами"
BTN_CHECK = "🔎 Проверить Оплату"


def rk_main() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_TELEGRAM), KeyboardButton(text=BTN_PROXY)],
            [KeyboardButton(text=BTN_FREE)],
            [KeyboardButton(text=BTN_PROFILE), KeyboardButton(text=BTN_PROMO)],
            [KeyboardButton(text=BTN_SUPPORT), KeyboardButton(text=BTN_PRIVACY)],
            [KeyboardButton(text=BTN_REF_SYSTEM)],
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
        input_field_placeholder="Выберите действие…",
    )


def rk_telegram() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_PREMIUM)],
            [KeyboardButton(text=BTN_STARS)],
            [KeyboardButton(text=BTN_EMPTY)],
            [KeyboardButton(text=BTN_BACK)],
        ],
        resize_keyboard=True,
    )


def rk_premium_periods() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🗓 1 Месяц"), KeyboardButton(text="📆 12 Месяцев")],
            [KeyboardButton(text=BTN_BACK)],
        ],
        resize_keyboard=True,
    )


def rk_stars_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="⭐ 300"),
                KeyboardButton(text="⭐ 600"),
                KeyboardButton(text="⭐ 1200"),
            ],
            [KeyboardButton(text="✏️ Свой Объём")],
            [KeyboardButton(text=BTN_BACK)],
        ],
        resize_keyboard=True,
    )


def rk_proxy() -> ReplyKeyboardMarkup:
    rows: List[List[KeyboardButton]] = []
    row: List[KeyboardButton] = []
    for i, c in enumerate(PROXY_COUNTRIES, 1):
        row.append(KeyboardButton(text=f"🌐 {c} - {PROXY_PRICE_RUB:.0f}₽"))
        if i % 2 == 0:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([KeyboardButton(text=BTN_BACK)])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def rk_profile() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_TOPUP)],
            [KeyboardButton(text=BTN_BACK)],
        ],
        resize_keyboard=True,
    )


def rk_topup() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="💵 100₽"),
                KeyboardButton(text="💵 300₽"),
                KeyboardButton(text="💵 500₽"),
            ],
            [KeyboardButton(text="✏️ Своя Сумма")],
            [KeyboardButton(text=BTN_BACK)],
        ],
        resize_keyboard=True,
    )


def rk_payment_actions() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_PAY_YM)],
            [KeyboardButton(text=BTN_PAY_BAL), KeyboardButton(text=BTN_PAY_STARS)],
            [KeyboardButton(text=BTN_CHECK)],
            [KeyboardButton(text=BTN_BACK)],
        ],
        resize_keyboard=True,
    )


def rk_payment_actions_yoomoney_only() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_PAY_YM)],
            [KeyboardButton(text=BTN_CHECK)],
            [KeyboardButton(text=BTN_BACK)],
        ],
        resize_keyboard=True,
    )


user_states: Dict[int, Dict[str, Optional[str]]] = {}
admin_states: Dict[int, Dict[str, Optional[str]]] = {}


def set_user_state(
    uid: int, state: Optional[str], tmp: Optional[str] = None, extra: Optional[Dict[str, Any]] = None
):
    user_states[uid] = {"state": state, "tmp": tmp}
    if extra:
        user_states[uid].update(extra)


def get_user_state(uid: int) -> Dict[str, Optional[str]]:
    return user_states.get(uid, {"state": None, "tmp": None})


def clear_user_state(uid: int):
    user_states.pop(uid, None)


def set_admin_state(uid: int, state: Optional[str], tmp: Optional[str] = None):
    admin_states[uid] = {"state": state, "tmp": tmp}


def get_admin_state(uid: int) -> Dict[str, Optional[str]]:
    return admin_states.get(uid, {"state": None, "tmp": None})


def clear_admin_state(uid: int):
    admin_states.pop(uid, None)


async def main_menu_text() -> str:
    txt = "🌟 <b>Главное Меню</b>\n\nВыберите категорию ниже."
    show = (await get_setting("show_discount", "1")) == "1"
    if show:
        lines: List[str] = []
        for key in ("premium", "stars", "empty", "proxy"):
            mode, value = await get_item_discount(key)
            if mode in ("percent", "fixed") and value > 0:
                name = DISCOUNT_ITEM_KEYS.get(key, key)
                if mode == "percent":
                    val_txt = f"{value:.0f}%"
                else:
                    val_txt = f"{value:.0f}₽"
                lines.append(f"- {name}: <b>{val_txt}</b>")
        if lines:
            txt += "\n\n🎉 <b>Акции</b>:\n" + "\n".join(lines)
    return txt


FREE_PREMIUM_TEXT = (
    "💠 <b>Бесплатная Премка</b>\n\n"
    "🫂 Приглашай друзей по своей реферальной ссылке и получай рубли на баланс.\n"
    "💖 Копи баланс и бери ⭐ звёзды, премиум, аккаунты и прокси вообще без вложений.\n\n"
    "📌 <b>Как это работает</b>:\n"
    "• 👫 За каждого друга, который подпишется на канал, вы оба получаете бонус на баланс.\n"
    "• 🎁 Активируй промокоды — тоже получай рубли на счёт.\n\n"
    "💳 Всё, что есть в магазине, можно оплатить с баланса — то есть получить бесплатно."
)


async def check_subscription_status(user_id: int) -> bool:
    if not CHANNEL_USERNAME or not CHANNEL_USERNAME.startswith("@"):
        return True
    try:
        m = await bot.get_chat_member(CHANNEL_USERNAME, user_id)
        status = (m.status or "").lower()
        return status in ("member", "administrator", "creator")
    except Exception:
        return False


def ik_subscribe() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📲 Подписаться на канал", url=CHANNEL_INVITE_LINK)],
        ]
    )


async def send_subscribe_required(message: Message, was_subscribed: bool):
    if was_subscribed:
        text = (
            "📴 <b>Вы отписались от нашего канала</b>.\n\n"
            "Чтобы дальше пользоваться ботом, снова подпишитесь на канал и вернитесь в бота 🫶\n\n"
            "После подписки нажмите <b>/start</b>."
        )
    else:
        text = (
            "🔔 <b>Добро пожаловать!</b>\n\n"
            "Чтобы пользоваться ботом, нужно быть подписанным на наш канал 💠\n\n"
            "Подпишитесь, а потом вернитесь в бота и нажмите <b>/start</b>."
        )
    await message.answer(text, reply_markup=ik_subscribe())


async def check_and_handle_subscription(message: Message, send_thanks: bool = False) -> bool:
    uid = message.from_user.id
    prof = await get_profile(uid)
    prev_sub = int(prof.get("subscribed", 0))
    subscribed = await check_subscription_status(uid)
    async with DB_LOCK:
        await DB_CONN.execute(
            "UPDATE profiles SET subscribed = ? WHERE user_id = ?",
            (1 if subscribed else 0, uid),
        )
        await DB_CONN.commit()
    if not subscribed:
        await send_subscribe_required(message, was_subscribed=bool(prev_sub))
        return False
    if send_thanks and not prev_sub and subscribed:
        await message.answer(
            "✅ <b>Спасибо за подписку!</b>\n\n"
            "Теперь вы можете пользоваться ботом. Вывожу главное меню 👇"
        )
    return True


async def try_pay_with_balance(user_id: int, amount_rub: float) -> bool:
    prof = await get_profile(user_id)
    if prof["real_balance"] < amount_rub:
        return False
    return await deduct_real_balance(user_id, amount_rub)


@dp.message(Command("start"))
async def cmd_start(message: Message):
    if await is_banned(message.from_user.id):
        reason = await get_ban_reason(message.from_user.id)
        await message.answer(
            "⛔ Вы не можете использовать бота, так как забанены навсегда.\n"
            f"Причина: {reason or 'не указана'}"
        )
        return
    args = ""
    try:
        args = (
            (message.text or "").split(maxsplit=1)[1]
            if " " in (message.text or "")
            else ""
        )
    except Exception:
        args = ""
    await add_user(
        message.from_user.id, message.from_user.username, message.from_user.first_name
    )
    if args and args.startswith("ref"):
        try:
            rid = int(args.replace("ref", "").strip())
            if rid and rid != message.from_user.id:
                await set_profile_referrer(message.from_user.id, rid)
        except Exception:
            pass

    ok = await check_and_handle_subscription(message, send_thanks=True)
    if not ok:
        return

    await award_referral_if_needed(message.from_user.id, REFERRAL_BONUS_RUB)

    set_user_state(message.from_user.id, "main")
    caption = await main_menu_text()
    if os.path.exists(BANNER_FILE):
        try:
            await bot.send_photo(
                chat_id=message.chat.id,
                photo=InputFile(BANNER_FILE),
                caption=caption,
                reply_markup=rk_main(),
            )
        except Exception:
            await message.answer(caption, reply_markup=rk_main())
    else:
        await message.answer(caption, reply_markup=rk_main())


@dp.message(Command("profile"))
async def cmd_profile(message: Message):
    if await is_banned(message.from_user.id):
        reason = await get_ban_reason(message.from_user.id)
        await message.answer(
            "⛔ Вы не можете использовать бота, так как забанены навсегда.\n"
            f"Причина: {reason or 'не указана'}"
        )
        return
    if not await check_and_handle_subscription(message):
        return
    await open_profile(message)


@dp.message(Command("referral"))
async def cmd_referral(message: Message):
    if await is_banned(message.from_user.id):
        reason = await get_ban_reason(message.from_user.id)
        await message.answer(
            "⛔ Вы не можете использовать бота, так как забанены навсегда.\n"
            f"Причина: {reason or 'не указана'}"
        )
        return
    if not await check_and_handle_subscription(message):
        return
    await open_referrals(message)


@dp.message(Command("promocode"))
async def cmd_promocode(message: Message):
    if await is_banned(message.from_user.id):
        reason = await get_ban_reason(message.from_user.id)
        await message.answer(
            "⛔ Вы не можете использовать бота, так как забанены навсегда.\n"
            f"Причина: {reason or 'не указана'}"
        )
        return
    if not await check_and_handle_subscription(message):
        return
    set_user_state(message.from_user.id, "promo_wait")
    await message.answer("🎫 Отправьте промокод одним сообщением.")


@dp.message(Command("politics"))
async def cmd_politics(message: Message):
    await message.answer(PRIVACY_URL)


@dp.message(Command("admin"))
async def cmd_admin(message: Message):
    if not await is_admin(message.from_user.id):
        await message.reply("🚫 Доступ запрещён.")
        return
    if not await check_and_handle_subscription(message):
        return
    show = (await get_setting("show_discount", "1")) == "1"
    show_label = "Вкл" if show else "Выкл"
    max_stars = await get_setting("max_stars", str(DEFAULT_MAX_STARS))
    min_stars = await get_setting("min_stars", str(DEFAULT_MIN_STARS))
    rate = await get_exchange_rate_value()
    auto = (await get_setting("auto_rate", "0")) == "1"
    auto_label = "Вкл" if auto else "Выкл"
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📣 Рассылка", callback_data="admin_broadcast"
                ),
                InlineKeyboardButton(
                    text="📦 Просмотр заказов", callback_data="admin_view_orders"
                ),
            ],
            [
                InlineKeyboardButton(
                    text="🔎 Проверить платёж", callback_data="admin_check_payment"
                ),
                InlineKeyboardButton(
                    text="✅ Отметить оплату", callback_data="admin_mark_paid"
                ),
            ],
            [
                InlineKeyboardButton(
                    text="💸 Управление скидками", callback_data="admin_discounts"
                ),
                InlineKeyboardButton(
                    text="⛔ Бан/Разбан", callback_data="admin_ban"
                ),
            ],
            [
                InlineKeyboardButton(
                    text="💼 Балансы", callback_data="admin_balances"
                )
            ],
            [
                InlineKeyboardButton(
                    text=f"🔔 Показать скидку: {show_label}",
                    callback_data="admin_toggle_discount_display",
                )
            ],
            [
                InlineKeyboardButton(
                    text=f"⭐ Макс звёзд: {max_stars}",
                    callback_data="admin_set_max_stars",
                ),
                InlineKeyboardButton(
                    text=f"⭐ Мин звёзд: {min_stars}",
                    callback_data="admin_set_min_stars",
                ),
            ],
            [
                InlineKeyboardButton(
                    text=f"💱 Коэфф.: {rate}", callback_data="admin_set_rate"
                )
            ],
            [
                InlineKeyboardButton(
                    text="👤 Назначить админа", callback_data="admin_add_admin"
                ),
                InlineKeyboardButton(
                    text="🚫 Снять админа", callback_data="admin_remove_admin"
                ),
            ],
            [
                InlineKeyboardButton(
                    text="📋 Список админов", callback_data="admin_list_admins"
                )
            ],
            [
                InlineKeyboardButton(
                    text="📊 Статистика", callback_data="admin_stats"
                )
            ],
            [
                InlineKeyboardButton(
                    text="🎫 Промокоды", callback_data="admin_promos"
                ),
                InlineKeyboardButton(
                    text="💳 Топапы", callback_data="admin_topups"
                ),
            ],
        ]
    )
    await message.answer("🛠️ <b>Админ-панель</b>", reply_markup=kb)


@dp.callback_query(lambda c: c.data and c.data.startswith("admin_"))
async def cb_admin_panel(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Доступ запрещён.", show_alert=True)
        return
    data = callback.data
    set_admin_state(callback.from_user.id, get_admin_state(callback.from_user.id).get("state"))
    if data == "admin_broadcast":
        set_admin_state(callback.from_user.id, "waiting_broadcast")
        await callback.message.edit_text(
            "📝 Режим рассылки:\n\nОтправьте в этот чат одно сообщение (текст/медиа)."
        )
    elif data == "admin_view_orders":
        rows = await list_orders(200)
        if not rows:
            await callback.message.edit_text("📭 Список заказов пуст.")
            await callback.answer()
            return
        lines = ["📦 <b>Последние заказы</b>:\n"]
        for r in rows[:120]:
            oid, uid, itype, iid, months, qty, rub, usd, stars, method, pref, paid, created_at = r
            status = "✅" if paid else "⏳"
            lines.append(
                f"{status} {oid} — {itype}:{iid or '-'} — {months}м/{qty}шт — {rub}₽ — {method} — ref:{pref or '-'} — user:{uid}"
            )
        await callback.message.edit_text("\n".join(lines))
    elif data == "admin_check_payment":
        set_admin_state(callback.from_user.id, "waiting_check_payment")
        await callback.message.edit_text("🔎 Отправьте плат.реф или order_id для проверки:")
    elif data == "admin_mark_paid":
        set_admin_state(callback.from_user.id, "waiting_mark_paid")
        await callback.message.edit_text("🟢 Отправьте order_id, чтобы пометить как оплачен:")
    elif data == "admin_discounts":
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="💎 Telegram Premium", callback_data="disc_item_premium"
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="⭐ Звёзды", callback_data="disc_item_stars"
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="🆕 Пустой аккаунт", callback_data="disc_item_empty"
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="🛰 Proxy/VPN", callback_data="disc_item_proxy"
                    )
                ],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")],
            ]
        )
        await callback.message.edit_text(
            "📊 <b>Управление скидками</b>\n\nВыберите товар, для которого хотите настроить скидку.",
            reply_markup=kb,
        )
    elif data == "admin_ban":
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="⛔ Заблокировать", callback_data="ban_user"
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="🔓 Разблокировать", callback_data="unban_user"
                    )
                ],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")],
            ]
        )
        await callback.message.edit_text("🚫 Бан / Разбан:", reply_markup=kb)
    elif data == "admin_balances":
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="➕ Выдать Баланс", callback_data="bal_money_add"
                    ),
                    InlineKeyboardButton(
                        text="➖ Забрать Баланс", callback_data="bal_money_sub"
                    ),
                ],
                [
                    InlineKeyboardButton(
                        text="🧹 Обнулить Баланс", callback_data="bal_money_zero"
                    )
                ],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")],
            ]
        )
        await callback.message.edit_text("💼 <b>Операции с балансом</b>", reply_markup=kb)
    elif data == "admin_toggle_discount_display":
        cur = (await get_setting("show_discount", "1")) == "1"
        await set_setting("show_discount", "0" if cur else "1")
        state = "Включено" if not cur else "Выключено"
        await callback.message.edit_text(f"🔔 Отображение текущей скидки теперь: {state}")
    elif data == "admin_stats":
        async with DB_LOCK:
            cur = await DB_CONN.execute("SELECT COUNT(*) FROM users")
            users_cnt = (await cur.fetchone())[0]
            cur = await DB_CONN.execute("SELECT COUNT(*) FROM orders")
            orders_cnt = (await cur.fetchone())[0]
            cur = await DB_CONN.execute("SELECT COUNT(*) FROM orders WHERE paid = 1")
            paid_cnt = (await cur.fetchone())[0]
        await callback.message.edit_text(
            f"📈 <b>Статистика</b>\n\nПользователей: {users_cnt}\nВсего заказов: {orders_cnt}\nОплачено: {paid_cnt}"
        )
    elif data == "admin_back":
        await cmd_admin(callback.message)
    elif data == "admin_set_max_stars":
        set_admin_state(callback.from_user.id, "waiting_set_max_stars")
        await callback.message.edit_text(
            "✏️ Введите новое значение максимума звёзд (целое число), например: 150000"
        )
    elif data == "admin_set_min_stars":
        set_admin_state(callback.from_user.id, "waiting_set_min_stars")
        await callback.message.edit_text(
            "✏️ Введите новое значение минимума звёзд (целое число), например: 300"
        )
    elif data == "admin_set_rate":
        set_admin_state(callback.from_user.id, "waiting_set_rate")
        await callback.message.edit_text(
            "✏️ Введите новый коэффициент (число), например: 85.0"
        )
    elif data == "admin_toggle_auto_rate":
        cur = (await get_setting("auto_rate", "0")) == "1"
        await set_setting("auto_rate", "0" if cur else "1")
        state = "Включено" if not cur else "Выключено"
        await callback.message.edit_text(f"⚙ Авто-коэффициент теперь: {state}")
    elif data == "admin_add_admin":
        set_admin_state(callback.from_user.id, "waiting_add_admin")
        await callback.message.edit_text("👤 Введите: user_id [причина]")
    elif data == "admin_remove_admin":
        set_admin_state(callback.from_user.id, "waiting_remove_admin")
        await callback.message.edit_text("👥 Введите: user_id [причина]")
    elif data == "admin_list_admins":
        admins = await list_admins_db()
        lines = [f"Основной админ: {MAIN_ADMIN_ID}"]
        if admins:
            lines.append("Доп. админы:")
            for a in admins:
                lines.append(f"• {a}")
        else:
            lines.append("Доп. админов нет.")
        await callback.message.edit_text("\n".join(lines))
    elif data == "admin_promos":
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="➕ Создать промокод", callback_data="promo_add"
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="➖ Удалить промокод", callback_data="promo_delete"
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="📋 Список промокодов", callback_data="promo_list"
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="🗑 Удалить все промокоды", callback_data="promo_delete_all"
                    )
                ],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")],
            ]
        )
        await callback.message.edit_text("🎫 <b>Промокоды</b>", reply_markup=kb)
    elif data == "admin_topups":
        rows = await get_unpaid_topups()
        if not rows:
            await callback.message.edit_text("Нет ожидающих пополнений.")
            await callback.answer()
            return
        lines = ["💳 <b>Ожидающие пополнения</b>:\n"]
        for oid, uid, amount in rows[:120]:
            lines.append(f"{oid} — {amount:.2f}₽ — user:{uid}")
        await callback.message.edit_text("\n".join(lines))
    await callback.answer()


@dp.callback_query(
    lambda c: c.data
    and c.data
    in [
        "discount_add",
        "discount_remove",
        "discount_list",
        "ban_user",
        "unban_user",
        "promo_add",
        "promo_list",
        "promo_delete",
        "promo_delete_all",
        "bal_money_add",
        "bal_money_sub",
        "bal_money_zero",
    ]
)
async def cb_admin_submenus(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Доступ запрещён.", show_alert=True)
        return
    data = callback.data
    if data == "discount_add":
        set_admin_state(callback.from_user.id, "waiting_discount_add")
        await callback.message.edit_text(
            "✏️ Введите: <b>Название %</b>\n\nНапример: <code>Осень 10</code> или просто <code>10</code>"
        )
    elif data == "discount_remove":
        set_admin_state(callback.from_user.id, "waiting_discount_remove")
        await callback.message.edit_text(
            "✏️ Введите <b>ID</b> или <b>Название</b> скидки для удаления:"
        )
    elif data == "discount_list":
        disc = await list_discounts()
        if not disc:
            await callback.message.edit_text("Скидок нет.")
        else:
            lines = ["🎉 <b>Скидки</b>:\n"]
            for i, t, p in disc:
                lines.append(f"{i}. {t} — <b>{p:.0f}%</b>")
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]
                ]
            )
            await callback.message.edit_text("\n".join(lines), reply_markup=kb)
    elif data == "ban_user":
        set_admin_state(callback.from_user.id, "waiting_ban_user")
        await callback.message.edit_text("⛔ Введите: <b>user_id причина</b>")
    elif data == "unban_user":
        set_admin_state(callback.from_user.id, "waiting_unban_user")
        await callback.message.edit_text("🔓 Введите: <b>user_id [причина]</b>")
    elif data == "promo_add":
        set_admin_state(callback.from_user.id, "waiting_promo_add")
        await callback.message.edit_text(
            "Введите: <b>КОД сумма активаций</b>\n\nПример: <code>SUPER 50 10</code>"
        )
    elif data == "promo_delete":
        set_admin_state(callback.from_user.id, "waiting_promo_delete")
        await callback.message.edit_text("Введите <b>КОД</b> промокода для удаления:")
    elif data == "promo_list":
        async with DB_LOCK:
            cur = await DB_CONN.execute(
                "SELECT code, amount, activations FROM promocodes ORDER BY code LIMIT 100"
            )
            rows = await cur.fetchall()
        if not rows:
            await callback.message.edit_text("Список пуст.")
        else:
            lines = ["🎫 <b>Промокоды</b>:\n"]
            for c, a, act in rows:
                lines.append(f"<b>{c}</b> — <b>{a:.0f}₽</b> — осталось <b>{act}</b>")
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]
                ]
            )
            await callback.message.edit_text("\n".join(lines), reply_markup=kb)
    elif data == "promo_delete_all":
        async with DB_LOCK:
            await DB_CONN.execute("DELETE FROM promocodes")
            await DB_CONN.execute("DELETE FROM promocode_uses")
            await DB_CONN.commit()
        await callback.message.edit_text("🗑 Все промокоды были удалены.")
    elif data == "bal_money_add":
        set_admin_state(callback.from_user.id, "waiting_bal_money_add")
        await callback.message.edit_text(
            "➕ Введите: <b>user_id сумма [причина]</b>"
        )
    elif data == "bal_money_sub":
        set_admin_state(callback.from_user.id, "waiting_bal_money_sub")
        await callback.message.edit_text(
            "➖ Введите: <b>user_id сумма [причина]</b>"
        )
    elif data == "bal_money_zero":
        set_admin_state(callback.from_user.id, "waiting_bal_money_zero")
        await callback.message.edit_text("🧹 Введите: <b>user_id [причина]</b>")
    await callback.answer()


@dp.callback_query(lambda c: c.data and c.data.startswith("disc_"))
async def cb_discounts_items(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Доступ запрещён.", show_alert=True)
        return
    data = callback.data
    if data.startswith("disc_item_"):
        key = data.replace("disc_item_", "", 1)
        mode, value = await get_item_discount(key)
        name = DISCOUNT_ITEM_KEYS.get(key, key)
        if mode == "percent":
            cur = f"{value:.0f}%"
        elif mode == "fixed":
            cur = f"{value:.0f}₽"
        else:
            cur = "нет"
        text = (
            f"📊 <b>Скидка для товара:</b> {name}\n\n"
            f"Текущая скидка: <b>{cur}</b>\n\n"
            "Выберите, как хотите задать скидку:"
        )
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="📉 Скидка в %", callback_data=f"disc_set_percent_{key}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="💵 Скидка в ₽", callback_data=f"disc_set_fixed_{key}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="🗑 Убрать скидку", callback_data=f"disc_clear_{key}"
                    )
                ],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_discounts")],
            ]
        )
        await callback.message.edit_text(text, reply_markup=kb)
        await callback.answer()
        return
    if data.startswith("disc_set_percent_"):
        key = data.replace("disc_set_percent_", "", 1)
        set_admin_state(callback.from_user.id, "waiting_discount_value", tmp=f"percent|{key}")
        name = DISCOUNT_ITEM_KEYS.get(key, key)
        await callback.message.edit_text(
            f"✏️ Введите размер скидки в процентах для <b>{name}</b>.\n\n"
            "Например: <code>10</code> (это будет 10%)."
        )
        await callback.answer()
        return
    if data.startswith("disc_set_fixed_"):
        key = data.replace("disc_set_fixed_", "", 1)
        set_admin_state(callback.from_user.id, "waiting_discount_value", tmp=f"fixed|{key}")
        name = DISCOUNT_ITEM_KEYS.get(key, key)
        await callback.message.edit_text(
            f"✏️ Введите размер скидки в рублях для <b>{name}</b>.\n\n"
            "Например: <code>50</code> (скидка 50₽)."
        )
        await callback.answer()
        return
    if data.startswith("disc_clear_"):
        key = data.replace("disc_clear_", "", 1)
        await set_item_discount(key, "none", 0.0)
        name = DISCOUNT_ITEM_KEYS.get(key, key)
        await callback.message.edit_text(f"🧹 Скидка для товара <b>{name}</b> отключена.")
        await callback.answer()
        return
    await callback.answer()


@dp.callback_query(lambda c: c.data == "referral_back_to_main")
async def cb_referral_back_to_main(callback: CallbackQuery):
    await callback.answer()
    await open_main(callback.message)


async def open_main(message: Message):
    set_user_state(message.from_user.id, "main")
    caption = await main_menu_text()
    if os.path.exists(BANNER_FILE):
        try:
            await bot.send_photo(
                chat_id=message.chat.id,
                photo=InputFile(BANNER_FILE),
                caption=caption,
                reply_markup=rk_main(),
            )
        except Exception:
            await message.answer(caption, reply_markup=rk_main())
    else:
        await message.answer(caption, reply_markup=rk_main())


async def open_telegram(message: Message):
    set_user_state(message.from_user.id, "cat_telegram")
    await message.answer("📦 <b>Telegram</b>\n\nВыберите товар:", reply_markup=rk_telegram())


async def open_proxy(message: Message):
    set_user_state(message.from_user.id, "cat_proxy")
    await message.answer(
        f"🛰 <b>Proxy/VPN</b>\n\n"
        f"<b>Технология</b>: {PROXY_TECH_DESCRIPTION}\n"
        f"<b>Цена</b>: {PROXY_PRICE_RUB:.0f}₽ за любой.\n\n"
        "Все — IPv4 Socks5.",
        reply_markup=rk_proxy(),
    )


async def open_free(message: Message):
    set_user_state(message.from_user.id, "free")
    await message.answer(
        FREE_PREMIUM_TEXT,
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text=BTN_BACK)]], resize_keyboard=True
        ),
    )


async def open_profile(message: Message):
    set_user_state(message.from_user.id, "profile")
    p = await get_profile(message.from_user.id)
    txt = (
        "👤 <b>Профиль</b>\n\n"
        f"💵 Баланс: <b>{p['real_balance']:.2f}₽</b>\n"
        f"👥 Рефералов: <b>{p['referrals_count']}</b>"
    )
    await message.answer(txt, reply_markup=rk_profile())


async def open_referrals(message: Message):
    uid = message.from_user.id
    me = await bot.me()
    link = f"https://t.me/{me.username}?start=ref{uid}"
    refs_done, earned_total = await get_referral_stats(uid)
    text = (
        "🫂 <b>Реферальная система</b>\n\n"
        "🔗 Ваша личная ссылка:\n"
        f"<code>{link}</code>\n\n"
        f"👥 Приглашено друзей: <b>{refs_done}</b>\n"
        f"💸 Заработано на рефералах: <b>{earned_total:.0f}₽</b>\n\n"
        "✨ За каждого друга, который подпишется на канал, вы и ваш друг получаете по "
        f"<b>{REFERRAL_BONUS_RUB:.0f}₽</b> на баланс!"
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔙 Назад в главное меню",
                    callback_data="referral_back_to_main",
                )
            ],
        ]
    )
    await message.answer(text, reply_markup=kb)


async def open_topup_menu(message: Message):
    set_user_state(message.from_user.id, "topup")
    await message.answer(
        "💳 <b>Пополнение Баланса</b>\n\nВыберите сумму или задайте свою:",
        reply_markup=rk_topup(),
    )


async def start_topup(message: Message, amount_rub: float):
    uid = message.from_user.id
    order_id = str(uuid.uuid4())
    receiver = YOOMONEY_RECEIVER or ""
    if not receiver:
        await message.answer(
            "❗ Укажите номер кошелька YooMoney.", reply_markup=rk_profile()
        )
        return
    link = quickpay_link(receiver, amount_rub, order_id, "AC")
    await save_order(
        order_id,
        uid,
        "topup",
        "",
        0,
        0,
        price_rub=amount_rub,
        method="yoomoney_topup",
        payment_ref=order_id,
    )
    set_user_state(uid, "await_payment", tmp=order_id, extra={"pay_kind": "topup"})
    stars_need = await stars_needed_for_rub(amount_rub)
    await message.answer(
        f"🧾 Пополнение: <b>{amount_rub:.2f}₽</b>\n"
        f"Номер заказа: <code>{order_id}</code>\n\n"
        f"🔗 Ссылка на оплату YooMoney:\n"
        f"<a href=\"{link}\">💳 Перейти к оплате</a>\n\n"
        f"⭐ Или подарите ровно <b>{stars_need}⭐</b> (или немного больше) профилю "
        f"<b>@{STARS_GIFT_USERNAME}</b>\n"
        f"и укажите в комментарии номер заказа:\n"
        f"<code>{order_id}</code>\n\n"
        "После оплаты нажмите «Проверить Оплату».",
        reply_markup=rk_payment_actions(),
    )


async def open_premium_periods(message: Message):
    set_user_state(message.from_user.id, "premium_period")
    await message.answer(
        "💎 <b>Telegram Premium</b>\n\nВыберите период:", reply_markup=rk_premium_periods()
    )


async def build_premium_order(message: Message, months: int):
    amount_rub = await premium_price_rub(months)
    uid = message.from_user.id
    order_id = str(uuid.uuid4())
    receiver = YOOMONEY_RECEIVER or ""
    link = (
        quickpay_link(receiver, amount_rub, order_id, "AC")
        if receiver
        else "(YooMoney отключён администратором)"
    )
    await save_order(
        order_id,
        uid,
        "premium",
        str(months),
        months,
        0,
        price_rub=amount_rub,
        price_usd=0.0,
        price_stars=0,
        method="yoomoney",
        payment_ref=order_id,
    )
    set_user_state(
        uid, "await_payment", tmp=order_id, extra={"pay_kind": "premium", "months": months}
    )
    gift_stars = await stars_needed_for_rub(amount_rub)
    await message.answer(
        f"📦 <b>Telegram Premium — {months} мес.</b>\n\n"
        f"💳 Цена: <b>{amount_rub:.2f}₽</b>\n\n"
        f"🔗 Ссылка на оплату YooMoney:\n"
        f"<a href=\"{link}\">💳 Перейти к оплате</a>\n\n"
        f"⭐ Или подарите ровно <b>{gift_stars}⭐</b> (или немного больше) профилю "
        f"<b>@{STARS_GIFT_USERNAME}</b>\n"
        f"и укажите в комментарии номер заказа:\n"
        f"<code>{order_id}</code>\n\n"
        "Можно также оплатить с баланса.",
        reply_markup=rk_payment_actions(),
    )


async def start_stars_flow(message: Message):
    set_user_state(message.from_user.id, "stars_menu")
    max_stars = int(await get_setting("max_stars", str(DEFAULT_MAX_STARS)))
    min_stars = int(await get_setting("min_stars", str(DEFAULT_MIN_STARS)))
    await message.answer(
        "💰 <b>Купить Звёзды</b>\n\n"
        f"Минимум: <b>{min_stars}⭐</b>\n"
        f"Максимум: <b>{max_stars}⭐</b>",
        reply_markup=rk_stars_menu(),
    )


async def create_stars_order(message: Message, stars: int):
    uid = message.from_user.id
    max_stars = int(await get_setting("max_stars", str(DEFAULT_MAX_STARS)))
    min_stars = int(await get_setting("min_stars", str(DEFAULT_MIN_STARS)))
    if stars < min_stars or stars > max_stars:
        await message.answer(f"Допустимо от {min_stars} до {max_stars}.")
        return
    rate = await get_exchange_rate_value()
    usd = stars / STARS_PER_USD
    amount_rub = math.ceil(usd * rate * 100) / 100.0
    order_id = str(uuid.uuid4())
    receiver = YOOMONEY_RECEIVER or ""
    link = (
        quickpay_link(receiver, amount_rub, order_id, "AC")
        if receiver
        else "(YooMoney отключён администратором)"
    )
    await save_order(
        order_id,
        uid,
        "stars",
        "",
        0,
        stars,
        price_rub=amount_rub,
        price_usd=0.0,
        price_stars=stars,
        method="yoomoney_stars",
        payment_ref=order_id,
    )
    set_user_state(uid, "await_payment", tmp=order_id, extra={"pay_kind": "stars"})
    await message.answer(
        f"🧾 <b>Заказ</b> <code>{order_id}</code>\n\n"
        f"Покупка: <b>{stars}⭐</b>\n"
        f"💳 Стоимость: <b>{amount_rub:.2f}₽</b>\n\n"
        f"🔗 <b>Оплата только через YooMoney</b>:\n"
        f"<a href=\"{link}\">💳 Перейти к оплате</a>\n\n"
        "После оплаты нажмите кнопку «🔎 Проверить Оплату».",
        reply_markup=rk_payment_actions_yoomoney_only(),
    )


async def create_empty_order(message: Message):
    uid = message.from_user.id
    price = EMPTY_ACCOUNT_PRICE_RUB
    order_id = str(uuid.uuid4())
    receiver = YOOMONEY_RECEIVER or ""
    link = (
        quickpay_link(receiver, price, order_id, "AC")
        if receiver
        else "(YooMoney отключён администратором)"
    )
    await save_order(
        order_id,
        uid,
        "empty",
        "",
        0,
        1,
        price_rub=price,
        method="yoomoney_empty",
        payment_ref=order_id,
    )
    set_user_state(uid, "await_payment", tmp=order_id, extra={"pay_kind": "empty"})
    gift_stars = await stars_needed_for_rub(price)
    await message.answer(
        "🆕 <b>Пустой Телеграм-аккаунт</b>\n\n"
        "🌍 Регион: США\n"
        f"💳 Цена: <b>{price:.0f}₽</b>\n\n"
        f"🔗 Ссылка на оплату YooMoney:\n"
        f"<a href=\"{link}\">💳 Перейти к оплате</a>\n\n"
        f"⭐ Или подарите ровно <b>{gift_stars}⭐</b> (или немного больше) профилю "
        f"<b>@{STARS_GIFT_USERNAME}</b>\n"
        "и укажите в комментарии номер заказа:\n"
        f"<code>{order_id}</code>",
        reply_markup=rk_payment_actions(),
    )


async def create_proxy_order(message: Message, country: str):
    uid = message.from_user.id
    price = PROXY_PRICE_RUB
    order_id = str(uuid.uuid4())
    receiver = YOOMONEY_RECEIVER or ""
    link = (
        quickpay_link(receiver, price, order_id, "AC")
        if receiver
        else "(YooMoney отключён администратором)"
    )
    await save_order(
        order_id,
        uid,
        "proxy",
        country,
        0,
        1,
        price_rub=price,
        method="yoomoney_proxy",
        payment_ref=order_id,
    )
    set_user_state(
        uid, "await_payment", tmp=order_id, extra={"pay_kind": "proxy", "country": country}
    )
    gift_stars = await stars_needed_for_rub(price)
    await message.answer(
        "🛰 <b>Proxy/VPN</b>\n\n"
        f"Страна: <b>{country}</b>\n"
        f"Технология: <b>{PROXY_TECH_DESCRIPTION}</b>\n"
        f"Цена: <b>{price:.0f}₽</b>\n\n"
        f"🔗 Ссылка на оплату YooMoney:\n"
        f"<a href=\"{link}\">💳 Перейти к оплате</a>\n\n"
        f"⭐ Или подарите ровно <b>{gift_stars}⭐</b> (или немного больше) профилю "
        f"<b>@{STARS_GIFT_USERNAME}</b>\n"
        "и укажите в комментарии номер заказа:\n"
        f"<code>{order_id}</code>",
        reply_markup=rk_payment_actions(),
    )


@dp.message(F.text)
async def text_router(message: Message):
    uid = message.from_user.id
    ban_reason = await get_ban_reason(uid)
    if ban_reason is not None:
        await message.answer(
            "⛔ Вы не можете использовать бота, так как забанены навсегда.\n"
            f"Причина: {ban_reason or 'не указана'}"
        )
        return
    txt = (message.text or "").strip()

    if txt.startswith("/"):
        return

    if not await check_and_handle_subscription(message):
        return

    if await is_admin(uid):
        ast = get_admin_state(uid)["state"]
        if ast:
            if ast == "waiting_broadcast":
                ids = await list_all_user_ids()
                sent = 0
                for u in ids:
                    try:
                        await message.copy_to(u)
                        sent += 1
                    except Exception:
                        pass
                clear_admin_state(uid)
                await message.reply(
                    "✅ Рассылка завершена.\n\n"
                    f"Доставлено: <b>{sent}</b>"
                )
                return
            if ast == "waiting_set_max_stars":
                try:
                    val = int(txt)
                    await set_setting("max_stars", str(val))
                    await message.reply(f"✅ Макс. звёзд = <b>{val}</b>")
                except Exception:
                    await message.reply("Введите целое число.")
                clear_admin_state(uid)
                return
            if ast == "waiting_set_min_stars":
                try:
                    val = int(txt)
                    await set_setting("min_stars", str(val))
                    await message.reply(f"✅ Мин. звёзд = <b>{val}</b>")
                except Exception:
                    await message.reply("Введите целое число.")
                clear_admin_state(uid)
                return
            if ast == "waiting_set_rate":
                try:
                    val = float(txt.replace(",", "."))
                    await set_setting("exchange_rate", str(val))
                    await message.reply(f"✅ Коэффициент обновлён: <b>{val}</b>")
                except Exception:
                    await message.reply("Введите число, например 85.0")
                clear_admin_state(uid)
                return
            if ast == "waiting_add_admin":
                try:
                    parts = txt.split()
                    a_id = int(parts[0])
                    reason = " ".join(parts[1:]).strip()
                    await add_admin_db(a_id)
                    await message.reply(f"✅ Админ <b>{a_id}</b> добавлен.")
                    try:
                        t = "👑 Вам выданы права администратора."
                        if reason:
                            t += f"\nПричина: {reason}"
                        await bot.send_message(a_id, t)
                    except Exception:
                        pass
                except Exception:
                    await message.reply("Формат: user_id [причина]")
                clear_admin_state(uid)
                return
            if ast == "waiting_remove_admin":
                try:
                    parts = txt.split()
                    a_id = int(parts[0])
                    reason = " ".join(parts[1:]).strip()
                    await remove_admin_db(a_id)
                    await message.reply(f"✅ Админ <b>{a_id}</b> удалён.")
                    try:
                        t = "⚠️ С вас сняты права администратора."
                        if reason:
                            t += f"\nПричина: {reason}"
                        await bot.send_message(a_id, t)
                    except Exception:
                        pass
                except Exception:
                    await message.reply("Формат: user_id [причина]")
                clear_admin_state(uid)
                return
            if ast == "waiting_ban_user":
                try:
                    parts = txt.split()
                    u_id = int(parts[0])
                    reason = " ".join(parts[1:]).strip()
                    if not reason:
                        await message.reply("Укажите причину бана.")
                        return
                    await ban_user_db(u_id, reason)
                    await message.reply(
                        f"✅ Пользователь <b>{u_id}</b> заблокирован навсегда."
                    )
                    try:
                        await bot.send_message(
                            u_id,
                            f"⛔ Вы заблокированы навсегда.\nПричина: {reason}",
                        )
                    except Exception:
                        pass
                except Exception:
                    await message.reply("Формат: user_id причина")
                clear_admin_state(uid)
                return
            if ast == "waiting_unban_user":
                try:
                    parts = txt.split()
                    u_id = int(parts[0])
                    reason = " ".join(parts[1:]).strip()
                    await unban_user_db(u_id)
                    await message.reply(
                        f"✅ Пользователь <b>{u_id}</b> разблокирован."
                    )
                    try:
                        t = (
                            "✅ Вы разблокированы и снова можете пользоваться ботом."
                        )
                        if reason:
                            t += f"\nКомментарий: {reason}"
                        await bot.send_message(u_id, t)
                    except Exception:
                        pass
                except Exception:
                    await message.reply("Формат: user_id [причина]")
                clear_admin_state(uid)
                return
            if ast == "waiting_promo_add":
                try:
                    parts = txt.split()
                    code = parts[0]
                    amount = float(parts[1])
                    activ = int(parts[2])
                    await create_promocode(code, amount, activ)
                    await message.reply(
                        f"✅ Промокод <b>{code.upper()}</b> создан: <b>{amount:.0f}₽</b>, активаций: <b>{activ}</b>"
                    )
                except Exception:
                    await message.reply(
                        "Формат: КОД сумма активаций\n\nПример: SUPER 50 10"
                    )
                clear_admin_state(uid)
                return
            if ast == "waiting_promo_delete":
                code = txt.strip().upper()
                ok = await delete_promocode(code)
                await message.reply(
                    f"✅ Промокод <b>{code}</b> удалён." if ok else "Код не найден."
                )
                clear_admin_state(uid)
                return
            if ast == "waiting_check_payment":
                ref = txt
                o = await get_order(ref)
                if o and o[10]:
                    ref = o[10]
                status = await get_payment_status(ref)
                await message.reply(
                    f"Статус: <b>{'Оплачено' if status.get('paid') else 'Не оплачено'}</b>"
                )
                clear_admin_state(uid)
                return
            if ast == "waiting_mark_paid":
                oid = txt.strip()
                ok = await mark_order_paid(oid)
                if ok:
                    o = await get_order(oid)
                    amount_rub = float(o[6] or 0.0) if o else 0.0
                    await notify_admins_paid(
                        o[1] if o else 0, oid, amount_rub, "Manual"
                    )
                    await message.reply("✅ Отмечено как оплачено.")
                else:
                    await message.reply("Не найден такой заказ.")
                clear_admin_state(uid)
                return
            if ast == "waiting_discount_add":
                try:
                    parts = txt.split()
                    if len(parts) == 1:
                        title, percent = f"Скидка {parts[0]}%", float(parts[0])
                    else:
                        title, percent = " ".join(parts[:-1]), float(parts[-1])
                    await create_discount(title, percent)
                    await message.reply(
                        f"✅ Добавлено: <b>{title}</b> — <b>{percent:.0f}%</b>"
                    )
                except Exception:
                    await message.reply(
                        "Формат: <Название> <Процент>\nНапр.: Осень 10"
                    )
                clear_admin_state(uid)
                return
            if ast == "waiting_discount_remove":
                ok = await delete_discount(txt.strip())
                await message.reply("✅ Удалено." if ok else "Не найдено.")
                clear_admin_state(uid)
                return
            if ast == "waiting_discount_value":
                tmp_state = get_admin_state(uid).get("tmp") or ""
                try:
                    mode, item_key = tmp_state.split("|", 1)
                except Exception:
                    clear_admin_state(uid)
                    await message.reply(
                        "⚠️ Ошибка состояния. Откройте управление скидками заново."
                    )
                    return
                try:
                    val = float(txt.replace(",", "."))
                except Exception:
                    await message.reply("Введите число, например 10 или 99.9")
                    return
                name = DISCOUNT_ITEM_KEYS.get(item_key, item_key)
                if val <= 0:
                    await set_item_discount(item_key, "none", 0.0)
                    await message.reply(
                        f"🧹 Скидка для товара <b>{name}</b> отключена."
                    )
                else:
                    mode_norm = "percent" if mode == "percent" else "fixed"
                    await set_item_discount(item_key, mode_norm, val)
                    unit = "%" if mode_norm == "percent" else "₽"
                    await message.reply(
                        f"✅ Скидка для <b>{name}</b> установлена: <b>{val:.0f}{unit}</b>."
                    )
                clear_admin_state(uid)
                return
            if ast == "waiting_bal_money_add":
                try:
                    parts = txt.split()
                    u = int(parts[0])
                    a = float(parts[1].replace(",", "."))
                    reason = " ".join(parts[2:]).strip()
                    await admin_add_money(u, a, reason)
                    await message.reply("✅ Готово.")
                except Exception:
                    await message.reply("Формат: user_id сумма [причина]")
                clear_admin_state(uid)
                return
            if ast == "waiting_bal_money_sub":
                try:
                    parts = txt.split()
                    u = int(parts[0])
                    a = float(parts[1].replace(",", "."))
                    reason = " ".join(parts[2:]).strip()
                    deducted = await admin_sub_money(u, a, reason)
                    await message.reply(f"✅ Списано: {deducted:.2f}₽.")
                except Exception:
                    await message.reply("Формат: user_id сумма [причина]")
                clear_admin_state(uid)
                return
            if ast == "waiting_bal_money_zero":
                try:
                    parts = txt.split()
                    u = int(parts[0])
                    reason = " ".join(parts[1:]).strip()
                    prev = await admin_zero_money(u, reason)
                    await message.reply(
                        f"✅ Баланс пользователя {u} обнулён (было {prev:.2f}₽)."
                    )
                except Exception:
                    await message.reply("Формат: user_id [причина]")
                clear_admin_state(uid)
                return

    st = get_user_state(uid)["state"]

    if txt == BTN_PRIVACY:
        await message.answer(PRIVACY_URL)
        return
    if txt == BTN_BACK:
        await open_main(message)
        return
    if txt == BTN_TELEGRAM:
        await open_telegram(message)
        return
    if txt == BTN_PROXY:
        await open_proxy(message)
        return
    if txt == BTN_FREE:
        await open_free(message)
        return
    if txt == BTN_PROFILE:
        await open_profile(message)
        return
    if txt == BTN_TOPUP:
        await open_topup_menu(message)
        return
    if txt == BTN_REF_SYSTEM:
        await open_referrals(message)
        return
    if txt == BTN_PROMO:
        set_user_state(uid, "promo_wait")
        await message.answer("🎫 Отправьте промокод одним сообщением.")
        return
    if txt == BTN_SUPPORT:
        await message.answer(f"🆘 Поддержка: {SUPPORT_URL}")
        return

    if st == "cat_telegram":
        if txt == BTN_PREMIUM:
            await open_premium_periods(message)
            return
        if txt == BTN_STARS:
            await start_stars_flow(message)
            return
        if txt == BTN_EMPTY:
            await create_empty_order(message)
            return

    if st == "premium_period":
        if txt.startswith("🗓"):
            await build_premium_order(message, 1)
            return
        if txt.startswith("📆"):
            await build_premium_order(message, 12)
            return

    if st == "stars_menu":
        if txt == "⭐ 300":
            await create_stars_order(message, 300)
            return
        if txt == "⭐ 600":
            await create_stars_order(message, 600)
            return
        if txt == "⭐ 1200":
            await create_stars_order(message, 1200)
            return
        if txt == "✏️ Свой Объём":
            set_user_state(uid, "waiting_stars_amount")
            await message.answer("Введите количество ⭐, число.")
            return

    if st == "waiting_stars_amount":
        try:
            stars = int(txt)
            await create_stars_order(message, stars)
            return
        except Exception:
            await message.reply("Введите целое число, например 2500")
            return

    if st == "promo_wait":
        code = (txt or "").strip()
        if not code:
            await message.reply("Отправьте текстом сам код.")
            return
        try:
            amount = await use_promocode(code, uid)
        except Exception as e:
            logger.exception("use_promocode failed: %s", e)
            await message.answer(
                "⚠️ Произошла ошибка при обработке промокода. "
                "Попробуйте позже или обратитесь в поддержку.",
                reply_markup=rk_main(),
            )
            clear_user_state(uid)
            return
        if amount:
            await message.answer(
                f"✅ Промокод активирован. Начислено <b>{amount:.0f}₽</b> на баланс.",
                reply_markup=rk_profile(),
            )
        else:
            await message.answer(
                "Промокод недействителен или уже использован.",
                reply_markup=rk_profile(),
            )
        clear_user_state(uid)
        return

    if st == "topup":
        if txt == "💵 100₽":
            await start_topup(message, 100.0)
            return
        if txt == "💵 300₽":
            await start_topup(message, 300.0)
            return
        if txt == "💵 500₽":
            await start_topup(message, 500.0)
            return
        if txt == "✏️ Своя Сумма":
            set_user_state(uid, "waiting_topup_amount")
            await message.answer(
                f"Введите сумму в ₽ (минимум {MIN_TOPUP_RUB:.0f})."
            )
            return

    if st == "waiting_topup_amount":
        try:
            amount = float(txt.replace(",", "."))
            if amount < MIN_TOPUP_RUB:
                await message.reply(f"Минимальная сумма: {MIN_TOPUP_RUB:.0f}₽.")
                return
            await start_topup(message, amount)
            return
        except Exception:
            await message.reply("Введите число, например 150")
            return

    if st == "cat_proxy":
        if txt.startswith("🌐 "):
            try:
                left = txt.replace("🌐", "").strip()
                country = left.split(" - ")[0].strip()
            except Exception:
                country = left.strip()
            if country not in PROXY_COUNTRIES:
                await message.reply("Выберите страну из списка.")
                return
            await create_proxy_order(message, country)
            return

    if st == "await_payment":
        st_data = get_user_state(uid)
        last_order = st_data.get("tmp")
        pay_kind = st_data.get("pay_kind")
        if not last_order:
            await message.reply("Нет активного заказа.", reply_markup=rk_main())
            clear_user_state(uid)
            return
        if txt == BTN_PAY_YM:
            o = await get_order(last_order)
            if not o:
                await message.reply("Заказ не найден.", reply_markup=rk_main())
                clear_user_state(uid)
                return
            amount_rub = float(o[6] or 0.0)
            receiver = YOOMONEY_RECEIVER or ""
            link = (
                quickpay_link(receiver, amount_rub, o[0], "AC")
                if receiver
                else "(YooMoney отключён администратором)"
            )
            kb = (
                rk_payment_actions_yoomoney_only()
                if pay_kind == "stars"
                else rk_payment_actions()
            )
            await message.answer(
                "🔗 Ссылка на оплату YooMoney:\n"
                f"<a href=\"{link}\">💳 Перейти к оплате</a>",
                reply_markup=kb,
            )
            return
        if txt == BTN_PAY_STARS:
            if pay_kind == "stars":
                await message.reply(
                    "❗ Для покупки звёзд оплата доступна только через YooMoney.\n\n"
                    "Пожалуйста, используйте кнопку «🔗 Оплатить YooMoney».",
                    reply_markup=rk_payment_actions_yoomoney_only(),
                )
                return
            o = await get_order(last_order)
            if not o:
                await message.reply("Заказ не найден.", reply_markup=rk_main())
                clear_user_state(uid)
                return
            amount_rub = float(o[6] or 0.0)
            need = await stars_needed_for_rub(amount_rub)
            await message.answer(
                "⭐ <b>Оплата Звёздами</b>\n\n"
                f"Подарите ровно <b>{need}⭐</b> (или немного больше) профилю "
                f"<b>@{STARS_GIFT_USERNAME}</b>\n"
                "и укажите в комментарии номер заказа:\n"
                f"<code>{o[0]}</code>\n\n"
                "После отправки нажмите «Проверить Оплату».",
                reply_markup=rk_payment_actions(),
            )
            return
        if txt == BTN_PAY_BAL:
            if pay_kind == "stars":
                await message.reply(
                    "❗ Для покупки звёзд нельзя платить с баланса.\n\n"
                    "Используйте кнопку «🔗 Оплатить YooMoney».",
                    reply_markup=rk_payment_actions_yoomoney_only(),
                )
                return
            o = await get_order(last_order)
            if not o:
                await message.reply("Заказ не найден.", reply_markup=rk_main())
                clear_user_state(uid)
                return
            if bool(o[11]):
                await message.reply("Уже оплачено.", reply_markup=rk_main())
                clear_user_state(uid)
                return
            amount_rub = float(o[6] or 0.0)
            ok_bal = await try_pay_with_balance(uid, amount_rub)
            if not ok_bal:
                await message.reply(
                    "Недостаточно средств на балансе.", reply_markup=rk_payment_actions()
                )
                return
            await mark_order_paid(last_order)
            await message.answer(
                "✅ Оплата с баланса прошла.\n\nНапишите в поддержку для получения услуги.",
                reply_markup=rk_main(),
            )
            await notify_admins_paid(uid, last_order, amount_rub, "Balance")
            clear_user_state(uid)
            return
        if txt == BTN_CHECK:
            o = await get_order(last_order)
            if not o:
                await message.reply("Заказ не найден.", reply_markup=rk_main())
                clear_user_state(uid)
                return
            if bool(o[11]):
                await message.reply("Уже оплачено.", reply_markup=rk_main())
                clear_user_state(uid)
                return
            status = await get_payment_status(o[10] or last_order)
            if status.get("paid"):
                await mark_order_paid(last_order)
                if o[2] == "topup":
                    await add_real_balance(uid, float(o[6] or 0.0))
                await message.answer(
                    "✅ Оплата получена.\n\nНапишите в поддержку для получения услуги.",
                    reply_markup=rk_main(),
                )
                await notify_admins_paid(
                    uid, last_order, float(o[6] or 0.0), "YooMoney"
                )
                clear_user_state(uid)
                return
            else:
                kb = (
                    rk_payment_actions_yoomoney_only()
                    if pay_kind == "stars"
                    else rk_payment_actions()
                )
                await message.reply(
                    "Платёж не найден.\n\nПопробуйте позже или свяжитесь с поддержкой.",
                    reply_markup=kb,
                )
                return

    if txt == BTN_PRIVACY:
        await message.answer(PRIVACY_URL)
        return

    await open_main(message)


@dp.pre_checkout_query()
async def pre_checkout_handler(query: PreCheckoutQuery):
    await query.answer(ok=True)


@dp.message(F.content_type == ContentType.SUCCESSFUL_PAYMENT)
async def successful_payment_handler(message: Message):
    sp = message.successful_payment
    if not sp:
        return
    order_id = (sp.invoice_payload or "").strip()
    if not order_id:
        return
    await mark_order_paid(order_id)
    o = await get_order(order_id)
    if o and o[2] == "topup":
        await add_real_balance(message.from_user.id, float(o[6] or 0.0))
    try:
        await bot.send_message(
            message.chat.id,
            "✅ Оплата прошла.\n\nНапишите в поддержку для получения услуги.",
        )
    except Exception:
        pass
    amount_rub = float(o[6] or 0.0) if o else 0.0
    await notify_admins_paid(
        message.from_user.id, order_id, amount_rub, "Stars(Invoice)"
    )


async def main():
    await init_db()
    await init_http()
    logger.info("Bot started")
    await dp.start_polling(
        bot, allowed_updates=["message", "callback_query", "chat_member"]
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
