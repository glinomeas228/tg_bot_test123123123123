import asyncio
import aiohttp
import time
import logging
import random
import aiosqlite
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command, StateFilter
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

BOT_TOKEN = "8549244898:AAFimpDBpJUSSQhuGq5ZNfmqk1N0ij2tqSU"
DB_NAME = "bot.db"

PROXY_URLS = [
    "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt",
    "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5_RAW.txt",
    "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt",
    "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks5.txt",
    "https://raw.githubusercontent.com/UptimerBot/proxy-list/main/proxies/socks5.txt",
    "https://raw.githubusercontent.com/prxchk/proxy-list/main/socks5.txt",
    "https://raw.githubusercontent.com/manuGMG/proxy-365/main/SOCKS5.txt",
    "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks5.txt"
]

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
sem = asyncio.Semaphore(300)


class BotStates(StatesGroup):
    waiting_for_new_target = State()
    waiting_for_proxy_check = State()


async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('''CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, 
            target_host TEXT
        )''')
        await db.commit()


async def get_user_target(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute('SELECT target_host FROM users WHERE user_id = ?', (user_id,)) as cursor:
            row = await cursor.fetchone()
            if row:
                return row[0]
            else:
                await db.execute('INSERT INTO users (user_id, target_host) VALUES (?, ?)', (user_id, "funtime.su"))
                await db.commit()
                return "funtime.su"


async def set_user_target(user_id, new_target):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('INSERT OR REPLACE INTO users (user_id, target_host) VALUES (?, ?)', (user_id, new_target))
        await db.commit()


async def fetch_proxies(session):
    headers = {"User-Agent": "Mozilla/5.0"}
    all_proxies = set()
    tasks = []

    async def fetch_url(url):
        try:
            async with session.get(url, headers=headers, ssl=False, timeout=4) as response:
                if response.status == 200:
                    text = await response.text()
                    for line in text.splitlines():
                        clean_line = line.strip()
                        if clean_line and ":" in clean_line and clean_line[0].isdigit():
                            all_proxies.add(clean_line)
        except:
            pass

    for url in PROXY_URLS:
        tasks.append(fetch_url(url))

    await asyncio.gather(*tasks)
    return list(all_proxies)


async def check_proxy(proxy):
    async with sem:
        try:
            ip, port = proxy.split(":")
            start_time = time.monotonic()

            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(ip, int(port)),
                timeout=0.6
            )

            ping = (time.monotonic() - start_time) * 1000
            writer.close()
            await writer.wait_closed()

            return (proxy, int(ping))
        except:
            return None


@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    user_target = await get_user_target(message.from_user.id)

    text = (
        "<b>🎮 Minecraft Proxy Master</b>\n\n"
        f"🎯 Твоя цель: <code>{user_target}</code>\n"
        "⚡ Режим: <b>SOCKS5</b>\n\n"
        "Выбери действие в меню:"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔎 Найти лучшие прокси", callback_data="find_proxies")],
        [InlineKeyboardButton(text="🎯 Сменить сервер (Target)", callback_data="change_target")],
        [InlineKeyboardButton(text="📡 Пингануть свой прокси", callback_data="check_single")]
    ])

    await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=kb)


@dp.callback_query(F.data == "change_target")
async def ask_target(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer(
        "✍️ <b>Введите новый IP или домен сервера.</b>\n"
        "Пример: <code>mc.hypixel.net</code> или <code>play.kaboom.pro</code>",
        parse_mode=ParseMode.HTML
    )
    await state.set_state(BotStates.waiting_for_new_target)
    await callback.answer()


@dp.message(StateFilter(BotStates.waiting_for_new_target))
async def set_target(message: types.Message, state: FSMContext):
    new_target = message.text.strip()
    await set_user_target(message.from_user.id, new_target)

    await message.answer(f"✅ Цель успешно изменена на: <code>{new_target}</code>", parse_mode=ParseMode.HTML)
    await cmd_start(message, state)


@dp.callback_query(F.data == "check_single")
async def ask_proxy(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer(
        "🕵️‍♂️ <b>Пришли мне прокси для проверки.</b>\n"
        "Формат: <code>ip:port</code>\n"
        "Пример: <code>192.168.0.1:1080</code>",
        parse_mode=ParseMode.HTML
    )
    await state.set_state(BotStates.waiting_for_proxy_check)
    await callback.answer()


@dp.message(StateFilter(BotStates.waiting_for_proxy_check))
async def check_single_proxy_handler(message: types.Message, state: FSMContext):
    proxy_input = message.text.strip()
    user_target = await get_user_target(message.from_user.id)

    if ":" not in proxy_input:
        await message.answer("❌ Неверный формат. Нужно <code>ip:port</code>", parse_mode=ParseMode.HTML)
        return

    msg = await message.answer("⏳ <i>Проверяю коннект...</i>", parse_mode=ParseMode.HTML)

    try:
        ip, port = proxy_input.split(":")
        start = time.monotonic()
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(ip, int(port)), timeout=2.0
        )
        ping = (time.monotonic() - start) * 1000
        writer.close()
        await writer.wait_closed()

        status = "🟢 ОТЛИЧНО" if ping < 150 else "🟡 НОРМ" if ping < 400 else "🔴 ПЛОХО"

        res_text = (
            f"📊 <b>Результат проверки:</b>\n\n"
            f"🌐 Прокси: <code>{proxy_input}</code>\n"
            f"🎯 Цель: <code>{user_target}</code>\n"
            f"📶 Статус: {status}\n"
            f"⚡ Пинг: <b>{ping:.0f} ms</b>"
        )
        await msg.edit_text(res_text, parse_mode=ParseMode.HTML)

    except Exception as e:
        await msg.edit_text(f"❌ <b>Прокси мертв</b> или недоступен.\nОшибка: {repr(e)}", parse_mode=ParseMode.HTML)

    await state.clear()

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 В меню", callback_data="back_menu")]])
    await message.answer("Что делаем дальше?", reply_markup=kb)


@dp.callback_query(F.data == "back_menu")
async def back_to_menu(callback: types.CallbackQuery, state: FSMContext):
    await cmd_start(callback.message, state)


@dp.callback_query(F.data == "find_proxies")
async def process_find(callback: types.CallbackQuery):
    user_target = await get_user_target(callback.from_user.id)

    start_msg = await callback.message.answer(
        f"📡 <i>Загружаю базы... Цель: {user_target}</i>",
        parse_mode=ParseMode.HTML
    )

    async with aiohttp.ClientSession() as session:
        proxies = await fetch_proxies(session)

    if not proxies:
        await start_msg.edit_text("❌ Ошибка загрузки баз.")
        return

    random.shuffle(proxies)
    proxies_to_check = proxies[:3000]

    tasks = [check_proxy(p) for p in proxies_to_check]
    results = await asyncio.gather(*tasks)

    valid_proxies = [res for res in results if res is not None]
    valid_proxies.sort(key=lambda x: x[1])
    top_10 = valid_proxies[:10]

    if not top_10:
        await start_msg.edit_text("😔 Живых прокси с хорошим пингом не найдено.")
        return

    response_text = "🏆 <b>ТОП-10 SOCKS5 (Minecraft Ready)</b>\n"
    response_text += f"🎯 Сервер: <code>{user_target}</code>\n\n"

    for p, ping in top_10:
        if ping < 100:
            status = "🟢 <b>RU/EU</b>"
        elif ping < 250:
            status = "🟡"
        else:
            status = "🟠"

        response_text += f"{status} <code>{p}</code> — <b>{ping:.0f} ms</b>\n"

    response_text += "\n<i>Кликни на IP для копирования</i> 📋"

    await start_msg.delete()
    await callback.message.answer(response_text, parse_mode=ParseMode.HTML)
    await callback.answer()


async def main():
    await init_db()
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass