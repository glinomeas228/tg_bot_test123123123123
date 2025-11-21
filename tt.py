import asyncio
import random
from telethon import TelegramClient, events
from telethon.tl.types import User
from telethon.tl.functions.account import UpdateStatusRequest

API_ID = 29367037
API_HASH = "f3eb7307d861c780a42fd3639e2c0992"
SESSION_NAME = 'user_session'

MSG_1 = "Привет, да, это я с тиктока. Подарить тебе колечко?"
MSG_2 = """Хорошо! Давай только ты сделаешь всего одно задание на пару минут, а я тебе уже готова выслать твоё колечко за 100⭐.

Тебе надо написать в комментарии под 10 любыми видео вот такой текст и лайкнуть свой комментарий:

`кому колечко, дарит @ulya080 💖`

Потом скинь мне скрины, потом скажешь, куда отправить колечко!❤️
"""
MSG_3 = "Давай, жду от тебя 10 скриншотов, потом получай подарок, солнышко!💘"
MSG_4 = "Ты молодец! Подарок можно забрать вот в этом боте @glino_premka_bot. Там всё написано 💝! Главное меню - бесплатная премка (не волнуйся, подарок там тоже можно получить! 😘)"

CHAT_STATES = {}

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

async def keep_online_status():
    print("--- [INFO] Статус 'В сети' активирован ---")
    while True:
        try:
            await client(UpdateStatusRequest(offline=False))
            await asyncio.sleep(60)
        except Exception:
            await asyncio.sleep(10)

async def simulate_typing_and_send(chat_id, message_text):
    await client.send_read_acknowledge(chat_id)
    await asyncio.sleep(random.uniform(2, 3))
    async with client.action(chat_id, 'typing'):
        await asyncio.sleep(random.uniform(3, 6))
    await client.send_message(chat_id, message_text, parse_mode='md')
    print(f"[SUCCESS] Ответ отправлен пользователю {chat_id}")

@client.on(events.NewMessage)
async def message_handler(event):
    if event.out or not event.is_private:
        return

    sender = await event.get_sender()
    if not sender or sender.bot:
        return

    user_id = sender.id
    current_state = CHAT_STATES.get(user_id, 0)

    if current_state == 0:
        CHAT_STATES[user_id] = 1
        await simulate_typing_and_send(user_id, MSG_1)

    elif current_state == 1:
        CHAT_STATES[user_id] = 2
        await simulate_typing_and_send(user_id, MSG_2)

    elif current_state == 2:
        photos_count = 0
        if event.grouped_id:
            photos_count = 10
        elif event.photo:
            photos_count = 1

        if photos_count >= 10:
            CHAT_STATES[user_id] = 3
            print(f"[LOG] Пользователь {user_id} прислал фото. Финализируем.")
            await simulate_typing_and_send(user_id, MSG_4)
        else:
            await simulate_typing_and_send(user_id, MSG_3)

    elif current_state == 3:
        pass

async def main():
    await client.start()
    print("DAN: Юзербот обновлен. Добавлены галочки и фикс дублей.")
    client.loop.create_task(keep_online_status())
    await client.run_until_disconnected()

if __name__ == '__main__':
    try:
        client.loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("\nСтоп.")