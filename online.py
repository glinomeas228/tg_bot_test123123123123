from telethon import TelegramClient, functions, events
import asyncio

api_id = 25971635
api_hash = '6d89e8b5c8b02af0d73a77863084c898'
my_id = 7418079991
session_name = 'status_keeper_v2'

client = TelegramClient(session_name, api_id, api_hash)

@client.on(events.UserUpdate)
async def handler(event):
    if event.user_id == my_id and not event.online:
        try:
            await client(functions.account.UpdateStatusRequest(offline=False))
        except Exception:
            pass

async def keep_online():
    while True:
        try:
            await client(functions.account.UpdateStatusRequest(offline=False))
            await asyncio.sleep(290)
        except Exception:
            await asyncio.sleep(30)

async def main():
    await client.start()
    client.loop.create_task(keep_online())
    await client.run_until_disconnected()

if __name__ == '__main__':
    client.loop.run_until_complete(main())
