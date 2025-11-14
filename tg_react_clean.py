import asyncio
import random
import time
import re
import json
import requests
import aiohttp
import sys
import os
import traceback
from collections import deque
from typing import List, Dict, Any, Optional
from telethon import TelegramClient, functions, types, events, errors
import logging
api_id = 29719355
api_hash = '2edbd4950ff8679a8548882bd9ec33db'
phone = '+79529040067'
session_name = 'session_combined5'
BOT_TOKEN = '8458732532:AAH2PQ66-6D5THOttp1RGotpbzcChFREmhM'
BOT_API_BASE = f'https://api.telegram.org/bot{BOT_TOKEN}'
DEBUG_MODE = False
TARGET_RATE = 3
CONCURRENCY = 3
MIN_DELAY = 2.0
MAX_DELAY = 4.0
PER_CHAT_MIN_INTERVAL = 5
REACT_PROBABILITY = 0.33
MAX_RETRIES = 3
BACKOFF_BASE = 2
SWEEP_ENABLED = False
SWEEP_INTERVAL = 3600
SWEEP_COUNT = 1
MAX_GLOBAL_COOLDOWN = 12 * 5
COOLDOWN_CHECK_INTERVAL = 1.0
RESET_COOLDOWN_FILENAME = 'reset_cooldown'
PROXY = None
EMOJI_MAP = {'1': '👍', 'like': '👍', 'лайк': '👍', '2': '👎', 'dislike': '👎', 'дизлайк': '👎', '3': '🤡', 'clown': '🤡', 'клоун': '🤡', '4': '❤️', 'heart': '❤️', 'love': '❤️', 'сердце': '❤️', 'люблю': '❤️', '5': '💘', 'sparkling_heart': '💘', 'сердечко': '💘', '6': '😁', 'smile': '😁', 'улыбка': '😁'}
logfile = 'combined_log.txt'
logging.basicConfig(level=logging.DEBUG if DEBUG_MODE else logging.INFO)
logger = logging.getLogger(__name__)
client: Optional[TelegramClient] = None
me = None
react_semaphore = asyncio.Semaphore(CONCURRENCY)
monitored_input_peers: Dict[int, Any] = {}
monitored_entity_map: Dict[int, Any] = {}
last_reacted_at: Dict[int, float] = {}
global_cooldown_until = 0.0
bot_http_session: Optional[aiohttp.ClientSession] = None
bot_poll_offset = 0
subscribers: Dict[int, Dict[str, Any]] = {}
stats_total_reactions = 0
stats_per_chat: Dict[int, int] = {}
recent_reactions: List[Dict[str, Any]] = []
RECENT_MAX = 500
reaction_queue: 'asyncio.Queue[Dict[str, Any]]' = asyncio.Queue()
cooldown_active_flag = False
config_lock = asyncio.Lock()
ignored_users: set = set()
ignored_phrases: List[str] = ['🩻 вы не подписаны на на основной канал @love_thxs', '🫀подпишитесь, чтобы публиковать идеи в канал!', 'соблюдайте пожалуйста правила, хотим чтобы всем было комфортно', '/top@love_thxs_ideas_bot']
recent_message_texts: Dict[str, float] = {}
DUPLICATE_WINDOW = 5 * 60
ALWAYS_ONLINE = True

class AdaptiveRateLimiter:

    def __init__(self, target_rate: int, period: float=1.0):
        self.target_rate = max(1, int(target_rate))
        self.current_rate = max(1, int(target_rate))
        self.period = period
        self.timestamps = deque()
        self.lock = asyncio.Lock()
        self._recovery_task = None
        self._recovery_delay = 5.0

    async def acquire(self):
        while True:
            async with self.lock:
                now = time.time()
                while self.timestamps and now - self.timestamps[0] >= self.period:
                    self.timestamps.popleft()
                if len(self.timestamps) < self.current_rate:
                    self.timestamps.append(now)
                    return
                oldest = self.timestamps[0]
                sleep_for = self.period - (now - oldest)
            await asyncio.sleep(max(0.001, sleep_for))

    async def shrink_rate(self, factor: float=0.5, min_rate: int=1):
        async with self.lock:
            old = self.current_rate
            new = max(min_rate, int(max(1, self.current_rate * factor)))
            self.current_rate = new
            log(f'[LIMITER] shrink rate {old} -> {new}')
        if self._recovery_task is None or self._recovery_task.done():
            self._recovery_task = asyncio.create_task(self._recover_loop())

    async def _recover_loop(self):
        try:
            while True:
                await asyncio.sleep(self._recovery_delay)
                async with self.lock:
                    if self.current_rate >= self.target_rate:
                        break
                    new_rate = min(self.target_rate, int(self.current_rate * 1.25) + 1)
                    log(f'[LIMITER] recovering rate {self.current_rate} -> {new_rate}')
                    self.current_rate = new_rate
        except asyncio.CancelledError:
            pass
limiter = AdaptiveRateLimiter(target_rate=TARGET_RATE, period=1.0)

def now_str():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

def log(msg: str):
    t = now_str()
    line = f'[{t}] {msg}'
    print(line, flush=True)
    try:
        with open(logfile, 'a', encoding='utf-8') as f:
            f.write(line + '\n')
    except Exception:
        pass

async def try_resolve_token(client_local: TelegramClient, token: str):
    if re.fullmatch('-?\\d+', token):
        try:
            nid = int(token)
        except Exception as e:
            log(f"[RESOLVE] numeric parse failed for '{token}': {repr(e)}")
            nid = None
        if nid is not None:
            attempts = [nid]
            if nid < 0:
                attempts.append(abs(nid))
            for v in attempts:
                try:
                    ent = await client_local.get_entity(v)
                    return ent
                except Exception as e:
                    log(f'[RESOLVE] get_entity({v}) failed: {repr(e)}')
            log(f"[RESOLVE] Cannot resolve numeric ID '{token}'.")
            return None
    else:
        try:
            ent = await client_local.get_entity(token)
            return ent
        except Exception as e:
            log(f"[RESOLVE] get_entity('{token}') failed: {repr(e)}")
            return None

def build_message_link(entity, msg_id: int) -> str:
    try:
        uname = getattr(entity, 'username', None)
        if uname:
            return f'https://t.me/{uname}/{msg_id}'
        eid = getattr(entity, 'id', None)
        if eid is not None:
            return f'https://t.me/c/{eid}/{msg_id}'
    except Exception:
        pass
    return f"msg://{getattr(entity, 'id', 'unknown')}/{msg_id}"

async def register_successful_reaction(entity, chat_key: int, msg_id: int, emoji: str):
    global stats_total_reactions, stats_per_chat, recent_reactions, last_reacted_at
    stats_total_reactions += 1
    stats_per_chat[chat_key] = stats_per_chat.get(chat_key, 0) + 1
    last_reacted_at[chat_key] = time.time()
    entry = {'time': int(time.time()), 'chat_id': chat_key, 'msg_id': msg_id, 'emoji': emoji, 'link': build_message_link(entity, msg_id)}
    recent_reactions.insert(0, entry)
    if len(recent_reactions) > RECENT_MAX:
        recent_reactions.pop()
    text = f"✅ Reaction {emoji} set for message:\n{entry['link']}\nchat_id={chat_key} msg_id={msg_id}"
    for uid, prefs in list(subscribers.items()):
        try:
            if prefs.get('notify', True):
                asyncio.create_task(bot_send_message(uid, text))
        except Exception:
            pass

async def purge_reaction_queue():
    removed = 0
    try:
        while True:
            task = reaction_queue.get_nowait()
            try:
                reaction_queue.task_done()
            except Exception:
                pass
            removed += 1
    except asyncio.QueueEmpty:
        pass
    log(f'[PURGE] cleared {removed} queued reaction tasks')

async def send_reaction_request(inp_peer, msg_id: int, emoji: str):
    await client(functions.messages.SendReactionRequest(peer=inp_peer, msg_id=msg_id, reaction=[types.ReactionEmoji(emoticon=emoji)], add_to_recent=True))

async def reaction_worker(worker_id: int, emoji: str):
    global global_cooldown_until, limiter, react_semaphore, cooldown_active_flag
    log(f'[WORKER-{worker_id}] started')
    while True:
        try:
            task = await reaction_queue.get()
            if not task:
                reaction_queue.task_done()
                continue
            chat_key = task['chat_key']
            inp_peer = task['inp_peer']
            entity = task['entity']
            msg_id = task['msg_id']
            now = time.time()
            if now < global_cooldown_until:
                log(f'[WORKER-{worker_id}] COOLDOWN ACTIVE - dropping old queued msg {msg_id} (chat {chat_key})')
                try:
                    reaction_queue.task_done()
                except Exception:
                    pass
                continue
            await limiter.acquire()
            async with react_semaphore:
                attempt = 0
                while attempt <= MAX_RETRIES:
                    try:
                        await send_reaction_request(inp_peer, msg_id, emoji)
                        log(f'[WORKER-{worker_id}] SENT {emoji} -> msg {msg_id} chat {chat_key} (attempt {attempt + 1})')
                        try:
                            asyncio.create_task(register_successful_reaction(entity, chat_key, msg_id, emoji))
                        except Exception:
                            pass
                        break
                    except errors.FloodWaitError as e:
                        wait_seconds = int(getattr(e, 'seconds', 60))
                        capped = min(wait_seconds, MAX_GLOBAL_COOLDOWN)
                        global_cooldown_until = time.time() + capped
                        cooldown_active_flag = True
                        log(f'[WORKER-{worker_id}] FLOODWAIT {wait_seconds}s -> applying global cooldown {capped}s until {int(global_cooldown_until)}')
                        try:
                            await purge_reaction_queue()
                        except Exception as ex:
                            log(f'[WORKER-{worker_id}] purge error: {ex}')
                        try:
                            await limiter.shrink_rate(factor=0.5, min_rate=1)
                        except Exception:
                            pass
                        await asyncio.sleep(capped + 0.5)
                        attempt += 1
                        break
                    except Exception as e:
                        attempt += 1
                        log(f'[WORKER-{worker_id}] WARN send_reaction failed for msg {msg_id} (attempt {attempt}) err={type(e).__name__}: {e}')
                        if attempt > MAX_RETRIES:
                            log(f'[WORKER-{worker_id}] giving up on msg {msg_id} after {attempt} attempts')
                            break
                        backoff = BACKOFF_BASE ** attempt
                        to_sleep = backoff + random.uniform(0, 0.3 * backoff)
                        log(f'[WORKER-{worker_id}] backing off {to_sleep:.2f}s before retry')
                        await asyncio.sleep(to_sleep)
            try:
                reaction_queue.task_done()
            except Exception:
                pass
        except Exception as e:
            log(f'[WORKER-{worker_id}] fatal err: {type(e).__name__}: {e}')
            await asyncio.sleep(1)

def normalize_chat_token(tok: str) -> str:
    tok = tok.strip()
    if not tok:
        return ''
    tok = re.sub('^https?://(www\\.)?t\\.me/', '', tok, flags=re.IGNORECASE)
    tok = tok.strip()
    if tok.startswith('@'):
        tok = tok[1:]
    return tok

def normalize_text_for_dup(txt: str) -> str:
    if txt is None:
        return ''
    s = txt.lower().strip()
    s = re.sub('\\s+', ' ', s)
    return s

async def safe_schedule_reaction_now(chat_key: int, inp_peer, entity, msg_id: int, emoji: str):
    global global_cooldown_until
    now = time.time()
    if now < global_cooldown_until:
        log(f'[SCHEDULE] global cooldown active, skipping enqueue for msg {msg_id} in chat {chat_key}')
        return
    p = random.random()
    if p > REACT_PROBABILITY:
        log(f'[SCHEDULE] skip by probability ({p:.3f} > {REACT_PROBABILITY}) for msg {msg_id} chat_key {chat_key}')
        return
    last = last_reacted_at.get(chat_key, 0.0)
    if now - last < PER_CHAT_MIN_INTERVAL:
        log(f'[SCHEDULE] skip due per-chat cooldown for chat_key {chat_key}: {now - last:.2f}s since last reaction')
        return
    task = {'chat_key': chat_key, 'inp_peer': inp_peer, 'entity': entity, 'msg_id': msg_id}
    await reaction_queue.put(task)
    log(f'[SCHEDULE] enqueued msg {msg_id} for chat_key {chat_key}')

async def message_has_reaction(entity, msg_id: int, emoji: str) -> bool:
    try:
        msg = await client.get_messages(entity, ids=msg_id)
        if not msg:
            return False
        r = getattr(msg, 'reactions', None)
        if r:
            try:
                results = getattr(r, 'results', None) or getattr(r, 'counts', None)
                if results:
                    for rc in results:
                        emot = getattr(rc, 'reaction', None)
                        emot_text = getattr(emot, 'emoticon', None) if emot else None
                        if emot_text == emoji:
                            return True
            except Exception:
                pass
        try:
            d = msg.to_dict()
            if emoji in str(d):
                return True
        except Exception:
            pass
        return False
    except Exception as e:
        log(f'[ERR] while checking reactions for msg {msg_id}: {type(e).__name__}: {e}')
        return False

async def handler_factory(emoji: str):

    async def handler(event: events.NewMessage.Event):
        try:
            if event.message is None:
                return

            def extract_chat_id(ev):
                cid = getattr(ev, 'chat_id', None)
                if cid is not None:
                    return cid
                msg = getattr(ev, 'message', None)
                if not msg:
                    return None
                pid = getattr(msg, 'peer_id', None) or getattr(msg, 'to_id', None)
                if not pid:
                    return None
                for attr in ('channel_id', 'chat_id', 'user_id'):
                    v = getattr(pid, attr, None)
                    if v is not None:
                        return v
                return None
            chat_id = extract_chat_id(event)
            txt = ''
            try:
                txt = (event.message.message or '')[:1000]
            except Exception:
                try:
                    txt = str(event.message)[:1000]
                except Exception:
                    txt = ''
            log(f"[EVENT] new msg: chat_id={chat_id} sender={event.message.sender_id} msg_id={getattr(event.message, 'id', None)} preview='{txt[:80]}'")
            possible_keys = set()
            if chat_id is not None:
                try:
                    c = int(chat_id)
                    possible_keys.add(c)
                    possible_keys.add(abs(c))
                    possible_keys.add(-c)
                    try:
                        possible_keys.add(int(f'-100{abs(c)}'))
                    except Exception:
                        pass
                except Exception:
                    pass
            matched_key = None
            for k in possible_keys:
                if k in monitored_input_peers:
                    matched_key = k
                    break
            if matched_key is None:
                return
            if getattr(event.message, 'out', False):
                log(f'[EVENT] skipping outgoing msg {event.message.id} in chat {chat_id}')
                return
            if me and event.message.sender_id == me.id:
                log(f'[EVENT] skipping msg from self {event.message.id}')
                return
            sender = getattr(event.message, 'sender_id', None)
            if sender is not None and sender in ignored_users:
                log(f'[EVENT] sender {sender} is ignored -> skipping msg {event.message.id}')
                return
            txt_norm = normalize_text_for_dup(txt)
            for ph in ignored_phrases:
                if ph and ph.lower().strip() in txt_norm:
                    log(f"[EVENT] message contains ignored phrase '{ph}' -> skipping msg {event.message.id}")
                    return
            now_ts = time.time()
            if txt_norm:
                last_seen = recent_message_texts.get(txt_norm)
                if last_seen and now_ts - last_seen < DUPLICATE_WINDOW:
                    log(f'[EVENT] duplicate message within {DUPLICATE_WINDOW}s -> skipping msg {event.message.id}')
                    return
                recent_message_texts[txt_norm] = now_ts
            inp_peer = monitored_input_peers.get(matched_key)
            entity = monitored_entity_map.get(matched_key, inp_peer)
            if inp_peer is None:
                try:
                    ent = await event.get_chat()
                    inp_peer = await client.get_input_entity(ent)
                    for k in canonical_keys_for_entity(ent):
                        monitored_input_peers[k] = inp_peer
                        monitored_entity_map[k] = ent
                except Exception as e:
                    log(f'[HANDLER] cannot resolve input_peer for chat {chat_id}: {repr(e)}')
                    return
            delay = random.uniform(MIN_DELAY, MAX_DELAY)

            async def delayed_enqueue(chat_key, inp_peer, entity, msg_id, emoji, delay_seconds, txt_norm_local, origin_ts):
                try:
                    log(f'[DELAY] waiting {delay_seconds:.2f}s before scheduling reaction for msg {msg_id} chat {chat_key}')
                    await asyncio.sleep(delay_seconds)
                    now2 = time.time()
                    if now2 < global_cooldown_until:
                        log(f'[DELAY] global cooldown active after delay -> skipping msg {msg_id}')
                        return
                    if txt_norm_local:
                        last_seen2 = recent_message_texts.get(txt_norm_local, 0)
                        if last_seen2 and now2 - last_seen2 < DUPLICATE_WINDOW and (last_seen2 != origin_ts):
                            log(f'[DELAY] duplicate detected after delay -> skipping msg {msg_id}')
                            return
                    await safe_schedule_reaction_now(chat_key, inp_peer, entity, msg_id, emoji)
                except Exception as e:
                    log(f'[DELAY ERR] {type(e).__name__}: {e}')
            asyncio.create_task(delayed_enqueue(matched_key, inp_peer, entity, event.message.id, emoji, delay, txt_norm, now_ts))
        except Exception as e:
            log(f'[HANDLER ERR] {type(e).__name__}: {e}')
    return handler

def canonical_keys_for_entity(ent):
    keys = set()
    try:
        eid = int(getattr(ent, 'id', None))
    except Exception:
        eid = None
    if eid is not None:
        keys.add(eid)
        keys.add(-eid)
        try:
            keys.add(int(f'-100{abs(eid)}'))
        except Exception:
            pass
    return list(keys)

async def probe_entity_access(ent):
    try:
        msgs = await client.get_messages(ent, limit=1)
        if msgs:
            m = msgs[0]
            preview = (m.message or '')[:100] if getattr(m, 'message', None) else '<non-text>'
            return (True, f"last_msg_id={m.id} preview='{preview}'")
        else:
            return (False, 'get_messages returned empty (no messages visible or not a member)')
    except Exception as e:
        return (False, f'get_messages failed: {type(e).__name__}: {e}')

async def periodic_sweep(emoji: str):
    while True:
        try:
            if not SWEEP_ENABLED:
                await asyncio.sleep(60)
                continue
            chat_ids = list(monitored_input_peers.keys())
            for chat_id in chat_ids:
                entity = monitored_entity_map.get(chat_id)
                inp_peer = monitored_input_peers.get(chat_id)
                if not entity or not inp_peer:
                    continue
                try:
                    msgs = await client.get_messages(entity, limit=SWEEP_COUNT)
                    if not msgs:
                        continue
                    for msg in msgs:
                        if msg.sender_id == me.id:
                            continue
                        confirmed = await message_has_reaction(entity, msg.id, emoji)
                        if not confirmed:
                            log(f'[SWEEP] msg {msg.id} in chat {chat_id} missing {emoji} -> ensuring')
                            await safe_schedule_reaction_now(chat_id, inp_peer, entity, msg.id, emoji)
                except Exception as e:
                    log(f'[SWEEP ERR] chat {chat_id}: {type(e).__name__}: {e}')
            await asyncio.sleep(SWEEP_INTERVAL)
        except Exception as e:
            log(f'[SWEEP LOOP ERR] {type(e).__name__}: {e}')
            await asyncio.sleep(60)

async def keep_presence_loop():
    while True:
        try:
            if ALWAYS_ONLINE:
                try:
                    await client(functions.account.UpdateStatusRequest(offline=False))
                    log('[PRESENCE] set online (periodic).')
                except Exception as e:
                    log(f'[PRESENCE] periodic online update failed: {type(e).__name__}: {e}')
            else:
                try:
                    await client(functions.account.UpdateStatusRequest(offline=True))
                    log('[PRESENCE] set offline (periodic).')
                except Exception as e:
                    log(f'[PRESENCE] periodic offline update failed: {type(e).__name__}: {e}')
        except Exception as e:
            log(f'[PRESENCE] loop err: {e}')
        await asyncio.sleep(60 * 5)

async def cooldown_monitor_loop():
    global global_cooldown_until, cooldown_active_flag
    was_cooling = False
    while True:
        now = time.time()
        if now < global_cooldown_until:
            remaining = int(global_cooldown_until - now)
            if not was_cooling:
                log(f'[COOLDOWN MON] cooldown started, remaining {remaining}s')
                was_cooling = True
                cooldown_active_flag = True
            else:
                log(f'[COOLDOWN MON] global cooldown active, remaining {remaining}s')
        else:
            if was_cooling:
                was_cooling = False
                cooldown_active_flag = False
                log('[COOLDOWN MON] global cooldown ended')
            try:
                if os.path.exists(RESET_COOLDOWN_FILENAME):
                    global_cooldown_until = 0.0
                    try:
                        os.remove(RESET_COOLDOWN_FILENAME)
                    except Exception:
                        pass
                    log('[COOLDOWN MON] reset_cooldown file detected — coolant cleared')
            except Exception as e:
                log(f'[COOLDOWN MON] file-check error: {e}')
        await asyncio.sleep(COOLDOWN_CHECK_INTERVAL)

async def console_input_loop():
    global global_cooldown_until
    if not sys.stdin or not sys.stdin.isatty():
        return
    loop = asyncio.get_event_loop()
    while True:
        try:
            cmd = await loop.run_in_executor(None, sys.stdin.readline)
            if not cmd:
                await asyncio.sleep(1)
                continue
            cmd = cmd.strip().lower()
            if cmd in ('clear', 'reset'):
                global_cooldown_until = 0.0
                log('[CONSOLE] global cooldown cleared by user command')
            elif cmd in ('status', 'st'):
                now = time.time()
                if now < global_cooldown_until:
                    log(f'[CONSOLE] cooldown active, remaining {int(global_cooldown_until - now)}s')
                else:
                    log('[CONSOLE] no global cooldown active')
            elif cmd in ('quit', 'exit'):
                log('[CONSOLE] exit requested by user')
                try:
                    await client.disconnect()
                except Exception:
                    pass
                sys.exit(0)
            else:
                log(f'[CONSOLE] unknown command: {cmd} (supported: clear/status/quit)')
        except Exception as e:
            log(f'[CONSOLE] input loop error: {e}')
            await asyncio.sleep(1)

async def bot_send_method(method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    global bot_http_session
    if bot_http_session is None:
        bot_http_session = aiohttp.ClientSession()
    url = f'{BOT_API_BASE}/{method}'
    try:
        async with bot_http_session.post(url, json=payload, timeout=30) as resp:
            data = await resp.text()
            try:
                return json.loads(data)
            except Exception:
                return {'ok': False, 'raw': data}
    except Exception as e:
        log(f'[BOT] HTTP error for {method}: {e}')
        return {'ok': False, 'error': str(e)}

async def bot_send_message(chat_id: int, text: str):
    payload = {'chat_id': chat_id, 'text': text, 'disable_web_page_preview': True}
    res = await bot_send_method('sendMessage', payload)
    return res

async def add_monitor_token(token: str) -> (bool, str):
    t = normalize_chat_token(token)
    if not t:
        return (False, 'empty token')
    ent = await try_resolve_token(client, t)
    if not ent:
        return (False, f"cannot resolve '{token}'")
    try:
        inp = await client.get_input_entity(ent)
        keys = canonical_keys_for_entity(ent)
        try:
            eid = int(getattr(ent, 'id', ent))
            keys.append(eid)
            keys.append(-eid)
            try:
                keys.append(int(f'-100{eid}'))
            except Exception:
                pass
        except Exception:
            pass
        for k in set(keys):
            monitored_input_peers[k] = inp
            monitored_entity_map[k] = ent
        log(f"[MONITOR ADD] {token} -> id={getattr(ent, 'id', None)}")
        return (True, f'added {token}')
    except Exception as e:
        return (False, f'error adding {token}: {e}')

async def remove_monitor_token(token: str) -> (bool, str):
    t = normalize_chat_token(token)
    if not t:
        return (False, 'empty token')
    ent = await try_resolve_token(client, t)
    if not ent:
        return (False, f"cannot resolve '{token}'")
    try:
        keys = canonical_keys_for_entity(ent)
        try:
            eid = int(getattr(ent, 'id', ent))
            keys.append(eid)
            keys.append(-eid)
            try:
                keys.append(int(f'-100{eid}'))
            except Exception:
                pass
        except Exception:
            pass
        removed = 0
        for k in set(keys):
            if k in monitored_input_peers:
                monitored_input_peers.pop(k, None)
                monitored_entity_map.pop(k, None)
                removed += 1
        log(f'[MONITOR REMOVE] {token} removed keys={removed}')
        return (True, f'removed {removed} keys for {token}')
    except Exception as e:
        return (False, f'error removing {token}: {e}')

async def handle_bot_update(upd: Dict[str, Any]):
    global REACT_PROBABILITY, CONCURRENCY, MIN_DELAY, MAX_DELAY, global_cooldown_until, react_semaphore, TARGET_RATE, limiter, ignored_users, ignored_phrases, ALWAYS_ONLINE
    try:
        if 'message' not in upd:
            return
        msg = upd['message']
        chat = msg.get('chat', {})
        chat_id = chat.get('id')
        text = msg.get('text', '') or ''
        from_user = msg.get('from', {})
        user_id = from_user.get('id')
        if not text or user_id is None:
            return
        text = text.strip()
        lower = text.lower()
        if lower.startswith('/start'):
            await bot_send_message(chat_id, 'Auto-react bot.\nCommands: /subscribe /unsubscribe /status /stats /recent /setprob /setrate /resetcooldown /help\n/ignore <user_id> /unignore <user_id>\n/ignoreword add|remove|list\n/add <chat>, /remove <chat>\n/presence on|off')
            return
        if lower.startswith('/help'):
            await bot_send_message(chat_id, 'Commands:\n/subscribe /unsubscribe\n/status\n/stats\n/recent\n/setprob <0..1>\n/setrate <int>\n/resetcooldown\n/ignore <user_id> /unignore <user_id>\n/ignoreword add <phrase> /ignoreword remove <phrase> /ignoreword list\n/add <username|id|t.me/link> /remove <token> /listmonitored\n/presence on|off')
            return
        if lower.startswith('/subscribe'):
            subscribers[user_id] = subscribers.get(user_id, {})
            subscribers[user_id]['notify'] = True
            await bot_send_message(chat_id, 'Subscribed to notifications.')
            return
        if lower.startswith('/unsubscribe'):
            subscribers[user_id] = subscribers.get(user_id, {})
            subscribers[user_id]['notify'] = False
            await bot_send_message(chat_id, 'Unsubscribed.')
            return
        if lower.startswith('/status'):
            async with config_lock:
                s = f'Settings:\nTARGET_RATE={TARGET_RATE}\nCURRENT_RATE={limiter.current_rate}\nCONCURRENCY={CONCURRENCY}\nMIN_DELAY={MIN_DELAY}\nMAX_DELAY={MAX_DELAY}\nPER_CHAT_MIN_INTERVAL={PER_CHAT_MIN_INTERVAL}\nREACT_PROBABILITY={REACT_PROBABILITY}\nALWAYS_ONLINE={ALWAYS_ONLINE}\nGlobal cooldown until (unix)={int(global_cooldown_until)}'
            await bot_send_message(chat_id, s)
            return
        if lower.startswith('/stats'):
            total = stats_total_reactions
            per = '\n'.join([f'{k}: {v}' for k, v in stats_per_chat.items()]) or 'none'
            await bot_send_message(chat_id, f'Total reactions: {total}\nPer chat:\n{per}')
            return
        if lower.startswith('/recent'):
            lines = []
            for e in recent_reactions[:20]:
                ts = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(e['time']))
                lines.append(f"{ts} {e['emoji']} {e['link']}")
            if not lines:
                await bot_send_message(chat_id, 'No recent reactions recorded.')
            else:
                text_out = 'Recent reactions:\n' + '\n'.join(lines)
                await bot_send_message(chat_id, text_out)
            return
        if lower.startswith('/resetcooldown'):
            global_cooldown_until = 0.0
            await bot_send_message(chat_id, 'Global cooldown reset.')
            return
        if lower.startswith('/setprob'):
            parts = text.split()
            if len(parts) >= 2:
                try:
                    v = float(parts[1])
                    if 0.0 <= v <= 1.0:
                        async with config_lock:
                            REACT_PROBABILITY = v
                        await bot_send_message(chat_id, f'REACT_PROBABILITY set to {v}')
                    else:
                        await bot_send_message(chat_id, 'Value must be between 0 and 1.')
                except Exception:
                    await bot_send_message(chat_id, 'Invalid float value.')
            else:
                await bot_send_message(chat_id, 'Usage: /setprob 0.05')
            return
        if lower.startswith('/setrate'):
            parts = text.split()
            if len(parts) >= 2:
                try:
                    r = int(parts[1])
                    if r >= 1 and r <= 200:
                        async with config_lock:
                            limiter.target_rate = r
                            limiter.current_rate = min(limiter.current_rate, r)
                        await bot_send_message(chat_id, f'TARGET_RATE set to {r}')
                    else:
                        await bot_send_message(chat_id, 'Value must be between 1 and 200')
                except Exception:
                    await bot_send_message(chat_id, 'Invalid integer value.')
            else:
                await bot_send_message(chat_id, 'Usage: /setrate 30')
            return
        if lower.startswith('/ignore '):
            parts = text.split()
            if len(parts) >= 2:
                try:
                    uid = int(parts[1])
                    ignored_users.add(uid)
                    await bot_send_message(chat_id, f'User {uid} added to ignore list.')
                except Exception:
                    await bot_send_message(chat_id, 'Invalid user id.')
            else:
                await bot_send_message(chat_id, 'Usage: /ignore <user_id>')
            return
        if lower.startswith('/unignore '):
            parts = text.split()
            if len(parts) >= 2:
                try:
                    uid = int(parts[1])
                    ignored_users.discard(uid)
                    await bot_send_message(chat_id, f'User {uid} removed from ignore list.')
                except Exception:
                    await bot_send_message(chat_id, 'Invalid user id.')
            else:
                await bot_send_message(chat_id, 'Usage: /unignore <user_id>')
            return
        if lower.startswith('/ignoreword '):
            parts = text.split(maxsplit=2)
            if len(parts) >= 2:
                sub = parts[1].lower()
                if sub == 'add' and len(parts) == 3:
                    phrase = parts[2].strip()
                    if phrase:
                        ignored_phrases.append(phrase.lower())
                        await bot_send_message(chat_id, f'Added ignore phrase: {phrase}')
                    else:
                        await bot_send_message(chat_id, 'Usage: /ignoreword add <phrase>')
                    return
                if sub == 'remove' and len(parts) == 3:
                    phrase = parts[2].strip().lower()
                    try:
                        ignored_phrases[:] = [p for p in ignored_phrases if p != phrase]
                        await bot_send_message(chat_id, f'Removed ignore phrase: {phrase}')
                    except Exception:
                        await bot_send_message(chat_id, 'Phrase removal failed.')
                    return
                if sub == 'list':
                    await bot_send_message(chat_id, 'Ignored phrases:\n' + '\n'.join(ignored_phrases) if ignored_phrases else 'None')
                    return
            await bot_send_message(chat_id, 'Usage: /ignoreword add|remove|list <phrase>')
            return
        if lower.startswith('/add '):
            parts = text.split(maxsplit=1)
            if len(parts) == 2:
                token = parts[1].strip()
                ok, info = await add_monitor_token(token)
                await bot_send_message(chat_id, f'/add -> {info}')
            else:
                await bot_send_message(chat_id, 'Usage: /add <username|id|t.me/link>')
            return
        if lower.startswith('/remove '):
            parts = text.split(maxsplit=1)
            if len(parts) == 2:
                token = parts[1].strip()
                ok, info = await remove_monitor_token(token)
                await bot_send_message(chat_id, f'/remove -> {info}')
            else:
                await bot_send_message(chat_id, 'Usage: /remove <username|id|t.me/link>')
            return
        if lower.startswith('/listmonitored'):
            lines = []
            seen = set()
            for k, ent in monitored_entity_map.items():
                try:
                    eid = getattr(ent, 'id', None)
                    name = getattr(ent, 'title', None) or getattr(ent, 'username', None) or str(ent)
                    if eid not in seen:
                        seen.add(eid)
                        lines.append(f'id={eid} name={name}')
                except Exception:
                    pass
            if not lines:
                await bot_send_message(chat_id, 'No monitored chats.')
            else:
                await bot_send_message(chat_id, 'Monitored:\n' + '\n'.join(lines))
            return
        if lower.startswith('/presence '):
            parts = text.split()
            if len(parts) >= 2:
                arg = parts[1].lower()
                if arg in ('on', '1', 'true'):
                    ALWAYS_ONLINE = True
                    await bot_send_message(chat_id, 'Presence: ALWAYS_ONLINE enabled (will keep account online).')
                elif arg in ('off', '0', 'false'):
                    ALWAYS_ONLINE = False
                    await bot_send_message(chat_id, 'Presence: ALWAYS_ONLINE disabled (will set account offline periodically).')
                else:
                    await bot_send_message(chat_id, 'Usage: /presence on|off')
            else:
                await bot_send_message(chat_id, 'Usage: /presence on|off')
            return
        await bot_send_message(chat_id, 'Unknown command. Use /help.')
    except Exception as e:
        log(f'[BOT HANDLER ERR] {type(e).__name__}: {e}')

async def bot_poll_loop():
    global bot_poll_offset, bot_http_session
    if bot_http_session is None:
        bot_http_session = aiohttp.ClientSession()
    log('[BOT] starting long-polling loop')
    while True:
        try:
            params = {'timeout': 20}
            if bot_poll_offset:
                params['offset'] = bot_poll_offset
            async with bot_http_session.get(f'{BOT_API_BASE}/getUpdates', params=params, timeout=25) as resp:
                data = await resp.json()
                if not data.get('ok'):
                    await asyncio.sleep(1)
                    continue
                for upd in data.get('result', []):
                    bot_poll_offset = max(bot_poll_offset, upd['update_id'] + 1)
                    asyncio.create_task(handle_bot_update(upd))
        except asyncio.CancelledError:
            break
        except Exception as e:
            log(f'[BOT POLL ERR] {e}')
            await asyncio.sleep(1)

async def main():
    global client, me, bot_http_session, react_semaphore, limiter, CONCURRENCY, TARGET_RATE, ALWAYS_ONLINE
    choice = "1"
    emoji = EMOJI_MAP.get(choice, EMOJI_MAP.get(choice.lower(), '🤡'))
    if not emoji and choice:
        emoji = EMOJI_MAP.get(choice.lower(), '🤡')
    log(f'Выбрано: {emoji}')
    raw = "https://t.me/love_thxs_ideas"
    if not raw:
        log('Чаты не указаны — автореакт не будет запущен.')
        tokens = []
    else:
        tokens = [normalize_chat_token(t) for t in raw.split() if normalize_chat_token(t)]
    bot_http_session = aiohttp.ClientSession()
    client_params = {}
    if PROXY:
        client_params['proxy'] = PROXY
    client = TelegramClient(session_name, api_id, api_hash, **client_params)
    client.flood_sleep_threshold = 0
    await client.start(phone=phone)
    me_local = await client.get_me()
    globals()['me'] = me_local
    log(f"Logged in as {getattr(me_local, 'first_name', None)} ({me_local.id}). Resolving chats...")
    try:
        if ALWAYS_ONLINE:
            await client(functions.account.UpdateStatusRequest(offline=False))
            log('[PRESENCE] attempted to set account status to online (initial).')
        else:
            await client(functions.account.UpdateStatusRequest(offline=True))
            log('[PRESENCE] attempted to set account status to offline (initial).')
    except Exception as e:
        log(f'[PRESENCE] could not set initial presence: {type(e).__name__}: {e}')
    asyncio.create_task(keep_presence_loop())
    asyncio.create_task(cooldown_monitor_loop())
    asyncio.create_task(console_input_loop())
    asyncio.create_task(bot_poll_loop())
    react_semaphore = asyncio.Semaphore(CONCURRENCY)
    workers = []
    for i in range(CONCURRENCY):
        w = asyncio.create_task(reaction_worker(i + 1, emoji))
        workers.append(w)
    resolved = {}
    for t in tokens:
        ent = None
        try:
            ent = await try_resolve_token(client, t)
        except Exception as e:
            log(f'[WARN] resolving {t} failed: {e}')
        if ent:
            resolved[t] = ent
            log(f"[OK] {t} -> {getattr(ent, 'title', getattr(ent, 'username', str(ent)))} (id={getattr(ent, 'id', None)})")
        else:
            log(f"[WARN] cannot resolve token '{t}' — skipping")
    if not resolved and tokens:
        log('Не удалось разрешить ни один чат — проверьте токены/участие в чатах.')
    if resolved:
        log('[DIAG] probing access to resolved entities (get_messages(limit=1))...')
        for t, ent in resolved.items():
            ok, info = await probe_entity_access(ent)
            if ok:
                log(f'[DIAG OK] {t} -> {info}')
            else:
                log(f"[DIAG NOACCESS] {t} -> {info} (you likely don't receive live updates from this chat)")
    for t, ent in resolved.items():
        try:
            inp = await client.get_input_entity(ent)
            keys = canonical_keys_for_entity(ent)
            try:
                eid = int(getattr(ent, 'id', ent))
                keys.append(eid)
                keys.append(-eid)
                try:
                    keys.append(int(f'-100{eid}'))
                except Exception:
                    pass
            except Exception:
                pass
            for k in set(keys):
                monitored_input_peers[k] = inp
                monitored_entity_map[k] = ent
        except Exception as e:
            log(f'[WARN] cannot get input entity for {t}: {repr(e)}')
    log(f'Monitoring canonical keys: {set(monitored_input_peers.keys())}')
    log(f'Parameters: TARGET_RATE={TARGET_RATE} CONCURRENCY={CONCURRENCY} PER_CHAT_MIN_INTERVAL={PER_CHAT_MIN_INTERVAL} REACT_PROBABILITY={REACT_PROBABILITY}')
    handler = await handler_factory(emoji)
    try:
        entities = []
        seen_eids = set()
        for ent in monitored_entity_map.values():
            try:
                eid = int(getattr(ent, 'id', None))
            except Exception:
                eid = None
            if eid is None:
                continue
            if eid in seen_eids:
                continue
            seen_eids.add(eid)
            entities.append(ent)
        if entities:
            client.add_event_handler(handler, events.NewMessage(chats=entities))
            log('[HANDLER] registered NewMessage with chats=entities')
        else:
            client.add_event_handler(handler, events.NewMessage())
            log('[HANDLER] registered NewMessage globally')
    except Exception as e:
        log(f'[HANDLER REG ERR] {e}')
    asyncio.create_task(periodic_sweep(emoji))
    log('Listening for new messages... (Ctrl+C to stop)')
    try:
        await client.run_until_disconnected()
    except asyncio.CancelledError:
        log('[MAIN] client.run_until_disconnected cancelled')
    except KeyboardInterrupt:
        log('Stopped by user (KeyboardInterrupt)')
    finally:
        log('[MAIN] shutdown: cancelling background tasks...')
        current = asyncio.current_task()
        tasks = [t for t in asyncio.all_tasks() if t is not current]
        for t in tasks:
            try:
                t.cancel()
            except Exception:
                pass
        await asyncio.gather(*tasks, return_exceptions=True)
        try:
            if bot_http_session:
                await bot_http_session.close()
        except Exception:
            pass
        try:
            await client.disconnect()
            log('[MAIN] client disconnected')
        except Exception as e:
            log(f'[MAIN] error during disconnect: {e}')
if __name__ == '__main__':
    try:
        asyncio.run(main())
    except SystemExit:
        log('Exiting due to SystemExit.')
    except Exception as e:
        log(f'Fatal error: {type(e).__name__}: {e}')
        try:
            with open(logfile, 'a', encoding='utf-8') as f:
                f.write(traceback.format_exc())
        except Exception:
            pass
