"""
⚡ BLADE USERWARE REP — Telegram Bot
Всё в одном файле.
"""
import asyncio, logging, os, re, json, time, random, string, sys
from collections import defaultdict
from typing import Any, Awaitable, Callable

import aiosqlite
import aiohttp
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, Router, F, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery, TelegramObject,
    InlineKeyboardMarkup
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest

load_dotenv()

# ═══════════════════════════════════════════════════════════
#  КОНФИГ
# ═══════════════════════════════════════════════════════════
BOT_TOKEN   = os.getenv("BOT_TOKEN", "")
_aids       = os.getenv("ADMIN_IDS", "")
ADMIN_IDS   = [int(x) for x in _aids.split(",") if x.strip().isdigit()]
DB_PATH     = os.getenv("DATABASE_PATH", "blade_bot.db")
RATE_LIMIT  = float(os.getenv("RATE_LIMIT", "2.0"))
MAX_HIST    = int(os.getenv("MAX_HISTORY", "50"))
MAX_FAV     = int(os.getenv("MAX_FAVORITES", "50"))
FRAG_TIMEOUT= int(os.getenv("FRAGMENT_TIMEOUT", "12"))
CACHE_TTL   = int(os.getenv("CACHE_TTL", "300"))

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не задан!")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-8s │ %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════
#  БАЗА ДАННЫХ
# ═══════════════════════════════════════════════════════════
_conn: aiosqlite.Connection | None = None

async def db_init():
    global _conn
    _conn = await aiosqlite.connect(DB_PATH)
    _conn.row_factory = aiosqlite.Row
    await _conn.execute("PRAGMA journal_mode=WAL")
    await _conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER UNIQUE NOT NULL,
            username TEXT, first_name TEXT,
            is_blocked INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS checks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER NOT NULL,
            username TEXT NOT NULL,
            result TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS favorites (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER NOT NULL,
            username TEXT NOT NULL,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(telegram_id, username)
        );
    """)
    await _conn.commit()

async def db_upsert_user(tid, username, first_name):
    await _conn.execute("""
        INSERT INTO users (telegram_id, username, first_name) VALUES (?,?,?)
        ON CONFLICT(telegram_id) DO UPDATE SET
            username=excluded.username, first_name=excluded.first_name,
            last_active=CURRENT_TIMESTAMP
    """, (tid, username, first_name))
    await _conn.commit()

async def db_is_blocked(tid) -> bool:
    async with _conn.execute("SELECT is_blocked FROM users WHERE telegram_id=?", (tid,)) as c:
        r = await c.fetchone()
        return bool(r and r["is_blocked"])

async def db_block(tid):
    await _conn.execute("UPDATE users SET is_blocked=1 WHERE telegram_id=?", (tid,))
    await _conn.commit()

async def db_unblock(tid):
    await _conn.execute("UPDATE users SET is_blocked=0 WHERE telegram_id=?", (tid,))
    await _conn.commit()

async def db_all_users():
    async with _conn.execute("SELECT telegram_id,username,first_name,is_blocked FROM users") as c:
        return [dict(r) for r in await c.fetchall()]

async def db_stats():
    async with _conn.execute("SELECT COUNT(*) as n FROM users") as c:
        u = (await c.fetchone())["n"]
    async with _conn.execute("SELECT COUNT(*) as n FROM checks") as c:
        ch = (await c.fetchone())["n"]
    async with _conn.execute("SELECT COUNT(*) as n FROM users WHERE DATE(last_active)=DATE('now')") as c:
        at = (await c.fetchone())["n"]
    async with _conn.execute("SELECT COUNT(*) as n FROM users WHERE is_blocked=1") as c:
        bl = (await c.fetchone())["n"]
    return {"total_users": u, "total_checks": ch, "active_today": at, "blocked": bl}

async def db_save_check(tid, username, result: dict):
    await _conn.execute(
        "INSERT INTO checks (telegram_id,username,result) VALUES (?,?,?)",
        (tid, username.lower().lstrip("@"), json.dumps(result, ensure_ascii=False))
    )
    await _conn.execute("""
        DELETE FROM checks WHERE telegram_id=? AND id NOT IN (
            SELECT id FROM checks WHERE telegram_id=? ORDER BY created_at DESC LIMIT ?
        )
    """, (tid, tid, MAX_HIST))
    await _conn.commit()

async def db_history(tid, limit=15):
    async with _conn.execute("""
        SELECT username,result,created_at FROM checks
        WHERE telegram_id=? ORDER BY created_at DESC LIMIT ?
    """, (tid, limit)) as c:
        rows = await c.fetchall()
    out = []
    for r in rows:
        d = dict(r)
        try: d["result"] = json.loads(d["result"]) if d["result"] else {}
        except: d["result"] = {}
        out.append(d)
    return out

async def db_add_fav(tid, username) -> bool:
    try:
        await _conn.execute(
            "INSERT INTO favorites (telegram_id,username) VALUES (?,?)",
            (tid, username.lower().lstrip("@"))
        )
        await _conn.commit()
        return True
    except aiosqlite.IntegrityError:
        return False

async def db_remove_fav(tid, username):
    await _conn.execute(
        "DELETE FROM favorites WHERE telegram_id=? AND username=?",
        (tid, username.lower().lstrip("@"))
    )
    await _conn.commit()

async def db_get_favs(tid):
    async with _conn.execute(
        "SELECT username FROM favorites WHERE telegram_id=? ORDER BY added_at DESC", (tid,)
    ) as c:
        return [r["username"] for r in await c.fetchall()]

async def db_is_fav(tid, username) -> bool:
    async with _conn.execute(
        "SELECT 1 FROM favorites WHERE telegram_id=? AND username=?",
        (tid, username.lower().lstrip("@"))
    ) as c:
        return (await c.fetchone()) is not None

# ═══════════════════════════════════════════════════════════
#  АЛГОРИТМ ОЦЕНКИ
# ═══════════════════════════════════════════════════════════
WORDS = {
    "dark","code","edge","fire","gate","iron","jade","king","link","moon","nova","peak",
    "rush","sage","tech","void","wave","wolf","zeus","cyan","dawn","dusk","echo","flux",
    "glow","grid","halo","hawk","hero","hive","icon","keen","lore","maze","mind","mint",
    "mist","neon","null","oath","onyx","port","pure","ring","root","ruby","rune","seed",
    "silk","sole","soul","star","stem","surf","sync","tale","tide","time","tone","tune",
    "twin","unit","vale","vast","veil","view","vine","volt","wake","wild","wind","wire",
    "wise","word","work","wrap","year","zero","zone","bold","burn","calm","core","data",
    "agent","alpha","angel","arrow","blade","blast","blaze","block","bloom","boost","brave",
    "chain","chaos","charm","chase","chess","chief","chord","class","clean","clear","cliff",
    "cloud","coast","cobra","craft","crest","cross","crown","cycle","dance","debug","delta",
    "dense","dodge","draft","drain","drama","drift","drone","drive","elite","ember","epoch",
    "event","extra","fable","fancy","field","final","first","flair","flame","flash","fleet",
    "float","floor","fluid","focus","force","forge","forum","frame","front","funds","gamma",
    "giant","globe","grace","grade","grail","grand","graph","grasp","great","green","grind",
    "guide","haven","heart","heavy","helix","laser","layer","light","limit","local","logic",
    "loyal","lunar","magic","major","march","match","media","merit","micro","might","model",
    "money","month","motor","mount","music","nexus","night","ninja","noble","north","novel",
    "orbit","order","panel","patch","peace","phase","pivot","pixel","place","plane","polar",
    "power","prism","proof","prose","proxy","pulse","quest","quiet","radar","radio","rapid",
    "ratio","realm","relay","remix","retro","rider","robot","rouge","round","royal","ruler",
    "saint","scale","scope","scout","sense","serve","shade","share","sharp","shell","shift",
    "shine","sigma","skill","slice","smart","solar","solid","solve","sonic","south","space",
    "spark","speed","spell","spire","split","sport","squad","stack","staff","stake","stark",
    "start","state","steel","stern","storm","story","style","suite","super","surge","sword",
    "teach","tiger","timer","titan","token","topic","total","touch","tower","track","trade",
    "trail","train","trend","tribe","trust","truth","ultra","union","unity","vault","verse",
    "viper","viral","vista","vital","voice","watch","water","weave","white","whole","width",
    "witch","woods","world","worth","yield","young","youth",
}
VOWELS_S = set("aeiou")
CONS_S   = set("bcdfghjklmnpqrstvwxyz")

def _pronounceable(s):
    cc = cv = 0
    for ch in s.lower():
        if ch in VOWELS_S:
            cc = 0; cv += 1
            if cv > 3: return False
        elif ch in CONS_S:
            cv = 0; cc += 1
            if cc > 3: return False
    return True

def score_username(username: str) -> dict:
    clean = username.lower().lstrip("@").strip()
    length = len(clean)
    has_digits    = bool(re.search(r"\d", clean))
    has_under     = "_" in clean
    only_letters  = bool(re.fullmatch(r"[a-z]+", clean))
    pronounceable = _pronounceable(clean) if only_letters else False
    is_word       = clean in WORDS
    unique_ratio  = len(set(clean)) / max(len(clean), 1)

    length_score = {3:100,4:92,5:80,6:65,7:50,8:36,9:24,10:15}.get(length, max(5,15-(length-10)))
    appeal = max(0, min(100,
        length_score
        + (-15 if has_digits else 0)
        + (-10 if has_under  else 0)
        + (12  if pronounceable else 0)
        + (18  if is_word else 0)
        + int((unique_ratio - 0.5) * 10)
    ))

    rarity_base = {3:100,4:95,5:80,6:60,7:40,8:25,9:15}.get(length, max(5,15-(length-9)*2))
    if only_letters: rarity_base += 5
    if not has_digits: rarity_base += 3
    if is_word: rarity_base += 7
    rarity = max(0, min(100, rarity_base))

    score = round(appeal * 0.55 + rarity * 0.45)

    base = {3:5000,4:2000,5:300,6:60,7:15,8:5,9:2}.get(length, 0.5)
    mult = 1 + (score - 50) / 50 * 1.5
    price = round(base * max(0.1, mult), 2)
    lo, hi = round(price*0.6,1), round(price*1.4,1)
    if price >= 100:
        price_range = f"{int(lo)}–{int(hi)} TON"
    elif price >= 1:
        price_range = f"{lo:.1f}–{hi:.1f} TON"
    else:
        price_range = f"{lo:.2f}–{hi:.2f} TON"

    if rarity >= 85:   rl = "🔥 Исключительная"
    elif rarity >= 65: rl = "💎 Высокая"
    elif rarity >= 45: rl = "⚡ Средняя"
    elif rarity >= 25: rl = "📊 Низкая"
    else:              rl = "⬇️ Очень низкая"

    return dict(appeal=appeal, rarity=rarity, rarity_label=rl, score=score,
                price_ton=price, price_range=price_range, length=length,
                has_digits=has_digits, has_underscore=has_under,
                is_word=is_word, pronounceable=pronounceable)

# ═══════════════════════════════════════════════════════════
#  FRAGMENT
# ═══════════════════════════════════════════════════════════
_frag_cache: dict[str, tuple[float, dict]] = {}
FRAG_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 12; Pixel 6) AppleWebKit/537.36 Chrome/120.0.0.0 Mobile Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

async def check_fragment(username: str) -> dict:
    clean = username.lower().lstrip("@").strip()
    if not re.fullmatch(r"[a-z][a-z0-9_]{2,30}", clean):
        return {"for_sale": False, "price": None, "url": "", "status": "invalid"}

    cached = _frag_cache.get(clean)
    if cached and time.time() - cached[0] < CACHE_TTL:
        return cached[1]

    url = f"https://fragment.com/username/{clean}"
    result = {"for_sale": False, "price": None, "url": url, "status": "ok"}

    try:
        timeout = aiohttp.ClientTimeout(total=FRAG_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.get(url, headers=FRAG_HEADERS, allow_redirects=True) as resp:
                if resp.status == 404:
                    result["status"] = "not_found"
                elif resp.status != 200:
                    result["status"] = "error"
                    result["message"] = f"HTTP {resp.status}"
                else:
                    html = await resp.text()
                    page = html.lower()
                    sale_signals = ["buy for","place a bid","buy now","current price","make an offer","top bid","auction"]
                    if any(sig in page for sig in sale_signals):
                        result["for_sale"] = True
                    for pat in [r"buy for\s*([\d,\.]+)\s*ton", r"(\d[\d,\.]+)\s*ton"]:
                        m = re.search(pat, page)
                        if m:
                            try:
                                result["price"] = f"{float(m.group(1).replace(',','')):.4g} TON"
                            except: pass
                            break
    except asyncio.TimeoutError:
        result["status"] = "error"; result["message"] = "Таймаут Fragment"
    except Exception as e:
        result["status"] = "error"; result["message"] = "Ошибка соединения"
        log.warning("Fragment error %s: %s", clean, e)

    _frag_cache[clean] = (time.time(), result)
    return result

# ═══════════════════════════════════════════════════════════
#  ГЕНЕРАТОР USERNAME
# ═══════════════════════════════════════════════════════════
WORDS_BY_LEN = {}
for w in WORDS:
    WORDS_BY_LEN.setdefault(len(w), []).append(w)

VOWELS = "aeiou"
CONS   = "bcdfghjklmnpqrstvwxyz"

def _rand_pron(length):
    out = []
    want_v = random.choice([True, False])
    for _ in range(length):
        out.append(random.choice(VOWELS if want_v else CONS))
        want_v = not want_v
    return "".join(out)

def gen_candidates(length, category, no_digits=False, no_under=False, count=30):
    res = set()
    def ok(w):
        if len(w) != length: return False
        if no_digits and re.search(r"\d", w): return False
        if no_under and "_" in w: return False
        return bool(re.fullmatch(r"[a-z0-9_]{4,32}", w))

    for _ in range(count * 20):
        if len(res) >= count: break
        if category == "words":
            pool = WORDS_BY_LEN.get(length, [])
            w = random.choice(pool) if pool else _rand_pron(length)
        elif category == "expensive":
            pool = WORDS_BY_LEN.get(length, [])
            w = random.choice(pool) if pool else _rand_pron(length)
        elif category == "cheap":
            w = "".join(random.choices(string.ascii_lowercase + string.digits, k=length))
        elif category == "rare":
            pfx = random.choice(["neo","pro","meta","dark","flux","void","apex","core"])
            sfx = random.choice(["lab","hub","io","ai","xr","pro","vx","fx","net","go"])
            combo = pfx + sfx
            if len(combo) < length: combo += _rand_pron(length - len(combo))
            w = combo[:length]
        elif category == "beautiful":
            half = length // 2
            part = _rand_pron(half)
            w = (part + part[::-1] if length % 2 == 0 else part + random.choice(VOWELS) + part[::-1])[:length]
        else:
            w = _rand_pron(length)
        if w and ok(w):
            res.add(w)
    return list(res)[:count]

def rank_candidates(candidates, limit=10):
    scored = []
    for c in candidates:
        s = score_username(c)
        scored.append({"username": f"@{c}", "score": s["score"],
                       "rarity_label": s["rarity_label"], "price_range": s["price_range"]})
    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored[:limit]

def auto_hunt(limit=15):
    cands = []
    for length in range(4, 8):
        pool = WORDS_BY_LEN.get(length, [])
        if pool: cands.extend(random.sample(pool, min(15, len(pool))))
        cands.extend(gen_candidates(length, "minimal", no_digits=True, count=8))
    return rank_candidates(list(set(cands)), limit=limit)

# ═══════════════════════════════════════════════════════════
#  THROTTLING MIDDLEWARE
# ═══════════════════════════════════════════════════════════
class ThrottlingMiddleware(BaseMiddleware):
    def __init__(self, rate=2.0):
        self.rate = rate
        self._last: dict[int, float] = defaultdict(float)
        self._warned: set[int] = set()

    async def __call__(self, handler: Callable[[TelegramObject, dict], Awaitable[Any]],
                       event: TelegramObject, data: dict) -> Any:
        user = data.get("event_from_user")
        if not user:
            return await handler(event, data)
        uid = user.id
        if time.monotonic() - self._last[uid] < self.rate:
            if uid not in self._warned:
                self._warned.add(uid)
                if isinstance(event, Message):
                    await event.answer(f"⏳ Подождите {self.rate:.0f} сек между запросами.")
                elif isinstance(event, CallbackQuery):
                    await event.answer(f"⏳ Подождите {self.rate:.0f} сек.", show_alert=False)
            return
        self._warned.discard(uid)
        self._last[uid] = time.monotonic()
        return await handler(event, data)

# ═══════════════════════════════════════════════════════════
#  КЛАВИАТУРЫ
# ═══════════════════════════════════════════════════════════
def kb_main() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="🔎 Проверить Username",  callback_data="menu:check")
    b.button(text="✨ Найти Username",       callback_data="menu:find")
    b.button(text="💎 Username Hunting",    callback_data="menu:hunting")
    b.button(text="📊 Топ Username",        callback_data="menu:top")
    b.button(text="⭐ Избранное",           callback_data="menu:favorites")
    b.button(text="📜 История проверок",    callback_data="menu:history")
    b.button(text="ℹ️ О боте",             callback_data="menu:about")
    b.adjust(2, 2, 2, 1)
    return b.as_markup()

def kb_back() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="◀️ Главное меню", callback_data="menu:main")
    return b.as_markup()

def kb_result(username: str, is_fav: bool) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    clean = username.lstrip("@")
    if is_fav:
        b.button(text="💔 Убрать из избранного", callback_data=f"fav:remove:{clean}")
    else:
        b.button(text="⭐ В избранное",           callback_data=f"fav:add:{clean}")
    b.button(text="🔄 Проверить снова",          callback_data=f"recheck:{clean}")
    b.button(text="◀️ Главное меню",             callback_data="menu:main")
    b.adjust(1)
    return b.as_markup()

def kb_favs(usernames: list) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for un in usernames:
        b.button(text=f"@{un}", callback_data=f"recheck:{un}")
        b.button(text="🗑",     callback_data=f"fav:remove:{un}")
    b.button(text="◀️ Главное меню", callback_data="menu:main")
    b.adjust(2, repeat=True)
    return b.as_markup()

def kb_history(items: list) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for item in items:
        b.button(text=f"🔎 @{item['username']}", callback_data=f"recheck:{item['username']}")
    b.button(text="◀️ Главное меню", callback_data="menu:main")
    b.adjust(1)
    return b.as_markup()

def kb_lengths() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for label, val in [("4 символа","4"),("5 символов","5"),("6 символов","6"),
                        ("7 символов","7"),("8 символов","8"),("9+ символов","9")]:
        b.button(text=label, callback_data=f"hunt:len:{val}")
    b.button(text="◀️ Назад", callback_data="menu:main")
    b.adjust(2, 2, 2, 1)
    return b.as_markup()

def kb_cats() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for label, val in [("💎 Дорогие","expensive"),("🪙 Дешёвые","cheap"),
                        ("🔥 Редкие","rare"),("✨ Красивые","beautiful"),
                        ("🧊 Минималистичные","minimal"),("🎯 Случайные","random"),
                        ("🔤 Англ. слова","words")]:
        b.button(text=label, callback_data=f"hunt:cat:{val}")
    b.button(text="◀️ Назад", callback_data="menu:hunting")
    b.adjust(2, 2, 2, 1, 1)
    return b.as_markup()

def kb_filters(no_dig=False, no_un=False) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text=f"{'✅' if no_dig else '☐'} Без цифр",         callback_data="hunt:filter:digits")
    b.button(text=f"{'✅' if no_un  else '☐'} Без подчёркивания", callback_data="hunt:filter:under")
    b.button(text="🔍 Найти!",   callback_data="hunt:search")
    b.button(text="◀️ Назад",   callback_data="menu:hunting")
    b.adjust(2, 1, 1)
    return b.as_markup()

def kb_results(more=True) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    if more: b.button(text="🔄 Ещё варианты", callback_data="hunt:more")
    b.button(text="⚙️ Новый поиск",  callback_data="menu:hunting")
    b.button(text="◀️ Главное меню", callback_data="menu:main")
    b.adjust(1)
    return b.as_markup()

def kb_admin() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="📊 Статистика",          callback_data="admin:stats")
    b.button(text="👥 Пользователи",        callback_data="admin:users")
    b.button(text="📣 Рассылка",             callback_data="admin:broadcast")
    b.button(text="🚫 Заблокировать",        callback_data="admin:block")
    b.button(text="🔓 Разблокировать",       callback_data="admin:unblock")
    b.button(text="⚙️ Настройки",           callback_data="admin:settings")
    b.adjust(2, 2, 1, 1)
    return b.as_markup()

def kb_admin_back() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="◀️ Админ-панель", callback_data="admin:menu")
    return b.as_markup()

# ═══════════════════════════════════════════════════════════
#  FSM STATES
# ═══════════════════════════════════════════════════════════
class St(StatesGroup):
    waiting_username  = State()
    hunt_length       = State()
    hunt_category     = State()
    hunt_filters      = State()
    hunt_results      = State()
    admin_broadcast   = State()
    admin_block_id    = State()
    admin_unblock_id  = State()

# ═══════════════════════════════════════════════════════════
#  ROUTER + HANDLERS
# ═══════════════════════════════════════════════════════════
router = Router()
USERNAME_RE = re.compile(r"^@?[a-zA-Z][a-zA-Z0-9_]{2,30}$")
CAT_LABELS = {"expensive":"💎 Дорогие","cheap":"🪙 Дешёвые","rare":"🔥 Редкие",
              "beautiful":"✨ Красивые","minimal":"🧊 Минималистичные",
              "random":"🎯 Случайные","words":"🔤 Англ. слова"}

# ─── Утилиты ───────────────────────────────────────────────
async def register(u):
    if u: await db_upsert_user(u.id, u.username, u.first_name)

async def blocked(uid) -> bool:
    return await db_is_blocked(uid)

# ─── START ─────────────────────────────────────────────────
WELCOME = (
    "⚡ <b>BLADE USERWARE REP</b>\n\n"
    "Анализ, поиск и оценка Telegram username\n"
    "на рынке Fragment / TON.\n\n"
    "Выберите действие:"
)

@router.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext):
    await register(msg.from_user)
    await state.clear()
    if await blocked(msg.from_user.id):
        await msg.answer("🚫 Доступ ограничен."); return
    await msg.answer(WELCOME, reply_markup=kb_main())

@router.callback_query(F.data == "menu:main")
async def cb_main(q: CallbackQuery, state: FSMContext):
    await register(q.from_user)
    await state.clear()
    if await blocked(q.from_user.id):
        await q.answer("🚫 Доступ ограничен.", show_alert=True); return
    await q.message.edit_text(WELCOME, reply_markup=kb_main())
    await q.answer()

@router.callback_query(F.data == "menu:about")
async def cb_about(q: CallbackQuery):
    await q.message.edit_text(
        "⚡ <b>BLADE USERWARE REP</b>\n\n"
        "<b>Версия:</b> 1.0.0\n"
        "<b>Стек:</b> Python 3.11 · aiogram 3 · SQLite\n\n"
        "🔎 Анализ username — занятость, оценка, Fragment\n"
        "💎 Hunting — подбор по параметрам\n"
        "⭐ Избранное & История\n\n"
        "<i>⚠️ Оценки приблизительные.\n"
        "Реальная стоимость определяется рынком.</i>",
        reply_markup=kb_back()
    )
    await q.answer()

# ─── ANALYZER ──────────────────────────────────────────────
@router.callback_query(F.data == "menu:check")
async def cb_check(q: CallbackQuery, state: FSMContext):
    await state.set_state(St.waiting_username)
    await q.message.edit_text(
        "🔎 <b>Проверка Username</b>\n\n"
        "Отправьте username.\n"
        "<i>Пример:</i> <code>@blade</code> или <code>blade</code>",
        reply_markup=kb_back()
    )
    await q.answer()

@router.callback_query(F.data == "menu:find")
async def cb_find(q: CallbackQuery, state: FSMContext):
    await state.set_state(St.waiting_username)
    await q.message.edit_text(
        "✨ <b>Поиск Username</b>\n\n"
        "Отправьте username для анализа и проверки на Fragment.",
        reply_markup=kb_back()
    )
    await q.answer()

@router.callback_query(F.data.startswith("recheck:"))
async def cb_recheck(q: CallbackQuery, bot: Bot):
    un = q.data.split(":", 1)[1]
    await q.answer("🔄 Проверяю...")
    await do_analysis(q.message, bot, un, edit=True)

@router.message(StateFilter(St.waiting_username))
async def handle_un_input(msg: Message, state: FSMContext, bot: Bot):
    raw = (msg.text or "").strip()
    if not USERNAME_RE.match(raw):
        await msg.answer(
            "⚠️ Некорректный username.\n"
            "Только латиница, цифры, _, от 5 до 32 символов.\n\n"
            "Попробуйте снова:", reply_markup=kb_back()
        ); return
    await state.clear()
    wait = await msg.answer("⏳ <i>Анализирую...</i>")
    await do_analysis(wait, bot, raw.lstrip("@"), edit=True)

async def do_analysis(msg: Message, bot: Bot, username: str, edit=False):
    clean = username.lower().lstrip("@")
    tg_un = f"@{clean}"
    sd = score_username(clean)

    # Telegram check
    tg_status = "❓ Не удалось проверить"
    try:
        chat = await bot.get_chat(tg_un)
        if chat: tg_status = "🔴 Занят"
    except TelegramBadRequest as e:
        err = str(e).lower()
        if any(x in err for x in ["not found","invalid","doesn't exist","username"]):
            tg_status = "🟢 Свободен (вероятно)"

    # Fragment check
    fd = await check_fragment(clean)
    if fd["status"] == "ok":
        if fd["for_sale"]:
            pr = fd.get("price") or "цена скрыта"
            frag_line = f"🛒 Выставлен на продажу · {pr}"
        else:
            frag_line = "✅ Объявление не найдено"
    elif fd["status"] == "not_found":
        frag_line = "✅ Не найден на Fragment"
    else:
        frag_line = f"❓ {fd.get('message','Не удалось проверить')}"

    score = sd["score"]
    icon = "🏆" if score >= 80 else "🥈" if score >= 60 else "🥉" if score >= 40 else "📉"

    text = (
        f"⚡ <b>BLADE USERWARE REP</b>\n"
        f"{'─'*28}\n\n"
        f"<b>Username:</b> <code>{tg_un}</code>\n"
        f"<b>Статус:</b> {tg_status}\n"
        f"<b>Длина:</b> {sd['length']} символов\n\n"
        f"{'─'*28}\n\n"
        f"{icon} <b>Оценка:</b> <code>{score}/100</code>\n"
        f"✨ <b>Привлекательность:</b> <code>{sd['appeal']}/100</code>\n"
        f"🔮 <b>Редкость:</b> {sd['rarity_label']} <code>({sd['rarity']}/100)</code>\n\n"
        f"{'─'*28}\n\n"
        f"💰 <b>Примерная стоимость:</b> {sd['price_range']}\n"
        f"🔗 <b>Fragment:</b> {frag_line}\n"
        f"<a href=\"https://fragment.com/username/{clean}\">Открыть на Fragment →</a>\n\n"
    )
    flags = []
    if sd["is_word"]:       flags.append("📖 Настоящее слово")
    if sd["pronounceable"]: flags.append("🗣 Легко произносится")
    if sd["has_digits"]:    flags.append("🔢 Есть цифры")
    if sd["has_underscore"]:flags.append("〰 Есть _")
    if flags:
        text += "🏷 <b>Особенности:</b> " + " · ".join(flags) + "\n\n"
    text += "<i>⚠️ Оценка приблизительная. Реальная стоимость\nзависит от рыночного спроса.</i>"

    uid = msg.chat.id
    await db_save_check(uid, clean, {"score": score, "appeal": sd["appeal"],
                                      "rarity": sd["rarity"], "price_range": sd["price_range"],
                                      "tg_status": tg_status, "fragment": frag_line})
    is_fav = await db_is_fav(uid, clean)
    kb = kb_result(clean, is_fav)
    try:
        if edit: await msg.edit_text(text, reply_markup=kb, disable_web_page_preview=True)
        else:    await msg.answer(text, reply_markup=kb, disable_web_page_preview=True)
    except Exception:
        await msg.answer(text, reply_markup=kb, disable_web_page_preview=True)

# ─── TOP ───────────────────────────────────────────────────
TOP = [("@god","3 симв · 98/100 · ~5000 TON"),("@pro","3 симв · 97/100 · ~3000 TON"),
       ("@dark","4 симв · 92/100 · ~1800 TON"),("@nova","4 симв · 90/100 · ~1500 TON"),
       ("@flux","4 симв · 88/100 · ~1200 TON"),("@blade","5 симв · 85/100 · ~400 TON"),
       ("@cyber","5 симв · 83/100 · ~350 TON"),("@alpha","5 симв · 80/100 · ~300 TON"),
       ("@void","4 симв · 87/100 · ~1100 TON"),("@zero","4 симв · 86/100 · ~1000 TON")]

@router.callback_query(F.data == "menu:top")
async def cb_top(q: CallbackQuery):
    lines = ["📊 <b>Топ Username по оценке BLADE</b>\n"]
    for i,(un,info) in enumerate(TOP,1):
        m = ["🥇","🥈","🥉"][min(i-1,2)] if i<=3 else f"{i}."
        lines.append(f"{m} <code>{un}</code>\n   <i>{info}</i>")
    lines.append("\n<i>⚠️ Данные ориентировочные.</i>")
    await q.message.edit_text("\n".join(lines), reply_markup=kb_back())
    await q.answer()

# ─── FAVORITES ─────────────────────────────────────────────
@router.callback_query(F.data == "menu:favorites")
async def cb_favs(q: CallbackQuery):
    items = await db_get_favs(q.from_user.id)
    if not items:
        await q.message.edit_text(
            "⭐ <b>Избранное</b>\n\nСписок пуст.\nПроверьте username и нажмите «⭐ В избранное».",
            reply_markup=kb_back()
        )
        await q.answer(); return
    await q.message.edit_text(
        f"⭐ <b>Избранное</b> ({len(items)} из {MAX_FAV})\n\n"
        "Нажмите на username чтобы проверить или 🗑 для удаления:",
        reply_markup=kb_favs(items)
    )
    await q.answer()

@router.callback_query(F.data.startswith("fav:add:"))
async def cb_fav_add(q: CallbackQuery):
    un = q.data.split(":",2)[2]
    uid = q.from_user.id
    items = await db_get_favs(uid)
    if len(items) >= MAX_FAV:
        await q.answer(f"Лимит {MAX_FAV} username в избранном.", show_alert=True); return
    added = await db_add_fav(uid, un)
    await q.answer("⭐ Добавлено!" if added else "Уже в избранном.")
    try: await q.message.edit_reply_markup(reply_markup=kb_result(un, True))
    except: pass

@router.callback_query(F.data.startswith("fav:remove:"))
async def cb_fav_remove(q: CallbackQuery):
    un = q.data.split(":",2)[2]
    uid = q.from_user.id
    await db_remove_fav(uid, un)
    await q.answer("💔 Удалено из избранного")
    text = q.message.text or ""
    if "Избранное" in text:
        items = await db_get_favs(uid)
        if items:
            await q.message.edit_text(
                f"⭐ <b>Избранное</b> ({len(items)} из {MAX_FAV})\n\n"
                "Нажмите на username чтобы проверить или 🗑 для удаления:",
                reply_markup=kb_favs(items)
            )
        else:
            await q.message.edit_text("⭐ <b>Избранное</b>\n\nСписок пуст.", reply_markup=kb_back())
    else:
        try: await q.message.edit_reply_markup(reply_markup=kb_result(un, False))
        except: pass

# ─── HISTORY ───────────────────────────────────────────────
@router.callback_query(F.data == "menu:history")
async def cb_history(q: CallbackQuery):
    items = await db_history(q.from_user.id)
    if not items:
        await q.message.edit_text(
            "📜 <b>История проверок</b>\n\nПусто. Проверьте любой username.",
            reply_markup=kb_back()
        )
        await q.answer(); return
    lines = ["📜 <b>История проверок</b>\n"]
    for item in items:
        un = item["username"]
        sc = item.get("result", {}).get("score", "—")
        st = item.get("result", {}).get("tg_status", "—")
        dt = str(item.get("created_at",""))[:16]
        lines.append(f"• <code>@{un}</code>  Score <b>{sc}</b>\n  {st} · <i>{dt}</i>")
    lines.append("\n<i>Нажмите для повторной проверки.</i>")
    await q.message.edit_text("\n".join(lines), reply_markup=kb_history(items))
    await q.answer()

# ─── HUNTING ───────────────────────────────────────────────
@router.callback_query(F.data == "menu:hunting")
async def cb_hunting(q: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(St.hunt_length)
    await q.message.edit_text(
        "💎 <b>Username Hunting</b>\n\nШаг 1 / 3 — выберите <b>длину</b>:",
        reply_markup=kb_lengths()
    )
    await q.answer()

@router.callback_query(F.data.startswith("hunt:len:"))
async def cb_hunt_len(q: CallbackQuery, state: FSMContext):
    ln = int(q.data.split(":")[-1])
    await state.update_data(length=ln, no_dig=False, no_un=False)
    await state.set_state(St.hunt_category)
    label = f"{ln} символов" if ln < 9 else "9+ символов"
    await q.message.edit_text(
        f"💎 <b>Username Hunting</b>\n\n✅ Длина: <b>{label}</b>\n\nШаг 2 / 3 — выберите <b>категорию</b>:",
        reply_markup=kb_cats()
    )
    await q.answer()

@router.callback_query(F.data.startswith("hunt:cat:"))
async def cb_hunt_cat(q: CallbackQuery, state: FSMContext):
    cat = q.data.split(":")[-1]
    await state.update_data(category=cat)
    await state.set_state(St.hunt_filters)
    data = await state.get_data()
    ln = data.get("length", 5)
    ll = f"{ln} символов" if ln < 9 else "9+ символов"
    cl = CAT_LABELS.get(cat, cat)
    await q.message.edit_text(
        f"💎 <b>Username Hunting</b>\n\n✅ Длина: <b>{ll}</b>\n✅ Категория: <b>{cl}</b>\n\n"
        "Шаг 3 / 3 — <b>фильтры</b> или сразу «Найти!»:",
        reply_markup=kb_filters(data.get("no_dig",False), data.get("no_un",False))
    )
    await q.answer()

@router.callback_query(F.data.startswith("hunt:filter:"))
async def cb_hunt_filter(q: CallbackQuery, state: FSMContext):
    fname = q.data.split(":")[-1]
    data = await state.get_data()
    if fname == "digits":    data["no_dig"] = not data.get("no_dig", False)
    elif fname == "under":   data["no_un"]  = not data.get("no_un",  False)
    await state.update_data(**data)
    ln = data.get("length", 5); cat = data.get("category","random")
    ll = f"{ln} символов" if ln < 9 else "9+ символов"
    cl = CAT_LABELS.get(cat, cat)
    await q.message.edit_text(
        f"💎 <b>Username Hunting</b>\n\n✅ Длина: <b>{ll}</b>\n✅ Категория: <b>{cl}</b>\n\n"
        "Шаг 3 / 3 — <b>фильтры</b> или сразу «Найти!»:",
        reply_markup=kb_filters(data.get("no_dig",False), data.get("no_un",False))
    )
    await q.answer()

@router.callback_query(F.data == "hunt:search")
async def cb_hunt_search(q: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    ln   = data.get("length", 5)
    cat  = data.get("category","random")
    nd   = data.get("no_dig",  False)
    nu   = data.get("no_un",   False)
    await q.answer("🔍 Генерирую...")
    actual = ln if ln < 9 else random.randint(9, 11)
    cands  = gen_candidates(actual, cat, nd, nu, count=40)
    ranked = rank_candidates(cands, limit=10)
    text   = _fmt_results(ranked, ln, cat, nd, nu)
    await state.set_state(St.hunt_results)
    await q.message.edit_text(text, reply_markup=kb_results(), disable_web_page_preview=True)

@router.callback_query(F.data == "hunt:more")
async def cb_hunt_more(q: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    ln   = data.get("length", 5); cat = data.get("category","random")
    nd   = data.get("no_dig", False); nu = data.get("no_un", False)
    await q.answer("🔄 Ищу ещё...")
    actual = ln if ln < 9 else random.randint(9, 11)
    cands  = gen_candidates(actual, cat, nd, nu, count=40)
    ranked = rank_candidates(cands, limit=10)
    text   = _fmt_results(ranked, ln, cat, nd, nu)
    await q.message.edit_text(text, reply_markup=kb_results(), disable_web_page_preview=True)

@router.callback_query(F.data == "hunt:auto")
async def cb_hunt_auto(q: CallbackQuery):
    await q.answer("🤖 Запускаю авто-поиск...")
    results = auto_hunt(limit=15)
    lines = ["🤖 <b>Авто-поиск · Потенциально ценные username</b>\n",
             "<i>Анализ слов, произношения и рыночных данных.</i>\n","──────────────────────────\n"]
    for i, item in enumerate(results, 1):
        lines.append(f"<b>{i:2}.</b> <code>{item['username']}</code>\n"
                     f"      Score <b>{item['score']}</b>  ·  {item['rarity_label']}\n"
                     f"      💰 {item['price_range']}\n")
    lines.append("\n<i>⚠️ Доступность не гарантирована без проверки.</i>")
    await q.message.edit_text("\n".join(lines), reply_markup=kb_back())

def _fmt_results(ranked, ln, cat, nd, nu):
    cl = CAT_LABELS.get(cat, cat)
    ll = f"{ln} симв" if ln < 9 else "9+ симв"
    fs = " · ".join(filter(None, ["без цифр" if nd else "", "без _" if nu else ""])) or "без фильтров"
    lines = [f"💎 <b>Результаты Hunting</b>\n",
             f"📐 <b>Длина:</b> {ll}  🏷 <b>Категория:</b> {cl}\n",
             f"⚙️ <b>Фильтры:</b> {fs}\n","──────────────────────────\n"]
    if not ranked:
        lines.append("😔 Ничего не найдено. Попробуйте другие параметры.")
        return "\n".join(lines)
    for i, item in enumerate(ranked, 1):
        lines.append(f"<b>{i:2}.</b> <code>{item['username']}</code>\n"
                     f"      Score <b>{item['score']}</b>  ·  {item['rarity_label']}\n"
                     f"      💰 {item['price_range']}\n")
    lines.append("\n<i>⚠️ Доступность не проверена через Telegram.</i>")
    return "\n".join(lines)

# ─── ADMIN ─────────────────────────────────────────────────
def is_admin(uid): return uid in ADMIN_IDS

@router.message(Command("admin"))
async def cmd_admin(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id):
        await msg.answer("⛔ Нет доступа."); return
    await state.clear()
    await msg.answer("🛡 <b>BLADE — Администрирование</b>\n\nВыберите действие:", reply_markup=kb_admin())

@router.callback_query(F.data == "admin:menu")
async def cb_admin_menu(q: CallbackQuery, state: FSMContext):
    if not is_admin(q.from_user.id):
        await q.answer("⛔ Нет доступа.", show_alert=True); return
    await state.clear()
    await q.message.edit_text("🛡 <b>BLADE — Администрирование</b>\n\nВыберите действие:", reply_markup=kb_admin())
    await q.answer()

@router.callback_query(F.data == "admin:stats")
async def cb_admin_stats(q: CallbackQuery):
    if not is_admin(q.from_user.id): await q.answer("⛔", show_alert=True); return
    s = await db_stats()
    await q.message.edit_text(
        f"📊 <b>Статистика</b>\n\n"
        f"👥 Пользователей: <b>{s['total_users']}</b>\n"
        f"🔎 Проверок: <b>{s['total_checks']}</b>\n"
        f"⚡ Активны сегодня: <b>{s['active_today']}</b>\n"
        f"🚫 Заблокировано: <b>{s['blocked']}</b>",
        reply_markup=kb_admin_back()
    )
    await q.answer()

@router.callback_query(F.data == "admin:users")
async def cb_admin_users(q: CallbackQuery):
    if not is_admin(q.from_user.id): await q.answer("⛔", show_alert=True); return
    users = await db_all_users()
    lines = [f"👥 <b>Пользователи</b> ({len(users)}):\n"]
    for u in users[:30]:
        bl = " 🚫" if u.get("is_blocked") else ""
        un = f"@{u['username']}" if u.get("username") else "—"
        lines.append(f"• <code>{u['telegram_id']}</code> {u.get('first_name','—')} {un}{bl}")
    if len(users) > 30: lines.append(f"\n<i>...и ещё {len(users)-30}</i>")
    await q.message.edit_text("\n".join(lines), reply_markup=kb_admin_back())
    await q.answer()

@router.callback_query(F.data == "admin:broadcast")
async def cb_admin_broadcast(q: CallbackQuery, state: FSMContext):
    if not is_admin(q.from_user.id): await q.answer("⛔", show_alert=True); return
    await state.set_state(St.admin_broadcast)
    await q.message.edit_text(
        "📣 <b>Рассылка</b>\n\nОтправьте текст сообщения.\n/cancel для отмены.",
        reply_markup=kb_admin_back()
    )
    await q.answer()

@router.message(StateFilter(St.admin_broadcast))
async def handle_broadcast(msg: Message, state: FSMContext, bot: Bot):
    if not is_admin(msg.from_user.id): return
    if (msg.text or "").strip() == "/cancel":
        await state.clear(); await msg.answer("Отменено.", reply_markup=kb_admin()); return
    text = msg.text or ""
    if not text.strip(): await msg.answer("⚠️ Пустое сообщение."); return
    users = await db_all_users()
    not_blocked = [u for u in users if not u.get("is_blocked")]
    sent = failed = 0
    sm = await msg.answer(f"📣 Рассылка... 0 / {len(not_blocked)}")
    for i, u in enumerate(not_blocked):
        try: await bot.send_message(u["telegram_id"], text); sent += 1
        except: failed += 1
        if (i+1) % 20 == 0:
            try: await sm.edit_text(f"📣 Рассылка... {i+1} / {len(not_blocked)}")
            except: pass
    await state.clear()
    await sm.edit_text(f"✅ <b>Готово</b>\nОтправлено: <b>{sent}</b>\nОшибок: <b>{failed}</b>")

@router.callback_query(F.data == "admin:block")
async def cb_admin_block(q: CallbackQuery, state: FSMContext):
    if not is_admin(q.from_user.id): await q.answer("⛔", show_alert=True); return
    await state.set_state(St.admin_block_id)
    await q.message.edit_text("🚫 Введите Telegram ID для блокировки:", reply_markup=kb_admin_back())
    await q.answer()

@router.message(StateFilter(St.admin_block_id))
async def handle_block(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    t = (msg.text or "").strip()
    if not t.isdigit(): await msg.answer("⚠️ Введите числовой ID."); return
    tid = int(t)
    if tid in ADMIN_IDS: await msg.answer("⛔ Нельзя заблокировать администратора."); await state.clear(); return
    await db_block(tid)
    await state.clear()
    await msg.answer(f"🚫 <code>{tid}</code> заблокирован.", reply_markup=kb_admin())

@router.callback_query(F.data == "admin:unblock")
async def cb_admin_unblock(q: CallbackQuery, state: FSMContext):
    if not is_admin(q.from_user.id): await q.answer("⛔", show_alert=True); return
    await state.set_state(St.admin_unblock_id)
    await q.message.edit_text("🔓 Введите Telegram ID для разблокировки:", reply_markup=kb_admin_back())
    await q.answer()

@router.message(StateFilter(St.admin_unblock_id))
async def handle_unblock(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    t = (msg.text or "").strip()
    if not t.isdigit(): await msg.answer("⚠️ Введите числовой ID."); return
    await db_unblock(int(t))
    await state.clear()
    await msg.answer(f"🔓 <code>{t}</code> разблокирован.", reply_markup=kb_admin())

@router.callback_query(F.data == "admin:settings")
async def cb_admin_settings(q: CallbackQuery):
    if not is_admin(q.from_user.id): await q.answer("⛔", show_alert=True); return
    await q.message.edit_text(
        f"⚙️ <b>Настройки</b>\n\n"
        f"⏱ Rate limit: <code>{RATE_LIMIT}s</code>\n"
        f"📜 Max history: <code>{MAX_HIST}</code>\n"
        f"⭐ Max favorites: <code>{MAX_FAV}</code>\n"
        f"⏳ Fragment timeout: <code>{FRAG_TIMEOUT}s</code>\n"
        f"🗄 Cache TTL: <code>{CACHE_TTL}s</code>\n\n"
        "<i>Настройки меняются через переменные окружения.</i>",
        reply_markup=kb_admin_back()
    )
    await q.answer()

# ═══════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════
async def main():
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp  = Dispatcher(storage=MemoryStorage())

    await db_init()
    log.info("DB initialised.")

    dp.message.middleware(ThrottlingMiddleware(rate=RATE_LIMIT))
    dp.callback_query.middleware(ThrottlingMiddleware(rate=1.0))
    dp.include_router(router)

    me = await bot.get_me()
    log.info("⚡ BLADE USERWARE REP  @%s  id=%s", me.username, me.id)
    log.info("Admin IDs: %s", ADMIN_IDS)

    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    asyncio.run(main())
