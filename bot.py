import asyncio
import logging
import json
import hashlib
import time
import aiosqlite
import aiohttp
import PyPDF2
import io
from datetime import datetime
from typing import Any, Awaitable, Callable, Dict
from docx import Document
from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
    FSInputFile, BufferedInputFile
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.filters import StateFilter

BOT_TOKEN = "8667653728:AAF3Ekms8refE2-BvS1tgDl03sVuLpvvpx0"
ADMIN_ID = 745613614
DEEPSEEK_API_KEY = "sk-a45c0fa810f4430e8a154955c153070d"
VIP_CHANNEL_URL = "https://t.me/squad_vpotoke"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DB_PATH = 'tokenbot.db'

class ScheduleStates(StatesGroup):
    waiting_for_day = State()
    waiting_for_time_task = State()
    waiting_for_week_parity = State()

class TokenBotDB:
    @staticmethod
    async def init_db():
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    tokens INTEGER DEFAULT 10,
                    referral_code TEXT UNIQUE,
                    referred_by INTEGER,
                    total_earned INTEGER DEFAULT 10,
                    total_spent INTEGER DEFAULT 0,
                    join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_banned INTEGER DEFAULT 0
                )
            ''')
            
            await db.execute('''
                CREATE TABLE IF NOT EXISTS chat_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    role TEXT,
                    content TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            await db.execute('''
                CREATE TABLE IF NOT EXISTS schedule (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    time TEXT,
                    day_of_week INTEGER,
                    task TEXT,
                    week_parity TEXT DEFAULT 'все',
                    enabled INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            await db.execute('''
                CREATE TABLE IF NOT EXISTS schedule_access (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER UNIQUE,
                    start_date TEXT,
                    active INTEGER DEFAULT 1
                )
            ''')
            
            await db.execute('''
                CREATE TABLE IF NOT EXISTS payments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    amount REAL,
                    tokens INTEGER,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    verified_at TIMESTAMP
                )
            ''')
            
            await db.execute('''
                CREATE TABLE IF NOT EXISTS referrals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    referrer_id INTEGER,
                    referred_id INTEGER,
                    bonus_tokens INTEGER DEFAULT 5,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            await db.execute("CREATE INDEX IF NOT EXISTS idx_chat_history_user ON chat_history(user_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_schedule_main ON schedule(user_id, day_of_week, enabled)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_id)")
            
            await db.commit()
        logger.info("База данных инициализирована")

    @staticmethod
    async def get_user(user_id: int):
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
                return await cursor.fetchone()

    @staticmethod
    async def create_user(user_id: int, username: str, first_name: str, referred_by: int = None):
        referral_code = hashlib.md5(f"{user_id}{time.time()}".encode()).hexdigest()[:8].upper()
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT OR REPLACE INTO users (user_id, username, first_name, referral_code, referred_by) VALUES (?, ?, ?, ?, ?)",
                (user_id, username or "", first_name or "User", referral_code, referred_by)
            )
            if referred_by:
                await db.execute("UPDATE users SET tokens = tokens + 5 WHERE user_id = ?", (referred_by,))
            await db.commit()
        return referral_code

    @staticmethod
    async def update_tokens(user_id: int, tokens: int):
        async with aiosqlite.connect(DB_PATH) as db:
            if tokens > 0:
                await db.execute("UPDATE users SET tokens = tokens + ?, total_earned = total_earned + ? WHERE user_id = ?", 
                               (tokens, tokens, user_id))
            else:
                await db.execute("UPDATE users SET tokens = tokens + ?, total_spent = total_spent + ? WHERE user_id = ?", 
                               (tokens, abs(tokens), user_id))
            await db.commit()

    @staticmethod
    async def get_chat_history(user_id: int, limit: int = 20):
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT role, content FROM chat_history WHERE user_id = ? ORDER BY timestamp ASC LIMIT ?",
                (user_id, limit)
            ) as cursor:
                return await cursor.fetchall()

    @staticmethod
    async def save_chat_message(user_id: int, role: str, content: str):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO chat_history (user_id, role, content) VALUES (?, ?, ?)",
                (user_id, role, content)
            )
            await db.commit()

    @staticmethod
    async def get_user_by_referral(code: str):
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT user_id FROM users WHERE referral_code = ?", (code,)) as cursor:
                return await cursor.fetchone()

    @staticmethod
    async def get_referrals_count(user_id: int):
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id = ?", (user_id,)) as cursor:
                result = await cursor.fetchone()
                return result[0] if result else 0

    @staticmethod
    async def get_schedule_tasks(user_id: int):
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT id, time, day_of_week, task, week_parity, enabled FROM schedule WHERE user_id = ? ORDER BY day_of_week, time",
                (user_id,)
            ) as cursor:
                return await cursor.fetchall()

    @staticmethod
    async def add_schedule_task(user_id: int, time: str, day: int, task: str):
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                "INSERT INTO schedule (user_id, time, day_of_week, task) VALUES (?, ?, ?, ?)",
                (user_id, time, day, task)
            )
            await db.commit()
            return cursor.lastrowid

    @staticmethod
    async def delete_schedule_task(task_id: int, user_id: int):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM schedule WHERE id = ? AND user_id = ?", (task_id, user_id))
            await db.commit()

    @staticmethod
    async def update_schedule_week_parity(task_id: int, user_id: int, week_parity: str):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE schedule SET week_parity = ? WHERE id = ? AND user_id = ?", 
                           (week_parity, task_id, user_id))
            await db.commit()

    @staticmethod
    async def toggle_schedule_task(task_id: int, user_id: int):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE schedule SET enabled = NOT enabled WHERE id = ? AND user_id = ?", 
                           (task_id, user_id))
            await db.commit()

    @staticmethod
    async def check_schedule_access(user_id: int):
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT start_date FROM schedule_access WHERE user_id = ? AND active = 1",
                (user_id,)
            ) as cursor:
                result = await cursor.fetchone()
            
            if not result:
                await db.execute(
                    "INSERT OR REPLACE INTO schedule_access (user_id, start_date, active) VALUES (?, date('now'), 1)",
                    (user_id,)
                )
                await db.commit()
                return True, 0, 0
            
            start_date = datetime.strptime(result[0], '%Y-%m-%d').date()
            days_used = (datetime.now().date() - start_date).days
            if days_used < 0:
                days_used = 0
            
            if days_used <= 13:
                return True, 0, days_used
            else:
                return True, 1, days_used

_main_keyboard = None

def get_main_keyboard():
    global _main_keyboard
    if _main_keyboard is None:
        _main_keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="💰 Баланс"), KeyboardButton(text="💳 Купить")],
                [KeyboardButton(text="👥 Рефералы"), KeyboardButton(text="📅 Расписание")],
                [KeyboardButton(text="📚 Команды"), KeyboardButton(text="ℹ️ О боте")],
                [KeyboardButton(text="🧹 Очистить")]
            ],
            resize_keyboard=True
        )
    return _main_keyboard

def get_schedule_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить", callback_data="schedule_add"),
         InlineKeyboardButton(text="❌ Удалить", callback_data="schedule_del")],
        [InlineKeyboardButton(text="🔄 Чёт/Нечет", callback_data="schedule_week"),
         InlineKeyboardButton(text="🔄 Вкл/Выкл", callback_data="schedule_toggle_list")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_main")]
    ])

def get_days_keyboard():
    days = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=day, callback_data=f"add_day_{i}") for i, day in enumerate(days)],
        [InlineKeyboardButton(text="◀️ Отмена", callback_data="schedule")]
    ])

def get_week_parity():
    week_number = datetime.now().isocalendar()[1]
    return "четная" if week_number % 2 == 0 else "нечетная"

def get_week_parity_russian():
    week_number = datetime.now().isocalendar()[1]
    return "Чётная" if week_number % 2 == 0 else "Нечётная"

async def ask_deepseek_stream(prompt: str, history=None, chat_id: int = None, message_id: int = None):
    headers = {
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
        "Content-Type": "application/json"
    }
    
    messages = []
    if history:
        for role, content in history:
            messages.append({"role": role, "content": content})
    messages.append({"role": "user", "content": prompt})
    
    data = {
        "model": "deepseek-chat",
        "messages": messages,
        "max_tokens": 2000,
        "stream": True,
        "temperature": 0.3
    }
    
    start_time = time.time()
    last_update_time = start_time
    
    try:
        if chat_id and message_id:
            try:
                bot = Bot.get_current()
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text="🤔 Думаю... (это может занять до 10 секунд)"
                )
            except:
                pass
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.deepseek.com/v1/chat/completions",
                headers=headers,
                json=data,
                timeout=aiohttp.ClientTimeout(total=25)
            ) as response:
                response.raise_for_status()
                
                full_response = ""
                buffer = ""
                finish_reason = None
                first_token_received = False
                
                async for line in response.content:
                    if time.time() - start_time > 20:
                        logger.warning("Превышено время ожидания ответа DeepSeek")
                        break
                    
                    line = line.decode('utf-8').strip()
                    if not line or not line.startswith("data: "):
                        continue
                    
                    chunk = line[6:]
                    if chunk == "[DONE]":
                        break
                    
                    try:
                        obj = json.loads(chunk)
                        choices = obj.get("choices", [{}])
                        
                        if choices and choices[0].get("finish_reason"):
                            finish_reason = choices[0]["finish_reason"]
                        
                        delta = choices[0]["delta"].get("content", "")
                        
                        if delta:
                            if not first_token_received:
                                first_token_received = True
                                start_time = time.time()
                                if chat_id and message_id:
                                    try:
                                        bot = Bot.get_current()
                                        await bot.edit_message_text(
                                            chat_id=chat_id,
                                            message_id=message_id,
                                            text="▌"
                                        )
                                    except:
                                        pass
                            
                            full_response += delta
                            buffer += delta
                            
                            if len(buffer) >= 30 and chat_id and message_id:
                                try:
                                    bot = Bot.get_current()
                                    await bot.edit_message_text(
                                        chat_id=chat_id,
                                        message_id=message_id,
                                        text=full_response + "▌"
                                    )
                                    buffer = ""
                                    last_update_time = time.time()
                                except:
                                    pass
                            
                            if time.time() - last_update_time > 5:
                                try:
                                    bot = Bot.get_current()
                                    await bot.edit_message_text(
                                        chat_id=chat_id,
                                        message_id=message_id,
                                        text=full_response + "\n\n⏳ Генерация идёт долго, но я ещё работаю..."
                                    )
                                except:
                                    pass
                                
                    except json.JSONDecodeError:
                        continue
                
                if not first_token_received:
                    if chat_id and message_id:
                        try:
                            bot = Bot.get_current()
                            await bot.edit_message_text(
                                chat_id=chat_id,
                                message_id=message_id,
                                text="😔 DeepSeek не отвечает. Попробуй позже."
                            )
                        except:
                            pass
                    return "😔 DeepSeek не отвечает. Попробуй позже."
                
                if chat_id and message_id:
                    final_text = full_response
                    if finish_reason == "length":
                        final_text += "\n\n⚠️ Ответ обрезан из-за ограничения длины"
                    
                    try:
                        bot = Bot.get_current()
                        await bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=message_id,
                            text=final_text
                        )
                    except:
                        pass
                
                return full_response
                
    except asyncio.TimeoutError:
        logger.error("Таймаут при запросе к DeepSeek")
        if chat_id and message_id:
            try:
                bot = Bot.get_current()
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text="⏱️ Превышено время ожидания (25 сек). DeepSeek временно недоступен."
                )
            except:
                pass
        return "⏱️ Превышено время ожидания. DeepSeek временно недоступен."
        
    except Exception as e:
        logger.error(f"DeepSeek stream error: {e}")
        if chat_id and message_id:
            try:
                bot = Bot.get_current()
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text="😔 Произошла ошибка при обращении к DeepSeek"
                )
            except:
                pass
        return "😔 Произошла ошибка при обращении к DeepSeek"

async def send_schedule_to_user(bot: Bot, user_id: int):
    try:
        today = datetime.now().strftime("%d.%m.%Y")
        day_of_week = datetime.now().weekday()
        week_parity = get_week_parity()
        days = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"]
        
        has_access, fee, days_used = await TokenBotDB.check_schedule_access(user_id)
        
        if fee > 0:
            user = await TokenBotDB.get_user(user_id)
            if not user or user[3] < 1:
                await bot.send_message(
                    chat_id=user_id,
                    text="❌ Недостаточно токенов для использования расписания. Пополни баланс через /buy",
                    reply_markup=get_main_keyboard()
                )
                return
            await TokenBotDB.update_tokens(user_id, -1)
        
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT time, task, week_parity FROM schedule WHERE user_id = ? AND day_of_week = ? AND enabled = 1 ORDER BY time",
                (user_id, day_of_week)
            ) as cursor:
                all_tasks = await cursor.fetchall()
        
        today_tasks = []
        for task_time, task, task_week_parity in all_tasks:
            if task_week_parity == "все" or task_week_parity == week_parity:
                today_tasks.append((task_time, task))
        
        free_days_left = max(0, 14 - days_used) if days_used < 14 else 0
        
        if today_tasks:
            schedule_text = f"🌅 Доброе утро! Твоё расписание на {today} ({days[day_of_week]}, {week_parity} нед.)\n\n"
            for task_time, task in today_tasks:
                schedule_text += f"⏰ {task_time} - {task}\n"
            
            if free_days_left > 0:
                schedule_text += f"\n📅 Бесплатных дней осталось: {free_days_left}"
            else:
                schedule_text += f"\n💰 Списано 1 токен за сегодня"
            
            schedule_text += "\n\nХорошего дня! 🔥"
            
            await bot.send_message(
                chat_id=user_id,
                text=schedule_text,
                reply_markup=get_main_keyboard()
            )
            logger.info(f"Утреннее расписание отправлено пользователю {user_id}")
        
    except Exception as e:
        logger.error(f"Ошибка при отправке расписания пользователю {user_id}: {e}")

async def schedule_checker(bot: Bot):
    last_sent_date = None
    while True:
        now = datetime.now()
        if now.hour == 9 and now.minute == 0 and last_sent_date != now.date():
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute(
                    "SELECT DISTINCT user_id FROM schedule WHERE enabled = 1"
                ) as cursor:
                    users = await cursor.fetchall()
            
            for user in users:
                user_id = user[0]
                try:
                    await send_schedule_to_user(bot, user_id)
                    await asyncio.sleep(0.5)
                except Exception as e:
                    logger.error(f"Не удалось отправить расписание пользователю {user_id}: {e}")
            
            last_sent_date = now.date()
        await asyncio.sleep(60)

class RateLimitMiddleware:
    def __init__(self, rate_limit: int = 1):
        self.rate_limit = rate_limit
        self.last_time = {}

    async def __call__(self, handler: Callable, event: types.Message, data: Dict[str, Any]) -> Any:
        user_id = event.from_user.id
        current_time = time.time()
        
        if user_id in self.last_time and current_time - self.last_time[user_id] < self.rate_limit:
            await event.answer("Подожди секунду!")
            return
        
        self.last_time[user_id] = current_time
        return await handler(event, data)

async def main():
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    
    dp.message.middleware(RateLimitMiddleware())
    
    @dp.message(CommandStart())
    async def cmd_start(message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        username = message.from_user.username or ""
        first_name = message.from_user.first_name or "User"
        
        await state.clear()
        
        args = message.text.split()
        referred_by = None
        if len(args) > 1:
            ref_code = args[1]
            referrer = await TokenBotDB.get_user_by_referral(ref_code)
            if referrer and referrer[0] != user_id:
                referred_by = referrer[0]
        
        user = await TokenBotDB.get_user(user_id)
        if user:
            if user[10]:
                await message.answer("Ты забанен!")
                return
            
            welcome_text = f"""С возвращением, {first_name}!

💰 Твой баланс: {user[3]} токенов
📊 Всего заработано: {user[6]} токенов
💬 Я помню последние 100 сообщений
📁 Могу читать файлы (PDF, Word, TXT)
📅 Можешь создать своё личное расписание
⚡️ Асинхронная версия на aiogram

VIP канал: {VIP_CHANNEL_URL}

Как это работает:
• 1 сообщение = 1 токен
• Загружай файлы - я прочитаю и отвечу
• Приводи друзей (+5 токенов)
• Покупай токены через Stars
• Управляй своим расписанием через 📅 Расписание
• Расписание: 14 дней бесплатно, потом 1 токен/день

Используй кнопки внизу для навигации! 👇"""
        else:
            ref_code = await TokenBotDB.create_user(user_id, username, first_name, referred_by)
            
            welcome_text = f"""Добро пожаловать в TokenBot, {first_name}!

🎁 Бонус: 10 бесплатных токенов!
🔗 Твой реферальный код: {ref_code}
💬 Я помню последние 100 сообщений
📁 Могу читать файлы (PDF, Word, TXT)
📅 Можешь создать своё личное расписание
⚡️ Асинхронная версия на aiogram

Как это работает:
• 10 токенов уже на твоем счету
• 1 сообщение = 1 токен
• Загружай файлы - я прочитаю и отвечу
• Приводи друзей (+5 токенов)
• Управляй своим расписанием через 📅 Расписание
• Расписание: 14 дней бесплатно, потом 1 токен/день

VIP канал: {VIP_CHANNEL_URL}

Используй кнопки внизу для навигации! 👇"""
            
            if referred_by:
                welcome_text += "\n🎊 Твой друг получил 5 токенов!"
        
        await message.answer(welcome_text, reply_markup=get_main_keyboard())

    @dp.message(F.text == "💰 Баланс")
    @dp.message(Command("balance"))
    async def cmd_balance(message: types.Message):
        user = await TokenBotDB.get_user(message.from_user.id)
        if not user:
            await message.answer("Используй /start")
            return
        
        referrals = await TokenBotDB.get_referrals_count(user[0])
        
        bot_username = (await message.bot.me()).username
        text = f"""💰 Твой кошелек

💎 Баланс: {user[3]} токенов
📈 Заработано: {user[6]} токенов
📉 Потрачено: {user[7]} токенов
👥 Рефералов: {referrals}

🔗 Твой код: {user[4]}
📱 Ссылка: https://t.me/{bot_username}?start={user[4]}

Приводи друзей и получай +5 токенов!"""
        
        await message.answer(text, reply_markup=get_main_keyboard())

    @dp.message(F.text == "💳 Купить")
    @dp.message(Command("buy"))
    async def cmd_buy(message: types.Message):
        text = """💳 Магазин токенов

🎯 Предложения:
• 100 токенов - 10 ⭐
• 500 токенов - 45 ⭐ (скидка 10%)
• 1000 токенов - 80 ⭐ (скидка 20%)
• 2000 токенов - 150 ⭐ (скидка 25%)

Оплата через Telegram Stars

Выбери пакет:"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="100 - 10⭐", callback_data="buy_100"),
             InlineKeyboardButton(text="500 - 45⭐", callback_data="buy_500")],
            [InlineKeyboardButton(text="1000 - 80⭐", callback_data="buy_1000"),
             InlineKeyboardButton(text="2000 - 150⭐", callback_data="buy_2000")]
        ])
        
        await message.answer(text, reply_markup=keyboard)

    @dp.message(F.text == "👥 Рефералы")
    @dp.message(Command("referral"))
    async def cmd_referral(message: types.Message):
        user = await TokenBotDB.get_user(message.from_user.id)
        if not user:
            await message.answer("Сначала используй /start")
            return
        
        referrals = await TokenBotDB.get_referrals_count(user[0])
        bot_username = (await message.bot.me()).username
        referral_link = f"https://t.me/{bot_username}?start={user[4]}"
        
        text = f"""🔗 Твоя реферальная ссылка

{referral_link}

Статистика:
👥 Приглашено: {referrals}
💰 Заработано: {referrals * 5} токенов

Как это работает:
• Отправляй ссылку друзьям
• Они регистрируются
• Ты получаешь +5 токенов
• Без ограничений!"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📱 Поделиться", url=f"https://t.me/share/url?url=https://t.me/{bot_username}?start={user[4]}&text=Заходи в этого бота! Тут можно общаться с ИИ за токены")]
        ])
        
        await message.answer(text, reply_markup=keyboard)

    @dp.message(F.text == "📚 Команды")
    @dp.message(Command("help"))
    async def cmd_help(message: types.Message):
        text = f"""📚 Список всех команд

👤 Основные команды:
/start - Запустить бота и регистрация
/balance - Проверить баланс токенов
/help - Показать это меню

💰 Покупка токенов:
/buy - Открыть магазин токенов

📅 Твоё личное расписание:
/schedule - Управление расписанием
• 14 дней бесплатно
• Потом 1 токен/день
• Чётные/нечётные недели

📁 Файлы:
(просто отправь PDF, DOCX или TXT файл)

👥 Реферальная система:
/referral - Твоя реферальная ссылка

VIP канал: {VIP_CHANNEL_URL}

👇 Используй кнопки внизу!"""
        
        await message.answer(text, reply_markup=get_main_keyboard())

    @dp.message(F.text == "ℹ️ О боте")
    @dp.message(Command("about"))
    async def cmd_about(message: types.Message):
        text = f"""🤖 О боте

Название: TokenBot
Версия: 6.0 (асинхронная)
Язык: Python + aiogram 3.x

Возможности:
• Умные ответы через DeepSeek AI (⚡️ стриминг)
• Чтение файлов (PDF, DOCX, TXT)
• Память на 100 сообщений
• Реферальная система (+5 токенов)
• Индивидуальное расписание для каждого
• Чётные/нечётные недели
• 14 дней бесплатно, потом 1 токен/день
• Оплата через Telegram Stars
• 🚀 Асинхронная обработка

Статистика:
• 1 сообщение = 1 токен
• Приведи друга = +5 токенов

VIP канал: {VIP_CHANNEL_URL}

Приятного использования! 🚀"""
        
        await message.answer(text, reply_markup=get_main_keyboard())

    @dp.message(F.text == "🧹 Очистить")
    @dp.message(Command("clear"))
    async def cmd_clear(message: types.Message):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM chat_history WHERE user_id = ?", (message.from_user.id,))
            await db.commit()
        
        await message.answer("🧹 История диалога очищена!", reply_markup=get_main_keyboard())

    @dp.message(F.text == "📅 Расписание")
    @dp.message(Command("schedule"))
    async def cmd_schedule(message: types.Message, state: FSMContext):
        await state.clear()
        user_id = message.from_user.id
        
        has_access, fee, days_used = await TokenBotDB.check_schedule_access(user_id)
        
        week_parity = get_week_parity_russian()
        days = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"]
        today = days[datetime.now().weekday()]
        
        async with aiosqlite.connect(DB_PATH) as db:
            day_of_week = datetime.now().weekday()
            async with db.execute(
                "SELECT time, task, week_parity FROM schedule WHERE user_id = ? AND day_of_week = ? AND enabled = 1 ORDER BY time",
                (user_id, day_of_week)
            ) as cursor:
                today_tasks_raw = await cursor.fetchall()
            
            today_tasks = []
            current_week_parity = get_week_parity()
            for task_time, task, task_week_parity in today_tasks_raw:
                if task_week_parity == "все" or task_week_parity == current_week_parity:
                    today_tasks.append((task_time, task))
            
            async with db.execute(
                "SELECT id, time, day_of_week, task, week_parity, enabled FROM schedule WHERE user_id = ? ORDER BY day_of_week, time",
                (user_id,)
            ) as cursor:
                all_tasks = await cursor.fetchall()
        
        free_days_left = max(0, 14 - days_used) if days_used < 14 else 0
        
        text = f"📅 ТВОЁ РАСПИСАНИЕ\n"
        text += f"└ Сегодня: {today}, {week_parity} неделя\n"
        
        if free_days_left > 0:
            text += f"└ Бесплатно: {free_days_left} дн.\n"
        
        if today_tasks:
            text += "\n🔹 НА СЕГОДНЯ:\n"
            for task_time, task in today_tasks:
                text += f"   ⏰ {task_time} - {task}\n"
        else:
            text += "\n🔹 НА СЕГОДНЯ: задач нет\n"
        
        text += "\n📋 ВСЕ ЗАДАЧИ:\n"
        if all_tasks:
            for task_id, task_time, task_day, task, week_parity_task, enabled in all_tasks:
                status = "✅" if enabled else "❌"
                week_mark = ""
                if week_parity_task == "четная":
                    week_mark = " [ч]"
                elif week_parity_task == "нечетная":
                    week_mark = " [нч]"
                text += f"{status} ID{task_id}: {days[task_day]} {task_time} - {task}{week_mark}\n"
        else:
            text += "   У тебя пока нет задач\n"
        
        await message.answer(text, reply_markup=get_schedule_keyboard())

    @dp.callback_query(F.data == "schedule_add")
    async def schedule_add_start(callback: types.CallbackQuery, state: FSMContext):
        await callback.message.edit_text(
            "Выбери день недели:",
            reply_markup=get_days_keyboard()
        )
        await callback.answer()

    @dp.callback_query(F.data.startswith("add_day_"))
    async def schedule_add_day(callback: types.CallbackQuery, state: FSMContext):
        day = int(callback.data.split("_")[2])
        await state.update_data(day=day)
        await state.set_state(ScheduleStates.waiting_for_time_task)
        
        await callback.message.edit_text(
            "Отправь время и задачу в формате:\nЧЧ:ММ Название задачи\n\nНапример:\n09:00 Подъём"
        )
        await callback.answer()

    @dp.message(ScheduleStates.waiting_for_time_task)
    async def schedule_add_task(message: types.Message, state: FSMContext):
        data = await state.get_data()
        day = data.get('day')
        
        parts = message.text.strip().split(' ', 1)
        if len(parts) != 2:
            await message.answer("❌ Неверный формат. Используй: ЧЧ:ММ Название")
            return
        
        task_time, task = parts
        
        try:
            datetime.strptime(task_time, "%H:%M")
        except:
            await message.answer("❌ Неверный формат времени. Используй ЧЧ:ММ")
            return
        
        task_id = await TokenBotDB.add_schedule_task(message.from_user.id, task_time, day, task)
        await state.clear()
        
        await message.answer(f"✅ Задача добавлена с ID {task_id}!")
        await cmd_schedule(message, state)

    @dp.callback_query(F.data == "schedule_del")
    async def schedule_del_menu(callback: types.CallbackQuery):
        tasks = await TokenBotDB.get_schedule_tasks(callback.from_user.id)
        
        if not tasks:
            await callback.answer("Нет задач для удаления")
            return
        
        days = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"]
        keyboard = InlineKeyboardBuilder()
        
        for task_id, task_time, task_day, task, week_parity, enabled in tasks:
            week_mark = " [ч]" if week_parity == "четная" else " [нч]" if week_parity == "нечетная" else ""
            btn_text = f"ID{task_id}: {days[task_day]} {task_time} - {task}{week_mark}"
            keyboard.button(text=btn_text, callback_data=f"del_task_{task_id}")
        
        keyboard.button(text="◀️ Назад", callback_data="schedule")
        keyboard.adjust(1)
        
        await callback.message.edit_text(
            "Выбери задачу для удаления:",
            reply_markup=keyboard.as_markup()
        )
        await callback.answer()

    @dp.callback_query(F.data.startswith("del_task_"))
    async def schedule_del_confirm(callback: types.CallbackQuery):
        task_id = int(callback.data.split("_")[2])
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да", callback_data=f"del_yes_{task_id}"),
             InlineKeyboardButton(text="❌ Нет", callback_data="schedule_del")]
        ])
        
        await callback.message.edit_text(
            f"Точно удалить задачу ID {task_id}?",
            reply_markup=keyboard
        )
        await callback.answer()

    @dp.callback_query(F.data.startswith("del_yes_"))
    async def schedule_del_execute(callback: types.CallbackQuery):
        task_id = int(callback.data.split("_")[2])
        await TokenBotDB.delete_schedule_task(task_id, callback.from_user.id)
        
        await callback.answer("✅ Задача удалена")
        await cmd_schedule(callback.message, None)

    @dp.callback_query(F.data == "schedule_week")
    async def schedule_week_menu(callback: types.CallbackQuery):
        tasks = await TokenBotDB.get_schedule_tasks(callback.from_user.id)
        
        if not tasks:
            await callback.answer("Сначала добавь задачи")
            return
        
        days = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"]
        keyboard = InlineKeyboardBuilder()
        
        for task_id, task_time, task_day, task, week_parity, enabled in tasks:
            week_mark = " [ч]" if week_parity == "четная" else " [нч]" if week_parity == "нечетная" else ""
            btn_text = f"ID{task_id}: {days[task_day]} {task_time} - {task}{week_mark}"
            keyboard.button(text=btn_text, callback_data=f"week_task_{task_id}")
        
        keyboard.button(text="◀️ Назад", callback_data="schedule")
        keyboard.adjust(1)
        
        await callback.message.edit_text(
            "Выбери задачу для настройки чётности/нечётности недели:",
            reply_markup=keyboard.as_markup()
        )
        await callback.answer()

    @dp.callback_query(F.data.startswith("week_task_"))
    async def schedule_week_set(callback: types.CallbackQuery):
        task_id = int(callback.data.split("_")[2])
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📅 Каждую", callback_data=f"week_set_{task_id}_все"),
             InlineKeyboardButton(text="🔢 Чётную", callback_data=f"week_set_{task_id}_четная")],
            [InlineKeyboardButton(text="🔢 Нечётную", callback_data=f"week_set_{task_id}_нечетная"),
             InlineKeyboardButton(text="◀️ Назад", callback_data="schedule_week")]
        ])
        
        await callback.message.edit_text(
            "На какой неделе выполнять эту задачу?",
            reply_markup=keyboard
        )
        await callback.answer()

    @dp.callback_query(F.data.startswith("week_set_"))
    async def schedule_week_save(callback: types.CallbackQuery):
        parts = callback.data.split("_")
        task_id = int(parts[2])
        week_parity = parts[3]
        
        await TokenBotDB.update_schedule_week_parity(task_id, callback.from_user.id, week_parity)
        await callback.answer("✅ Настройка сохранена")
        await cmd_schedule(callback.message, None)

    @dp.callback_query(F.data == "schedule_toggle_list")
    async def schedule_toggle_list(callback: types.CallbackQuery):
        tasks = await TokenBotDB.get_schedule_tasks(callback.from_user.id)
        
        if not tasks:
            await callback.answer("Нет задач")
            return
        
        days = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"]
        keyboard = InlineKeyboardBuilder()
        
        for task_id, task_time, task_day, task, week_parity, enabled in tasks:
            status = "✅" if enabled else "❌"
            week_mark = " [ч]" if week_parity == "четная" else " [нч]" if week_parity == "нечетная" else ""
            btn_text = f"{status} ID{task_id}: {days[task_day]} {task_time} - {task}{week_mark}"
            keyboard.button(text=btn_text, callback_data=f"toggle_task_{task_id}")
        
        keyboard.button(text="◀️ Назад", callback_data="schedule")
        keyboard.adjust(1)
        
        await callback.message.edit_text(
            "Выбери задачу для включения/выключения:",
            reply_markup=keyboard.as_markup()
        )
        await callback.answer()

    @dp.callback_query(F.data.startswith("toggle_task_"))
    async def schedule_toggle_task(callback: types.CallbackQuery):
        task_id = int(callback.data.split("_")[2])
        await TokenBotDB.toggle_schedule_task(task_id, callback.from_user.id)
        await callback.answer("✅ Статус изменён")
        await schedule_toggle_list(callback)

    @dp.callback_query(F.data == "back_to_main")
    async def back_to_main(callback: types.CallbackQuery):
        await callback.message.delete()
        await callback.message.answer("Главное меню:", reply_markup=get_main_keyboard())
        await callback.answer()

    @dp.callback_query(F.data == "schedule")
    async def schedule_callback(callback: types.CallbackQuery):
        await callback.message.delete()
        await cmd_schedule(callback.message, None)

    @dp.callback_query(F.data.startswith("buy_"))
    async def buy_callback(callback: types.CallbackQuery):
        packages = {
            'buy_100': {'tokens': 100, 'stars': 10},
            'buy_500': {'tokens': 500, 'stars': 45},
            'buy_1000': {'tokens': 1000, 'stars': 80},
            'buy_2000': {'tokens': 2000, 'stars': 150}
        }
        
        if callback.data not in packages:
            await callback.answer("Неверный пакет")
            return
        
        package = packages[callback.data]
        
        prices = [{"label": f"{package['tokens']} Токенов", "amount": package['stars']}]
        
        await callback.bot.send_invoice(
            chat_id=callback.from_user.id,
            title=f"{package['tokens']} Токенов",
            description=f"Покупка {package['tokens']} токенов",
            payload=f"tokens_{package['tokens']}",
            provider_token="",
            currency="XTR",
            prices=prices,
            start_parameter="buy_tokens"
        )
        
        await callback.answer()

    @dp.pre_checkout_query()
    async def pre_checkout_handler(pre_checkout_q: types.PreCheckoutQuery):
        await pre_checkout_q.answer(ok=True)

    @dp.message(F.successful_payment)
    async def successful_payment_handler(message: types.Message):
        user_id = message.from_user.id
        payload = message.successful_payment.invoice_payload
        tokens = int(payload.split('_')[1])
        
        await TokenBotDB.update_tokens(user_id, tokens)
        
        await message.answer(
            f"✅ Оплата прошла успешно!\n💰 Начислено: {tokens} токенов",
            reply_markup=get_main_keyboard()
        )

    @dp.message(F.document)
    async def handle_document(message: types.Message):
        user_id = message.from_user.id
        user = await TokenBotDB.get_user(user_id)
        
        if not user:
            await message.answer("Сначала зарегистрируйся через /start")
            return
        
        if user[10]:
            await message.answer("Ты забанен!")
            return
        
        if user[3] < 1:
            await message.answer("❌ Недостаточно токенов! Купи через /buy")
            return
        
        await message.answer("📥 Получаю файл, подожди...")
        
        file = await message.bot.get_file(message.document.file_id)
        file_path = file.file_path
        file_name = message.document.file_name
        file_ext = file_name.split('.')[-1].lower()
        
        file_content = await message.bot.download_file(file_path)
        file_bytes = file_content.read()
        
        text = ""
        
        if file_ext == 'pdf':
            pdf_reader = PyPDF2.PdfReader(io.BytesIO(file_bytes))
            for page in pdf_reader.pages:
                text += page.extract_text()
        elif file_ext == 'docx':
            doc = Document(io.BytesIO(file_bytes))
            text = '\n'.join([para.text for para in doc.paragraphs])
        elif file_ext == 'txt':
            text = file_bytes.decode('utf-8')
        else:
            await message.answer("Поддерживаются только PDF, DOCX и TXT файлы")
            return
        
        if len(text) > 4000:
            text = text[:4000] + "..."
        
        history = await TokenBotDB.get_chat_history(user_id, 20)
        
        sent_msg = await message.answer("▌")
        
        response = await ask_deepseek_stream(
            prompt=f"Содержимое файла:\n\n{text}\n\nОтветь на основе этого файла или просто проанализируй его.",
            history=history,
            chat_id=message.chat.id,
            message_id=sent_msg.message_id
        )
        
        await TokenBotDB.update_tokens(user_id, -1)
        await TokenBotDB.save_chat_message(user_id, "user", f"[Файл: {file_name}]")
        await TokenBotDB.save_chat_message(user_id, "assistant", response)

    @dp.message(F.text)
    async def handle_text(message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        user = await TokenBotDB.get_user(user_id)
        
        if not user:
            await cmd_start(message, state)
            return
        
        if user[10]:
            await message.answer("Ты забанен!")
            return
        
        if message.text.startswith('/'):
            return
        
        if user[3] < 1:
            await message.answer("❌ Недостаточно токенов! Купи через /buy")
            return
        
        history = await TokenBotDB.get_chat_history(user_id, 20)
        
        sent_msg = await message.answer("▌")
        
        response = await ask_deepseek_stream(
            prompt=message.text,
            history=history,
            chat_id=message.chat.id,
            message_id=sent_msg.message_id
        )
        
        await TokenBotDB.update_tokens(user_id, -1)
        await TokenBotDB.save_chat_message(user_id, "user", message.text)
        await TokenBotDB.save_chat_message(user_id, "assistant", response)

    @dp.message(Command("admin"))
    async def cmd_admin(message: types.Message):
        if message.from_user.id != ADMIN_ID:
            await message.answer("Только для админа!")
            return
        
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT COUNT(*) FROM users") as cursor:
                users = (await cursor.fetchone())[0]
            async with db.execute("SELECT SUM(tokens) FROM users") as cursor:
                tokens = (await cursor.fetchone())[0] or 0
            async with db.execute("SELECT COUNT(*) FROM schedule") as cursor:
                schedule_count = (await cursor.fetchone())[0]
            async with db.execute("SELECT COUNT(*) FROM payments WHERE status='pending'") as cursor:
                pending_payments = (await cursor.fetchone())[0]
            async with db.execute("SELECT COUNT(*) FROM users WHERE is_banned=1") as cursor:
                banned_users = (await cursor.fetchone())[0]
        
        text = f"""📊 **АДМИН ПАНЕЛЬ**

👥 Пользователи: {users}
💰 Всего токенов: {tokens}
📅 Задач в расписании: {schedule_count}
⏳ Ожидают платежей: {pending_payments}
🚫 Забанено: {banned_users}

Обновлено: {datetime.now().strftime('%d/%m/%Y %H:%M')}"""
        
        await message.answer(text)
    
    await TokenBotDB.init_db()
    asyncio.create_task(schedule_checker(bot))
    logger.info("Бот запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
