import os
from dotenv import load_dotenv

load_dotenv()
# ==================== ВАЛИДАЦИЯ ENV ====================
REQUIRED_ENV = [
    "API_TOKEN",
    "SUPER_ADMIN_ID",
    "ADMIN_CHAT_ID",
    "WEBAPP_URL",
    "HOSTING_FTP_HOST",
    "HOSTING_FTP_USER",
    "HOSTING_FTP_PASS",
]
for key in REQUIRED_ENV:
    if not os.getenv(key):
        raise RuntimeError(f"❌ Переменная окружения {key} не найдена (.env)")

import json
import logging
import asyncio
import io
import sqlite3
import csv
import re
import requests

from datetime import datetime, timedelta
from ftplib import FTP
from collections import defaultdict
from contextlib import contextmanager
from typing import Optional, Dict, Any, List
from urllib.request import urlopen
from urllib.error import URLError, HTTPError

from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    WebAppInfo,
    ContentType,
    ReplyKeyboardRemove,
    BufferedInputFile,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    TelegramObject,
)
from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram import BaseMiddleware
from typing import Callable, Awaitable

# ==== PDF / QR ====
import qrcode
import textwrap
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib.utils import ImageReader
from reportlab.lib import colors
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from PIL import Image

# ==================== НАСТРОЙКИ АДМИНИСТРАТОРОВ ====================

class AdminRole:
    """Роли администраторов"""
    SUPER_ADMIN = "super_admin"
    SALES = "sales"

# Загрузка ID администраторов из .env
SUPER_ADMIN_ID = int(os.getenv("SUPER_ADMIN_ID"))
SALES_ADMIN_IDS = [int(x.strip()) for x in os.getenv("SALES_ADMIN_IDS", "").split(",") if x.strip()]

# Объединенный список всех админов для rate limiting
ALL_ADMIN_IDS = [SUPER_ADMIN_ID] + SALES_ADMIN_IDS

# Названия категорий для отображения
CATEGORY_NAMES = {
    "cleaning": "Моющие средства",
    "plasticpe": "Вдувные ПЭ",
    "plasticpet": "ПЭТ",
    "plasticpp": "ПП",
    "plastictd": "Распылители & Дозаторы",
    "chemicals": "Химикаты",
    "fragrances": "Отдушки",
}

# Функция проверки прав доступа
def has_permission(user_id: int, required_role: str) -> bool:
    """Проверяет, есть ли у пользователя права для выполнения действия"""
    # Супер-админ имеет доступ ко всему
    if user_id == SUPER_ADMIN_ID:
        return True
    
    if required_role == AdminRole.SALES:
        return user_id in SALES_ADMIN_IDS
    
    return False


def get_admin_name(user_id: int) -> str:
    """Возвращает роль администратора"""
    if user_id == SUPER_ADMIN_ID:
        return "Супер-админ"
    elif user_id in SALES_ADMIN_IDS:
        return "Отдел продаж"
    return f"Админ {user_id}"


def get_order_category(order_items: list) -> str:
    """Определяет категорию заказа на основе товаров (первого товара)"""
    if not order_items:
        return None
    
    # Получаем ID первого товара
    first_item_id = order_items[0].get("id", 0)
    
    # Определяем категорию по диапазону ID
    if 10000 <= first_item_id < 20000:
        return "cleaning"
    elif 20000 <= first_item_id < 30000:
        return "plasticpe"
    elif 30000 <= first_item_id < 40000:
        return "plasticpet"
    elif 40000 <= first_item_id < 50000:
        return "plasticpp"
    elif 50000 <= first_item_id < 60000:
        return "plastictd"
    elif 60000 <= first_item_id < 70000:
        return "chemicals"
    elif 70000 <= first_item_id < 80000:
        return "fragrances"
    
    return None


def get_category_by_item_id(item_id: int) -> str:
    """Определяет категорию по ID товара"""
    if 10000 <= item_id < 20000:
        return "cleaning"
    elif 20000 <= item_id < 30000:
        return "plasticpe"
    elif 30000 <= item_id < 40000:
        return "plasticpet"
    elif 40000 <= item_id < 50000:
        return "plasticpp"
    elif 50000 <= item_id < 60000:
        return "plastictd"
    elif 60000 <= item_id < 70000:
        return "chemicals"
    elif 70000 <= item_id < 80000:
        return "fragrances"
    return None


def group_items_by_category(order_items: list) -> dict:
    """Группирует товары по категориям
    
    Возвращает словарь: {category: [items]}
    """
    grouped = {}
    for item in order_items:
        item_id = item.get("id", 0)
        category = get_category_by_item_id(item_id)
        if category:
            if category not in grouped:
                grouped[category] = []
            grouped[category].append(item)
    return grouped


def get_category_name(category: str) -> str:
    """Возвращает название категории"""
    return CATEGORY_NAMES.get(category, "Неизвестная категория")


# Эмодзи для категорий
CATEGORY_EMOJIS = {
    "cleaning": "🧴",
    "plasticpe": "🔵",
    "plasticpet": "♻️",
    "plasticpp": "🟣",
    "plastictd": "💧",
    "chemicals": "🧪",
    "fragrances": "🌸",
}


def get_category_emoji(category: str) -> str:
    """Возвращает эмодзи категории"""
    return CATEGORY_EMOJIS.get(category, "📦")

# ==================== СТАТУСЫ ЗАКАЗОВ ====================

class OrderStatus:
    """Статусы заказов"""
    PENDING = "pending"          # Ожидает одобрения админом
    APPROVED = "approved"        # Одобрен админом
    REJECTED = "rejected"        # Отклонен админом


def get_status_emoji(status: str) -> str:
    """Возвращает эмодзи для статуса"""
    emojis = {
        OrderStatus.PENDING: "⏳",
        OrderStatus.APPROVED: "✅",
        OrderStatus.REJECTED: "❌",
    }
    return emojis.get(status, "❓")


def get_status_name_ru(status: str) -> str:
    """Возвращает название статуса на русском"""
    names = {
        OrderStatus.PENDING: "Ожидает одобрения",
        OrderStatus.APPROVED: "Одобрен",
        OrderStatus.REJECTED: "Отклонен",
    }
    return names.get(status, "Неизвестно")


def get_status_name_uz(status: str) -> str:
    """Возвращает название статуса на узбекском"""
    names = {
        OrderStatus.PENDING: "Tasdiqlanish kutilmoqda",
        OrderStatus.APPROVED: "Tasdiqlangan",
        OrderStatus.REJECTED: "Rad etilgan",
    }
    return names.get(status, "Noma'lum")

# ==================== ЛОГИРОВАНИЕ ====================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot.log", encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)

# ==================== TELEGRAM BOT ====================

API_TOKEN = os.getenv("API_TOKEN")
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID"))
WEBAPP_URL = os.getenv("WEBAPP_URL")

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

# ==================== FTP ====================

FTP_HOST = os.getenv("HOSTING_FTP_HOST")
FTP_USER = os.getenv("HOSTING_FTP_USER")
FTP_PASS = os.getenv("HOSTING_FTP_PASS")

AIOFTP_AVAILABLE = False
try:
    import aioftp
    AIOFTP_AVAILABLE = True
    logger.info("✅ aioftp available")
except ImportError:
    logger.warning("⚠️  aioftp not installed. Using sync FTP")


async def upload_to_ftp_async(local_bytes: bytes, remote_filename: str):
    """Загрузка на FTP (async если доступен aioftp)"""
    if AIOFTP_AVAILABLE:
        try:
            async with aioftp.Client.context(FTP_HOST, user=FTP_USER, password=FTP_PASS) as client:
                stream = io.BytesIO(local_bytes)
                await client.upload_stream(stream, remote_filename)
                logger.info(f"FTP upload (async): {remote_filename}")
        except Exception as e:
            logger.exception(f"Async FTP upload failed for {remote_filename}")
            raise
    else:
        await asyncio.to_thread(_upload_to_ftp_sync, local_bytes, remote_filename)


def _upload_to_ftp_sync(local_bytes: bytes, remote_filename: str):
    """Синхронная загрузка на FTP"""
    try:
        with FTP(FTP_HOST) as ftp:
            ftp.login(FTP_USER, FTP_PASS)
            stream = io.BytesIO(local_bytes)
            ftp.storbinary(f"STOR {remote_filename}", stream)
            logger.info(f"FTP upload (sync): {remote_filename}")
    except Exception as e:
        logger.exception(f"Sync FTP upload failed for {remote_filename}")
        raise

# ==================== БАЗА ДАННЫХ ====================

DB_FILE = "orders.db"


@contextmanager
def get_db():
    """Контекстный менеджер для работы с БД"""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    """Инициализация БД"""
    with get_db() as conn:
        c = conn.cursor()
        
        # Таблица пользователей
        c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            lang TEXT DEFAULT 'ru',
            created_at TEXT DEFAULT (datetime('now'))
        )
        """)
        
        # Таблица заказов
        c.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            client_name TEXT,
            client_phone TEXT,
            client_address TEXT,
            items_json TEXT NOT NULL,
            total REAL NOT NULL,
            created_at TEXT DEFAULT (datetime('now')),
            status TEXT DEFAULT 'pending',
            pdf_draft BLOB,
            pdf_final BLOB,
            approved_at TEXT,
            approved_by INTEGER,
            rejected_at TEXT,
            rejected_by INTEGER,
            reject_reason TEXT,
            category TEXT,
            FOREIGN KEY(user_id) REFERENCES users(user_id)
        )
        """)
        
        # Индексы
        c.execute("CREATE INDEX IF NOT EXISTS idx_orders_user ON orders(user_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_orders_category ON orders(category)")
        
        conn.commit()
        logger.info("✅ Database initialized")


def register_user(user_id: int, username: str = None, first_name: str = None, last_name: str = None):
    """Регистрация/обновление пользователя"""
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""
        INSERT INTO users (user_id, username, first_name, last_name)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username = excluded.username,
            first_name = excluded.first_name,
            last_name = excluded.last_name
        """, (user_id, username, first_name, last_name))
        conn.commit()


def set_user_lang(user_id: int, lang: str):
    """Установка языка пользователя"""
    with get_db() as conn:
        c = conn.cursor()
        c.execute("UPDATE users SET lang = ? WHERE user_id = ?", (lang, user_id))
        conn.commit()


def get_user_lang(user_id: int) -> str:
    """Получение языка пользователя"""
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT lang FROM users WHERE user_id = ?", (user_id,))
        row = c.fetchone()
        return row["lang"] if row else "ru"


def create_order(
    order_id: str,
    user_id: int,
    client_name: str,
    client_phone: str,
    client_address: str,
    items: list,
    total: float,
    pdf_draft: bytes = None,
    category: str = None
):
    """Создание заказа"""
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""
        INSERT INTO orders (
            order_id, user_id, client_name, client_phone, client_address,
            items_json, total, pdf_draft, category, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            order_id, user_id, client_name, client_phone, client_address,
            json.dumps(items, ensure_ascii=False), total, pdf_draft, category,
            OrderStatus.PENDING
        ))
        conn.commit()


def update_order_status(order_id: str, status: str, admin_id: int = None, reason: str = None):
    """Обновление статуса заказа"""
    with get_db() as conn:
        c = conn.cursor()
        
        if status == OrderStatus.APPROVED:
            c.execute("""
            UPDATE orders 
            SET status = ?, approved_at = datetime('now'), approved_by = ?
            WHERE order_id = ?
            """, (status, admin_id, order_id))
        elif status == OrderStatus.REJECTED:
            c.execute("""
            UPDATE orders 
            SET status = ?, rejected_at = datetime('now'), rejected_by = ?, reject_reason = ?
            WHERE order_id = ?
            """, (status, admin_id, reason, order_id))
        else:
            c.execute("UPDATE orders SET status = ? WHERE order_id = ?", (status, order_id))
        
        conn.commit()


def get_order_raw(order_id: str) -> Optional[Dict]:
    """Получение заказа по ID"""
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM orders WHERE order_id = ?", (order_id,))
        row = c.fetchone()
        return dict(row) if row else None


def get_order_for_user(order_id: str, user_id: int) -> Optional[Dict]:
    """Получение заказа для конкретного пользователя"""
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM orders WHERE order_id = ? AND user_id = ?", (order_id, user_id))
        row = c.fetchone()
        return dict(row) if row else None


def get_user_orders(user_id: int, limit: int = 20) -> List[Dict]:
    """Получение заказов пользователя"""
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""
        SELECT * FROM orders 
        WHERE user_id = ? 
        ORDER BY created_at DESC 
        LIMIT ?
        """, (user_id, limit))
        return [dict(row) for row in c.fetchall()]


def get_all_orders(limit: int = 100) -> List[Dict]:
    """Получение всех заказов"""
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM orders ORDER BY created_at DESC LIMIT ?", (limit,))
        return [dict(row) for row in c.fetchall()]


def get_all_user_ids() -> List[int]:
    """Получение всех ID пользователей"""
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT DISTINCT user_id FROM users")
        return [row["user_id"] for row in c.fetchall()]

# ==================== RATE LIMITING ====================

# Лимиты: обычные пользователи - 30/мин, админы - без лимита
user_request_times = defaultdict(list)
RATE_LIMIT_SECONDS = 60
RATE_LIMIT_MAX_REQUESTS = 30


class RateLimitMiddleware(BaseMiddleware):
    """Middleware для rate limiting"""
    
    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        user_id = None
        
        if isinstance(event, Message):
            user_id = event.from_user.id
        elif isinstance(event, CallbackQuery):
            user_id = event.from_user.id
        
        # Админы не ограничены
        if user_id and user_id in ALL_ADMIN_IDS:
            return await handler(event, data)
        
        if user_id:
            now = datetime.now()
            cutoff = now - timedelta(seconds=RATE_LIMIT_SECONDS)
            
            # Удаляем старые запросы
            user_request_times[user_id] = [
                t for t in user_request_times[user_id] if t > cutoff
            ]
            
            if len(user_request_times[user_id]) >= RATE_LIMIT_MAX_REQUESTS:
                if isinstance(event, Message):
                    await event.answer(
                        "⚠️ Слишком много запросов. Подождите минуту.\n"
                        "⚠️ Juda ko'p so'rovlar. Bir daqiqa kuting."
                    )
                return
            
            user_request_times[user_id].append(now)
        
        return await handler(event, data)


dp.message.middleware(RateLimitMiddleware())
dp.callback_query.middleware(RateLimitMiddleware())

# ==================== PDF ГЕНЕРАЦИЯ ====================

def load_fonts():
    """Загрузка шрифтов для PDF"""
    try:
        # Шрифт DejaVu Sans поддерживает кириллицу
        font_path = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
        font_bold_path = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
        
        if os.path.exists(font_path):
            pdfmetrics.registerFont(TTFont("DejaVuSans", font_path))
            pdfmetrics.registerFont(TTFont("DejaVuSans-Bold", font_bold_path))
            return True
        else:
            logger.warning("DejaVu fonts not found. Using default fonts.")
            return False
    except Exception as e:
        logger.exception(f"Error loading fonts: {e}")
        return False


# Загрузка шрифтов при старте
FONTS_LOADED = load_fonts()


def generate_pdf(
    order_id: str,
    client_name: str,
    client_phone: str,
    client_address: str,
    items: list,
    total: float,
    qr_data: str = None
) -> bytes:
    """Генерация PDF заказа"""
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4
    
    # Используем DejaVu если доступен
    if FONTS_LOADED:
        font_regular = "DejaVuSans"
        font_bold = "DejaVuSans-Bold"
    else:
        font_regular = "Helvetica"
        font_bold = "Helvetica-Bold"
    
    y = height - 40 * mm
    
    # Заголовок
    c.setFont(font_bold, 20)
    c.drawString(20 * mm, y, f"Заказ №{order_id}")
    y -= 15 * mm
    
    # Информация о клиенте
    c.setFont(font_bold, 12)
    c.drawString(20 * mm, y, "Информация о клиенте:")
    y -= 7 * mm
    
    c.setFont(font_regular, 10)
    c.drawString(20 * mm, y, f"Имя: {client_name}")
    y -= 5 * mm
    c.drawString(20 * mm, y, f"Телефон: {client_phone}")
    y -= 5 * mm
    
    # Адрес может быть длинным, разбиваем на строки
    address_lines = textwrap.wrap(f"Адрес: {client_address}", width=80)
    for line in address_lines:
        c.drawString(20 * mm, y, line)
        y -= 5 * mm
    
    y -= 5 * mm
    
    # Товары
    c.setFont(font_bold, 12)
    c.drawString(20 * mm, y, "Товары:")
    y -= 7 * mm
    
    c.setFont(font_regular, 9)
    
    # Заголовки таблицы
    c.drawString(20 * mm, y, "№")
    c.drawString(30 * mm, y, "Название")
    c.drawString(120 * mm, y, "Кол-во")
    c.drawString(150 * mm, y, "Цена")
    c.drawString(175 * mm, y, "Сумма")
    y -= 5 * mm
    
    # Линия
    c.line(20 * mm, y, 190 * mm, y)
    y -= 5 * mm
    
    # Товары
    for idx, item in enumerate(items, 1):
        name = item.get("name", "")
        qty = item.get("quantity", 0)
        price = item.get("price", 0)
        subtotal = qty * price
        
        # Название может быть длинным
        name_lines = textwrap.wrap(name, width=40)
        first_line = name_lines[0] if name_lines else ""
        
        c.drawString(20 * mm, y, str(idx))
        c.drawString(30 * mm, y, first_line)
        c.drawString(120 * mm, y, str(qty))
        c.drawString(150 * mm, y, f"{price:,.0f}")
        c.drawString(175 * mm, y, f"{subtotal:,.0f}")
        y -= 5 * mm
        
        # Дополнительные строки названия
        for line in name_lines[1:]:
            c.drawString(30 * mm, y, line)
            y -= 5 * mm
        
        # Проверка на конец страницы
        if y < 40 * mm:
            c.showPage()
            y = height - 40 * mm
            c.setFont(font_regular, 9)
    
    # Итого
    y -= 5 * mm
    c.line(20 * mm, y, 190 * mm, y)
    y -= 7 * mm
    
    c.setFont(font_bold, 12)
    c.drawString(150 * mm, y, "Итого:")
    c.drawString(175 * mm, y, f"{total:,.0f}")
    
    # QR-код
    if qr_data:
        y -= 40 * mm
        qr = qrcode.QRCode(box_size=3, border=1)
        qr.add_data(qr_data)
        qr.make(fit=True)
        
        qr_img = qr.make_image(fill_color="black", back_color="white")
        qr_buffer = io.BytesIO()
        qr_img.save(qr_buffer, format="PNG")
        qr_buffer.seek(0)
        
        img = ImageReader(qr_buffer)
        c.drawImage(img, 20 * mm, y, width=30 * mm, height=30 * mm)
        
        c.setFont(font_regular, 8)
        c.drawString(55 * mm, y + 25 * mm, "Отсканируйте QR-код")
        c.drawString(55 * mm, y + 20 * mm, "для отслеживания заказа")
    
    c.save()
    buffer.seek(0)
    return buffer.read()

# ==================== FSM ====================

class OrderForm(StatesGroup):
    """Состояния для оформления заказа"""
    waiting_for_signature = State()

# ==================== КОМАНДЫ БОТА ====================

@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    """Команда /start"""
    await state.clear()
    
    user = message.from_user
    register_user(user.id, user.username, user.first_name, user.last_name)
    
    lang = get_user_lang(user.id)
    
    # Клавиатура выбора языка
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="🇷🇺 Русский"),
                KeyboardButton(text="🇺🇿 O'zbekcha")
            ]
        ],
        resize_keyboard=True
    )
    
    if lang == "ru":
        await message.answer(
            "👋 Добро пожаловать в систему заказов!\n\n"
            "Выберите язык:",
            reply_markup=kb
        )
    else:
        await message.answer(
            "👋 Buyurtmalar tizimiga xush kelibsiz!\n\n"
            "Tilni tanlang:",
            reply_markup=kb
        )


@router.message(F.text.in_(["🇷🇺 Русский", "🇺🇿 O'zbekcha"]))
async def handle_language_selection(message: Message):
    """Обработка выбора языка"""
    user_id = message.from_user.id
    
    if message.text == "🇷🇺 Русский":
        set_user_lang(user_id, "ru")
        lang = "ru"
    else:
        set_user_lang(user_id, "uz")
        lang = "uz"
    
    # Главная клавиатура
    if lang == "ru":
        kb = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="🛒 Сделать заказ", web_app=WebAppInfo(url=WEBAPP_URL))],
                [KeyboardButton(text="📋 Мои заказы")],
                [KeyboardButton(text="🌐 Изменить язык")]
            ],
            resize_keyboard=True
        )
        await message.answer(
            "✅ Язык изменен на русский.\n\n"
            "Выберите действие:",
            reply_markup=kb
        )
    else:
        kb = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="🛒 Buyurtma berish", web_app=WebAppInfo(url=WEBAPP_URL))],
                [KeyboardButton(text="📋 Mening buyurtmalarim")],
                [KeyboardButton(text="🌐 Tilni o'zgartirish")]
            ],
            resize_keyboard=True
        )
        await message.answer(
            "✅ Til o'zbek tiliga o'zgartirildi.\n\n"
            "Harakatni tanlang:",
            reply_markup=kb
        )


@router.message(F.text.in_(["🌐 Изменить язык", "🌐 Tilni o'zgartirish"]))
async def cmd_change_language(message: Message):
    """Изменение языка"""
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="🇷🇺 Русский"),
                KeyboardButton(text="🇺🇿 O'zbekcha")
            ]
        ],
        resize_keyboard=True
    )
    
    lang = get_user_lang(message.from_user.id)
    
    if lang == "ru":
        await message.answer("Выберите язык:", reply_markup=kb)
    else:
        await message.answer("Tilni tanlang:", reply_markup=kb)


@router.message(F.text.in_(["📋 Мои заказы", "📋 Mening buyurtmalarim"]))
async def cmd_my_orders(message: Message):
    """Просмотр заказов пользователя"""
    user_id = message.from_user.id
    lang = get_user_lang(user_id)
    
    orders = get_user_orders(user_id, limit=10)
    
    if not orders:
        if lang == "ru":
            await message.answer("У вас пока нет заказов.")
        else:
            await message.answer("Sizda hali buyurtmalar yo'q.")
        return
    
    if lang == "ru":
        text = "📋 *Ваши заказы:*\n\n"
    else:
        text = "📋 *Sizning buyurtmalaringiz:*\n\n"
    
    for o in orders:
        order_id = o["order_id"]
        status = o["status"]
        created = o["created_at"]
        total = o["total"]
        
        status_emoji = get_status_emoji(status)
        if lang == "ru":
            status_name = get_status_name_ru(status)
            text += (
                f"🆔 *{order_id}*\n"
                f"📅 {created}\n"
                f"💰 {total:,.0f} сум\n"
                f"{status_emoji} {status_name}\n\n"
            )
        else:
            status_name = get_status_name_uz(status)
            text += (
                f"🆔 *{order_id}*\n"
                f"📅 {created}\n"
                f"💰 {total:,.0f} so'm\n"
                f"{status_emoji} {status_name}\n\n"
            )
    
    if lang == "ru":
        text += "\nДля получения PDF заказа используйте:\n`/get_pdf номер_заказа`"
    else:
        text += "\nBuyurtma PDF olish uchun foydalaning:\n`/get_pdf buyurtma_raqami`"
    
    await message.answer(text, parse_mode="Markdown")


@router.message(F.web_app_data)
async def handle_web_app_data(message: Message, state: FSMContext):
    """Обработка данных из Web App"""
    try:
        data = json.loads(message.web_app_data.data)
        logger.info(f"Received web app data: {data}")
        
        order_data = data.get("order", {})
        items = order_data.get("items", [])
        total = order_data.get("total", 0)
        client_name = order_data.get("clientName", "")
        client_phone = order_data.get("clientPhone", "")
        client_address = order_data.get("clientAddress", "")
        
        if not items:
            lang = get_user_lang(message.from_user.id)
            if lang == "ru":
                await message.answer("❌ Заказ пуст.")
            else:
                await message.answer("❌ Buyurtma bo'sh.")
            return
        
        # Сохраняем данные в состояние
        await state.update_data(
            items=items,
            total=total,
            client_name=client_name,
            client_phone=client_phone,
            client_address=client_address
        )
        
        lang = get_user_lang(message.from_user.id)
        
        # Запрос подписи
        if lang == "ru":
            await message.answer(
                "📝 Пожалуйста, отправьте вашу подпись (изображение):",
                reply_markup=ReplyKeyboardRemove()
            )
        else:
            await message.answer(
                "📝 Iltimos, imzoingizni yuboring (rasm):",
                reply_markup=ReplyKeyboardRemove()
            )
        
        await state.set_state(OrderForm.waiting_for_signature)
    
    except Exception as e:
        logger.exception(f"Error handling web app data")
        lang = get_user_lang(message.from_user.id)
        if lang == "ru":
            await message.answer("❌ Ошибка обработки заказа.")
        else:
            await message.answer("❌ Buyurtmani qayta ishlashda xatolik.")


@router.message(OrderForm.waiting_for_signature, F.photo)
async def handle_signature(message: Message, state: FSMContext):
    """Обработка подписи"""
    try:
        user_data = await state.get_data()
        items = user_data.get("items", [])
        total = user_data.get("total", 0)
        client_name = user_data.get("client_name", "")
        client_phone = user_data.get("client_phone", "")
        client_address = user_data.get("client_address", "")
        
        user_id = message.from_user.id
        lang = get_user_lang(user_id)
        
        # Генерация ID заказа
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        order_id = f"ORD-{timestamp}-{user_id}"
        
        # Группировка товаров по категориям
        grouped_items = group_items_by_category(items)
        
        if lang == "ru":
            await message.answer("⏳ Обрабатываю заказ...")
        else:
            await message.answer("⏳ Buyurtma qayta ishlanmoqda...")
        
        # Создаем подзаказы для каждой категории
        part_num = 1
        for category, category_items in grouped_items.items():
            sub_order_id = f"{order_id}-P{part_num}"
            
            # Подсчет итога для этой категории
            category_total = sum(
                item.get("quantity", 0) * item.get("price", 0)
                for item in category_items
            )
            
            # Генерация PDF
            qr_data = f"ORDER:{sub_order_id}"
            pdf_bytes = generate_pdf(
                sub_order_id,
                client_name,
                client_phone,
                client_address,
                category_items,
                category_total,
                qr_data
            )
            
            # Сохранение в БД
            create_order(
                sub_order_id,
                user_id,
                client_name,
                client_phone,
                client_address,
                category_items,
                category_total,
                pdf_bytes,
                category
            )
            
            # Загрузка на FTP
            try:
                await upload_to_ftp_async(pdf_bytes, f"{sub_order_id}.pdf")
            except Exception as e:
                logger.exception(f"FTP upload failed for {sub_order_id}")
            
            # Отправка клиенту
            category_name = get_category_name(category)
            category_emoji = get_category_emoji(category)
            
            if lang == "ru":
                client_text = (
                    f"✅ Заказ оформлен!\n\n"
                    f"🆔 Номер: *{sub_order_id}*\n"
                    f"{category_emoji} Категория: {category_name}\n"
                    f"💰 Сумма: {category_total:,.0f} сум\n\n"
                    f"⏳ Ожидает одобрения администратором."
                )
            else:
                client_text = (
                    f"✅ Buyurtma qabul qilindi!\n\n"
                    f"🆔 Raqam: *{sub_order_id}*\n"
                    f"{category_emoji} Kategoriya: {category_name}\n"
                    f"💰 Summa: {category_total:,.0f} so'm\n\n"
                    f"⏳ Administrator tasdiqini kutmoqda."
                )
            
            pdf_file = BufferedInputFile(pdf_bytes, filename=f"order_{sub_order_id}.pdf")
            await message.answer_document(
                document=pdf_file,
                caption=client_text,
                parse_mode="Markdown"
            )
            
            # Отправка админу
            admin_text = (
                f"📦 *Новый заказ!*\n\n"
                f"🆔 {sub_order_id}\n"
                f"{category_emoji} {category_name}\n"
                f"👤 {client_name}\n"
                f"📞 {client_phone}\n"
                f"📍 {client_address}\n"
                f"💰 {category_total:,.0f} сум\n\n"
                f"👤 От: {message.from_user.full_name} (@{message.from_user.username or 'нет'})"
            )
            
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Одобрить", callback_data=f"approve:{sub_order_id}"),
                    InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject:{sub_order_id}")
                ]
            ])
            
            try:
                pdf_file = BufferedInputFile(pdf_bytes, filename=f"order_{sub_order_id}.pdf")
                await bot.send_document(
                    chat_id=ADMIN_CHAT_ID,
                    document=pdf_file,
                    caption=admin_text,
                    reply_markup=kb,
                    parse_mode="Markdown"
                )
                logger.info(f"Order part {sub_order_id} (category: {category_name}) sent to admin chat {ADMIN_CHAT_ID}")
            except Exception as e:
                logger.exception(f"Failed to send order part {sub_order_id} to admin chat {ADMIN_CHAT_ID}")
            
            part_num += 1
        
        # Возврат к главному меню
        if lang == "ru":
            kb = ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="🛒 Сделать заказ", web_app=WebAppInfo(url=WEBAPP_URL))],
                    [KeyboardButton(text="📋 Мои заказы")],
                    [KeyboardButton(text="🌐 Изменить язык")]
                ],
                resize_keyboard=True
            )
        else:
            kb = ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="🛒 Buyurtma berish", web_app=WebAppInfo(url=WEBAPP_URL))],
                    [KeyboardButton(text="📋 Mening buyurtmalarim")],
                    [KeyboardButton(text="🌐 Tilni o'zgartirish")]
                ],
                resize_keyboard=True
            )
        
        if lang == "ru":
            await message.answer("Главное меню:", reply_markup=kb)
        else:
            await message.answer("Asosiy menyu:", reply_markup=kb)
        
        await state.clear()
    
    except Exception as e:
        logger.exception(f"Error in order signature handler")
        lang = get_user_lang(message.from_user.id)
        if lang == "ru":
            await message.answer("❌ Произошла ошибка при обработке заказа. Попробуйте позже.")
        else:
            await message.answer("❌ Buyurtmani qayta ishlashda xatolik yuz berdi. Keyinroq urinib ko'ring.")
        await state.clear()


# ==================== CALLBACK ОБРАБОТЧИКИ ====================

@router.callback_query(F.data.startswith("approve:"))
async def callback_approve_order(callback: CallbackQuery):
    """Одобрение заказа"""
    admin_id = callback.from_user.id
    
    # Проверка прав
    if not has_permission(admin_id, AdminRole.SALES):
        await callback.answer("❌ У вас нет прав для одобрения заказов.", show_alert=True)
        return
    
    order_id = callback.data.split(":", 1)[1]
    order = get_order_raw(order_id)
    
    if not order:
        await callback.answer("❌ Заказ не найден.", show_alert=True)
        return
    
    if order["status"] != OrderStatus.PENDING:
        await callback.answer("❌ Заказ уже обработан.", show_alert=True)
        return
    
    # Обновление статуса
    update_order_status(order_id, OrderStatus.APPROVED, admin_id)
    
    # Уведомление клиента
    user_id = order["user_id"]
    lang = get_user_lang(user_id)
    
    category = order.get("category", "")
    category_name = get_category_name(category)
    category_emoji = get_category_emoji(category)
    
    if lang == "ru":
        notification = (
            f"✅ *Заказ одобрен!*\n\n"
            f"🆔 {order_id}\n"
            f"{category_emoji} {category_name}\n"
            f"💰 {order['total']:,.0f} сум\n\n"
            f"Ваш заказ был одобрен администратором."
        )
    else:
        notification = (
            f"✅ *Buyurtma tasdiqlandi!*\n\n"
            f"🆔 {order_id}\n"
            f"{category_emoji} {category_name}\n"
            f"💰 {order['total']:,.0f} so'm\n\n"
            f"Buyurtmangiz administrator tomonidan tasdiqlandi."
        )
    
    try:
        await bot.send_message(user_id, notification, parse_mode="Markdown")
    except Exception as e:
        logger.exception(f"Failed to notify user {user_id} about order approval")
    
    # Обновление сообщения админа
    admin_name = get_admin_name(admin_id)
    await callback.message.edit_caption(
        caption=f"{callback.message.caption}\n\n✅ *Одобрено* ({admin_name})",
        parse_mode="Markdown"
    )
    
    await callback.answer("✅ Заказ одобрен!")


@router.callback_query(F.data.startswith("reject:"))
async def callback_reject_order(callback: CallbackQuery):
    """Отклонение заказа"""
    admin_id = callback.from_user.id
    
    # Проверка прав
    if not has_permission(admin_id, AdminRole.SALES):
        await callback.answer("❌ У вас нет прав для отклонения заказов.", show_alert=True)
        return
    
    order_id = callback.data.split(":", 1)[1]
    order = get_order_raw(order_id)
    
    if not order:
        await callback.answer("❌ Заказ не найден.", show_alert=True)
        return
    
    if order["status"] != OrderStatus.PENDING:
        await callback.answer("❌ Заказ уже обработан.", show_alert=True)
        return
    
    # Обновление статуса
    update_order_status(order_id, OrderStatus.REJECTED, admin_id, "Отклонено администратором")
    
    # Уведомление клиента
    user_id = order["user_id"]
    lang = get_user_lang(user_id)
    
    category = order.get("category", "")
    category_name = get_category_name(category)
    category_emoji = get_category_emoji(category)
    
    if lang == "ru":
        notification = (
            f"❌ *Заказ отклонен*\n\n"
            f"🆔 {order_id}\n"
            f"{category_emoji} {category_name}\n"
            f"💰 {order['total']:,.0f} сум\n\n"
            f"К сожалению, ваш заказ был отклонен администратором."
        )
    else:
        notification = (
            f"❌ *Buyurtma rad etildi*\n\n"
            f"🆔 {order_id}\n"
            f"{category_emoji} {category_name}\n"
            f"💰 {order['total']:,.0f} so'm\n\n"
            f"Afsuski, buyurtmangiz administrator tomonidan rad etildi."
        )
    
    try:
        await bot.send_message(user_id, notification, parse_mode="Markdown")
    except Exception as e:
        logger.exception(f"Failed to notify user {user_id} about order rejection")
    
    # Обновление сообщения админа
    admin_name = get_admin_name(admin_id)
    await callback.message.edit_caption(
        caption=f"{callback.message.caption}\n\n❌ *Отклонено* ({admin_name})",
        parse_mode="Markdown"
    )
    
    await callback.answer("❌ Заказ отклонен!")


# ==================== АДМИН КОМАНДЫ ====================

@router.message(Command("orders_export"))
async def cmd_orders_export(message: Message):
    """Экспорт заказов (только супер-админ)"""
    if message.from_user.id != SUPER_ADMIN_ID:
        return
    
    orders = get_all_orders(limit=10000)
    
    if not orders:
        await message.answer("В базе нет заказов.")
        return
    
    output = io.StringIO()
    writer = csv.writer(output, delimiter=";")
    writer.writerow(["order_id", "client_name", "user_id", "total", "created_at", "status", "category"])
    
    for o in orders:
        writer.writerow([
            o["order_id"],
            o["client_name"],
            o["user_id"],
            o["total"],
            o["created_at"],
            o["status"] or "",
            o.get("category", ""),
        ])
    
    csv_bytes = output.getvalue().encode("utf-8-sig")
    output.close()
    
    filename = f"orders_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    file = BufferedInputFile(csv_bytes, filename=filename)
    
    await message.answer_document(document=file, caption="Экспорт заказов (CSV)")


@router.message(Command("sendall"))
async def cmd_sendall(message: Message):
    """Массовая рассылка (только супер-админ)"""
    if message.from_user.id != SUPER_ADMIN_ID:
        return
    
    text_part = ""
    
    if message.text:
        parts = message.text.split(" ", 1)
        if len(parts) > 1:
            text_part = parts[1].strip()
    
    if message.caption:
        parts = message.caption.split(" ", 1)
        if len(parts) > 1:
            text_part = parts[1].strip()
    
    if not text_part:
        await message.answer(
            "Использование:\n"
            "• Текст: `/sendall текст`\n"
            "• Фото/видео: отправь медиа с подписью `/sendall текст`",
            parse_mode="Markdown"
        )
        return
    
    user_ids = get_all_user_ids()
    if not user_ids:
        await message.answer("Нет пользователей.")
        return
    
    ok = 0
    fail = 0
    
    if message.photo:
        file_id = message.photo[-1].file_id
        for uid in user_ids:
            try:
                await bot.send_photo(uid, file_id, caption=text_part)
                ok += 1
            except (TelegramForbiddenError, TelegramBadRequest):
                fail += 1
            except Exception:
                fail += 1
    
    elif message.video:
        file_id = message.video.file_id
        for uid in user_ids:
            try:
                await bot.send_video(uid, file_id, caption=text_part)
                ok += 1
            except (TelegramForbiddenError, TelegramBadRequest):
                fail += 1
            except Exception:
                fail += 1
    
    else:
        for uid in user_ids:
            try:
                await bot.send_message(uid, text_part)
                ok += 1
            except (TelegramForbiddenError, TelegramBadRequest):
                fail += 1
            except Exception:
                fail += 1
    
    await message.answer(f"✅ Отправлено: {ok}\n❌ Не доставлено: {fail}")


@router.message(Command("get_pdf"))
async def cmd_get_pdf(message: Message):
    """Получить PDF заказа"""
    user_id = message.from_user.id
    lang = get_user_lang(user_id)
    
    args = message.text.split()
    if len(args) < 2:
        if lang == "ru":
            await message.answer("Использование: /get_pdf <номер_заказа>")
        else:
            await message.answer("Foydalanish: /get_pdf <buyurtma_raqami>")
        return
    
    order_id = args[1].strip()
    
    # Админы могут получать любые заказы
    if user_id in ALL_ADMIN_IDS:
        record = get_order_raw(order_id)
    else:
        record = get_order_for_user(order_id, user_id)
    
    if not record:
        if lang == "ru":
            await message.answer("Заказ не найден.")
        else:
            await message.answer("Buyurtma topilmadi.")
        return
    
    pdf_bytes = record.get("pdf_final") or record.get("pdf_draft")
    if not pdf_bytes:
        if lang == "ru":
            await message.answer("PDF не доступен.")
        else:
            await message.answer("PDF mavjud emas.")
        return
    
    pdf_file = BufferedInputFile(pdf_bytes, filename=f"order_{order_id}.pdf")
    
    if lang == "ru":
        caption = f"PDF заказа №{order_id}"
    else:
        caption = f"Buyurtma №{order_id} PDF"
    
    await message.answer_document(document=pdf_file, caption=caption)


# ==================== ЗАПУСК ====================

async def on_startup(bot: Bot):
    """Действия при запуске"""
    logger.info("=" * 50)
    logger.info("🤖 Bot starting up...")
    logger.info(f"Bot username: {(await bot.get_me()).username}")
    logger.info(f"Super Admin ID: {SUPER_ADMIN_ID}")
    logger.info(f"Sales Admins: {SALES_ADMIN_IDS}")
    logger.info(f"Rate limiting: ✅")
    logger.info(f"Async FTP: {'✅' if AIOFTP_AVAILABLE else '⚠️  Fallback to sync'}")
    logger.info("=" * 50)
    
    try:
        init_db()
        logger.info("✅ Database initialized")
    except Exception as e:
        logger.exception(f"❌ Database init failed: {e}")
        raise
    
    try:
        await bot.send_message(
            ADMIN_CHAT_ID,
            "🤖 Бот запущен!\n\n"
            f"Супер-админ: 1\n"
            f"Отдел продаж: {len(SALES_ADMIN_IDS)}\n\n"
            f"✨ Процесс заказа: одобрение/отклонение администратором"
        )
    except Exception as e:
        logger.warning(f"Cannot notify admin: {e}")


async def on_shutdown(bot: Bot):
    """Действия при остановке"""
    logger.info("🛑 Bot shutting down...")
    try:
        await bot.send_message(ADMIN_CHAT_ID, "🛑 Бот остановлен")
    except:
        pass


async def main():
    """Главная функция"""
    logger.info("Starting bot initialization...")
    
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    
    try:
        logger.info("Starting polling...")
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    except Exception as e:
        logger.exception(f"Critical error: {e}")
    finally:
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
