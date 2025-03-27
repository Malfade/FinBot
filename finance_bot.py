import os
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F, html
from aiogram.types import (
    Message, ReplyKeyboardMarkup, KeyboardButton,
    ReplyKeyboardRemove, FSInputFile, BufferedInputFile
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import Command, CommandStart
import aiosqlite
import matplotlib.pyplot as plt
import io
import tempfile
import csv
import calendar
from collections import defaultdict

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

load_dotenv()
API_TOKEN = os.getenv('API_TOKEN')

if not API_TOKEN:
    raise ValueError("Не указан API_TOKEN в переменных окружения")

bot = Bot(token=API_TOKEN)
dp = Dispatcher()

# --- Константы ---
DEFAULT_INCOME_CATEGORIES = ["Зарплата", "Подарок", "Инвестиции", "Фриланс"]
DEFAULT_EXPENSE_CATEGORIES = ["Еда", "Транспорт", "Жилье", "Развлечения", "Здоровье"]
CURRENCY = "₽"

# --- База данных ---
class Database:
    def __init__(self, db_name='finance.db'):
        self.db_name = db_name
        self.conn = None

    async def connect(self):
        self.conn = await aiosqlite.connect(self.db_name)
        await self._enable_foreign_keys()
        await self._run_migrations()
    
    async def _enable_foreign_keys(self):
        """Включаем поддержку внешних ключей"""
        await self.conn.execute("PRAGMA foreign_keys = ON")
        await self.conn.commit()

    async def _run_migrations(self):
        """Выполняем все миграции последовательно"""
        await self._create_migrations_table()
        
        migrations = [
            self._migration_initial_schema,
            self._migration_add_description_column,
            self._migration_fix_budget_limits
        ]
        
        for migration in migrations:
            if not await self._is_migration_applied(migration.__name__):
                try:
                    await migration()
                    await self._mark_migration_applied(migration.__name__)
                except Exception as e:
                    logger.error(f"Migration {migration.__name__} failed: {e}")
                    raise

    async def _create_migrations_table(self):
        """Создаем таблицу для отслеживания примененных миграций"""
        await self.conn.execute("""
            CREATE TABLE IF NOT EXISTS _migrations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await self.conn.commit()

    async def _is_migration_applied(self, migration_name: str) -> bool:
        """Проверяем, применена ли миграция"""
        cursor = await self.conn.execute(
            "SELECT 1 FROM _migrations WHERE name = ?", 
            (migration_name,)
        )
        return bool(await cursor.fetchone())

    async def _mark_migration_applied(self, migration_name: str):
        """Отмечаем миграцию как примененную"""
        await self.conn.execute(
            "INSERT INTO _migrations (name) VALUES (?)",
            (migration_name,)
        )
        await self.conn.commit()

    async def _migration_initial_schema(self):
        """Первоначальная схема базы данных"""
        await self.conn.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                type TEXT NOT NULL CHECK(type IN ('income', 'expense')),
                amount REAL NOT NULL,
                category TEXT NOT NULL,
                date TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        await self.conn.execute("""
            CREATE TABLE IF NOT EXISTS user_categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                category_name TEXT NOT NULL,
                transaction_type TEXT NOT NULL CHECK(transaction_type IN ('income', 'expense')),
                UNIQUE(user_id, category_name, transaction_type)
            )
        """)
        
        await self.conn.execute("""
            CREATE TABLE IF NOT EXISTS budget_limits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                category TEXT NOT NULL,
                limit_amount REAL NOT NULL,
                transaction_type TEXT NOT NULL CHECK(transaction_type IN ('income', 'expense'))
            )
        """)
        
        await self.conn.execute("""
            CREATE TABLE IF NOT EXISTS recurring_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                type TEXT NOT NULL CHECK(type IN ('income', 'expense')),
                amount REAL NOT NULL,
                category TEXT NOT NULL,
                frequency TEXT NOT NULL CHECK(frequency IN ('daily', 'weekly', 'monthly')),
                start_date TEXT NOT NULL,
                last_processed TEXT,
                is_active INTEGER DEFAULT 1
            )
        """)
        
        # Создаем индексы
        await self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_transactions_user_date 
            ON transactions(user_id, date)
        """)
        
        await self.conn.commit()

    async def _migration_add_description_column(self):
        """Добавляем колонку description в таблицу transactions"""
        await self.conn.execute("""
            ALTER TABLE transactions ADD COLUMN description TEXT
        """)
        await self.conn.commit()

    async def _migration_fix_budget_limits(self):
        """Исправляем таблицу budget_limits, добавляем period"""
        # Создаем временную таблицу с новой структурой
        await self.conn.execute("""
            CREATE TABLE IF NOT EXISTS budget_limits_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                category TEXT NOT NULL,
                limit_amount REAL NOT NULL,
                transaction_type TEXT NOT NULL CHECK(transaction_type IN ('income', 'expense')),
                period TEXT NOT NULL CHECK(period IN ('day', 'week', 'month')) DEFAULT 'month',
                UNIQUE(user_id, category, transaction_type, period)
            )
        """)
        
        # Копируем данные из старой таблицы
        await self.conn.execute("""
            INSERT INTO budget_limits_new 
            (id, user_id, category, limit_amount, transaction_type, period)
            SELECT id, user_id, category, limit_amount, transaction_type, 'month' 
            FROM budget_limits
        """)
        
        # Удаляем старую таблицу и переименовываем новую
        await self.conn.execute("DROP TABLE budget_limits")
        await self.conn.execute("ALTER TABLE budget_limits_new RENAME TO budget_limits")
        await self.conn.commit()

    async def close(self):
        if self.conn:
            await self.conn.close()

    # --- Методы для работы с транзакциями ---
    async def add_transaction(self, user_id: int, transaction_type: str, amount: float, 
                            category: str, description: str = None) -> bool:
        try:
            async with self.conn.cursor() as cursor:
                await cursor.execute(
                    """INSERT INTO transactions 
                    (user_id, type, amount, category, date, description)
                    VALUES (?, ?, ?, ?, ?, ?)""",
                    (user_id, transaction_type, amount, category, 
                     datetime.now().strftime('%Y-%m-%d'), description)
                )
                await self.conn.commit()
                return True
        except Exception as e:
            logger.error(f"Error adding transaction: {e}")
            return False

    async def get_transactions(self, user_id: int, start_date: str = None, 
                             end_date: str = None, limit: int = 100) -> List[Tuple]:
        try:
            async with self.conn.cursor() as cursor:
                query = """SELECT type, amount, category, date, description 
                          FROM transactions WHERE user_id = ?"""
                params = [user_id]
                
                if start_date and end_date:
                    query += " AND date BETWEEN ? AND ?"
                    params.extend([start_date, end_date])
                elif start_date:
                    query += " AND date >= ?"
                    params.append(start_date)
                elif end_date:
                    query += " AND date <= ?"
                    params.append(end_date)
                
                query += " ORDER BY date DESC LIMIT ?"
                params.append(limit)
                
                await cursor.execute(query, params)
                return await cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting transactions: {e}")
            return []

    async def get_transaction_summary(self, user_id: int, period: str = 'month') -> Dict:
        """Получение сводки по доходам/расходам за период"""
        try:
            date_format = {
                'day': '%Y-%m-%d',
                'week': '%Y-%W',
                'month': '%Y-%m'
            }.get(period, '%Y-%m')
            
            async with self.conn.cursor() as cursor:
                await cursor.execute(
                    f"""SELECT type, strftime('{date_format}', date) as period, 
                        SUM(amount) as total 
                        FROM transactions 
                        WHERE user_id = ? 
                        GROUP BY type, period
                        ORDER BY period DESC
                        LIMIT 10""",
                    (user_id,)
                )
                return await cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting transaction summary: {e}")
            return []

    # --- Методы для работы с категориями ---
    async def get_user_categories(self, user_id: int, transaction_type: str) -> List[str]:
        try:
            async with self.conn.cursor() as cursor:
                await cursor.execute(
                    """SELECT category_name FROM user_categories 
                    WHERE user_id = ? AND transaction_type = ?""",
                    (user_id, transaction_type)
                )
                result = await cursor.fetchall()
                return [row[0] for row in result]
        except Exception as e:
            logger.error(f"Error getting user categories: {e}")
            return []

    async def add_user_category(self, user_id: int, category_name: str, 
                              transaction_type: str) -> bool:
        try:
            async with self.conn.cursor() as cursor:
                await cursor.execute(
                    """INSERT INTO user_categories 
                    (user_id, category_name, transaction_type)
                    VALUES (?, ?, ?)""",
                    (user_id, category_name, transaction_type)
                )
                await self.conn.commit()
                return True
        except Exception as e:
            logger.error(f"Error adding user category: {e}")
            return False

    async def delete_user_category(self, user_id: int, category_name: str, 
                                 transaction_type: str) -> bool:
        try:
            async with self.conn.cursor() as cursor:
                await cursor.execute(
                    """DELETE FROM user_categories 
                    WHERE user_id = ? AND category_name = ? AND transaction_type = ?""",
                    (user_id, category_name, transaction_type)
                )
                await self.conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error deleting user category: {e}")
            return False

    # --- Методы для работы с лимитами ---
    async def add_budget_limit(self, user_id: int, category: str, limit_amount: float, 
                             transaction_type: str, period: str) -> bool:
        try:
            async with self.conn.cursor() as cursor:
                await cursor.execute(
                    """INSERT OR REPLACE INTO budget_limits 
                    (user_id, category, limit_amount, transaction_type, period)
                    VALUES (?, ?, ?, ?, ?)""",
                    (user_id, category, limit_amount, transaction_type, period)
                )
                await self.conn.commit()
                return True
        except Exception as e:
            logger.error(f"Error adding budget limit: {e}")
            return False

    async def get_budget_limits(self, user_id: int) -> List[Tuple]:
        try:
            async with self.conn.cursor() as cursor:
                await cursor.execute(
                    """SELECT category, limit_amount, transaction_type, period 
                    FROM budget_limits WHERE user_id = ?""",
                    (user_id,)
                )
                return await cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting budget limits: {e}")
            return []

    async def get_category_spending(self, user_id: int, category: str, 
                                  transaction_type: str, period: str) -> float:
        try:
            date_condition = {
                'day': "date = date('now')",
                'week': "strftime('%Y-%W', date) = strftime('%Y-%W', 'now')",
                'month': "strftime('%Y-%m', date) = strftime('%Y-%m', 'now')"
            }.get(period, "1=1")
            
            async with self.conn.cursor() as cursor:
                await cursor.execute(
                    f"""SELECT COALESCE(SUM(amount), 0) 
                    FROM transactions 
                    WHERE user_id = ? AND category = ? AND type = ? AND {date_condition}""",
                    (user_id, category, transaction_type)
                )
                result = await cursor.fetchone()
                return result[0] if result else 0
        except Exception as e:
            logger.error(f"Error getting category spending: {e}")
            return 0

    # --- Методы для регулярных транзакций ---
    async def add_recurring_transaction(self, user_id: int, transaction_type: str, 
                                      amount: float, category: str, frequency: str, 
                                      description: str = None) -> bool:
        try:
            async with self.conn.cursor() as cursor:
                await cursor.execute(
                    """INSERT INTO recurring_transactions 
                    (user_id, type, amount, category, description, frequency, start_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (user_id, transaction_type, amount, category, description, 
                     frequency, datetime.now().strftime('%Y-%m-%d'))
                )
                await self.conn.commit()
                return True
        except Exception as e:
            logger.error(f"Error adding recurring transaction: {e}")
            return False

    async def get_recurring_transactions(self, user_id: int = None) -> List[Tuple]:
        try:
            async with self.conn.cursor() as cursor:
                if user_id:
                    await cursor.execute(
                        """SELECT id, type, amount, category, description, frequency, 
                        start_date, last_processed, is_active
                        FROM recurring_transactions 
                        WHERE user_id = ? AND is_active = 1""",
                        (user_id,)
                    )
                else:
                    await cursor.execute(
                        """SELECT id, user_id, type, amount, category, description, 
                        frequency, last_processed, is_active
                        FROM recurring_transactions 
                        WHERE is_active = 1""")
                return await cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting recurring transactions: {e}")
            return []

    async def update_recurring_transaction(self, transaction_id: int, **kwargs) -> bool:
        try:
            async with self.conn.cursor() as cursor:
                set_clause = ", ".join(f"{key} = ?" for key in kwargs)
                await cursor.execute(
                    f"""UPDATE recurring_transactions 
                    SET {set_clause} 
                    WHERE id = ?""",
                    (*kwargs.values(), transaction_id)
                )
                await self.conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error updating recurring transaction: {e}")
            return False

db = Database()

# --- Состояния FSM ---
class Form(StatesGroup):
    # Для добавления транзакций
    transaction_type = State()
    transaction_amount = State()
    transaction_category = State()
    transaction_description = State()
    
    # Для управления категориями
    category_type = State()
    category_name = State()
    delete_category = State()
    
    # Для управления лимитами
    limit_category_type = State()
    limit_category = State()
    limit_amount = State()
    limit_period = State()
    
    # Для регулярных транзакций
    recurring_type = State()
    recurring_amount = State()
    recurring_category = State()
    recurring_description = State()
    recurring_frequency = State()
    
    # Для экспорта
    export_start_date = State()
    export_end_date = State()
    export_format = State()

# --- Вспомогательные функции ---
def create_main_keyboard() -> ReplyKeyboardMarkup:
    """Создает основную клавиатуру меню"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📈 Доход"), KeyboardButton(text="📉 Расход")],
            [KeyboardButton(text="💰 Баланс"), KeyboardButton(text="📊 Статистика")],
            [KeyboardButton(text="🔁 Регулярные"), KeyboardButton(text="📋 История")],
            [KeyboardButton(text="⚙️ Настройки")]
        ],
        resize_keyboard=True,
        input_field_placeholder="Выберите действие..."
    )

def create_cancel_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура с кнопкой отмены"""
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ Отмена")]],
        resize_keyboard=True
    )

def create_categories_keyboard(categories: List[str]) -> ReplyKeyboardMarkup:
    """Создает клавиатуру с категориями"""
    buttons = [KeyboardButton(text=category) for category in categories]
    # Разбиваем на ряды по 2 кнопки
    keyboard = [buttons[i:i + 2] for i in range(0, len(buttons), 2)]
    keyboard.append([KeyboardButton(text="❌ Отмена")])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def create_period_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура для выбора периода"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="День"), KeyboardButton(text="Неделя")],
            [KeyboardButton(text="Месяц"), KeyboardButton(text="❌ Отмена")]
        ],
        resize_keyboard=True
    )

def create_frequency_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура для выбора частоты регулярных транзакций"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Ежедневно"), KeyboardButton(text="Еженедельно")],
            [KeyboardButton(text="Ежемесячно"), KeyboardButton(text="❌ Отмена")]
        ],
        resize_keyboard=True
    )

def create_settings_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура настроек"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📋 Категории"), KeyboardButton(text="💸 Лимиты")],
            [KeyboardButton(text="📤 Экспорт"), KeyboardButton(text="⬅️ Назад")]
        ],
        resize_keyboard=True
    )

def format_amount(amount: float) -> str:
    """Форматирование суммы с валютой"""
    return f"{amount:.2f}{CURRENCY}"

async def generate_statistics_image(data: List[Tuple], title: str) -> BufferedInputFile:
    """Генерация изображения со статистикой"""
    if not data:
        return None
    
    categories = [row[0] for row in data]
    amounts = [row[1] for row in data]
    
    plt.figure(figsize=(10, 6))
    plt.bar(categories, amounts, color=['green' if amt > 0 else 'red' for amt in amounts])
    plt.title(title)
    plt.ylabel(f"Сумма ({CURRENCY})")
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=100)
    buf.seek(0)
    plt.close()
    
    return BufferedInputFile(buf.read(), filename="statistics.png")

async def generate_pie_chart(data: List[Tuple], title: str) -> BufferedInputFile:
    """Генерация круговой диаграммы"""
    if not data:
        return None
    
    categories = [row[0] for row in data]
    amounts = [abs(row[1]) for row in data]
    
    plt.figure(figsize=(8, 8))
    plt.pie(amounts, labels=categories, autopct='%1.1f%%', startangle=90)
    plt.title(title)
    
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=100)
    buf.seek(0)
    plt.close()
    
    return BufferedInputFile(buf.read(), filename="pie_chart.png")

async def get_current_balance(user_id: int) -> float:
    """Получение текущего баланса пользователя"""
    try:
        async with db.conn.cursor() as cursor:
            await cursor.execute(
                """SELECT 
                COALESCE(SUM(CASE WHEN type = 'income' THEN amount ELSE 0 END), 0) -
                COALESCE(SUM(CASE WHEN type = 'expense' THEN amount ELSE 0 END), 0)
                FROM transactions WHERE user_id = ?""",
                (user_id,)
            )
            result = await cursor.fetchone()
            return result[0] if result else 0
    except Exception as e:
        logger.error(f"Error getting balance: {e}")
        return 0

async def process_recurring_transactions():
    """Обработка регулярных транзакций"""
    while True:
        try:
            now = datetime.now()
            transactions = await db.get_recurring_transactions()
            
            for trans in transactions:
                trans_id, user_id, trans_type, amount, category, desc, freq, last_processed, _ = trans
                
                # Проверяем, нужно ли обрабатывать транзакцию
                process = False
                last_date = datetime.strptime(last_processed, '%Y-%m-%d') if last_processed else None
                
                if freq == 'daily':
                    process = not last_date or last_date.date() < now.date()
                elif freq == 'weekly':
                    process = not last_date or (now.date() - last_date.date()).days >= 7
                elif freq == 'monthly':
                    process = not last_date or last_date.month < now.month or last_date.year < now.year
                
                if process:
                    await db.add_transaction(user_id, trans_type, amount, category, desc)
                    await db.update_recurring_transaction(
                        trans_id, 
                        last_processed=now.strftime('%Y-%m-%d')
                    )
                    logger.info(f"Processed recurring transaction {trans_id} for user {user_id}")
            
            # Проверяем каждые 6 часов
            await asyncio.sleep(6 * 3600)
            
        except Exception as e:
            logger.error(f"Error in recurring transactions processing: {e}")
            await asyncio.sleep(3600)  # Ждем 1 час при ошибке

# --- Обработчики команд ---
@dp.message(CommandStart())
async def cmd_start(message: Message):
    """Обработчик команды /start"""
    await message.answer(
        "💼 <b>Финансовый помощник</b>\n\n"
        "Я помогу вам вести учет доходов и расходов, "
        "устанавливать лимиты и анализировать ваши финансы.\n\n"
        "Используйте кнопки меню для управления:",
        reply_markup=create_main_keyboard(),
        parse_mode="HTML"
    )

@dp.message(F.text.in_(["⬅️ Назад", "❌ Отмена", "🔙 Назад"]))
async def cancel_handler(message: Message, state: FSMContext):
    """Универсальный обработчик отмены/возврата"""
    current_state = await state.get_state()
    if current_state:
        await state.clear()
    
    await message.answer(
        "Главное меню:",
        reply_markup=create_main_keyboard()
    )

# Для кнопки "Назад" в настройках
@dp.message(F.text == "⬅️ Назад в настройки")
async def back_to_settings(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "⚙️ Настройки:",
        reply_markup=create_settings_keyboard()
    )

@dp.message(F.text.in_(["📈 Доход", "📉 Расход"]))
async def transaction_type_handler(message: Message, state: FSMContext):
    """Обработчик выбора типа транзакции"""
    trans_type = "income" if message.text == "📈 Доход" else "expense"
    await state.set_state(Form.transaction_type)
    await state.update_data(transaction_type=trans_type)
    
    await state.set_state(Form.transaction_amount)
    await message.answer(
        f"Введите сумму { 'дохода' if trans_type == 'income' else 'расхода' }:",
        reply_markup=create_cancel_keyboard()
    )

@dp.message(Form.transaction_amount)
async def transaction_amount_handler(message: Message, state: FSMContext):
    """Обработчик ввода суммы транзакции"""
    try:
        amount = float(message.text.replace(',', '.'))
        if amount <= 0:
            raise ValueError
        
        await state.update_data(transaction_amount=amount)
        data = await state.get_data()
        trans_type = data.get('transaction_type')
        
        # Получаем категории пользователя + дефолтные
        user_categories = await db.get_user_categories(message.from_user.id, trans_type)
        categories = user_categories + (
            DEFAULT_INCOME_CATEGORIES if trans_type == "income" 
            else DEFAULT_EXPENSE_CATEGORIES
        )
        
        if not categories:
            await message.answer("У вас нет категорий. Сначала добавьте их в настройках.")
            await state.clear()
            return
        
        await state.set_state(Form.transaction_category)
        await message.answer(
            "Выберите категорию:",
            reply_markup=create_categories_keyboard(categories)
        )
    except ValueError:
        await message.answer("❌ Пожалуйста, введите положительное число!")

@dp.message(Form.transaction_category)
async def transaction_category_handler(message: Message, state: FSMContext):
    """Обработчик выбора категории транзакции"""
    await state.update_data(transaction_category=message.text)
    await state.set_state(Form.transaction_description)
    await message.answer(
        "Введите описание (или нажмите 'Пропустить'):",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Пропустить"), KeyboardButton(text="❌ Отмена")]],
            resize_keyboard=True
        )
    )

@dp.message(Form.transaction_description)
async def transaction_description_handler(message: Message, state: FSMContext):
    """Обработчик описания транзакции"""
    description = None if message.text == "Пропустить" else message.text
    data = await state.get_data()
    
    success = await db.add_transaction(
        user_id=message.from_user.id,
        transaction_type=data['transaction_type'],
        amount=data['transaction_amount'],
        category=data['transaction_category'],
        description=description
    )
    
    if success:
        trans_type = "доход" if data['transaction_type'] == "income" else "расход"
        amount = format_amount(data['transaction_amount'])
        await message.answer(
            f"✅ {trans_type.capitalize()} {amount} в категории "
            f"\"{data['transaction_category']}\" успешно добавлен!",
            reply_markup=create_main_keyboard()
        )
    else:
        await message.answer(
            "❌ Не удалось добавить транзакцию. Попробуйте позже.",
            reply_markup=create_main_keyboard()
        )
    
    await state.clear()

@dp.message(F.text == "💰 Баланс")
async def balance_handler(message: Message):
    """Обработчик запроса баланса"""
    balance = await get_current_balance(message.from_user.id)
    
    # Получаем доходы и расходы за текущий месяц
    today = datetime.now()
    first_day = today.replace(day=1).strftime('%Y-%m-%d')
    last_day = today.strftime('%Y-%m-%d')
    
    async with db.conn.cursor() as cursor:
        await cursor.execute(
            """SELECT 
            COALESCE(SUM(CASE WHEN type = 'income' THEN amount ELSE 0 END), 0) as income,
            COALESCE(SUM(CASE WHEN type = 'expense' THEN amount ELSE 0 END), 0) as expense
            FROM transactions 
            WHERE user_id = ? AND date BETWEEN ? AND ?""",
            (message.from_user.id, first_day, last_day)
        )
        monthly = await cursor.fetchone()
    
    income = monthly[0] if monthly else 0
    expense = monthly[1] if monthly else 0
    
    text = (
        f"💵 <b>Ваш баланс:</b> {format_amount(balance)}\n\n"
        f"📈 <b>Доходы в этом месяце:</b> {format_amount(income)}\n"
        f"📉 <b>Расходы в этом месяце:</b> {format_amount(expense)}\n\n"
        f"💹 <b>Накопления:</b> {format_amount(income - expense)}"
    )
    
    await message.answer(text, parse_mode="HTML")

@dp.message(F.text == "📊 Статистика")
async def statistics_handler(message: Message):
    """Обработчик запроса статистики"""
    today = datetime.now()
    first_day = today.replace(day=1).strftime('%Y-%m-%d')
    last_day = today.strftime('%Y-%m-%d')
    
    # Получаем статистику по категориям за месяц
    async with db.conn.cursor() as cursor:
        await cursor.execute(
            """SELECT category, SUM(amount) as total
            FROM transactions
            WHERE user_id = ? AND date BETWEEN ? AND ? AND type = 'income'
            GROUP BY category
            ORDER BY total DESC
            LIMIT 10""",
            (message.from_user.id, first_day, last_day)
        )
        income_stats = await cursor.fetchall()
        
        await cursor.execute(
            """SELECT category, SUM(amount) as total
            FROM transactions
            WHERE user_id = ? AND date BETWEEN ? AND ? AND type = 'expense'
            GROUP BY category
            ORDER BY total DESC
            LIMIT 10""",
            (message.from_user.id, first_day, last_day)
        )
        expense_stats = await cursor.fetchall()
    
    # Генерируем изображения
    income_image = await generate_pie_chart(
        income_stats, "Доходы по категориям") if income_stats else None
    expense_image = await generate_pie_chart(
        expense_stats, "Расходы по категориям") if expense_stats else None
    
    if income_image:
        await message.answer_photo(income_image, caption="📊 <b>Доходы по категориям</b>", parse_mode="HTML")
    else:
        await message.answer("Нет данных о доходах за этот месяц.")
    
    if expense_image:
        await message.answer_photo(expense_image, caption="📊 <b>Расходы по категориям</b>", parse_mode="HTML")
    else:
        await message.answer("Нет данных о расходах за этот месяц.")

@dp.message(F.text == "📋 История")
async def history_handler(message: Message, state: FSMContext):
    """Обработчик запроса истории транзакций"""
    transactions = await db.get_transactions(message.from_user.id, limit=10)
    
    if not transactions:
        await message.answer("У вас пока нет транзакций.")
        return
    
    text = "📋 <b>Последние транзакции:</b>\n\n"
    for trans in transactions:
        trans_type, amount, category, date, description = trans
        emoji = "📈" if trans_type == "income" else "📉"
        text += (
            f"{emoji} <b>{category}</b>: {format_amount(amount)}\n"
            f"📅 {date}\n"
        )
        if description:
            text += f"📝 {html.quote(description)}\n"
        text += "\n"
    
    await message.answer(text, parse_mode="HTML")

@dp.message(F.text == "⚙️ Настройки")
async def settings_handler(message: Message):
    """Обработчик меню настроек"""
    await message.answer(
        "⚙️ <b>Настройки</b>\n\n"
        "Здесь вы можете управлять категориями, лимитами и экспортировать данные.",
        reply_markup=create_settings_keyboard(),
        parse_mode="HTML"
    )

@dp.message(F.text == "📋 Категории")
async def categories_menu_handler(message: Message):
    """Обработчик меню категорий"""
    await message.answer(
        "📋 <b>Управление категориями</b>\n\n"
        "Вы можете добавить или удалить категории для доходов и расходов.",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="➕ Добавить категорию"), KeyboardButton(text="🗑️ Удалить категорию")],
                [KeyboardButton(text="⬅️ Назад")]
            ],
            resize_keyboard=True
        ),
        parse_mode="HTML"
    )

@dp.message(F.text == "➕ Добавить категорию")
async def add_category_start(message: Message, state: FSMContext):
    """Начало добавления категории"""
    await state.set_state(Form.category_type)
    await message.answer(
        "Выберите тип категории:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="📈 Доходы"), KeyboardButton(text="📉 Расходы")],
                [KeyboardButton(text="❌ Отмена")]
            ],
            resize_keyboard=True
        )
    )

@dp.message(Form.category_type, F.text.in_(["📈 Доходы", "📉 Расходы"]))
async def add_category_type(message: Message, state: FSMContext):
    """Обработчик типа категории"""
    trans_type = "income" if message.text == "📈 Доходы" else "expense"
    await state.update_data(category_type=trans_type)
    await state.set_state(Form.category_name)
    await message.answer(
        "Введите название новой категории:",
        reply_markup=create_cancel_keyboard()
    )

@dp.message(Form.category_name)
async def add_category_name(message: Message, state: FSMContext):
    """Обработчик названия категории"""
    category_name = message.text.strip()
    data = await state.get_data()
    trans_type = data.get('category_type')
    
    if not category_name or len(category_name) > 30:
        await message.answer("❌ Название категории должно быть от 1 до 30 символов!")
        return
    
    success = await db.add_user_category(
        user_id=message.from_user.id,
        category_name=category_name,
        transaction_type=trans_type
    )
    
    if success:
        await message.answer(
            f"✅ Категория \"{category_name}\" успешно добавлена!",
            reply_markup=create_settings_keyboard()
        )
    else:
        await message.answer(
            "❌ Не удалось добавить категорию. Возможно, она уже существует.",
            reply_markup=create_settings_keyboard()
        )
    
    await state.clear()

@dp.message(F.text == "🗑️ Удалить категорию")
async def delete_category_start(message: Message, state: FSMContext):
    """Начало удаления категории"""
    await state.set_state(Form.category_type)
    await message.answer(
        "Выберите тип категории для удаления:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="📈 Доходы"), KeyboardButton(text="📉 Расходы")],
                [KeyboardButton(text="❌ Отмена")]
            ],
            resize_keyboard=True
        )
    )

@dp.message(Form.category_type, F.text.in_(["📈 Доходы", "📉 Расходы"]))
async def delete_category_type(message: Message, state: FSMContext):
    """Обработчик типа категории для удаления"""
    trans_type = "income" if message.text == "📈 Доходы" else "expense"
    await state.update_data(category_type=trans_type)
    
    # Получаем категории пользователя
    categories = await db.get_user_categories(message.from_user.id, trans_type)
    
    if not categories:
        await message.answer(
            f"У вас нет пользовательских категорий для { 'доходов' if trans_type == 'income' else 'расходов' }.",
            reply_markup=create_settings_keyboard()
        )
        await state.clear()
        return
    
    await state.set_state(Form.delete_category)
    await message.answer(
        "Выберите категорию для удаления:",
        reply_markup=create_categories_keyboard(categories)
    )

@dp.message(Form.delete_category)
async def delete_category_confirm(message: Message, state: FSMContext):
    """Подтверждение удаления категории"""
    category_name = message.text
    data = await state.get_data()
    trans_type = data.get('category_type')
    
    success = await db.delete_user_category(
        user_id=message.from_user.id,
        category_name=category_name,
        transaction_type=trans_type
    )
    
    if success:
        await message.answer(
            f"✅ Категория \"{category_name}\" успешно удалена!",
            reply_markup=create_settings_keyboard()
        )
    else:
        await message.answer(
            "❌ Не удалось удалить категорию. Возможно, она не существует.",
            reply_markup=create_settings_keyboard()
        )
    
    await state.clear()

@dp.message(F.text == "💸 Лимиты")
async def limits_menu_handler(message: Message):
    """Обработчик меню лимитов"""
    limits = await db.get_budget_limits(message.from_user.id)
    
    if not limits:
        await message.answer(
            "💸 <b>Лимиты бюджета</b>\n\n"
            "У вас пока не установлены лимиты.",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="➕ Установить лимит")],
                    [KeyboardButton(text="⬅️ Назад")]
                ],
                resize_keyboard=True
            ),
            parse_mode="HTML"
        )
        return
    
    text = "💸 <b>Ваши лимиты:</b>\n\n"
    for limit in limits:
        category, limit_amount, trans_type, period = limit
        current = await db.get_category_spending(
            message.from_user.id, category, trans_type, period)
        
        percentage = (current / limit_amount) * 100 if limit_amount > 0 else 0
        emoji = "🟢" if percentage < 80 else "🟡" if percentage < 100 else "🔴"
        
        text += (
            f"{emoji} <b>{category}</b> ({trans_type}, {period}):\n"
            f"{format_amount(current)} / {format_amount(limit_amount)}\n"
            f"({percentage:.1f}% от лимита)\n\n"
        )
    
    await message.answer(
        text,
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="➕ Установить лимит")],
                [KeyboardButton(text="⬅️ Назад")]
            ],
            resize_keyboard=True
        ),
        parse_mode="HTML"
    )

@dp.message(F.text == "➕ Установить лимит")
async def set_limit_start(message: Message, state: FSMContext):
    """Начало установки лимита"""
    await state.set_state(Form.limit_category_type)
    await message.answer(
        "Выберите тип лимита:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="📈 Лимит доходов"), KeyboardButton(text="📉 Лимит расходов")],
                [KeyboardButton(text="❌ Отмена")]
            ],
            resize_keyboard=True
        )
    )

@dp.message(Form.limit_category_type, F.text.in_(["📈 Лимит доходов", "📉 Лимит расходов"]))
async def set_limit_type(message: Message, state: FSMContext):
    """Обработчик типа лимита"""
    trans_type = "income" if message.text == "📈 Лимит доходов" else "expense"
    await state.update_data(limit_category_type=trans_type)
    
    # Получаем категории пользователя + дефолтные
    categories = await db.get_user_categories(message.from_user.id, trans_type)
    categories = categories + (
        DEFAULT_INCOME_CATEGORIES if trans_type == "income" 
        else DEFAULT_EXPENSE_CATEGORIES
    )
    
    if not categories:
        await message.answer(
            f"У вас нет категорий для { 'доходов' if trans_type == 'income' else 'расходов' }. "
            "Сначала добавьте их в настройках.",
            reply_markup=create_settings_keyboard()
        )
        await state.clear()
        return
    
    await state.set_state(Form.limit_category)
    await message.answer(
        "Выберите категорию:",
        reply_markup=create_categories_keyboard(categories)
    )

@dp.message(Form.limit_category)
async def set_limit_category(message: Message, state: FSMContext):
    """Обработчик категории лимита"""
    await state.update_data(limit_category=message.text)
    await state.set_state(Form.limit_period)
    await message.answer(
        "Выберите период для лимита:",
        reply_markup=create_period_keyboard()
    )

@dp.message(Form.limit_period)
async def set_limit_period(message: Message, state: FSMContext):
    """Обработчик периода лимита"""
    period_map = {
        "День": "day",
        "Неделя": "week",
        "Месяц": "month"
    }
    
    if message.text not in period_map:
        await message.answer("❌ Пожалуйста, выберите период из предложенных.")
        return
    
    await state.update_data(limit_period=period_map[message.text])
    await state.set_state(Form.limit_amount)
    await message.answer(
        "Введите сумму лимита:",
        reply_markup=create_cancel_keyboard()
    )

@dp.message(Form.limit_amount)
async def set_limit_amount(message: Message, state: FSMContext):
    """Обработчик суммы лимита"""
    try:
        amount = float(message.text.replace(',', '.'))
        if amount <= 0:
            await message.answer("❌ Сумма должна быть больше нуля!")
            return
        
        data = await state.get_data()
        
        # Проверяем, что все необходимые данные есть
        if not all(k in data for k in ['limit_category_type', 'limit_category', 'limit_period']):
            await message.answer("❌ Ошибка данных. Попробуйте снова.")
            await state.clear()
            return
        
        success = await db.add_budget_limit(
            user_id=message.from_user.id,
            category=data['limit_category'],
            limit_amount=amount,
            transaction_type=data['limit_category_type'],
            period=data['limit_period']
        )
        
        if success:
            await message.answer(
                f"✅ Лимит для категории \"{data['limit_category']}\" установлен!",
                reply_markup=create_settings_keyboard()
            )
        else:
            await message.answer(
                "❌ Не удалось установить лимит. Попробуйте позже.",
                reply_markup=create_settings_keyboard()
            )
    except ValueError:
        await message.answer("❌ Пожалуйста, введите положительное число!")
    finally:
        await state.clear()

@dp.message(F.text == "🔁 Регулярные")
async def recurring_menu_handler(message: Message):
    """Обработчик меню регулярных транзакций"""
    transactions = await db.get_recurring_transactions(message.from_user.id)
    
    if not transactions:
        await message.answer(
            "🔁 <b>Регулярные транзакции</b>\n\n"
            "У вас пока нет регулярных транзакций.",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="➕ Добавить регулярную")],
                    [KeyboardButton(text="⬅️ Назад")]
                ],
                resize_keyboard=True
            ),
            parse_mode="HTML"
        )
        return
    
    text = "🔁 <b>Ваши регулярные транзакции:</b>\n\n"
    for trans in transactions:
        _, trans_type, amount, category, desc, freq, start_date, last_processed, _ = trans
        emoji = "📈" if trans_type == "income" else "📉"
        text += (
            f"{emoji} <b>{category}</b>: {format_amount(amount)}\n"
            f"🔄 Частота: {freq}\n"
            f"📅 Начало: {start_date}\n"
        )
        if last_processed:
            text += f"⏱ Последнее выполнение: {last_processed}\n"
        if desc:
            text += f"📝 {html.quote(desc)}\n"
        text += "\n"
    
    await message.answer(
        text,
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="➕ Добавить регулярную")],
                [KeyboardButton(text="⬅️ Назад")]
            ],
            resize_keyboard=True
        ),
        parse_mode="HTML"
    )

@dp.message(F.text == "➕ Добавить регулярную")
async def add_recurring_start(message: Message, state: FSMContext):
    """Начало добавления регулярной транзакции"""
    await state.set_state(Form.recurring_type)
    await message.answer(
        "Выберите тип регулярной транзакции:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="📈 Доход"), KeyboardButton(text="📉 Расход")],
                [KeyboardButton(text="❌ Отмена")]
            ],
            resize_keyboard=True
        )
    )

@dp.message(Form.recurring_type, F.text.in_(["📈 Доход", "📉 Расход"]))
async def add_recurring_type(message: Message, state: FSMContext):
    """Обработчик типа регулярной транзакции"""
    trans_type = "income" if message.text == "📈 Доход" else "expense"
    await state.update_data(recurring_type=trans_type)
    await state.set_state(Form.recurring_amount)
    await message.answer(
        "Введите сумму:",
        reply_markup=create_cancel_keyboard()
    )

@dp.message(Form.recurring_amount)
async def add_recurring_amount(message: Message, state: FSMContext):
    """Обработчик суммы регулярной транзакции"""
    try:
        amount = float(message.text.replace(',', '.'))
        if amount <= 0:
            raise ValueError
        
        await state.update_data(recurring_amount=amount)
        data = await state.get_data()
        trans_type = data.get('recurring_type')
        
        # Получаем категории пользователя + дефолтные
        categories = await db.get_user_categories(message.from_user.id, trans_type)
        categories = categories + (
            DEFAULT_INCOME_CATEGORIES if trans_type == "income" 
            else DEFAULT_EXPENSE_CATEGORIES
        )
        
        if not categories:
            await message.answer(
                f"У вас нет категорий для { 'доходов' if trans_type == 'income' else 'расходов' }. "
                "Сначала добавьте их в настройках.",
                reply_markup=create_settings_keyboard()
            )
            await state.clear()
            return
        
        await state.set_state(Form.recurring_category)
        await message.answer(
            "Выберите категорию:",
            reply_markup=create_categories_keyboard(categories)
        )
    except ValueError:
        await message.answer("❌ Пожалуйста, введите положительное число!")

@dp.message(Form.recurring_category)
async def add_recurring_category(message: Message, state: FSMContext):
    """Обработчик категории регулярной транзакции"""
    await state.update_data(recurring_category=message.text)
    await state.set_state(Form.recurring_description)
    await message.answer(
        "Введите описание (или нажмите 'Пропустить'):",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Пропустить"), KeyboardButton(text="❌ Отмена")]],
            resize_keyboard=True
        )
    )

@dp.message(Form.recurring_description)
async def add_recurring_description(message: Message, state: FSMContext):
    """Обработчик описания регулярной транзакции"""
    description = None if message.text == "Пропустить" else message.text
    await state.update_data(recurring_description=description)
    await state.set_state(Form.recurring_frequency)
    await message.answer(
        "Выберите частоту выполнения:",
        reply_markup=create_frequency_keyboard()
    )

@dp.message(Form.recurring_frequency)
async def add_recurring_frequency(message: Message, state: FSMContext):
    """Обработчик частоты регулярной транзакции"""
    freq_map = {
        "Ежедневно": "daily",
        "Еженедельно": "weekly",
        "Ежемесячно": "monthly"
    }
    
    if message.text not in freq_map:
        await message.answer("❌ Пожалуйста, выберите частоту из предложенных.")
        return
    
    data = await state.get_data()
    success = await db.add_recurring_transaction(
        user_id=message.from_user.id,
        transaction_type=data['recurring_type'],
        amount=data['recurring_amount'],
        category=data['recurring_category'],
        description=data['recurring_description'],
        frequency=freq_map[message.text]
    )
    
    if success:
        await message.answer(
            "✅ Регулярная транзакция успешно добавлена!",
            reply_markup=create_settings_keyboard()
        )
    else:
        await message.answer(
            "❌ Не удалось добавить регулярную транзакцию. Попробуйте позже.",
            reply_markup=create_settings_keyboard()
        )
    
    await state.clear()

@dp.message(F.text == "📤 Экспорт")
async def export_start(message: Message, state: FSMContext):
    """Начало процесса экспорта"""
    await state.set_state(Form.export_start_date)
    await message.answer(
        "Введите начальную дату экспорта (ГГГГ-ММ-ДД):",
        reply_markup=create_cancel_keyboard()
    )

@dp.message(Form.export_start_date)
async def export_end_date(message: Message, state: FSMContext):
    """Обработчик конечной даты экспорта"""
    try:
        start_date = datetime.strptime(message.text, '%Y-%m-%d').strftime('%Y-%m-%d')
        await state.update_data(export_start_date=start_date)
        await state.set_state(Form.export_end_date)
        await message.answer(
            "Введите конечную дату экспорта (ГГГГ-ММ-ДД):",
            reply_markup=create_cancel_keyboard()
        )
    except ValueError:
        await message.answer("❌ Неверный формат даты. Используйте ГГГГ-ММ-ДД.")

@dp.message(Form.export_end_date)
async def export_format(message: Message, state: FSMContext):
    """Обработчик формата экспорта"""
    try:
        end_date = datetime.strptime(message.text, '%Y-%m-%d').strftime('%Y-%m-%d')
        await state.update_data(export_end_date=end_date)
        await state.set_state(Form.export_format)
        await message.answer(
            "Выберите формат экспорта:",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="CSV"), KeyboardButton(text="TXT")],
                    [KeyboardButton(text="❌ Отмена")]
                ],
                resize_keyboard=True
            )
        )
    except ValueError:
        await message.answer("❌ Неверный формат даты. Используйте ГГГГ-ММ-ДД.")

@dp.message(Form.export_format, F.text.in_(["CSV", "TXT"]))
async def export_data(message: Message, state: FSMContext):
    """Обработчик экспорта данных"""
    data = await state.get_data()
    start_date = data.get('export_start_date')
    end_date = data.get('export_end_date')
    export_format = message.text.lower()
    
    transactions = await db.get_transactions(
        message.from_user.id, start_date, end_date, limit=1000)
    
    if not transactions:
        await message.answer(
            "Нет транзакций за указанный период.",
            reply_markup=create_settings_keyboard()
        )
        await state.clear()
        return
    
    try:
        with tempfile.NamedTemporaryFile(mode='w', delete=False, 
                                       suffix=f'.{export_format}',
                                       encoding='utf-8') as tmp:
            if export_format == 'csv':
                writer = csv.writer(tmp)
                writer.writerow(['Тип', 'Сумма', 'Категория', 'Дата', 'Описание'])
                for trans in transactions:
                    writer.writerow(trans)
            else:  # TXT
                tmp.write("Ваши транзакции:\n\n")
                for trans in transactions:
                    trans_type, amount, category, date, desc = trans
                    tmp.write(
                        f"{'Доход' if trans_type == 'income' else 'Расход'}: {amount} {CURRENCY}\n"
                        f"Категория: {category}\n"
                        f"Дата: {date}\n"
                    )
                    if desc:
                        tmp.write(f"Описание: {desc}\n")
                    tmp.write("\n")
        
        await message.answer_document(
            FSInputFile(tmp.name, filename=f'transactions_{start_date}_to_{end_date}.{export_format}'),
            caption=f"Экспорт транзакций с {start_date} по {end_date}"
        )
        os.unlink(tmp.name)
    except Exception as e:
        logger.error(f"Export error: {e}")
        await message.answer(
            "❌ Произошла ошибка при экспорте. Попробуйте позже.",
            reply_markup=create_settings_keyboard()
        )
    
    await state.clear()

# --- Запуск приложения ---
async def on_startup():
    """Действия при запуске бота"""
    logger.info("Подключение к базе данных...")
    await db.connect()
    logger.info("Запуск обработчика регулярных транзакций...")
    asyncio.create_task(process_recurring_transactions())

async def on_shutdown():
    """Действия при выключении бота"""
    logger.info("Отключение от базы данных...")
    await db.close()

async def main():
    """Основная функция запуска бота"""
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    
    try:
        logger.info("Запуск бота...")
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Ошибка при работе бота: {e}")
    finally:
        logger.info("Бот остановлен")

if __name__ == "__main__":
    asyncio.run(main())