import os
import logging
import asyncio
from datetime import datetime
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, ReplyKeyboardMarkup, KeyboardButton,
    ReplyKeyboardRemove, FSInputFile, BufferedInputFile
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
import aiosqlite
import matplotlib.pyplot as plt
import io
import tempfile

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()
API_TOKEN = os.getenv('API_TOKEN', 'ваш_резервный_токен')

bot = Bot(token=API_TOKEN)
dp = Dispatcher()

# --- Функция для генерации графиков ---
async def generate_pie_chart(data: list[tuple], title: str) -> io.BytesIO:
    categories = [item[0] for item in data]
    amounts = [item[1] for item in data]
    
    plt.figure(figsize=(8, 6))
    plt.pie(amounts, labels=categories, autopct='%1.1f%%', startangle=90)
    plt.title(title)
    plt.axis('equal')
    
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=100)
    buf.seek(0)
    plt.close()
    return buf

# --- База данных ---
class Database:
    def __init__(self, db_name='finance.db'):
        self.db_name = db_name
        self.conn = None
        self.cursor = None

    async def connect(self):
        self.conn = await aiosqlite.connect(self.db_name)
        self.cursor = await self.conn.cursor()
        await self.create_tables()

    async def create_tables(self):
        await self.cursor.execute("""CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            type TEXT,
            amount REAL,
            category TEXT,
            date TEXT
        )""")
        await self.cursor.execute("""CREATE TABLE IF NOT EXISTS user_categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            category_name TEXT,
            transaction_type TEXT
        )""")
        await self.conn.commit()

    async def add_transaction(self, user_id, transaction_type, amount, category):
        try:
            await self.cursor.execute(
                "INSERT INTO transactions (user_id, type, amount, category, date) VALUES (?, ?, ?, ?, ?)", 
                (user_id, transaction_type, amount, category, datetime.now().strftime('%Y-%m-%d'))
            )
            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка БД: {e}")
            return False
    
    async def add_user_category(self, user_id, category_name, transaction_type):
        try:
            await self.cursor.execute(
                "INSERT INTO user_categories VALUES (NULL, ?, ?, ?)",
                (user_id, category_name, transaction_type))
            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления категории: {e}")
            return False

    async def get_user_categories(self, user_id, transaction_type):
        await self.cursor.execute(
            "SELECT category_name FROM user_categories WHERE user_id = ? AND transaction_type = ?",
            (user_id, transaction_type))
        return [row[0] for row in await self.cursor.fetchall()]

    async def get_balance(self, user_id):
        await self.cursor.execute(
            "SELECT SUM(amount) FROM transactions WHERE user_id = ? AND type = 'income'", (user_id,))
        income = (await self.cursor.fetchone())[0] or 0
        
        await self.cursor.execute(
            "SELECT SUM(amount) FROM transactions WHERE user_id = ? AND type = 'expense'", (user_id,))
        expense = (await self.cursor.fetchone())[0] or 0
        
        return income, expense

    async def get_monthly_stats(self, user_id, transaction_type=None):
        current_month = datetime.now().strftime('%Y-%m')
        
        if transaction_type:
            query = """
                SELECT category, SUM(amount) FROM transactions
                WHERE user_id = ? AND type = ? AND strftime('%Y-%m', date) = ?
                GROUP BY category
                ORDER BY SUM(amount) DESC
            """
            params = (user_id, transaction_type, current_month)
        else:
            query = """
                SELECT category, SUM(amount) FROM transactions
                WHERE user_id = ? AND strftime('%Y-%m', date) = ?
                GROUP BY category
                ORDER BY SUM(amount) DESC
            """
            params = (user_id, current_month)
        
        await self.cursor.execute(query, params)
        return await self.cursor.fetchall()

    async def close(self):
        await self.conn.close()

# --- States ---
class TransactionStates(StatesGroup):
    waiting_for_transaction = State()
    transaction_type = State()
    category = State()
    amount = State()
    adding_new_category = State()

db = Database()

# --- Клавиатуры ---
def create_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📈 Добавить доход"), KeyboardButton(text="📉 Добавить расход")],
            [KeyboardButton(text="💰 Баланс"), KeyboardButton(text="📊 Статистика")],
            [KeyboardButton(text="⚙️ Настроить категории")]
        ],
        resize_keyboard=True
    )

def create_category_keyboard(categories):
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=cat)] for cat in categories],
        resize_keyboard=True
    )

# --- Хэндлеры ---
@dp.message(F.text == "/start")
async def start(message: Message):
    await message.answer(
        "👋 Привет! Я твой финансовый помощник.\n"
        "Используй кнопки ниже для управления бюджетом:",
        reply_markup=create_keyboard()
    )

@dp.message(F.text == "⚙️ Настроить категории")
async def manage_categories(message: Message):
    await message.answer(
        "Выберите действие:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="📈 Добавить категорию доходов")],
                [KeyboardButton(text="📉 Добавить категорию расходов")],
                [KeyboardButton(text="🔙 Назад")]
            ],
            resize_keyboard=True
        )
    )

@dp.message(F.text == "📈 Добавить категорию доходов")
async def add_income_category(message: Message, state: FSMContext):
    await state.set_state(TransactionStates.adding_new_category)
    await state.update_data(transaction_type="income")
    await message.answer("Введите название категории:", reply_markup=ReplyKeyboardRemove())

@dp.message(F.text == "📉 Добавить категорию расходов")
async def add_expense_category(message: Message, state: FSMContext):
    await state.set_state(TransactionStates.adding_new_category)
    await state.update_data(transaction_type="expense")
    await message.answer("Введите название категории:", reply_markup=ReplyKeyboardRemove())

@dp.message(TransactionStates.adding_new_category)
async def save_category(message: Message, state: FSMContext):
    data = await state.get_data()
    if await db.add_user_category(
        user_id=message.from_user.id,
        category_name=message.text,
        transaction_type=data["transaction_type"]
    ):
        await message.answer("✅ Категория добавлена!", reply_markup=create_keyboard())
    else:
        await message.answer("❌ Ошибка! Попробуйте снова.")
    await state.clear()

@dp.message(F.text == "📈 Добавить доход")
async def add_income(message: Message, state: FSMContext):
    await state.set_state(TransactionStates.waiting_for_transaction)
    await state.update_data(transaction_type="income")
    categories = await db.get_user_categories(message.from_user.id, "income")
    categories = categories + ["Зарплата", "Подарок", "Прочее"]  # Дефолтные категории
    await message.answer("Выберите категорию:", reply_markup=create_category_keyboard(categories))

@dp.message(F.text == "📉 Добавить расход")
async def add_expense(message: Message, state: FSMContext):
    await state.set_state(TransactionStates.waiting_for_transaction)
    await state.update_data(transaction_type="expense")
    categories = await db.get_user_categories(message.from_user.id, "expense")
    categories = categories + ["Еда", "Транспорт", "Прочее"]
    await message.answer("Выберите категорию:", reply_markup=create_category_keyboard(categories))

@dp.message(TransactionStates.waiting_for_transaction)
async def select_category(message: Message, state: FSMContext):
    await state.update_data(category=message.text)
    await state.set_state(TransactionStates.amount)
    await message.answer("Введите сумму:", reply_markup=ReplyKeyboardRemove())

@dp.message(TransactionStates.amount)
async def save_amount(message: Message, state: FSMContext):
    try:
        amount = float(message.text)
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введите положительное число!")
        return
    
    data = await state.get_data()
    if await db.add_transaction(
        user_id=message.from_user.id,
        transaction_type=data["transaction_type"],
        amount=amount,
        category=data["category"]
    ):
        await message.answer("✅ Транзакция сохранена!", reply_markup=create_keyboard())
    else:
        await message.answer("❌ Ошибка сохранения!")
    await state.clear()

@dp.message(F.text == "💰 Баланс")
async def show_balance(message: Message):
    income, expense = await db.get_balance(message.from_user.id)
    balance = income - expense
    await message.answer(
        f"💵 Доходы: {income:.2f} руб\n"
        f"💸 Расходы: {expense:.2f} руб\n"
        f"💰 Баланс: {balance:.2f} руб"
    )

@dp.message(F.text == "📊 Статистика")
async def show_stats(message: Message):
    # Получаем данные
    income, expense = await db.get_balance(message.from_user.id)
    balance = income - expense
    
    # Получаем статистику по категориям
    income_stats = await db.get_monthly_stats(message.from_user.id, "income")
    expense_stats = await db.get_monthly_stats(message.from_user.id, "expense")
    
    # Текстовая статистика
    text = (
        f"💰 Общий баланс: {balance:.2f} руб\n"
        f"💵 Доходы: {income:.2f} руб\n"
        f"💸 Расходы: {expense:.2f} руб\n\n"
    )
    
    # Создаём графики
    try:
        # Создаём фигуру с несколькими графиками
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
        
        # График доходов
        if income_stats:
            income_categories = [item[0] for item in income_stats]
            income_amounts = [item[1] for item in income_stats]
            ax1.pie(income_amounts, labels=income_categories, autopct='%1.1f%%', startangle=90)
            ax1.set_title('Доходы по категориям')
        else:
            ax1.text(0.5, 0.5, 'Нет данных о доходах', ha='center', va='center')
        
        # График расходов
        if expense_stats:
            expense_categories = [item[0] for item in expense_stats]
            expense_amounts = [item[1] for item in expense_stats]
            ax2.pie(expense_amounts, labels=expense_categories, autopct='%1.1f%%', startangle=90)
            ax2.set_title('Расходы по категориям')
        else:
            ax2.text(0.5, 0.5, 'Нет данных о расходах', ha='center', va='center')
        
        plt.tight_layout()
        
        # Сохраняем в буфер
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=100)
        buf.seek(0)
        plt.close()
        
        # Создаём временный файл
        with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp:
            tmp.write(buf.getvalue())
            tmp_path = tmp.name
        
        # Отправляем фото
        photo = FSInputFile(tmp_path)
        await message.answer_photo(photo, caption=text)
        
        # Удаляем временный файл
        os.unlink(tmp_path)
    
    except Exception as e:
        logger.error(f"Ошибка графика: {e}")
        await message.answer(text + "\nНе удалось сгенерировать графики.")

@dp.message(F.text == "🔙 Назад")
async def back_to_menu(message: Message):
    await message.answer("Главное меню:", reply_markup=create_keyboard())

@dp.message()
async def unknown_command(message: Message):
    await message.answer("Используйте кнопки меню", reply_markup=create_keyboard())

# --- Запуск ---
async def main():
    await db.connect()
    await dp.start_polling(bot)
    await db.close()

if __name__ == "__main__":
    asyncio.run(main())