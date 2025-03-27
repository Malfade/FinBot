import os
import sqlite3
import logging
from datetime import datetime
from aiogram import Bot, Dispatcher, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, Message
from aiogram.types.reply_keyboard_remove import ReplyKeyboardRemove

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load Bot Token from environment variable (recommended)
API_TOKEN = os.getenv('API_TOKEN', 'ваш_резервный_токен')

# Initialize bot and Dispatcher
bot = Bot(token=API_TOKEN)
dp = Dispatcher()

# Database connection
class Database:
    def __init__(self, db_name='finance.db'):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self.create_tables()

    def create_tables(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                type TEXT,
                amount REAL,
                category TEXT,
                date TEXT
            )
        """)
        self.conn.commit()

    def add_transaction(self, user_id, transaction_type, amount, category):
        try:
            self.cursor.execute(
                "INSERT INTO transactions (user_id, type, amount, category, date) VALUES (?, ?, ?, ?, ?)", 
                (user_id, transaction_type, amount, category, datetime.now().strftime('%Y-%m-%d'))
            )
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Database error: {e}")
            return False

    def get_balance(self, user_id):
        self.cursor.execute("SELECT SUM(amount) FROM transactions WHERE user_id = ? AND type = 'income'", (user_id,))
        total_income = self.cursor.fetchone()[0] or 0
        
        self.cursor.execute("SELECT SUM(amount) FROM transactions WHERE user_id = ? AND type = 'expense'", (user_id,))
        total_expense = self.cursor.fetchone()[0] or 0
        
        return total_income, total_expense

    def get_monthly_stats(self, user_id):
        current_month = datetime.now().strftime('%Y-%m')
        self.cursor.execute("""
            SELECT category, SUM(amount) as total 
            FROM transactions 
            WHERE user_id = ? AND type = 'expense' 
            AND strftime('%Y-%m', date) = ?
            GROUP BY category 
            ORDER BY total DESC
        """, (user_id, current_month))
        
        return self.cursor.fetchall()

    def close(self):
        self.conn.close()

# State machine for handling transaction input
class TransactionStates(StatesGroup):
    waiting_for_transaction = State()
    transaction_type = State()

# Initialize database
db = Database()

# Keyboard setup
def create_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="📈 Добавить доход"),
                KeyboardButton(text="📉 Добавить расход")
            ],
            [
                KeyboardButton(text="💰 Баланс"),
                KeyboardButton(text="📊 Статистика")
            ]
        ],
        resize_keyboard=True
    )
    return keyboard

# Start command handler
@dp.message(F.text == "/start")
async def start_command(message: Message):
    await message.answer(
        "👋 Привет! Я твой личный финансовый помощник. \n\n"
        "Используй кнопки внизу для управления финансами:\n"
        "📈 Добавить доход - записать полученные деньги\n"
        "📉 Добавить расход - записать потраченные деньги\n"
        "💰 Баланс - посмотреть общую финансовую статистику\n"
        "📊 Статистика - детальная статистика расходов за месяц",
        reply_markup=create_keyboard()
    )

# Income handler
@dp.message(F.text == "📈 Добавить доход")
async def add_income(message: Message, state: FSMContext):
    await state.set_state(TransactionStates.waiting_for_transaction)
    await state.update_data(transaction_type="income")
    await message.answer("💸 Введите сумму дохода и категорию (например: 50000 зарплата)")

# Expense handler
@dp.message(F.text == "📉 Добавить расход")
async def add_expense(message: Message, state: FSMContext):
    await state.set_state(TransactionStates.waiting_for_transaction)
    await state.update_data(transaction_type="expense")
    await message.answer("💳 Введите сумму расхода и категорию (например: 1500 еда)")

# Balance handler
@dp.message(F.text == "💰 Баланс")
async def get_balance(message: Message):
    total_income, total_expense = db.get_balance(message.from_user.id)
    balance = total_income - total_expense
    
    await message.answer(
        f"💰 Ваш финансовый баланс:\n"
        f"Доходы: {total_income:.2f} руб.\n"
        f"Расходы: {total_expense:.2f} руб.\n"
        f"Итого: {balance:.2f} руб."
    )

# Monthly statistics handler
@dp.message(F.text == "📊 Статистика")
async def get_statistics(message: Message):
    stats = db.get_monthly_stats(message.from_user.id)
    
    if not stats:
        await message.answer("📊 За этот месяц нет статистики.")
        return
    
    response = "📊 Статистика расходов за месяц:\n"
    for category, total in stats:
        response += f"{category}: {total:.2f} руб.\n"
    
    await message.answer(response)

# Transaction processing
@dp.message(TransactionStates.waiting_for_transaction)
async def process_transaction(message: Message, state: FSMContext):
    user_id = message.from_user.id
    data = message.text.split(" ", 1)
    
    try:
        # Validate input
        if len(data) < 2:
            await message.answer("❌ Ошибка! Введите сумму и категорию через пробел.")
            return
        
        amount = float(data[0])
        category = data[1].strip()
        
        # Prevent negative amounts
        if amount <= 0:
            await message.answer("❌ Сумма должна быть положительной.")
            return
        
        # Get transaction type from state data
        state_data = await state.get_data()
        transaction_type = state_data.get('transaction_type', 'expense')
        
        # Add transaction to database
        if db.add_transaction(user_id, transaction_type, amount, category):
            await message.answer("✅ Запись успешно добавлена!", reply_markup=create_keyboard())
        else:
            await message.answer("❌ Не удалось сохранить транзакцию. Попробуйте снова.")
        
        # Reset state
        await state.clear()
    
    except ValueError:
        await message.answer("❌ Ошибка! Сумма должна быть числом.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        await message.answer("Произошла непредвиденная ошибка. Попробуйте снова.")

# Prevent unknown messages from breaking the flow
@dp.message()
async def handle_unknown(message: Message):
    await message.answer("❓ Я не понял вашу команду. Используйте кнопки меню.")

async def main():
    try:
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Bot initialization error: {e}")
    finally:
        await bot.session.close()
        db.close()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())