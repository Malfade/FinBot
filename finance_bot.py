import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, Message
from aiogram.types.reply_keyboard_remove import ReplyKeyboardRemove
import aiosqlite

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()
# Load Bot Token from environment variable (recommended)
API_TOKEN = os.getenv('API_TOKEN', 'ваш_резервный_токен')

# Initialize bot and Dispatcher
bot = Bot(token=API_TOKEN)
dp = Dispatcher()

# Database connection (asynchronous with aiosqlite)
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
        await self.cursor.execute(""" 
            CREATE TABLE IF NOT EXISTS transactions ( 
                id INTEGER PRIMARY KEY AUTOINCREMENT, 
                user_id INTEGER, 
                type TEXT, 
                amount REAL, 
                category TEXT, 
                date TEXT 
            ) 
        """)
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
            logger.error(f"Database error: {e}")
            return False

    async def get_balance(self, user_id):
        await self.cursor.execute("SELECT SUM(amount) FROM transactions WHERE user_id = ? AND type = 'income'", (user_id,))
        total_income = await self.cursor.fetchone()  # здесь добавляем await
        total_income = total_income[0] if total_income else 0  # если fetchone возвращает None, устанавливаем 0

        await self.cursor.execute("SELECT SUM(amount) FROM transactions WHERE user_id = ? AND type = 'expense'", (user_id,))
        total_expense = await self.cursor.fetchone()  # здесь добавляем await
        total_expense = total_expense[0] if total_expense else 0  # если fetchone возвращает None, устанавливаем 0

        return total_income, total_expense

    async def get_monthly_stats(self, user_id):
        current_month = datetime.now().strftime('%Y-%m')
        await self.cursor.execute("""
            SELECT category, SUM(amount) as total 
            FROM transactions 
            WHERE user_id = ? AND type = 'expense' 
            AND strftime('%Y-%m', date) = ? 
            GROUP BY category 
            ORDER BY total DESC
        """, (user_id, current_month))
        
        return await self.cursor.fetchall()

    async def close(self):
        await self.conn.close()


# State machine for handling transaction input
class TransactionStates(StatesGroup):
    waiting_for_transaction = State()
    transaction_type = State()
    category = State()
    amount = State()

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

# Category selection keyboard
def create_category_keyboard(categories):
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=category)] for category in categories],
        resize_keyboard=True
    )

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
    categories = ["Зарплата", "Подарок", "Прочее"]
    await message.answer("💸 Выберите категорию дохода", reply_markup=create_category_keyboard(categories))

# Expense handler
@dp.message(F.text == "📉 Добавить расход")
async def add_expense(message: Message, state: FSMContext):
    await state.set_state(TransactionStates.waiting_for_transaction)
    await state.update_data(transaction_type="expense")
    categories = ["Еда", "Транспорт", "Прочее"]
    await message.answer("💳 Выберите категорию расхода", reply_markup=create_category_keyboard(categories))

# Category selection handler
@dp.message(TransactionStates.waiting_for_transaction)
async def select_category(message: Message, state: FSMContext):
    selected_category = message.text.strip()
    
    # Validate selected category
    state_data = await state.get_data()
    if state_data.get('transaction_type') == 'income':
        categories = ["Зарплата", "Подарок", "Прочее"]
    else:
        categories = ["Еда", "Транспорт", "Прочее"]
        
    if selected_category not in categories:
        await message.answer(f"❌ Ошибка! Выберите корректную категорию из списка.")
        return
    
    # Save the category and ask for amount
    await state.update_data(category=selected_category)
    await state.set_state(TransactionStates.amount)
    
    await message.answer("💰 Введите сумму транзакции (например: 50000)")

# Process amount input
@dp.message(TransactionStates.amount)
async def process_amount(message: Message, state: FSMContext):
    user_id = message.from_user.id
    data = message.text.strip()
    
    try:
        amount = float(data)
        
        if amount <= 0:
            await message.answer("❌ Сумма должна быть положительной.")
            return
        
        # Get transaction type and category from state data
        state_data = await state.get_data()
        transaction_type = state_data.get('transaction_type')
        category = state_data.get('category')
        
        # Add transaction to database
        if await db.add_transaction(user_id, transaction_type, amount, category):
            await message.answer("✅ Транзакция успешно добавлена!", reply_markup=create_keyboard())
        else:
            await message.answer("❌ Не удалось сохранить транзакцию. Попробуйте снова.")
        
        # Reset state and return to main menu
        await state.clear()
        await message.answer("📝 Выберите одну из опций ниже:", reply_markup=create_keyboard())
    
    except ValueError:
        await message.answer("❌ Ошибка! Введите корректную сумму в числовом формате.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        await message.answer("Произошла непредвиденная ошибка. Попробуйте снова.")

# Balance handler
@dp.message(F.text == "💰 Баланс")
async def get_balance(message: Message):
    total_income, total_expense = await db.get_balance(message.from_user.id)
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
    stats = await db.get_monthly_stats(message.from_user.id)
    
    if not stats:
        await message.answer("📊 За этот месяц нет статистики.")
        return
    
    response = "📊 Статистика расходов за месяц:\n"
    for category, total in stats:
        response += f"{category}: {total:.2f} руб.\n"
    
    await message.answer(response)

# Prevent unknown messages from breaking the flow
@dp.message()
async def handle_unknown(message: Message):
    await message.answer("❓ Я не понял вашу команду. Используйте кнопки меню.")

# Main polling loop
async def main():
    try:
        await db.connect()
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Bot initialization error: {e}")
    finally:
        await bot.session.close()
        await db.close()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
