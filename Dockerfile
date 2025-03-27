FROM python:3.9
WORKDIR /app
COPY . .
RUN pip install aiogram python-dotenv
CMD ["python", "finance_bot.py"]