import os
from dotenv import load_dotenv

load_dotenv()

ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")

if not ALPHA_VANTAGE_API_KEY:
    raise ValueError("❌ Отсутствует ключ API для Alpha Vantage. Установите его в .env или переменную окружения.")
