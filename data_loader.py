import pandas as pd
import requests
import time
import logging
from config import TICKERS, INTERVALS, OUTPUTSIZE, MAX_RETRIES, RETRY_DELAY
from secret_config import ALPHA_VANTAGE_API_KEY

logger = logging.getLogger(__name__)
BASE_URL = "https://www.alphavantage.co/query"

def fetch_stock_data():
    all_data = []

    for ticker in TICKERS:
        success = False

        for interval in INTERVALS:
            function = {
                "Daily": "TIME_SERIES_DAILY",
                "Weekly": "TIME_SERIES_WEEKLY"
            }.get(interval)

            if function is None:
                logger.warning(f"⚠️ Неподдерживаемый интервал: {interval}")
                continue

            for attempt in range(1, MAX_RETRIES + 1):
                logger.info(f"📅 [{ticker}] Попытка {attempt}: {function}")
                params = {
                    "function": function,
                    "symbol": ticker,
                    "outputsize": OUTPUTSIZE,
                    "apikey": ALPHA_VANTAGE_API_KEY
                }

                try:
                    response = requests.get(BASE_URL, params=params)
                    response.raise_for_status()
                    json_data = response.json()

                    key = next((k for k in json_data if "Time Series" in k), None)
                    if not key or key not in json_data:
                        raise ValueError(f"Нет данных в ответе: {json_data}")

                    df = pd.DataFrame.from_dict(json_data[key], orient="index")
                    df.index = pd.to_datetime(df.index)
                    df.rename(columns={
                        "1. open": "Open",
                        "2. high": "High",
                        "3. low": "Low",
                        "4. close": "Close",
                        "5. volume": "Volume"
                    }, inplace=True)

                    df["Ticker"] = ticker
                    df.reset_index(inplace=True)
                    df.rename(columns={"index": "timestamp"}, inplace=True)
                    all_data.append(df)
                    success = True
                    break

                except Exception as e:
                    logger.error(f"❌ Ошибка загрузки {ticker} ({function}): {e}")
                    time.sleep(RETRY_DELAY)

            if success:
                break

        if not success:
            logger.error(f"‼️ Не удалось загрузить данные для {ticker}")

    if not all_data:
        logger.error("‼️ Данные не загружены ни для одного тикера.")
        return None

    return pd.concat(all_data, ignore_index=True)


def prepare_data_for_spark(raw_data):
    if raw_data is None or raw_data.empty:
        logger.warning("⚠️ Пустой DataFrame")
        return None

    try:
        df = raw_data.copy()

        # Приводим значения к числовому типу
        df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
        df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce")

        # Удаляем строки, где нет ключевых значений
        df = df.dropna(subset=["Close", "Volume"])

        return df

    except Exception as e:
        logger.error(f"❌ Ошибка подготовки данных: {e}")
        return None
