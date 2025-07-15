import streamlit as st
from data_loader import fetch_stock_data, prepare_data_for_spark
from spark_processor import PandasDataProcessor  # pandas-версия процессора
from charts import plot_live_prices, plot_summary_stats
from config import TICKERS

# Настройка интерфейса Streamlit
st.set_page_config(page_title="Financial Streaming Dashboard", layout="wide")
st.title("📈 Real-Time Stock Market Dashboard")

# Инициализация pandas-процессора
data_processor = PandasDataProcessor()

# Кэшируем загрузку и обработку данных
@st.cache_data(ttl=120, show_spinner="🔄 Загружаем данные с рынка...")
def load_and_process_data():
    try:
        raw = fetch_stock_data()
        if raw is None or raw.empty:
            return None, None

        prepared = prepare_data_for_spark(raw)
        if prepared is None or prepared.empty:
            return None, None

        # Обработка данных pandas-процессором (возвращает pandas DataFrame)
        processed = data_processor.process_data(prepared)
        return prepared, processed if processed is not None else None
    except Exception as e:
        st.error(f"❌ Ошибка при загрузке или обработке данных: {e}")
        return None, None

# Загрузка и обработка
with st.spinner("⏳ Обновляем данные..."):
    prepared_data, processed_data = load_and_process_data()

if prepared_data is None:
    st.warning("⚠️ Нет подготовленных данных для отображения.")
    st.stop()

# Интерфейс выбора тикера
available_tickers = prepared_data["Ticker"].unique().tolist()
selected_ticker = st.sidebar.selectbox("🎯 Выберите тикер:", available_tickers)

# Отображение графика цен
st.subheader("💹 Цена и индикаторы")
price_chart = plot_live_prices(prepared_data, selected_ticker)
if price_chart:
    st.plotly_chart(price_chart, use_container_width=True)

# Сводная статистика
if processed_data is not None:
    st.subheader("📊 Сводная статистика по акциям")
    summary_chart = plot_summary_stats(processed_data)
    if summary_chart:
        st.plotly_chart(summary_chart, use_container_width=True)

# Автообновление
st.caption("🔄 Данные обновляются автоматически каждые 2 минуты.")

