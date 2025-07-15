import pandas as pd

class PandasDataProcessor:
    def __init__(self):
        pass

    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()

        # Убедимся, что Close и Volume имеют числовой тип
        df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
        df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce")

        # Отфильтруем строки с пропущенными значениями после преобразования
        df = df.dropna(subset=["Close", "Volume"])

        # Группировка и агрегация
        result_df = (
            df.groupby("Ticker", as_index=False)
              .agg(avg_close=("Close", "mean"), total_volume=("Volume", "sum"))
              .sort_values("Ticker")
        )
        return result_df

    def stop(self):
        pass



