import pandas as pd

class PandasDataProcessor:
    def __init__(self):
        # В pandas инициализация обычно не нужна
        pass

    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()  # пустой DataFrame при отсутствии данных

        # Группируем по тикеру, считаем среднее Close и сумму Volume
        result_df = (
            df.groupby("Ticker")
            .agg(
                avg_close=pd.NamedAgg(column="Close", aggfunc="mean"),
                total_volume=pd.NamedAgg(column="Volume", aggfunc="sum"),
            )
            .reset_index()
            .sort_values("Ticker")
        )
        return result_df

    def stop(self):
        # В pandas ничего останавливать не нужно
        pass


