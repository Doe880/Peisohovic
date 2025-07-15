import os
import sys
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # для агрегаций
from config import SPARK_CONFIG

class SparkDataProcessor:
    def __init__(self):
        # Настраиваем интерпретаторы Python для PySpark в виртуальной среде
        venv_path = os.path.dirname(os.path.dirname(sys.executable))
        os.environ["PYSPARK_PYTHON"] = os.path.join(venv_path, "Scripts", "python.exe")
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

        # Создаём SparkSession с конфигурацией из config.py
        builder = SparkSession.builder.appName("StockDataProcessor")
        for key, value in SPARK_CONFIG.items():
            builder = builder.config(key, value)

        self.spark = builder.getOrCreate()

    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        # Создаём Spark DataFrame из Pandas DataFrame
        spark_df = self.spark.createDataFrame(df)

        # Группируем по тикеру, считаем среднее Close и сумму Volume
        result_df = (
            spark_df.groupBy("Ticker")
            .agg(
                F.avg("Close").alias("avg_close"),
                F.sum("Volume").alias("total_volume")
            )
            .orderBy("Ticker")
        )

        # Возвращаем обратно Pandas DataFrame
        return result_df.toPandas()

    def stop(self):
        if self.spark:
            self.spark.stop()

