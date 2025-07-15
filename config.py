TICKERS = ["AAPL", "MSFT", "GOOG", "AMZN", "META"]
INTERVALS = ["Daily", "Weekly"]  # Alpha Vantage поддерживает: "1min", "5min", "15min", "30min", "60min", "Daily", "Weekly", "Monthly"
OUTPUTSIZE = "compact"  # или "full"

MAX_RETRIES = 3
RETRY_DELAY = 5  # секунд

SPARK_CONFIG = {
    "spark.master": "local[2]",
    "spark.driver.memory": "2g",
    "spark.executor.memory": "2g",
    "spark.sql.shuffle.partitions": "4",
    "spark.driver.maxResultSize": "1g",
    "spark.ui.showConsoleProgress": "true",
    "spark.logConf": "true",
    "spark.executor.heartbeatInterval": "60s",
    "spark.network.timeout": "120s"
}
