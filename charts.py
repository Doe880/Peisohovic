import plotly.express as px
import pandas as pd

def plot_live_prices(data: pd.DataFrame, ticker: str):
    ticker_data = data[data["Ticker"] == ticker]
    if ticker_data.empty:
        return None

    fig = px.line(
        ticker_data,
        x="timestamp",
        y="Close",
        title=f"{ticker} Stock Price",
        labels={"Close": "Цена", "timestamp": "Дата"}
    )
    fig.update_layout(margin=dict(l=20, r=20, t=40, b=20))
    return fig

def plot_summary_stats(data: pd.DataFrame):
    if data.empty:
        return None

    fig = px.bar(
        data,
        x="Ticker",
        y="avg_close",
        text="avg_close",
        title="Средняя цена закрытия по тикерам",
        labels={"avg_close": "Среднее закрытие", "Ticker": "Тикер"},
    )
    fig.update_traces(texttemplate='%{text:.2f}', textposition='outside')
    fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
    return fig
