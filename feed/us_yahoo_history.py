import yfinance as yf
import pandas as pd
import os
import logging


def get_yahoo_history(tickers, start_date, end_date):
    history = []
    for t in tickers:
        try:
            temp_ticker = yf.Ticker(t)
            temp_history = temp_ticker.history(t, auto_adjust=True, start=start_date, end=end_date, interval="1d")
            temp_history["TICKER"] = t
            temp_history = temp_history.reset_index()
            temp_history = temp_history.rename(
                columns={"Date": "DATE", "Open": "OPEN", "High": "HIGH", "Low": "LOW", "Close": "CLOSE",
                         "Volume": "VOLUME"})
            temp_history["DATE"] = temp_history["DATE"].map(lambda x: str(pd.to_datetime(x))[:10])
            history.append(temp_history)
        except Exception as e:
            logging.info("{} skipped".format(t))
            logging.info(str(e))
    history_all = pd.concat(history, ignore_index=True)
    logging.info("original ticker count = {}, downloaded ticker count = {}".format(str(len(tickers)),
                                                                                   str(len(
                                                                                       history_all.TICKER.unique()))))
    return history_all


def test_get_yahoo_history():
    test_tickers = ["AAPL", "FB", "005930.KS"]
    actual = get_yahoo_history(test_tickers, "2020-01-01", "2020-02-01")
    logging.info(actual.tail().to_string())
    logging.info(actual.head().to_string())
    logging.info(actual.dtypes)
    assert len(actual.TICKER.unique()) == len(test_tickers)
