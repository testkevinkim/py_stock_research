import pandas as pd
import numpy as np
import requests
import logging
import time


def get_per_share(ticker):
    time.sleep(0.3)
    ticker = ticker.replace("A", "")
    url = "http://search.itooza.com/search.htm?seName={}#indexTable2".format(str(ticker))
    try:
        rr = requests.get(url)
        dfs = pd.read_html(rr.text)
        period = [int(x) for x in ["20" + x[:2] for x in list(dfs[3].columns)[1:]]]
        col_loc = [0, 3, 5]

        def _get_per_share(df, loc):
            raw = list(dfs[3].T.iloc[1:, loc])
            return [0.0 if np.isnan(x) else x for x in raw]

        series = [_get_per_share(dfs[2], x) for x in col_loc]
        pdf = pd.DataFrame(zip(series[0], series[1], series[2], period), columns=["EPS", "BPS", "DPS", "YEAR"])
        pdf["TICKER"] = ticker
    except:
        pdf = None
        logging.info("{} is skipped".format(ticker))
    return pdf


def per_share_download(tickers):  # -> pdf
    logging.info("source ticker count = {}".format(str(len(tickers))))
    dfs = [get_per_share(x) for x in tickers]
    dfs_valid = [x for x in dfs if x is not None]
    comb = pd.concat(dfs_valid)
    logging.info("result ticker count = {}".format(str(len(comb.TICKER.unique()))))
    return comb


def test_per_share_download():
    test_tickers = ["005930", "000660"]
    actual = per_share_download(test_tickers)
    logging.info(actual.shape)
    logging.info(actual.head())
    logging.info(actual.tail())
    logging.info(actual.dtypes)
    logging.info(actual.TICKER.unique())
