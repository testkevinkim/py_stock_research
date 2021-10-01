import time

import pandas as pd
import requests
import json
import logging


def get_price_from_daum(ticker, datacount=2000, adj_flag=True, interval="days"):
    """
    unadj is working at only "days" case
    otherwise, always do adjustment

    :param ticker:
    :param datacount:
    :param adj_flag:
    :param interval:
    :return:
    """
    url = "https://finance.daum.net/api/charts/A{}/{}?limit=200&adjusted={}".format(str(ticker), interval,
                                                                                    "true" if adj_flag else "false")
    headers = {
        "referer": "https://finance.daum.net/chart/A{}".format(str(ticker)),
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36"
    }
    if adj_flag:
        params = {
            "limit": str(datacount),
            "adjusted": "true"
        }
    else:
        params = {
            "limit": str(datacount),
            "adjusted": "false"
        }

    try:
        resp = requests.get(url, headers=headers, params=params)
        data = resp.json()
        result = pd.DataFrame.from_dict(data["data"])
        result["symbolCode"] = result["symbolCode"].map(lambda x: x[1:])
        result["candleTime"] = result["candleTime"].map(lambda x: x[:10])
        result["ADJ"] = "ADJ" if adj_flag else "UNADJ"
        result = result.rename(columns={"date": "DATE", "candleTime": "STARTDATE", "tradePrice": "CLOSE",
                                        "highPrice": "HIGH", "lowPrice": "LOW", "openingPrice": "OPEN",
                                        "candleAccTradeVolume": "VOLUME", "symbolCode": "TICKER"})
        result = result[["DATE", "STARTDATE", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "TICKER", "ADJ"]]
    except:
        result = None
    return result


def test_get_price_from_daum():
    test_ticker = "035720"
    test_datacount = 40
    test_interval = "weeks"  # days
    test_adjust = True

    logging.info("daum history")
    actual_adj = get_price_from_daum(test_ticker, test_datacount, test_adjust, test_interval)
    test_adjust = False
    actual_unadj = get_price_from_daum(test_ticker, test_datacount, test_adjust, test_interval)
    actual_comb = pd.merge(actual_adj[["TICKER", "DATE", "CLOSE", "ADJ"]].rename(columns={"CLOSE": "ADJCLOSE"}),
                           actual_unadj[["TICKER", "DATE", "CLOSE", "ADJ"]].rename(columns={"ADJ": "UNADJ_ADJ",
                                                                                            "CLOSE": "UNADJCLOSE"}),
                           on=["TICKER", "DATE"], how="inner")
    logging.info(actual_comb.dtypes)
    logging.info(actual_comb.head(100))

    day_unadj = get_price_from_daum(test_ticker, 30, False, "weeks")
    logging.info(day_unadj.shape)
    logging.info(day_unadj[day_unadj["DATE"].map(lambda x: pd.to_datetime(x, format="%Y-%m-%d").month in [2, 3, 4])])


def get_adj_unadj_day_price(tickers, datacount, sleep_time_every_100=10):
    failed = []
    prices = []
    for i, t in enumerate(tickers):
        try:
            adj = get_price_from_daum(t, datacount, True, "days").drop(columns=["ADJ"])
            unadj = get_price_from_daum(t, datacount, False, "days")
            both = pd.merge(adj,
                            unadj[["TICKER", "DATE", "CLOSE"]].rename(columns={"CLOSE": "UNADJCLOSE"}),
                            on=["TICKER", "DATE"], how="inner")
            prices.append(both)
            logging.info("{}, {}% ticker = {} history download - adj, unadj both".format(str(i), str(round(
                i / len(tickers) * 100, 2)), t))
            if i % 100 == 99:
                time.sleep(sleep_time_every_100)
        except Exception as e:
            logging.info("{} skipped because of {}".format(t, str(e)))
            failed.append(t)
    return pd.concat(prices, ignore_index=True), failed


def test_get_adj_unadj_day_price():
    test_tickers = ["005930", "035720"]
    test_datacount = 2000
    actual, failed = get_adj_unadj_day_price(test_tickers, test_datacount)
    logging.info(actual.TICKER.unique())
    logging.info(actual.head())
    logging.info(actual[actual["DATE"].map(
        lambda x: pd.to_datetime(x, format="%Y-%m-%d").year * 100 + pd.to_datetime(x, format="%Y-%m-%d").month in [
            202103, 201803, 201804])])
    logging.info(actual.dtypes)
