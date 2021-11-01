import math

import pandas as pd
import re
from requests import get
import logging
import os
from feed import us_yahoo_feed

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
large_cap_universe_url = "https://stockmarketmba.com/stocksinthesp500.php"
small_cap_universe_url = "https://stockmarketmba.com/stocksinthespsmallcap600.php"
mid_cap_universe_url = "https://stockmarketmba.com/stocksinthespmidcap400.php"


def get_universe_info(url):
    dfs = pd.read_html(get(url, headers=headers).text)
    df = dfs[0][["Symbol", "Dividend yield"]].rename(columns={"Symbol": "TICKER", "Dividend yield": "DIV_Y"})
    df["DIV_Y"] = df["DIV_Y"].map(lambda x: float(x.replace("%", "")))
    return df


def download_current_dividend_info():
    large = get_universe_info(large_cap_universe_url)
    large["MARKETCAP_TYPE"] = "largecap"
    mid = get_universe_info(mid_cap_universe_url)
    mid["MARKETCAP_TYPE"] = "midcap"
    small = get_universe_info(small_cap_universe_url)
    small["MARKETCAP_TYPE"] = "smallcap"
    comb = pd.concat([large, mid, small], ignore_index=True).query("DIV_Y > 0")
    comb["DIV_Y_RANK"] = comb.groupby("MARKETCAP_TYPE")["DIV_Y"].rank(ascending=False)
    selected = comb.query("DIV_Y_RANK <= 50")
    return selected


def get_dividend_history(ticker):
    txt = get("https://stockmarketmba.com/charts/dividends.php?s={}".format(ticker), headers=headers).text
    value_pattern = "date\: newDate\, \\r\\n value\: [0-9]+(\.[0-9][0-9]?)?"
    date_pattern = 'new Date\([A-Za-z0-9_]................'
    dates = [x.replace("new Date(", "")[:10].split(",") for x in re.findall(re.compile(date_pattern), txt)]
    years = [int(x[0]) for x in dates]
    months = [int(x[1]) for x in dates]
    values = [float(x) for x in re.findall(re.compile(value_pattern), txt)]
    df = pd.DataFrame(zip(years, months, values), columns=["YEAR", "MONTH", "DIVIDEND"])
    df["TICKER"] = ticker
    return df


def download_dividend_history(universe):
    dfs = []
    failed = []
    for t in universe:
        try:
            temp = get_dividend_history(t)
            dfs.append(temp)
        except Exception as e:
            failed.append(t)
            logging.error(e, exc_info=True)
    df = pd.concat(dfs, ignore_index=True)
    logging.info(("failed", len(failed), failed))
    return df


def get_dividend_target_price(dividend_universe, target_yield=0.2, min_history_years=10):
    dividend_history = download_dividend_history(dividend_universe)

    dividend_history_yearly = dividend_history.groupby(["TICKER", "YEAR"]).agg(DIVSUM=("DIVIDEND", "sum"),
                                                                               DIVCNT=(
                                                                                   "DIVIDEND", "count")).reset_index()
    dividend_history_cnt = dividend_history_yearly.groupby("TICKER").agg(
        DIVCNT_MEDIAN=("DIVCNT", "median")).reset_index()
    dividend_history_cnt["DIVCNT_MEDIAN"] = dividend_history_cnt["DIVCNT_MEDIAN"].map(lambda x: math.ceil(x))

    dividend_history["YRMO"] = dividend_history["YEAR"] * 100 + dividend_history["MONTH"]
    dividend_history["YRMO"] = pd.to_numeric(dividend_history["YRMO"])
    logging.info(dividend_history.dtypes)
    dividend_history["YRMORANK"] = dividend_history.groupby("TICKER")["YRMO"].rank(ascending=False)
    dividend_history = dividend_history.merge(dividend_history_cnt, on="TICKER", how="inner")
    dividend_history_recent_raw = dividend_history[dividend_history["YRMORANK"] <= dividend_history["DIVCNT_MEDIAN"]]

    dividend_history_median = dividend_history_yearly.groupby("TICKER").agg(DIVSUM_MEDIAN=("DIVSUM", "median"),
                                                                            DIVSUM_SD=("DIVSUM", "std"),
                                                                            DIVSUM_LENGTH=(
                                                                                "YEAR", "count")).reset_index()
    dividend_history_median["DIVSUM_SD_PERC"] = dividend_history_median["DIVSUM_SD"] / dividend_history_median[
        "DIVSUM_MEDIAN"]
    dividend_history_recent = dividend_history_recent_raw.groupby("TICKER").agg(
        RECENT_DIVSUM=("DIVIDEND", "sum")).reset_index()
    dividend_view = dividend_history_median.merge(dividend_history_recent, on="TICKER", how="inner")

    dividend_view["TARGET_Y"] = target_yield
    dividend_view["TARGET_PRICE"] = dividend_view["RECENT_DIVSUM"] / dividend_view["TARGET_Y"]
    return dividend_view.sort_values(by="DIVSUM_SD_PERC").query("DIVSUM_SD_PERC < 0.5").query(
        "DIVSUM_LENGTH >= {}".format(str(min_history_years)))


def get_current_price(tickers) -> pd.DataFrame:
    feed_keys = ["bid", "ask", "bidSize", "askSize", "tradeable", "symbol",
                 "marketState", "regularMarketPrice", "regularMarketVolume", "postMarketPrice"]
    feed = us_yahoo_feed.yahoo_feed(tickers, feed_keys)
    feed_df = pd.DataFrame.from_dict(feed).rename(columns={"symbol": "TICKER", "regularMarketPrice": "CURRENT_PRICE"})
    feed_df = feed_df[["TICKER", "CURRENT_PRICE", "ask", "askSize"]]
    return feed_df


def save_entry(path, df) -> pd.DataFrame:
    if os.path.exists(path):
        existing_df = pd.read_json(path, convert_dates=False)
        new_df = pd.concat([existing_df, df], ignore_index=True)
        logging.info(("old entry size = ", existing_df.shape[0], " appended entry size = ", new_df.shape[0]))
    else:
        new_df = df
        logging.info(("init entry saved, size = ", new_df.shape[0]))
    new_df.to_json(path)
    logging.info(("entry", new_df.dtypes))
    return new_df
