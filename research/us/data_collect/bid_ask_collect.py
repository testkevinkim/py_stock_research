# by using questrade api, collect actual bid-ask data
import time

from feed import us_yahoo_feed
from qtrade import Questrade
import pandas as pd
import re
from requests import get
import logging
import os

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
large_cap_universe_url = "https://markets.businessinsider.com/index/components/s&p_500?p={}"
small_cap_universe_url = "https://stockmarketmba.com/stocksinthespsmallcap600.php"
mid_cap_universe_url = "https://stockmarketmba.com/sp400mostrecentearnings.php"


def init_qtrade(yaml_token_path) -> Questrade:
    # do init_qtrade then refresh_token
    # qtrade = init_qtrade(path)
    # qtrade = refresh_token(qtrade)
    # get_bid_ask(ticker_list, qtrade)
    return Questrade(token_yaml=yaml_token_path)


def refresh_token(qtrade, access_token_path):
    qtrade.refresh_access_token(from_yaml=True, yaml_path=access_token_path)
    return qtrade


def get_bid_ask(tickers, qt) -> pd.DataFrame:
    quotes = []
    for t in tickers:
        try:
            quotes.append(pd.DataFrame.from_dict([qt.get_quote(t)]))
        except Exception as e:
            logging.info((str(e), t))
    bid_ask = pd.concat(quotes, ignore_index=True)
    bid_ask["date"] = bid_ask["lastTradeTime"].map(lambda x: x[:10])
    return bid_ask


def get_feed(tickers) -> pd.DataFrame:
    feed_keys = ["bid", "ask", "bidSize", "askSize", "tradeable", "symbol",
                 "marketState", "regularMarketPrice", "regularMarketVolume", "postMarketPrice"]
    feed = us_yahoo_feed.yahoo_feed(tickers, feed_keys)
    feed_df = pd.DataFrame.from_dict(feed)
    return feed_df


def get_universe() -> [str]:
    # large cap
    next_page = 1
    tickers = []
    while next_page > 0:
        result = get_snp500(next_page)
        if result and len(result) > 20:
            tickers.extend(result)
            next_page += 1
            time.sleep(0.2)
        else:
            next_page = 0  # stop
    logging.info(("us large cap universe size = ", len(tickers)))
    # mid cap
    tickers.extend(get_snp400())
    logging.info(("us large + mid cap universe size = ", len(tickers)))
    # small cap
    tickers.extend(get_snp600())
    logging.info(("us large + mid + small cap universe size = ", len(tickers)))

    return list(set(tickers))


def get_snp600():
    mid_text = get(small_cap_universe_url, headers=headers).text
    return [x.upper() for x in list(pd.read_html(mid_text)[0].Symbol.unique())]


def get_snp400():
    mid_text = get(mid_cap_universe_url, headers=headers).text
    return [x.upper() for x in list(pd.read_html(mid_text)[0].Symbol.unique())]


def get_snp500(page_n):
    regular_re_pattern = 'href="\/stocks/\w*-stock"'
    special_case_re_pattern = 'href="\/stocks/\w*-\w*-stock"'

    def _regex(pattern, txt):
        return list(set([s.replace('href="/stocks/', '').replace('-stock"', '') for s in re.findall(pattern, txt)]))

    try:
        logging.info(page_n)
        web_text = get(large_cap_universe_url.format(str(page_n)), headers=headers).text
        regular, special_case = [_regex(x, web_text) for x in [regular_re_pattern, special_case_re_pattern]]
        return [x.upper() for x in list(set(regular + special_case))]

    except Exception as e:
        logging.info((str(e), page_n))
        return None


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
