# by using questrade api, collect actual bid-ask data
import time

from feed import us_yahoo_feed
from qtrade import Questrade
from requests import get
import string
import random
import pandas as pd
from research.ca.data_collect import config
import logging
from rank_selection_main import utils
from feed import us_yahoo_history
import os
import numpy as np

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
            temp = pd.DataFrame.from_dict([qt.get_quote(t)])
            temp["askPrice"] = pd.to_numeric(temp["askPrice"])
            temp["askSize"] = pd.to_numeric(temp["askSize"])
            temp["lastTradePriceTrHrs"] = pd.to_numeric(temp["lastTradePriceTrHrs"])
            quotes.append(temp)
        except Exception as e:
            logging.info((str(e), t))
    bid_ask = pd.concat(quotes, ignore_index=True)
    bid_ask["date"] = bid_ask["lastTradeTime"].map(lambda x: x[:10])
    return bid_ask


def get_history_from_qt(tickers, start_date, end_date, qt):
    history = []
    for i, t in enumerate(tickers):
        try:
            temp = pd.DataFrame.from_dict(qt.get_historical_data(t, start_date, end_date, "OneDay"))
            temp = temp[["start", "open", "high", "low", "close", "volume"]]
            temp = temp.rename(columns={"start": "DATE", "open": "OPEN", "high": "HIGH", "low": "LOW", "close": "CLOSE",
                                        "volume": "VOLUME"})
            temp["DATE"] = temp["DATE"].map(lambda x: x[:10])
            temp["TICKER"] = t
            history.append(temp)
        except Exception as e:
            logging.info("{} failed".format(str(t)))
            logging.error(e, exc_info=True)
    return pd.concat(history, ignore_index=True)


def test_get_history_from_qt():
    given_universe = ['ENB.TO', 'SLF.TO', 'MFC.TO', 'RY.TO', 'SHOP.TO', 'BMO.TO', 'CNQ.TO', 'BCE.TO', 'SU.TO', 'TD.TO']
    qt = init_qtrade(config.yaml_token_path)
    qt = refresh_token(qt, config.access_token_path)
    history_df = get_history_from_qt(given_universe, "2021-01-01", "2021-11-11", qt)
    logging.info(history_df.head())
    logging.info(history_df.shape)
    logging.info(history_df.dtypes)


def get_feed(tickers) -> pd.DataFrame:
    feed_keys = ["bid", "ask", "bidSize", "askSize", "tradeable", "symbol",
                 "marketState", "regularMarketPrice", "regularMarketVolume", "postMarketPrice"]
    feed = us_yahoo_feed.yahoo_feed(tickers, feed_keys)
    feed_df = pd.DataFrame.from_dict(feed)
    return feed_df


def test_get_feed():
    universe = get_universe(200, 2, 1)
    logging.info(len(universe))
    feed = get_feed(universe)
    logging.info(len(list(feed.symbol.unique())))
    logging.info(feed.head())
    logging.info(list(feed.symbol.unique())[:10])


def get_universe(top_n, min_amt, min_price) -> [str]:
    dfs = []
    for i, s in enumerate(string.ascii_uppercase):
        temp = get_tsx_universe(s)
        logging.info((i, s))
        if temp.shape[0] > 0:
            dfs.append(temp)
    comb = pd.concat(dfs, ignore_index=True)
    logging.info((comb.head(), comb.dtypes))
    comb["TICKER"] = comb["TICKER"].map(lambda x: str(x))
    comb = comb[comb["TICKER"].map(lambda x: ".WT" not in x and ".PR" not in x and '.DB' not in x)]
    logging.info((comb.head(), comb.dtypes))
    comb_reduced = comb.query("AMT > {}".format(str(min_amt)))
    comb_reduced = comb_reduced.query("CLOSE > {}".format(str(min_price)))
    comb_reduced = comb_reduced.sort_values(by="AMT", ascending=False)
    return [x.replace(".", "-") + ".TO" for x in list(comb_reduced.head(top_n).TICKER.unique())]


def get_tsx_universe(first_key="A"):
    url_template = "http://eoddata.com/stocklist/TSX/{}.htm".format(first_key.upper())
    logging.info(("tsx universe url", url_template))
    htmltext = get(url_template, headers=headers).text
    df = pd.read_html(htmltext)[4]
    df = df[["Code", "Close", "Volume"]].rename(columns={"Code": "TICKER", "Close": "CLOSE", "Volume": "VOLUME"})
    df["AMT"] = df["CLOSE"] * df["VOLUME"] / (10 ** 6)
    return df


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


def build_universe(configs, given_universe=None):
    if given_universe:
        logging.info("used given universe")
        return given_universe
    else:
        return get_universe(configs.top_n, configs.min_price, configs.min_amt)


def capture_current_price(universe, time):
    price = get_feed(universe)
    price["time"] = time
    logging.info(("current_price", price.shape[0]))
    return price


def calculate_price_drop(prices, post_market_flag) -> pd.DataFrame:
    pre, post = sorted(list(prices.time.unique()))
    times_names = [(pre, "pre"), (post, "post")]

    def _filter_rename(df, filter_time, new_name, post_market=True):
        subset = df.query("time == '{}'".format(filter_time))
        subset = subset.query("regularMarketVolume > 10000")
        if post_market:
            subset = subset.rename(columns={"postMarketPrice": new_name})
        else:
            subset = subset.rename(columns={"regularMarketPrice": new_name})
        subset = subset[["symbol", new_name]]
        return subset

    dfs = [_filter_rename(prices, x[0], x[1], post_market_flag) for x in times_names]
    df_comb = dfs[0].merge(dfs[1], on="symbol", how="inner")
    df_comb["price_down"] = df_comb["post"] / df_comb["pre"] - 1
    return df_comb


def capture_bid_ask(tickers, qt):
    bid_ask = get_bid_ask(tickers, qt)
    bid_ask["askPrice"] = pd.to_numeric(bid_ask["askPrice"])
    bid_ask["askSize"] = pd.to_numeric(bid_ask["askSize"])
    bid_ask["lastTradePriceTrHrs"] = pd.to_numeric(bid_ask["lastTradePriceTrHrs"])
    logging.info(("bid_ask_size", bid_ask.shape[0]))
    return bid_ask


def reduce_entry(entry_var, report_entry_cnt):
    logging.info(entry_var.dtypes)
    entry_var["ask_price_down_rank"] = entry_var.groupby(["date"])["ask_price_down"].rank(method="first")
    logging.info(("before apply report entry cnt filter,", entry_var.shape[0]))
    entry_var = entry_var.query("ask_price_down_rank <= {}".format(str(report_entry_cnt)))
    logging.info(("after apply report entry cnt filter,", entry_var.shape[0]))
    return entry_var


def main(configs):
    try:
        prices = []
        if utils.is_market_open(configs.override, configs.tz_name, configs.ex_name):
            # market open or override
            new_universe = build_universe(configs, configs.test_universe)
            logging.info(("universe size: ", len(new_universe)))

            utils.wait_until(configs.first_capture_time, configs.tz_name, configs.override)
            logging.info("1st wait until finished")
            pre = capture_current_price(new_universe, utils.local_time(configs.tz_name))
            prices.append(pre)

            if configs.override:
                utils.sleepby(1)

            utils.wait_until(configs.second_capture_time, configs.tz_name, configs.override)
            logging.info("2nd wait until finished")
            post = capture_current_price(new_universe, utils.local_time(configs.tz_name))
            if config.override:
                post["regularMarketPrice"] = post["regularMarketPrice"].map(
                    lambda x: x * (1 + random.uniform(-0.1, 0.1)))
            prices.append(post)

            price = pd.concat(prices, ignore_index=True)
            calculated = calculate_price_drop(price, configs.post_market_flag)
            calculated = calculated.sort_values(by="price_down")
            candidates = list(calculated.head(configs.entry_cnt).symbol.unique())
            # entry_cnt is large in order capture more bid/ask

            logging.info(("candidates size: ", len(candidates)))

            qt = init_qtrade(configs.yaml_token_path)
            qt = refresh_token(qt, configs.access_token_path)
            bid_ask = capture_bid_ask(candidates, qt)
            bid_ask = bid_ask.query("askSize >= 1").query("bidSize >= 1")
            bid_ask_price_down = bid_ask.merge(calculated[["symbol", "price_down"]], on="symbol", how="inner")
            bid_ask_price_down["ask_price_down"] = bid_ask_price_down["askPrice"] / bid_ask_price_down[
                "lastTradePriceTrHrs"] - 1
            entry_pre = save_entry(configs.entry_path, bid_ask_price_down)
            entry_pre["askPrice"] = pd.to_numeric(entry_pre["askPrice"])
            entry_pre["askSize"] = pd.to_numeric(entry_pre["askSize"])
            entry_pre["lastTradePriceTrHrs"] = pd.to_numeric(entry_pre["lastTradePriceTrHrs"])
            entry = reduce_entry(entry_pre, configs.report_entry_cnt)  # to reduce entry size for report
            logging.info(("entry dtypes", entry.dtypes))

            # build gain report
            entry_universe = list(entry.symbol.unique())
            logging.info(("entry universe size = ", len(entry_universe)))
            entry_dates = sorted(list(entry.date.unique()))
            entry_start_date = entry_dates[0]
            if len(entry_dates) >= 3:
                history = get_history_from_qt(entry_universe, entry_start_date, utils.local_date(configs.tz_name), qt)
                history.to_json(configs.history_path)
                logging.info("history saved to {}".format(configs.history_path))
                history_reduced = history[["TICKER", "DATE", "OPEN"]]
                entry_reduced = entry[["symbol", "date", "askPrice"]]
                entry_reduced = entry_reduced.rename(
                    columns={"symbol": "TICKER", "date": "DATE", "askPrice": "ENTRY_PRICE"})
                report = utils.build_gain_report(entry_reduced, history_reduced, configs.exit_ndays)
                report["GM_AFTER_FEE"] = report["GM"].map(lambda x: x - configs.fee_perc)
                report["GM_AFTER_FEE_NORM"] = report["GM_AFTER_FEE"].map(lambda x: x / configs.exit_ndays + 1)
                cr = np.array(list(report.GM_AFTER_FEE_NORM)).cumprod()
                report["CR"] = cr
                logging.info("report built")
                logging.info(report.head().to_string())
                report.to_json(configs.report_path)

                # send
                if report.shape[0] > 0:
                    utils.send_email_with_df("ca-bid-ask-collect: gain report", configs.email_cred, report)
                else:
                    logging.info(("report not sent because report size = 0", report.shape[0]))
                    utils.send_status_email("ca-bid-ask-collect: entry is too short", configs.email_cred,
                                            "entry size = {}".format(str(entry_pre.shape[0])))

            else:
                logging.info(("too short entry size, entry dates count = ", len(entry_dates)))
                utils.send_status_email("ca-bid-ask-collect: entry is too short", configs.email_cred,
                                        "entry size = {}".format(str(entry_pre.shape[0])))
        else:
            # close
            utils.send_status_email("ca-bid-ask-collect: market closed or holiday", configs.email_cred)
    except Exception as e:
        logging.error(e, exc_info=True)
        utils.send_status_email(
            "check questrade auth issue - regenerate access token from api hub or some detail in body",
            configs.email_cred, str(e))


def test_build_universe():
    given_universe = ['ENB.TO', 'SLF.TO', 'MFC.TO', 'RY.TO', 'SHOP.TO', 'BMO.TO', 'CNQ.TO', 'BCE.TO', 'SU.TO', 'TD.TO']
    actual = build_universe(config, given_universe)
    logging.info(("given_universe", actual))
    actual = build_universe(config, None)
    logging.info(("universe_size", len(actual)))
    assert len(actual) > 10


def test_capture_current_price():
    given_universe = ['ENB.TO', 'SLF.TO', 'MFC.TO', 'RY.TO', 'SHOP.TO', 'BMO.TO', 'CNQ.TO', 'BCE.TO', 'SU.TO', 'TD.TO']
    actual = capture_current_price(given_universe, "test")
    logging.info(("price", actual.head().to_string()))
    assert len(actual.symbol.unique()) == len(given_universe)
    logging.info(actual.dtypes)
    logging.info(actual.head())
    # python -m pytest research/us/data_collect/get_candidate.py::test_capture_current_price --log-cli-level=INFO


def test_calculate_price_drop():
    given_universe = ['ENB.TO', 'SLF.TO', 'MFC.TO', 'RY.TO', 'SHOP.TO', 'BMO.TO', 'CNQ.TO', 'BCE.TO', 'SU.TO', 'TD.TO']
    actual = capture_current_price(given_universe, "1")
    actual2 = capture_current_price(given_universe, "2")
    actual2["regularMarketPrice"] = actual2["regularMarketPrice"].map(lambda x: x * (1 - random.uniform(0, 0.1)))
    comb = pd.concat([actual, actual2], ignore_index=True)
    logging.info(comb.head())
    logging.info(comb.dtypes)
    post_flag = False
    result = calculate_price_drop(comb, post_market_flag=post_flag)
    logging.info(result.head().to_string())
    logging.info(result.shape)
    logging.info(result.dtypes)
    assert "price_down" in result.columns
    # python -m pytest research/us/data_collect/get_candidate.py::test_calculate_price_drop --log-cli-level=INFO


def test_capture_bid_ask():
    given_universe = ['ENB.TO', 'SLF.TO', 'MFC.TO', 'RY.TO', 'SHOP.TO', 'BMO.TO', 'CNQ.TO', 'BCE.TO', 'SU.TO', 'TD.TO']
    qt = init_qtrade(config.yaml_token_path)
    qt = refresh_token(qt, config.access_token_path)
    actual = capture_bid_ask(given_universe, qt)
    logging.info(actual.head().to_string())
    logging.info(actual.dtypes)
    # python -m pytest research/us/data_collect/get_candidate.py::test_capture_bid_ask --log-cli-level=INFO


def test_build_report():
    test_entry = pd.DataFrame(
        [("TEST1", "2020-11-22", 100), ("TEST2", "2020-11-23", 200), ("TEST3", "2020-11-24", 300)],
        columns=["TICKER", "DATE", "ENTRY_PRICE"])
    test_history = pd.DataFrame(
        [("TEST1", "2020-11-22", 100), ("TEST1", "2020-11-23", 110), ("TEST1", "2020-11-24", 120),
         ("TEST2", "2020-11-22", 190), ("TEST2", "2020-11-23", 200), ("TEST2", "2020-11-24", 210),
         ("TEST3", "2020-11-22", 280), ("TEST3", "2020-11-23", 290), ("TEST3", "2020-11-24", 300)],
        columns=["TICKER", "DATE", "OPEN"])
    report = utils.build_gain_report(test_entry, test_history, 1)
    logging.info(report.dtypes)
    logging.info(report.head())
    # python -m pytest research/us/data_collect/get_candidate.py::test_build_report --log-cli-level=INFO


def test_save_entry():
    test_entry = pd.DataFrame(
        [("TEST1", "2020-11-22", 100), ("TEST2", "2020-11-23", 200), ("TEST3", "2020-11-24", 300)],
        columns=["TICKER", "DATE", "ENTRY_PRICE"])
    test_path = os.path.join(config.root_path, "unittest_entry_save.json")
    if os.path.exists(test_path):
        os.remove(test_path)
    save_entry(test_path, test_entry)

    load_entry = pd.read_json(test_path, convert_dates=False)
    pd.testing.assert_frame_equal(test_entry, load_entry)
    os.remove(test_path)
    # python -m pytest research/us/data_collect/get_candidate.py::test_save_entry --log-cli-level=INFO


def test_reduce_entry():
    test_entry = pd.DataFrame(
        [("TEST1", "2020-11-22", 100, -0.1), ("TEST2", "2020-11-22", 200, -0.2), ("TEST3", "2020-11-22", 300, -0.3),
         ("TEST1", "2020-11-23", 100, -0.1), ("TEST2", "2020-11-23", 200, -0.02), ("TEST3", "2020-11-23", 300, -0.3),
         ("TEST1", "2020-11-24", 100, -0.01), ("TEST2", "2020-11-24", 200, -0.2), ("TEST3", "2020-11-24", 300, -0.03)],
        columns=["symbol", "date", "close", "price_down"])
    actual = reduce_entry(test_entry, 1)
    logging.info((actual.dtypes, actual.shape))
    logging.info(actual.head())
    # python -m pytest research/us/data_collect/get_candidate.py::test_reduce_entry --log-cli-level=INFO


def test_main():
    config.override = True
    config.test_universe = ['ENB.TO', 'SLF.TO', 'MFC.TO', 'RY.TO', 'SHOP.TO', 'BMO.TO', 'CNQ.TO', 'BCE.TO', 'SU.TO',
                            'TD.TO']
    email_cred = utils.read_json_to_dict(config.email_path)
    config.email_cred = email_cred
    main(config)
    # python -m pytest research/us/data_collect/get_candidate.py::test_main --log-cli-level=INFO
