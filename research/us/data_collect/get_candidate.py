import random
# run at 16:00 -> capture price
# run at 17:00 -> capture price, calculate price drop, reduce candidates -> capture bid/ask of the candidates
# if entry exists, calculate gain -> build report -> send email
import pandas as pd

from research.us.data_collect import bid_ask_collect, config
import logging
from rank_selection_main import utils
from feed import us_yahoo_history
import os

email_cred = utils.read_json_to_dict(config.email_path)
config.email_cred = email_cred


def build_universe(given_universe=None):
    if given_universe:
        logging.info("used given universe")
        return given_universe
    else:
        return bid_ask_collect.get_universe()


def capture_current_price(universe, time):
    price = bid_ask_collect.get_feed(universe)
    price["time"] = time
    logging.info(("current_price", price.shape[0]))
    return price


def calculate_price_drop(prices) -> pd.DataFrame:
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

    dfs = [_filter_rename(prices, x[0], x[1]) for x in times_names]
    df_comb = dfs[0].merge(dfs[1], on="symbol", how="inner")
    df_comb["price_down"] = df_comb["post"] / df_comb["pre"] - 1
    return df_comb


def capture_bid_ask(tickers, qt):
    bid_ask = bid_ask_collect.get_bid_ask(tickers, qt)
    logging.info(("bid_ask_size", bid_ask.shape[0]))
    return bid_ask


def reduce_entry(entry, report_entry_cnt):
    entry["price_down_rank"] = entry.groupby(["date"])["ask_price_down"].rank(method="first")
    logging.info(("before apply report entry cnt filter,", entry.shape[0]))
    entry = entry.query("price_down_rank <= {}".format(str(report_entry_cnt)))
    entry = entry.drop(columns=["price_down_rank"])
    logging.info(("after apply report entry cnt filter,", entry.shape[0]))
    return entry


def main(configs):
    try:
        prices = []
        if utils.is_market_open(configs.override, configs.tz_name, configs.ex_name):
            # market open or override
            new_universe = build_universe(configs.test_universe)
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
                post["regularMarketPrice"] = post["regularMarketPrice"].map(lambda x: x * (1 + random.uniform(-0.1, 0.1)))
            prices.append(post)

            price = pd.concat(prices, ignore_index=True)
            calculated = calculate_price_drop(price)
            calculated = calculated.sort_values(by="price_down")
            candidates = list(calculated.head(configs.entry_cnt).symbol.unique())
            # entry_cnt is large in order capture more bid/ask

            logging.info(("candidates size: ", len(candidates)))

            try:
                qt = bid_ask_collect.init_qtrade(configs.yaml_token_path)
                qt = bid_ask_collect.refresh_token(qt, configs.access_token_path)
            except Exception as e:
                logging.info(("questrade auth issue", str(e)))
                utils.send_status_email("check questrade auth issue - regenerate access token from api hub",
                                        configs.email_cred)
            bid_ask = capture_bid_ask(candidates, qt)
            bid_ask = bid_ask.query("askSize >= 1").query("bidSize >= 1")
            bid_ask_price_down = bid_ask.merge(calculated[["symbol", "price_down"]], on="symbol", how="inner")
            bid_ask_price_down["ask_price_down"] = bid_ask_price_down["askPrice"]/bid_ask_price_down["lastTradePriceTrHrs"]-1
            entry_pre = bid_ask_collect.save_entry(configs.entry_path, bid_ask_price_down)
            entry = reduce_entry(entry_pre, configs.report_entry_cnt)  # to reduce entry size for report
            logging.info(("entry dtypes", entry.dtypes))

            # build gain report
            entry_universe = list(entry.symbol.unique())
            logging.info(("entry universe size = ", len(entry_universe)))
            entry_dates = sorted(list(entry.date.unique()))
            entry_start_date = entry_dates[0]
            if len(entry_dates) >= 3:
                history = us_yahoo_history.get_yahoo_history(entry_universe, entry_start_date,
                                                             utils.local_date(configs.tz_name))
                history_reduced = history[["TICKER", "DATE", "OPEN"]]
                entry_reduced = entry[["symbol", "date", "askPrice"]]
                entry_reduced = entry_reduced.rename(
                    columns={"symbol": "TICKER", "date": "DATE", "askPrice": "ENTRY_PRICE"})
                report = utils.build_gain_report(entry_reduced, history_reduced, configs.exit_ndays)
                logging.info("report built")
                logging.info(report.head().to_string())
                report.to_json(configs.report_path)

                # send
                if report.shape[0] > 0:
                    utils.send_email_with_df("us-bid-ask-collect: gain report", configs.email_cred, report)
                else:
                    logging.info(("report not sent because report size = 0", report.shape[0]))
            else:
                logging.info(("too short entry size, entry dates count = ", len(entry_dates)))
        else:
            # close
            utils.send_status_email("us-bid-ask-collect: market closed or holiday", configs.email_cred)
    except Exception as e:
        logging.error(e, exc_info=True)


def test_build_universe():
    given_universe = ["AAPL", "FB", "F", "MMM"]
    actual = build_universe(given_universe)
    logging.info(("given_universe", actual))
    actual = build_universe(None)
    logging.info(("universe_size", len(actual)))
    assert len(actual) > 100


def test_capture_current_price():
    given_universe = ["AAPL", "FB", "F", "MMM"]
    actual = capture_current_price(given_universe, "test")
    logging.info(("price", actual.head().to_string()))
    assert len(actual.symbol.unique()) == len(given_universe)
    all_universe = build_universe(None)
    actual = capture_current_price(all_universe, "test")
    assert actual.shape[0] > 100
    logging.info(actual.dtypes)
    logging.info(actual.head())
    # python -m pytest research/us/data_collect/get_candidate.py::test_capture_current_price --log-cli-level=INFO


def test_calculate_price_drop():
    given_universe = ["AAPL", "FB", "F", "MMM"]
    actual = capture_current_price(given_universe, "1")
    actual2 = capture_current_price(given_universe, "2")
    actual2["regularMarketPrice"] = actual2["regularMarketPrice"].map(lambda x: x * (1 - random.uniform(0, 0.1)))
    comb = pd.concat([actual, actual2], ignore_index=True)
    result = calculate_price_drop(comb)
    logging.info(result.head().to_string())
    logging.info(result.shape)
    logging.info(result.dtypes)
    assert "price_down" in result.columns
    # python -m pytest research/us/data_collect/get_candidate.py::test_calculate_price_drop --log-cli-level=INFO


def test_capture_bid_ask():
    given_universe = ["AAPL", "FB", "F", "MMM"]
    qt = bid_ask_collect.init_qtrade(config.yaml_token_path)
    qt = bid_ask_collect.refresh_token(qt, config.access_token_path)
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
    bid_ask_collect.save_entry(test_path, test_entry)

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
    main(config)
    # python -m pytest research/us/data_collect/get_candidate.py::test_main --log-cli-level=INFO
