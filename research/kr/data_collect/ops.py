import random

import pandas as pd
import logging
from research.kr.utils import marketcap_dataprep, unadj_day_price_dataprep
from rank_selection_main import utils
import os
from datetime import datetime, timedelta
import pytz


def delayed_offer(ticker):
    url_template = "https://finance.naver.com/item/sise.naver?code={}&asktype=10"
    datalist = pd.read_html(url_template.format(str(ticker)), skiprows=1)
    offer = datalist[2]
    bidPrice = offer.iloc[0, 3]
    bidSize = offer.iloc[0, 4]
    askPrice = offer.iloc[0, 1]
    askSize = offer.iloc[0, 0]
    offer = pd.DataFrame([(ticker, bidPrice, bidSize, askPrice, askSize)],
                         columns=["TICKER", "BID_PRICE", "BID_SIZE", "ASK_PRICE", "ASK_SIZE"])
    offer["ASK_AMT"] = offer["ASK_PRICE"] * offer["ASK_SIZE"]
    logging.info(("bid ask captured for -> ", ticker))
    return offer


def capture_bidask(tickers, date, time):
    bidasks = []
    for t in tickers:
        try:
            temp = delayed_offer(t)
            bidasks.append(temp)
        except Exception as e:
            logging.info((str(e), "{} skipped".format(str(t))))
    result = pd.concat(bidasks, ignore_index=True)
    result["DATE"] = date
    result["TIME"] = time
    logging.info(("bid ask", result.head(), result.dtypes))
    return result


def get_universe(max_page=None):
    universe = list(marketcap_dataprep.get_all_marketcap_shares(max_page).TICKER.unique())
    logging.info(("universe size", len(universe)))
    return universe


def get_snapshots(universe, snapshot_times, tz_name, override=False):
    snapshots = []
    while len(snapshot_times) > 0:
        if utils.local_time(tz_name) < snapshot_times[0]:
            utils.wait_until(snapshot_times[0], tz_name, override)
        else:  # snapshot taken
            temp = utils.kr_realtime_feed(universe)
            temp["snapshot_time"] = snapshot_times[0]
            snapshots.append(temp)
            logging.info(("snapshot taken, size = ", temp.shape[0]))
            snapshot_times = snapshot_times[1:]
            logging.info(("remaining snapshot time", len(snapshot_times)))
            if len(snapshot_times) > 0:
                utils.wait_until(snapshot_times[0], tz_name, override)
    logging.info("all snapshots taken")
    return pd.concat(snapshots, ignore_index=True)


def select_price_drop(df: pd.DataFrame, entry_cnt: int, exclude_first_cnt: int, override=False):
    # df: combined dataframe with 2 snapshots
    snapshot_times = sorted(list(df.snapshot_time.unique()))
    logging.info(snapshot_times)
    pre = df[df["snapshot_time"].map(lambda x: x == snapshot_times[0])]
    pre = pre[["TICKER", "CLOSE"]].rename(columns={"CLOSE": "PRE_CLOSE"})
    post = df[df["snapshot_time"].map(lambda x: x == snapshot_times[1])]
    post = post[["TICKER", "CLOSE", "VOLUME"]].rename(columns={"CLOSE": "POST_CLOSE"})
    pre_post = pre.merge(post, on="TICKER", how="inner")
    logging.info(("pre_post size", pre_post.shape[0]))
    pre_post["PRICE_DOWN"] = pre_post["POST_CLOSE"] / pre_post["PRE_CLOSE"] - 1
    pre_post["AMT"] = pre_post["POST_CLOSE"] * pre_post["VOLUME"] / 10 ** 8
    logging.info(pre_post)
    if not override:
        price_down_candidates = pre_post.query("AMT > 5").query("POST_CLOSE > 1000").query(
            "PRICE_DOWN < 0")
    else:  # override
        price_down_candidates = pre_post.query("AMT > 5").query("POST_CLOSE > 1000")
    logging.info(("price_down_candidates size", price_down_candidates.shape[0]))
    price_down_candidates = price_down_candidates.sort_values(by="PRICE_DOWN").head(entry_cnt + exclude_first_cnt).tail(
        entry_cnt)
    logging.info(("entry cnt ", entry_cnt, "price drop candidates cnt ", len(price_down_candidates.TICKER.unique())))
    return price_down_candidates[["TICKER", "PRICE_DOWN", "POST_CLOSE", "PRE_CLOSE"]]


def save_entry(path, df) -> pd.DataFrame:
    if os.path.exists(path):
        existing_df = pd.read_json(path, convert_dates=False)
        new_df = pd.concat([existing_df, df], ignore_index=True)
        logging.info(("old entry size = ", existing_df.shape[0], " appended entry size = ", new_df.shape[0]))
    else:
        new_df = df
        logging.info(("init entry saved, size = ", new_df.shape[0]))
    new_df.to_json(path)
    return new_df


def get_history(universe, pdays):
    history, failed = unadj_day_price_dataprep.get_adj_unadj_day_price(universe, pdays)
    # "DATE", "STARTDATE", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "TICKER", "UNADJCLOSE"
    history["ADJ_RATIO"] = history["CLOSE"] / history["UNADJCLOSE"]
    logging.info(("history size", history.shape[0]))
    return history


def adj_entry(entry, adj_history):
    logging.info(("entry date min max", entry.DATE.min(), entry.DATE.max()))
    logging.info(("history date min max", adj_history.DATE.min(), adj_history.DATE.max()))
    adj_history = adj_history[["TICKER", "DATE", "ADJ_RATIO"]]
    adj_entry_df = entry.merge(adj_history, on=["TICKER", "DATE"], how="inner")
    adj_entry_df["ADJ_ASK_PRICE"] = adj_entry_df["ASK_PRICE"] * adj_entry_df["ADJ_RATIO"]
    logging.info("adj entry")
    logging.info(adj_entry_df.head().to_string())
    return adj_entry_df


def build_snapshot_times(first_time, interval_seconds, tz_name):
    localdate = utils.local_date(tz_name)
    localtime_datetime = datetime.strptime(localdate + " " + first_time, "%Y-%m-%d %H:%M:%S")
    snapshot_times = [localtime_datetime + timedelta(seconds=x) for x in [0, interval_seconds]]
    snapshot_times = [x.strftime("%H:%M:%S") for x in snapshot_times]
    logging.info(("snapshot times", snapshot_times))
    return snapshot_times


def capture_ask_price_down(bid_ask, candidates, entry_cnt):
    bid_ask_candidates = bid_ask.merge(candidates, on="TICKER", how="inner")
    bid_ask_candidates = bid_ask_candidates.query("ASK_AMT > 1000000")
    bid_ask_candidates["ASK_PRICE_DOWN"] = bid_ask_candidates["ASK_PRICE"] / bid_ask_candidates["PRE_CLOSE"] - 1
    bid_ask_candidates = bid_ask_candidates.sort_values(by="ASK_PRICE_DOWN")
    bid_ask_candidates = bid_ask_candidates.head(entry_cnt * 2)
    bid_ask_candidates_face = bid_ask_candidates.sort_values(by="ASK_PRICE")
    bid_ask_candidates_face = bid_ask_candidates_face.head(entry_cnt)
    return bid_ask_candidates_face


def main(configs):
    logging.info("                       ")
    logging.info(" ******* start ******** ")
    try:
        universe = get_universe(configs.universe_max_page)
        snapshot_times = build_snapshot_times(configs.first_snapshot_time, configs.snapshot_interval_seconds,
                                              configs.tz_name)
        snapshot = get_snapshots(universe, snapshot_times, configs.tz_name, configs.override)
        candidates = select_price_drop(snapshot, configs.entry_cnt * 4, configs.first_exclusion_cnt, configs.override)
        if not configs.override:
            utils.sleepby(20 * 60)  # bid/ask delayed by 20 minutes
        logging.info("capture bid ask started")
        bid_ask = capture_bidask(list(candidates.TICKER.unique()), utils.local_date(configs.tz_name),
                                 utils.local_time(configs.tz_name))
        bid_ask_candidates_face = capture_ask_price_down(bid_ask, candidates, configs.entry_cnt)
        appended_entry = save_entry(configs.entry_path, bid_ask_candidates_face)
        # history
        entry_universe = list(appended_entry.TICKER.unique())
        entry_dates = sorted(list(appended_entry.DATE.unique()))
        history = get_history(entry_universe, len(entry_dates) + 20)
        # adj entry
        adjusted_entry = adj_entry(appended_entry, history)
        reduced_entry = adjusted_entry[["TICKER", "DATE", "ADJ_ASK_PRICE"]].rename(
            columns={"ADJ_ASK_PRICE": "ENTRY_PRICE"})
        reduced_history = history[["TICKER", "DATE", "OPEN"]]
        # build report
        if len(entry_dates) > configs.exit_ndays:
            report = utils.build_gain_report(entry_df=reduced_entry, history_df=reduced_history,
                                             exit_ndays=configs.exit_ndays)
            report.to_json(configs.report_path)
            # email report
            utils.send_email_with_df("kr-price-drop-bid-ask: gain report", configs.email_cred, report)
            logging.info("gain report sent via email")
        else:
            utils.send_status_email("kr-price-drop-bid-ask: NO gain report - too short entry", configs.email_cred)
            logging.info("no gain report sent because of too short entry")

    except Exception as e:
        logging.error(e, exc_info=True)


def test_build_snapshot_times():
    test_start_time = "11:22:33"
    test_interval_seconds = 60 * 60
    test_snapshot_times = build_snapshot_times(test_start_time, test_interval_seconds, "Asia/Seoul")
    logging.info((test_start_time, test_interval_seconds, test_snapshot_times))
    # python -m pytest /Users/KXK2ZO/py_stock_research/research/kr/data_collect/ops.py::test_build_snapshot_times --log-cli-level=INFO


def test_features():
    test_tz_name = "Asia/Seoul"
    test_override = True
    test_universe = get_universe(1)[:10]
    logging.info(("test universe size", len(test_universe), test_universe[:10]))
    localtime = utils.local_time(test_tz_name)
    logging.info(("local time", localtime))
    test_snapshot_times = build_snapshot_times(localtime, 10, test_tz_name)
    logging.info(test_snapshot_times)
    test_snapshot = get_snapshots(test_universe, test_snapshot_times, test_tz_name, test_override)
    logging.info(test_snapshot.head().to_string())
    test_snapshot["CLOSE"] = test_snapshot["CLOSE"].map(lambda x: x * (1 + random.uniform(-0.1, -0.01)))
    logging.info(test_snapshot[["TICKER", "DATE", "CLOSE", "TIME", "snapshot_time"]].sort_values(by=["TICKER", "TIME"]))
    test_price_drop = select_price_drop(test_snapshot, 4, 0)
    logging.info(test_price_drop)
    test_bid_ask = capture_bidask(test_universe, utils.local_date(test_tz_name), utils.local_time(test_tz_name))
    test_ask_price_down = capture_ask_price_down(test_bid_ask, test_price_drop, 1)
    logging.info((test_ask_price_down.dtypes, test_ask_price_down))

    test_history = get_history(test_universe, 20)
    logging.info((test_history.DATE.max(), test_history.DATE.min()))
    logging.info(test_history.dtypes)

    test_entry = test_bid_ask
    logging.info(test_entry.dtypes)
    test_adj_entry = adj_entry(test_entry, test_history)
    logging.info((test_adj_entry.shape[0], test_adj_entry.dtypes))

    test_entry = test_history.copy(True)[["TICKER", "DATE", "LOW"]].rename(columns={"LOW": "ENTRY_PRICE"})
    test_report = utils.build_gain_report(test_entry, test_history, 1)
    logging.info((test_report.head(), test_report.dtypes))

    # python -m pytest /Users/KXK2ZO/py_stock_research/research/kr/data_collect/ops.py::test_features --log-cli-level=INFO
