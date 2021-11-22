import json
import pandas as pd
import requests
import logging
from research.kr.utils import marketcap_dataprep, unadj_day_price_dataprep
from rank_selection_main import utils
from datetime import datetime, timedelta
import os
import numpy as np
from research.kr.kosdaq150 import config, ops
from rank_selection_main import utils

email_cred = utils.read_json_to_dict(config.email_path)
config.email_cred = email_cred  # for unittest - main


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
    universe = [str(x).zfill(6) for x in universe]
    history, failed = unadj_day_price_dataprep.get_adj_unadj_day_price(universe, pdays)
    # "DATE", "STARTDATE", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "TICKER", "UNADJCLOSE"
    history["ADJ_RATIO"] = history["CLOSE"] / history["UNADJCLOSE"]
    logging.info(("history size", history.shape[0]))
    return history


def get_ticker_name(parsed_str):
    return [x.split('</a></td>\n\t\t\t\t\t<td class="number">')[0] for x in
            parsed_str.split('href=')[1].split('main.naver?code=')[1].split('" class="tltle">')]


def get_current_marketcap_shares(url):
    name_ticker_feature = None
    temp = pd.read_html(url, encoding='euc-kr')[1].iloc[1:, [1, 2, 6, 7, 8, 9, 10, 11]]
    temp.columns = ["NAME", "PRICE", "MARKETCAP", "SHARES", "FOREIGN", "VOLUME", "PER", "ROE"]
    feature = temp[temp["NAME"].map(lambda x: pd.notnull(x))]
    if feature.shape[0] > 0:
        parsed_list = requests.get(url).text.split('<td class="no">')[1:]
        ticker_name = []
        for ps in parsed_list:
            temp = get_ticker_name(ps)
            ticker_name.append((temp[0], temp[1]))
        name_ticker = pd.DataFrame(ticker_name, columns=["TICKER", "NAME"])
        name_ticker = name_ticker[name_ticker["NAME"].map(lambda x: "호스팩" not in x)]
        name_ticker_feature = pd.merge(name_ticker, feature, on="NAME", how="inner").drop(columns=["NAME"])
    return name_ticker_feature


def get_kosdaq150_universe(max_page=1):
    urls = "https://finance.naver.com/sise/sise_market_sum.naver?sosok=1&page={}"
    result = []
    counter = 0
    while counter < max_page:
        counter += 1
        op_url = urls.format(str(counter))
        temp = get_current_marketcap_shares(op_url)
        result.append(temp)

    return pd.concat(result, ignore_index=True)


def get_past_12mo_consensus(ticker, fy=2):
    url_template = "https://comp.fnguide.com/SVO2/json/chart/07_02/chart_A{}_D_FY{}.json"
    json_body = requests.get(url_template.format(str(ticker), str(fy))).text.encode().decode('utf-8-sig')
    content = json.loads(json_body)
    consensus_history = content["CHART"]
    return consensus_history


def get_eps_change(ticker, fy):
    consensus_history_list = get_past_12mo_consensus(ticker, fy)
    date_eps = [{"DATE": x["STD_DT"], "EPS": x["EPS"]} for x in consensus_history_list]
    date_eps = sorted(date_eps, key=lambda x: x["DATE"])
    current_eps = float(date_eps[-1]["EPS"].replace(",", ""))
    p1_eps = float(date_eps[-2]["EPS"].replace(",", ""))
    return {"TICKER": ticker, "FY": fy, "EPS": current_eps, "P1EPS": p1_eps}


def get_all_eps_change(universe: list, fy):
    all_eps_change_list = []
    for i, t in enumerate(universe):
        try:
            all_eps_change_list.append(get_eps_change(t, fy))
            logging.info((i, "{} - fy {} eps downloaded".format(str(t), str(fy))))
        except Exception as e:
            logging.info((i, "{} failed".format(str(t))))
            logging.error(e, exc_info=True)
    result = pd.DataFrame(all_eps_change_list)
    result["TICKER"] = result["TICKER"].map(lambda x: str(x).zfill(6))
    result["EPS_CHG"] = result["EPS"] / result["P1EPS"] - 1
    return result


def test_get_all_eps_change():
    test_universe = ["005930", "000660"]
    actual = get_all_eps_change(test_universe, 2)
    logging.info(actual.dtypes)
    logging.info(actual.tail())


def get_wein_gigan(ticker, page_n):
    url_template = "https://finance.naver.com/item/frgn.naver?code={}&page={}"
    url = url_template.format(str(ticker), str(page_n))
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

    htmltext = requests.get(url, headers=headers).text
    dfs = pd.read_html(htmltext)
    df = dfs[2]
    df.columns = ["DATE", "CLOSE", "CHANGE", "CHANGE_PERC", "VOLUME", "GIGAN", "WEIN", "HOLDING", "WEIN_PORTION"]
    df = df[df["DATE"].map(lambda x: not pd.isna(x))]
    df = df[["DATE", "CLOSE", "VOLUME", "GIGAN", "WEIN"]]
    df["TICKER"] = ticker
    return df


def get_all_wein_gigan(universe):
    pages = [1]
    gigan_wein_dfs = []
    for i, t in enumerate(universe):
        min_date = None
        for j, p in enumerate(pages):
            try:
                temp = get_wein_gigan(t, p)
                if min_date == temp.DATE.min():
                    logging.info("ticker={}, page={} no more new data- stop".format(str(t), str(p)))
                    break
                gigan_wein_dfs.append(temp)
                logging.info("ticker={}, page={} captured".format(str(t), str(p)))
                min_date = temp.DATE.min()
            except Exception as e:
                logging.info("ticker={}, page={} skipped".format(str(t), str(p)))
                logging.error(e, exc_info=True)
    gigan_wein = pd.concat(gigan_wein_dfs, ignore_index=True)
    return gigan_wein


def test_get_all_wein_gigan():
    test_universe = ["005930", "000660"]
    actual = get_all_wein_gigan(test_universe)
    logging.info(actual.groupby("TICKER").agg(CNT=("DATE", "count")).reset_index())


def calculate_ant_portion(gigan_wein):
    gigan_wein["TICKER"] = gigan_wein["TICKER"].map(lambda x: str(x).zfill(6))
    gigan_wein["DATE"] = gigan_wein["DATE"].map(lambda x: x.replace(".", "-"))
    gigan_wein["GIGAN"] = gigan_wein["GIGAN"].map(lambda x: float(str(x).replace(",", "").replace("+", "")))
    gigan_wein["WEIN"] = gigan_wein["WEIN"].map(lambda x: float(str(x).replace(",", "").replace("+", "")))
    gigan_wein["GIGAN_WEIN_SUM"] = gigan_wein["GIGAN"] + gigan_wein["WEIN"]
    gigan_wein["ANT"] = gigan_wein["GIGAN_WEIN_SUM"].map(lambda x: -1 * x)
    gigan_wein["ANT_SUM"] = gigan_wein.groupby("TICKER")["ANT"].sum().reset_index(0, drop=True)
    gigan_wein["VOLUME_SUM"] = gigan_wein.groupby("TICKER")["VOLUME"].sum().reset_index(0, drop=True)
    gigan_wein["ANT_SUM_PORTION"] = gigan_wein["ANT_SUM"] / gigan_wein["VOLUME_SUM"]
    gigan_wein = gigan_wein[gigan_wein["ANT_SUM_PORTION"].map(lambda x: not pd.isna(x))]
    return gigan_wein[["TICKER", "ANT_SUM_PORTION", "ANT_SUM"]]


def rank_then_select(dt, first_feature, second_feature, entry_cnt_var, first_asc=True, second_asc=True):
    feature_1_filtered = dt.copy(True)
    feature_1_filtered["{}_RANK".format(first_feature)] = feature_1_filtered[first_feature].rank(
        method="first", ascending=first_asc)
    feature_1_filtered = feature_1_filtered.query("{}_RANK <= {}".format(first_feature, str(2 * entry_cnt_var)))
    feature_1_filtered["{}_RANK".format(second_feature)] = feature_1_filtered[second_feature].rank(
        method="first", ascending=second_asc)
    feature_2_filtered = feature_1_filtered.query("{}_RANK <= {}".format(second_feature, str(1 * entry_cnt_var)))
    feature_2_filtered["IDEA"] = first_feature + "/" + second_feature
    return feature_2_filtered[["TICKER", "IDEA"]]


def main(configs):
    logging.info("config override = {}".format("TRUE" if configs.override else "FALSE"))
    try:
        # collect universe
        universe = list(get_kosdaq150_universe(configs.universe_max_page).TICKER.unique())
        logging.info("universe size = {}".format(str(len(universe))))
        # collect consensus
        consensus = get_all_eps_change(universe, configs.eps_fy)
        consensus = consensus[["TICKER", "EPS_CHG"]]
        # collect gigan wein
        start_time_gigan_wein = datetime.utcnow()
        gigan_wein_p20 = get_all_wein_gigan(universe)
        ant_result = calculate_ant_portion(gigan_wein_p20)[["TICKER", "ANT_SUM_PORTION"]]
        logging.info(("gigan wein download run time:", datetime.utcnow() - start_time_gigan_wein))
        # collect realtime price
        feed = utils.kr_realtime_feed(universe)
        feed["AMT"] = feed["CLOSE"] * feed["VOLUME"] / 10 ** 8
        old_feed_cnt = len(feed.TICKER.unique())
        feed = feed.query("AMT >= {}".format(str(configs.min_amt)))
        feed = feed.query("CLOSE >= {}".format(str(configs.min_face)))
        logging.info("feed reduced from {} to {} because of min_amt, min_face".format(str(old_feed_cnt), str(len(
            feed.TICKER.unique()))))
        # feature rank -> select entry -> unique entry candidates
        logging.info(("feed", feed.tail(), "consensus", consensus.tail(), "ant_result", ant_result.tail()))
        eps_amt = feed.merge(consensus, on="TICKER", how="inner")
        logging.info(("eps_amt", eps_amt.dtypes, eps_amt.shape, eps_amt.tail()))
        eps_amt_select = rank_then_select(eps_amt.copy(True), "EPS_CHG", "AMT", configs.entry_cnt_per_idea, False, True)
        # up EPS, small amt

        eps_ant = consensus.merge(ant_result, on="TICKER", how="inner")
        logging.info(("eps_ant", eps_ant.dtypes, eps_ant.shape, eps_ant.tail()))
        eps_ant_select = rank_then_select(eps_ant.copy(True), "EPS_CHG", "ANT_SUM_PORTION", configs.entry_cnt_per_idea,
                                          False,
                                          False)
        # up EPS, large ant
        unique_entry_tickers = list(
            set(list(eps_amt_select.TICKER.unique()) + list(eps_ant_select.TICKER.unique())))
        logging.info(
            ("eps_*AMT* tickers",
             len(list(eps_amt_select.TICKER.unique())),
             sorted(list(eps_amt_select.TICKER.unique())),
             "****"
             "eps_*ANT* ickers", len(list(eps_ant_select.TICKER.unique())),
             sorted(list(eps_ant_select.TICKER.unique()))))
        logging.info(("unique_entry_tickers", len(unique_entry_tickers), unique_entry_tickers))
        unique_entry = feed[feed["TICKER"].map(lambda x: x in unique_entry_tickers)][
            ["TICKER", "DATE", "CLOSE", "AMT"]]
        logging.info(("new entry", unique_entry.shape[0], unique_entry.tail(), unique_entry.dtypes))
        # save entry
        utils.send_email_with_df("kr_eps_change: entry", configs.email_cred, unique_entry)
        append_entry = save_entry(configs.entry_path, unique_entry)
        append_entry_universe = list(append_entry.TICKER.unique())
        append_entry_dates = list(append_entry.DATE.unique())
        # build report -> email report
        if len(append_entry.DATE.unique()) > configs.exit_ndays + 2:
            history_pdays = len(append_entry_dates) + configs.exit_ndays + 10
            history = get_history(append_entry_universe, history_pdays)
            history.to_json(configs.history_path)
            logging.info("history saved to {}".format(configs.history_path))
            reduced_history = history[["DATE", "TICKER", "OPEN"]]
            logging.info("report built")
            reduced_entry = history.merge(append_entry[["TICKER", "DATE"]], on=["TICKER", "DATE"], how="inner")
            reduced_entry["ENTRY_PRICE"] = reduced_entry["CLOSE"]
            reduced_entry = reduced_entry[["TICKER", "DATE", "ENTRY_PRICE"]]
            report = utils.build_gain_report(entry_df=reduced_entry, history_df=reduced_history,
                                             exit_ndays=configs.exit_ndays)
            report["GM_AFTER_FEE"] = report["GM"].map(lambda x: x - configs.fee_perc)
            report["GM_AFTER_FEE_NORM"] = report["GM_AFTER_FEE"].map(lambda x: x / configs.exit_ndays + 1)
            cr = np.array(list(report.GM_AFTER_FEE_NORM)).cumprod()
            report["CR"] = cr
            report.to_json(configs.report_path)
            # email report
            utils.send_email_with_df("kr_eps_change: gain report", configs.email_cred, report)
            logging.info("gain report sent via email")
        else:
            logging.info("entry is too short")
            utils.send_status_email("kr_eps_change: gain report - entry is too short", configs.email_cred,
                                    "entry date cnt = {}".format(str(len(append_entry_dates))))
    except Exception as e:
        logging.error(e, exc_info=True)
        utils.send_status_email("kr_eps_change: error", configs.email_cred, str(e))


def test_main():
    config.override = True
    config.universe_max_page = 1
    main(config)
