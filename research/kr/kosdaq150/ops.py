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

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}


def save_entry(path, df) -> pd.DataFrame:
    if os.path.exists(path):
        existing_df = pd.read_json(path, convert_dates=False)
        new_df = pd.concat([existing_df, df], ignore_index=True)
        logging.info(("old entry size = ", existing_df.shape[0], " appended entry size = ", new_df.shape[0]))
    else:
        new_df = df
        logging.info(("init entry saved, size = ", new_df.shape[0]))
    new_df.to_json(path)
    new_df["TICKER"] = new_df["TICKER"].map(lambda x: str(x).zfill(6))
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
    gigan_wein_ant_sum = gigan_wein.groupby("TICKER").agg(ANT_SUM=("ANT", "sum")).reset_index()
    gigan_wein_volume_sum = gigan_wein.groupby("TICKER").agg(VOLUME_SUM=("VOLUME", "sum")).reset_index()
    gigan_wein_comb = gigan_wein_ant_sum.merge(gigan_wein_volume_sum, on="TICKER", how="inner")
    gigan_wein_comb["ANT_SUM_PORTION"] = gigan_wein_comb["ANT_SUM"] / gigan_wein_comb["VOLUME_SUM"]
    gigan_wein_comb = gigan_wein_comb[gigan_wein_comb["ANT_SUM_PORTION"].map(lambda x: not pd.isna(x))]
    return gigan_wein_comb[["TICKER", "ANT_SUM_PORTION", "ANT_SUM"]]


def rank_then_select(dt, first_feature, second_feature, entry_cnt_var, first_asc=True, second_asc=True,
                     first_multiple=2, second_multiple=1):
    feature_1_filtered = dt.copy(True).dropna()
    feature_1_filtered["{}_RANK".format(first_feature)] = feature_1_filtered[first_feature].rank(
        method="first", ascending=first_asc)
    feature_1_filtered = feature_1_filtered.query(
        "{}_RANK <= {}".format(first_feature, str(first_multiple * entry_cnt_var)))
    feature_1_filtered["{}_RANK".format(second_feature)] = feature_1_filtered[second_feature].rank(
        method="first", ascending=second_asc)
    feature_2_filtered = feature_1_filtered.query(
        "{}_RANK <= {}".format(second_feature, str(second_multiple * entry_cnt_var)))
    feature_2_filtered["IDEA"] = first_feature + "/" + second_feature
    return feature_2_filtered[["TICKER", "IDEA"]]


def get_recent_earnings(ticker, annual_flag=True):
    """
    :param ticker:
    :param annual_flag: True -> annual, False -> qtr
    :return:
    """
    if annual_flag:
        json_url = "https://comp.fnguide.com/SVO2/json/data/01_06/01_A{}_a_d.json".format(str(ticker).zfill(6))
    else:
        json_url = "https://comp.fnguide.com/SVO2/json/data/01_06/01_A{}_q_d.json".format(str(ticker).zfill(6))
    result_dict = json.loads(requests.get(json_url, headers=headers).text.encode().decode('utf-8-sig'))["comp"]
    yrmo = sorted(
        {k: int(v.replace("/", "")[:6]) for k, v in [x for x in result_dict if x["SORT_ORDER"] == "0"][0].items() if
         "D_" in k}.items(),
        key=lambda x: x[1])

    keys = [x[0] for x in yrmo]
    yrmo = [x[1] for x in yrmo]

    eps = {k: float(0 if v == "" else v.replace(",", "")) for k, v in
           [x for x in result_dict if "EPS" in x["ACCOUNT_NM"]][0].items() if
           "D_" in k}
    eps_value = []
    for k in keys:
        eps_value.append(eps[k])

    eps = eps_value
    return pd.DataFrame(zip(yrmo, eps), columns=["YRMO", "EPS"])


def get_past_12_month_earings(ticker, fy_value):
    url = "https://comp.fnguide.com/SVO2/json/chart/07_02/chart_A{}_D_fy{}.json".format(str(ticker).zfill(6),
                                                                                        str(fy_value))
    return json.loads(requests.get(url, headers=headers).text.encode().decode('utf-8-sig'))["CHART"]


def get_analyst_forecast(ticker):
    url = "http://comp.fnguide.com/SVO2/json/data/01_06/03_A{}.json".format(str(ticker).zfill(6))
    return json.loads(requests.get(url, headers=headers).text.encode().decode('utf-8-sig'))["comp"]


def get_eps_from_itooza(ticker, ttm_flag=True):
    url = "https://search.itooza.com/search.htm?seName={}&jl=&search_ck=&sm=&sd=2021-10-26&ed=2021-11-25&ds_de=&page=&cpv=#indexTable1".format(
        ticker)
    dfs = pd.read_html(url)
    if ttm_flag:
        df_id = 2
        type_str = "TTM"
    else:
        df_id = 3
        type_str = "YEAR"

    yrmo = [("20" + x.split(".")[0]) + "-" + x.split(".")[1][:2] for x in list(dfs[df_id].columns)[1:]]
    eps = list(dfs[df_id].iloc[0,])[1:]
    yrmo_eps = pd.DataFrame(zip(yrmo, eps), columns=["YRMO", "EPS"])
    yrmo_eps["TYPE"] = type_str
    yrmo_eps["TICKER"] = str(ticker).zfill(6)
    yrmo_eps = yrmo_eps.dropna()
    return yrmo_eps


def save_past_eps(ticker, rootpath):
    result = None
    try:
        temp = get_recent_earnings(ticker, True)
        temp["TICKER"] = ticker
        temp.to_json(os.path.join(rootpath, "past_eps_{}.json".format(str(ticker))))
        print("{} saved".format(ticker))
    except Exception as e:
        print(("{} skipped - ".format(ticker), str(e)))
        result = ticker
    return result


def save_past_price(ticker, rootpath, pdays):
    result = None
    try:
        temp = unadj_day_price_dataprep.get_price_from_daum(ticker, pdays)
        temp.to_json(os.path.join(rootpath, "past_price_{}.json".format(str(ticker))))
        print("{} price saved".format(ticker))
    except Exception as e:
        print(("{} skipped - ".format(ticker), str(e)))
        result = ticker
    return result


def save_past_eps_itooza(ticker, rootpath):
    result = None
    try:
        temp_ttm = get_eps_from_itooza(ticker, True)
        temp_year = get_eps_from_itooza(ticker, False)
        temp = pd.concat([temp_ttm, temp_year], ignore_index=True)
        temp.to_json(os.path.join(rootpath, "past_eps_itooza_{}.json".format(str(ticker))))
        print("{} eps itooza saved".format(ticker))
    except Exception as e:
        print(("{} skipped - ".format(ticker), str(e)))
        result = ticker
    return result


def main(configs):
    logging.info("config override = {}".format("TRUE" if configs.override else "FALSE"))
    try:
        # collect universe
        universe = list(get_kosdaq150_universe(configs.universe_max_page).TICKER.unique())
        logging.info("universe size = {}".format(str(len(universe))))
        # collect consensus
        consensus = get_all_eps_change(universe, configs.eps_fy)
        consensus = consensus[["TICKER", "EPS_CHG"]]

        # collect realtime price
        feed = utils.kr_realtime_feed(universe)
        feed["AMT"] = feed["CLOSE"] * feed["VOLUME"] / 10 ** 8
        feed["OC"] = feed["OPEN"] / feed["CLOSE"] - 1
        old_feed_cnt = len(feed.TICKER.unique())
        feed = feed.query("AMT >= {}".format(str(configs.min_amt)))
        feed = feed.query("CLOSE >= {}".format(str(configs.min_face)))
        logging.info("feed reduced from {} to {} because of min_amt, min_face".format(str(old_feed_cnt), str(len(
            feed.TICKER.unique()))))

        if not configs.esp_only_flag:
            # collect gigan wein
            start_time_gigan_wein = datetime.utcnow()
            gigan_wein_p20 = get_all_wein_gigan(universe)
            ant_result = calculate_ant_portion(gigan_wein_p20)[["TICKER", "ANT_SUM_PORTION"]]
            logging.info(("gigan wein download run time:", datetime.utcnow() - start_time_gigan_wein))

            # feature rank -> select entry -> unique entry candidates
            eps_amt = feed.merge(consensus, on="TICKER", how="inner")
            eps_amt_select = rank_then_select(eps_amt.copy(True), "EPS_CHG", "AMT", configs.entry_cnt_per_idea, False,
                                              True)
            # up EPS, small amt

            eps_ant = consensus.merge(ant_result, on="TICKER", how="inner")
            eps_ant_select = rank_then_select(eps_ant.copy(True), "EPS_CHG", "ANT_SUM_PORTION",
                                              configs.entry_cnt_per_idea,
                                              False,
                                              False)
            # up EPS, large ant
            eps_oc = feed.merge(consensus, on="TICKER", how="inner")
            eps_oc_select = rank_then_select(eps_oc.copy(True), "EPS_CHG", "OC", configs.entry_cnt_per_idea, False,
                                             True)
            # up EPS, low oc
            unique_entry_tickers = list(
                set(list(eps_amt_select.TICKER.unique()) +
                    list(eps_ant_select.TICKER.unique()) +
                    list(eps_oc_select.TICKER.unique())
                    )
            )
            logging.info("entry - combined conditions")

        else:  # eps only
            eps_only = feed.merge(consensus, on="TICKER", how="inner")
            eps_only_select = rank_then_select(eps_only.copy(True), "EPS_CHG", "AMT", configs.entry_cnt_per_idea, False,
                                               True, 1, 1)
            unique_entry_tickers = list(eps_only_select.TICKER.unique())
            logging.info("entry - EPS CHG up only")
            # up EPS only

        logging.info(("unique_entry_tickers", len(unique_entry_tickers), unique_entry_tickers))
        unique_entry = feed[feed["TICKER"].map(lambda x: x in unique_entry_tickers)][
            ["TICKER", "DATE", "CLOSE", "AMT"]]
        logging.info(("new entry", unique_entry.shape[0], unique_entry.tail(), unique_entry.dtypes))
        # save entry
        utils.send_email_with_df("kr_eps_change: entry", configs.email_cred, unique_entry)
        utils.send_email_with_df("kr_eps_change: entry", configs.email_cred, unique_entry, configs.mother_email)
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
