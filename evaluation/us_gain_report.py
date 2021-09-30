# entry history: IDEA, TICKER, PRICE, DATE, TIME
# history of entry universe : DATE, OHLCV, TICKER
# input: exit_ndays, fee_perc
# build dateindex : Date, Index
# entry: merge(entry, history, on=TICKER, how=inner) -> ENTRYPRICE from history
# exit: merge(entry, history adjusted by dateindex, on=TICKER, how=left) -> EXITPRICE from history, replace Nan with 0.0
# gain : EXITPRICE/ENTRYPRICE-1 - fee_perc
# aggregate gain ,cnt, by = idea, date

import pandas as pd
import os
import logging
import numpy as np


def gain_report(entry, history, exit_ndays, fee_perc):
    dates = sorted(list(history.DATE.unique()))
    dateindex = pd.DataFrame(zip(dates, list(range(len(dates)))), columns=["DATE", "INDEX"])
    exclude_index = -1*(exit_ndays+1)
    last_ndays = sorted(list(dateindex.DATE.unique()))[exclude_index:]
    entry_index = pd.merge(entry, dateindex, on="DATE", how="inner")
    entry_index["EXIT_INDEX"] = entry_index["INDEX"].map(lambda x: x + exit_ndays)
    history_index = pd.merge(history, dateindex, on="DATE", how="inner")
    entry_history_index = history_index.copy(True).rename(columns={"CLOSE": "ENTRY_PRICE"})[
        ["TICKER", "INDEX", "ENTRY_PRICE"]]

    exit_history_index = (
        history_index.copy(True).rename(columns={"OPEN": "EXIT_PRICE", "VOLUME": "EXIT_VOLUME", "INDEX": "EXIT_INDEX"})[
            ["TICKER", "EXIT_INDEX", "EXIT_PRICE", "EXIT_VOLUME"]])
    entry_exit = pd.merge(entry_index, entry_history_index, on=["INDEX", "TICKER"], how="left")
    entry_exit["ENTRY_PRICE"] = entry_exit["ENTRY_PRICE"].map(lambda x: 1 if pd.isna(x) else x)
    entry_exit = pd.merge(entry_exit, exit_history_index, on=["EXIT_INDEX", "TICKER"], how="left")
    entry_exit["EXIT_PRICE"] = entry_exit["EXIT_PRICE"].map(lambda x: 0 if pd.isna(x) else x)
    entry_exit["GAIN"] = entry_exit["EXIT_PRICE"] / entry_exit["ENTRY_PRICE"] - 1 - fee_perc

    entry_exit = entry_exit[entry_exit["DATE"].map(lambda x: x not in last_ndays)]
    logging.info("after excluding last ndays")
    logging.info(entry_exit.shape)
    logging.info(entry_exit.head())
    report = entry_exit.groupby(["IDEA", "DATE"]).agg(GM=("GAIN", "mean"), CNT=("TICKER", "count")).reset_index()
    report["EXITNDAYS"] = exit_ndays
    return report, entry_exit


def test_gain_report():
    test_entry = pd.DataFrame([("2020-01-01", 100, "TEST1", "IDEA1"),
                               ("2020-01-01", 1000, "TEST2", "IDEA2"),
                               ("2020-01-01", 100, "TEST11", "IDEA1"),
                               ("2020-01-02", 1000, "TEST11", "IDEA1"),
                               ("2020-01-02", 110, "TEST1", "IDEA1"),
                               ("2020-01-02", 1100, "TEST2", "IDEA2")], columns=["DATE", "CLOSE", "TICKER", "IDEA"])
    test_history = pd.DataFrame([("2020-01-01", 100, 0, "TEST1", 100),
                                 ("2020-01-02", 111, 1, "TEST1", 101),
                                 ("2020-01-03", 222, 2, "TEST1", 202),
                                 ("2020-01-04", 333, 3, "TEST1", 303),
                                 ("2020-01-01", 1000, 0, "TEST2", 1000),
                                 ("2020-01-02", 1110, 1, "TEST2", 1001),
                                 ("2020-01-03", 2220, 2, "TEST2", 2002),
                                 ("2020-01-04", 3330, 3, "TEST2", 3003)],
                                columns=["DATE", "OPEN", "VOLUME", "TICKER", "CLOSE"])
    test_exit_ndays = 2
    re, ee = gain_report(test_entry, test_history, test_exit_ndays, 0)
    logging.info(re.to_string())
    logging.info(ee.to_string())


def agg_performance(report):
    def _cal_mdd(cr_list):
        df = pd.concat([pd.Series(cr_list).cummax(), pd.Series(cr_list)], axis=1)
        df.columns = ["CRMAX", "CR"]
        df["MDD"] = df["CR"] / df["CRMAX"] - 1
        return min(df.MDD.to_list())

    report["NORMGM"] = (1 + report["GM"] / report["EXITNDAYS"])
    df = report.groupby("IDEA")["NORMGM"].apply(list).reset_index(name="NORMGM_LIST")
    df["CR"] = df["NORMGM_LIST"].map(lambda x: list(np.array(x).cumprod()))
    df["CAGR"] = df["CR"].map(lambda x: x[-1] ** (1 / (len(x) / 250)) - 1)
    df["LASTCR"] = df["CR"].map(lambda x: x[-1])
    df["DAYS"] = df["CR"].map(lambda x: len(x))
    df["MDD"] = df["CR"].map(lambda x: _cal_mdd(x))
    return df[["IDEA", "CAGR", "LASTCR", "DAYS", "MDD"]]
