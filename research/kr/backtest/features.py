# feature generation: DIS, AMT, MARKETCAP, PBR, EXITPRICE, EXITVOLUME, GAIN, NORMGAIN
# build condition list[dict] : [{"dis_up_amt_down_face_down":[("DIS","UP",4),("AMT","DOWN",2),("FACE","DOWN",1)]}]
# universal condition: min_amt, max_amt, min_face from unadj_close
# def rank_selection(df, single_condition_list, entry_count)
# -> {"idea":"", "gain_geometric_mean":, "years":, "last_cr":, "mdd":,}
# sample data : few days (~200 days)
# select best 10 conditions
# run rank_selection to full scale data

import pandas as pd
import os
import logging
from datetime import datetime, timedelta

root_path = ""
price_path = os.path.join(root_path, "price.parquet")
financials_path = os.path.join(root_path, "financials.parquet")
universe_path = os.path.join(root_path, "universe.parquet")


def calculate_disparity(df: pd.DataFrame, pdays, col_name="CLOSE", new_col_name="CLOSE_AVG"):
    df = df.sort_values(by=["TICKER", "DATE"])
    df[new_col_name] = df.groupby("TICKER")[col_name].transform(lambda x: x.rolling(pdays, min_periods=pdays).mean())
    df["DISPARITY"] = df[col_name] / df[new_col_name] - 1
    return df


def test_calculate_disparity():
    test_df = pd.DataFrame([("005930", "2020-11-22", 100),
                            ("005930", "2020-11-23", 110),
                            ("005930", "2020-11-24", 120),
                            ("005930", "2020-11-25", 130),
                            ("005930", "2020-11-26", 140),
                            ("005930", "2020-11-27", 150),
                            ("005935", "2020-11-22", 1100),
                            ("005935", "2020-11-23", 1110),
                            ("005935", "2020-11-24", 1120),
                            ("005935", "2020-11-25", 1130),
                            ("005935", "2020-11-26", 1140),
                            ("005935", "2020-11-27", 1150)], columns=["TICKER", "DATE", "CLOSE"])
    test_pday = 3
    actual = calculate_disparity(test_df, test_pday, "CLOSE", "CLOSE_AVG")
    logging.info(actual.head())
    logging.info(actual.dtypes)


def match_financials(price_df, financial_df):
    price_df["FINYEAR"] = price_df["DATE"].map(
        lambda x: pd.to_datetime(x).year - 2 if pd.to_datetime(x).month <= 3 else pd.to_datetime(x).year - 1)
    price_df.set_index(["TICKER", "FINYEAR"], inplace=True)
    financial_df["FINYEAR"] = financial_df["YEAR"].map(lambda x: int(x))
    financial_df.set_index(["TICKER", "FINYEAR"], inplace=True)
    match = pd.concat([price_df, financial_df], axis=1)
    return match


def get_exit_price(df: pd.DataFrame, ndays, col_name, new_col_name):
    df = df.sort_values(by=["TICKER", "DATE"])
    df[new_col_name] = df.groupby(["TICKER"])[col_name].transform(lambda x: x.shift(ndays * (-1)))
    return df


def test_get_exit_price():
    test_df = pd.DataFrame([("005930", "2020-11-22", 100),
                            ("005930", "2020-11-23", 110),
                            ("005930", "2020-11-24", 120),
                            ("005930", "2020-11-25", 130),
                            ("005930", "2020-11-26", 140),
                            ("005930", "2020-11-27", 150),
                            ("005935", "2020-11-22", 1100),
                            ("005935", "2020-11-23", 1110),
                            ("005935", "2020-11-24", 1120),
                            ("005935", "2020-11-25", 1130),
                            ("005935", "2020-11-26", 1140),
                            ("005935", "2020-11-27", 1150)], columns=["TICKER", "DATE", "CLOSE"])
    actual = get_exit_price(test_df, 2, "CLOSE", "NCLSOE")
    logging.info(actual.dtypes)
    logging.info(actual.head())
    logging.info(actual[actual["TICKER"].map(lambda x: x == "005930")].tail())
    logging.info(actual[actual["TICKER"].map(lambda x: x == "005935")].tail())


def build_conditions(features, multiples, directions):
    features_with_blank = features + [""] if "" not in features else features
    conditions = []
    for f in features:
        rest = [x for x in features_with_blank if x not in [f]]
        for r in rest:
            last = [x for x in features_with_blank if x not in [f] + [r]]
            for l in last:
                if r != "":
                    temp = [f, r]
                    if l != "":
                        temp.append(l)
                        if temp not in conditions:
                            conditions.append(temp)
                    else:
                        if temp not in conditions:
                            conditions.append(temp)

                else:
                    temp = [f]
                    if temp not in conditions:
                        conditions.append(temp)
    comb_conditions = {}
    for c in conditions:
        logging.info(c)
        for d in directions:
            temp = []
            key = ""
            reduce_multiples = multiples[-1 * len(c):]
            for i in range(len(c)):
                fmd = (c[i], reduce_multiples[i], d)
                temp.append(fmd)
                key = key + ("_" if len(key) > 0 else "") + str(c[i]) + "_" + d
            comb_conditions[key] = temp
    return comb_conditions


def test_build_conditions():
    test_f = ["A", "B", "C", "D"]
    test_m = [4, 2, 1]
    test_d = ["UP", "DOWN"]
    actual = build_conditions(test_f, test_m, test_d)
    logging.info(actual)
    logging.info(len(actual))
    assert [("A", 4, "DOWN"), ("B", 2, "DOWN"), ("C", 1, "DOWN")] in actual.values()
    assert [("A", 4, "UP"), ("B", 2, "UP"), ("C", 1, "UP")] in actual.values()
