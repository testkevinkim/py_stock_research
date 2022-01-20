# building blocks
# select universe (price, min_amt, start_yrmo, end_yrmo, eps_df) -> reduced eps df
# run multiple mf (eps_df, mask_ratio, n_run, target_yrmo, cutoff_list) -> forest eps df: yrmo, ticker, forecast_eps_growth_class, forecast_cnt, pre_eps
# calculate gain (price_df, forecast_df, entry_count, multiple_list, exit_ndays, min_amt, min_face) -> date, gm, count, multiples, entry_count


import pandas as pd
import requests
import os
import random, math
import numpy as np
from datetime import datetime, timedelta
from matrix_factorization import BaselineModel, KernelMF
import logging

def convert_yrmo_to_date(yrmo_val, day_val):
    yrmo_date_str = yrmo_val + "-" + str(day_val).zfill(2)
    return yrmo_date_str, datetime.strptime(yrmo_date_str, "%Y-%m-%d")


def cal_median_amt(price_df, start_yrmo, end_yrmo):
    start_date, _ = convert_yrmo_to_date(start_yrmo, "01")
    end_date, _ = convert_yrmo_to_date(end_yrmo, "01")
    reduced_price_df = price_df.query("DATE >= '{}'".format(start_date)).query("DATE <= '{}'".format(end_date))
    if "AMT" not in price_df.columns:
        reduced_price_df["AMT"] = reduced_price_df["CLOSE"] * reduced_price_df["VOLUME"] / (10 ** 6)
    reduced_price_df = reduced_price_df.query("VOLUME > 0")
    price_agg = reduced_price_df.groupby("TICKER").agg(AMTMEDIAN=("AMT", "median")).reset_index()
    return price_agg


def get_complete_tickers(df):
    min_yrmo = df.YRMO.min()
    max_yrmo = df.YRMO.max()
    valid_tickers = list(
        df[df["YRMO"].map(lambda x: x in [min_yrmo, max_yrmo])]
            .groupby("TICKER")
            .agg(CNT=("YRMO", "count"))
            .reset_index()
            .query("CNT ==2").TICKER.unique()
    )
    return df[df["TICKER"].map(lambda x: x in valid_tickers)]


def get_eps_growth(df):
    df = df.sort_values(by=["TICKER", "YRMO"])
    df["PRE_EPS"] = df.groupby("TICKER")["EPS_TTM"].shift(1)
    df = df[df["PRE_EPS"].map(lambda x: not pd.isna(x))]
    df["EPS_GROWTH"] = (df["EPS_TTM"] - df["PRE_EPS"]) / abs(df["PRE_EPS"])
    return df


def classify_eps_growth(df, cutoff_list=[-0.5, -0.1, 0.1, 0.5]):
    def _get_class(x, sorted_cutoff):
        result = None
        for i, a in enumerate(sorted_cutoff):
            if result is None:
                if i == 0:
                    result = i if x < a else None
                    prev = a
                else:
                    result = i if prev <= x < a else None
                    prev = a

                if result is None:
                    if x >= max(cutoff_list):
                        result = len(cutoff_list)
            else:
                break
        return result

    df["CLASS"] = df["EPS_GROWTH"].map(lambda x: _get_class(x, cutoff_list))
    return df


def split_training_test(eps_df, mask_ratio):
    max_yrmo = eps_df.YRMO.max()
    all_tickers = list(eps_df.query("YRMO =='{}'".format(max_yrmo)).TICKER.unique())
    masked_tickers = random.sample(all_tickers, math.floor(len(all_tickers) * mask_ratio))
    eps_df["TEST"] = eps_df.apply(lambda x: True if x.TICKER in masked_tickers and x.YRMO == max_yrmo else False,
                                  axis=1)
    return eps_df[eps_df["TEST"].map(lambda x: not x)], eps_df[eps_df["TEST"]]  # tr, test


def do_mf(tr_df, test_df, z_min, z_max):
    tr_df["user_id"] = tr_df["TICKER"]
    tr_df["item_id"] = tr_df["YRMO"]
    tr_df["rating"] = tr_df["CLASS"]
    test_df["user_id"] = test_df["TICKER"]
    test_df["item_id"] = test_df["YRMO"]
    x_train = tr_df[["user_id", "item_id"]]
    y_train = tr_df.rating
    matrix_fact = KernelMF(n_epochs=150, n_factors=20, verbose=0, lr=0.1, reg=0.01, min_rating=z_min, max_rating=z_max)
    matrix_fact.fit(x_train, y_train)
    pred = matrix_fact.predict(test_df)
    test_df["PRED_CLASS"] = pred
    return test_df


def do_multiple_mf(all_df, n_run, mask_ratio, min_z, max_z):
    results = []
    for i in range(n_run):
        tr, ts = split_training_test(all_df, mask_ratio)
        ts_result = do_mf(tr, ts, min_z, max_z)
        results.append(ts_result)
    concat_result = pd.concat(results, ignore_index=True)
    logging.info(concat_result.dtypes)
    agg_result = concat_result.groupby(["TICKER", "YRMO"]).agg(FORECAST_CNT=("PRED_CLASS", "count"),
                                                               PRED_CLASS_MEAN=("PRED_CLASS", "mean"),
                                                               PRED_CLASS_MEDIAN=("PRED_CLASS", "median")).reset_index()
    return agg_result


def select_top_n_forecast(ts_df, sort_col_str, top_n):
    sorted_ts_df = ts_df.sort_values(by=sort_col_str)
    return sorted_ts_df.tail(top_n), sorted_ts_df.head(top_n)  # highest, lowest


def convert_forecast_yrmo_to_price_yrmo(forecast_df):
    forecast_yrmo = forecast_df.YRMO.max()
    _, forecast_date = convert_yrmo_to_date(forecast_yrmo, "15")
    from_date = forecast_date - timedelta(days=30)
    to_date = forecast_date + timedelta(days=30)
    from_yrmo = from_date.strftime("%Y-%m")
    to_yrmo = to_date.strftime("%Y-%m")
    converted_forecast_dfs = []
    for i in [from_yrmo, forecast_yrmo, to_yrmo]:
        temp = forecast_df.copy(True)
        temp["MATCHYRMO"] = i
        converted_forecast_dfs.append(temp)
    comb = pd.concat(converted_forecast_dfs, ignore_index=True)
    return comb


def cal_gain(price_df, min_amt, min_face, exit_ndays):
    price_df = price_df.query("AMT > {}".format(str(min_amt))).query("FACE > {}".format(str(min_face)))
    price_df = price_df.sort_values(by=["TICKER", "DATE"])
    price_df["ENTRY"] = price_df["CLOSE"]
    price_df["EXIT"] = price_df.groupby("TICKER")["OPEN"].shift((-1) * exit_ndays)
    price_df["EXITDATE"] = price_df.groupby("TICKER")["DATE"].shift((-1) * exit_ndays)
    price_df["GAIN"] = price_df["EXIT"] / price_df["ENTRY"] - 1
    return price_df
