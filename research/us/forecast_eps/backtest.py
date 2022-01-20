import sys
import platform

if "darwin" in platform.system().lower():
    # mac
    sys.path.append("/Users/KXK2ZO/py_stock_research")
else:  # linux
    sys.path.append("/home/py_stock_research")

import pandas as pd
import requests
import os
import random, math
import numpy as np
from datetime import datetime, timedelta
from matrix_factorization import BaselineModel, KernelMF
from research.us.forecast_eps import ops
import logging

root_path = "/Users/KXK2ZO/minor_research/R/jupyter_R/us_study/data"


def prep_eps_df(raw_eps_df):
    raw_eps_df = raw_eps_df[raw_eps_df["EPS_TTM"].map(lambda x: not pd.isna(x))]
    raw_eps_df["EPS_TTM"] = raw_eps_df["EPS_TTM"].map(lambda x: float(str(x).replace("M", "")))
    raw_eps_df["MONTH"] = raw_eps_df["YRMO"].map(lambda x: int(x.split("-")[1]))
    eps_ttm_df_max_month = raw_eps_df.groupby("TICKER").agg(max_month=("MONTH", "max")).reset_index()
    end_dec_universe = list(eps_ttm_df_max_month.query("max_month==12").TICKER.unique())
    return raw_eps_df[raw_eps_df["TICKER"].map(lambda x: x in end_dec_universe)]


def single_backtest(yrmo_list, price_df, eps_df, min_amt, mf_run_count, threshold_list, mask_ratio):
    _, price_start_date = ops.convert_yrmo_to_date(max(yrmo_list), "01")
    _, price_end_date = ops.convert_yrmo_to_date(max(yrmo_list), "28")
    price_start_date = price_start_date - timedelta(days=90)
    price_end_date = price_end_date + timedelta(days=150)
    reduced_price = price_df.query("DATE >= '{}'".format(price_start_date.strftime("%Y-%m-%d"))).query(
        "DATE <= '{}'".format(price_end_date.strftime("%Y-%m-%d")))
    reduced_price["AMT"] = reduced_price["CLOSE"] * reduced_price["VOLUME"] / (10 ** 6)
    amt_universe = list(ops.cal_median_amt(reduced_price, min(yrmo_list), max(yrmo_list)).query(
        "AMTMEDIAN > {}".format(str(min_amt))).TICKER.unique())
    reduced_eps_df = eps_df[eps_df["YRMO"].map(lambda x: x in yrmo_list)]
    reduced_eps_df = reduced_eps_df[reduced_eps_df["TICKER"].map(lambda x: x in amt_universe)]
    valid_eps_df = ops.get_complete_tickers(reduced_eps_df)
    valid_eps_df_growth = ops.get_eps_growth(valid_eps_df)
    valid_eps_df_growth = valid_eps_df_growth[valid_eps_df_growth["EPS_GROWTH"].map(lambda x: not pd.isna(x))]
    valid_eps_df_growth_class = ops.classify_eps_growth(valid_eps_df_growth, threshold_list)
    logging.info(valid_eps_df_growth_class.head())
    class_list = sorted(list(valid_eps_df_growth_class.CLASS.unique()))
    logging.info(class_list)
    logging.info(len(valid_eps_df_growth_class.TICKER.unique()))
    multiple_mf_result = ops.do_multiple_mf(valid_eps_df_growth_class, mf_run_count, mask_ratio, min(class_list),
                                            max(class_list))
    match_yrmo_result = ops.convert_forecast_yrmo_to_price_yrmo(multiple_mf_result)
    pre_eps_df = valid_eps_df_growth.copy(True)[["TICKER", "PRE_EPS", "YRMO"]]
    match_yrmo_result_pre_eps = match_yrmo_result.merge(pre_eps_df, on=["TICKER", "YRMO"], how="inner")
    return match_yrmo_result_pre_eps


def test_single_backtest():
    test_price = pd.read_csv(os.path.join(root_path, "us_study_day_price.csv"))
    test_yrmos = ['2009-09', '2009-12', '2010-03', '2010-06', '2010-09']
    test_min_amt = 5
    raw_eps = pd.read_json(os.path.join(root_path, "us_eps_ttm.json"), convert_dates=False)
    test_eps_df = prep_eps_df(raw_eps)
    actual = single_backtest(yrmo_list=test_yrmos,
                             price_df=test_price,
                             eps_df=test_eps_df,
                             min_amt=test_min_amt,
                             mf_run_count=20,
                             threshold_list=[-0.5, -0.1, 0.1, 0.5],
                             mask_ratio=0.5)
    logging.info(len(actual.TICKER.unique()))
    logging.info(actual.head())
    logging.info(actual.PRED_CLASS_MEDIAN.describe())
    logging.info(actual.PRED_CLASS_MEAN.describe())
    assert actual[actual["PRE_EPS"].map(lambda x: pd.isna(x))].shape[0] == 0


def main():
    print("main started")
    min_amt = 0.5
    mf_run_cnt = 20
    threshold_list_input = [-0.7, -0.5, -0.3, -0.1, 0.1, 0.3, 0.5, 0.7]
    mask_ratio_input = 0.5
    price = pd.read_csv(os.path.join(root_path, "us_study_day_price.csv"))
    raw_eps = pd.read_json(os.path.join(root_path, "us_eps_ttm.json"), convert_dates=False)
    all_eps_df = prep_eps_df(raw_eps)
    all_yrmos = sorted(
        list(all_eps_df.groupby("YRMO").agg(CNT=("TICKER", "count")).reset_index().query("CNT > 1000").YRMO.unique()))[
                1:]
    mf_past_yrmo_length = 10
    mf_results = []
    while len(all_yrmos) >= mf_past_yrmo_length:
        subset_yrmos = all_yrmos[:mf_past_yrmo_length]
        all_yrmos = all_yrmos[1:]  # reduce by one from first
        print((max(subset_yrmos), len(all_yrmos)))
        temp = single_backtest(subset_yrmos, price, all_eps_df, min_amt, mf_run_cnt, threshold_list_input,
                               mask_ratio_input)
        print(len(temp.TICKER.unique()))
        print(temp.head())
        mf_results.append(temp)

    mf_result = pd.concat(mf_results, ignore_index=True)
    # mf_result.to_json(os.path.join(root_path, "multiple_mf_eps_ttm_forest_next_qtr.json"))
    print(mf_result.YRMO.unique())
    pass


if __name__ == "__main__":
    main()
