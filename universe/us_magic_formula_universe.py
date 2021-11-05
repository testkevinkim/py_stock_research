import pandas as pd
import requests
import time
import logging

finviz_url_template = "https://finviz.com/screener.ashx?v=152&f=cap_largeunder,fa_estltgrowth_pos,fa_fpe_profitable,fa_roa_pos,fa_sales5years_pos,geo_usa,ipodate_more5,sh_instown_o10&ft=2&o=forwardpe&r={}"
cols = "&c=0,1,2,3,4,6,7,8,10,11,12,15,16,17,18,19,21,23,27,28,32,33,38,39,63,65,66,67,69,70"
finviz_url_template = finviz_url_template + cols

finviz_numeric_cols = ['Market Cap',
                       'P/E',
                       'Fwd P/E',
                       'P/B',
                       'P/S',
                       'P/C',
                       'Payout Ratio',
                       'EPS next Y',
                       'EPS past 5Y',
                       'Sales Q/Q',
                       'Insider Trans',
                       'Inst Own',
                       'ROA',
                       'ROE',
                       'Debt/Eq',
                       'Gross M',
                       'EPS',
                       'EPS this Y',
                       'Sales past 5Y',
                       'Avg Volume',
                       'Price',
                       'Volume',
                       'Change',
                       'Target Price']


def finviz_features(url_template, start_i=1):
    url = url_template.format(str(start_i))
    header = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest"
    }
    dfs = pd.read_html((requests.get(url, headers=header)).text)
    valid_df_i = 7  # among downloaded dataframes, which dataframe has relevant data
    result = dfs[valid_df_i]
    first_row = result.loc[0, :].values.tolist()
    result.columns = first_row
    result = result[1:]
    return result


def convert_numeric(download_result, numeric_cols):
    def to_numeric(x):
        if isinstance(x, str):
            if x == "-":
                result = float(-1000)
            else:
                result = float(
                    eval(x.replace("%", "").replace("K", "*1000").replace("M", "*1000000").replace("B", "*1000000000")))
        else:
            result = x
        return result

    for a in numeric_cols:
        download_result[a] = download_result[a].map(lambda x: to_numeric(x))
    return download_result


def build_magic_formula_universe(url_template, num_cols, page_i_max=None) -> pd.DataFrame:
    next_flag = True
    page_i = 1
    result_dfs = []
    while next_flag:
        print("page :{}".format(str(page_i)))
        time.sleep(0.5)
        df = finviz_features(url_template, page_i)
        df = df[df['Industry'].map(lambda x: x != "Shell Companies")]  # excluding shell companies SPAC
        df = df[df['Company'].map(lambda x: "fund" not in x.lower())]  # excluding fund
        df = convert_numeric(df, num_cols)
        if df.shape[0] > 0:
            result_dfs.append(df)
            if df.shape[0] < 20:
                next_flag = False
            page_i = page_i + 20
            if page_i_max is not None:
                if page_i >= page_i_max:
                    next_flag = False
        else:
            next_flag = False
    result = pd.concat(result_dfs, ignore_index=True)
    return result


def test_build_magic_formula_universe():
    actual = finviz_features(finviz_url_template)
    logging.info(actual.dtypes)
    actual_numeric = convert_numeric(actual, finviz_numeric_cols)
    logging.info(actual_numeric.dtypes)
    multiple_pages = build_magic_formula_universe(finviz_url_template, finviz_numeric_cols, 60)
    logging.info(multiple_pages.dtypes)
    logging.info(multiple_pages.head(5).to_dict("records"))
    logging.info(multiple_pages.shape)
    logging.info(len(multiple_pages.Company.unique()))
    assert len(multiple_pages.Company.unique()) == 60


# python -m pytest universe/us_magic_formula_universe.py::test_build_magic_formula_universe --log-cli-level=INFO


def get_magic_candidates(df, entry_cnt):
    df["Fwd_Adj_E"] = df["P/E"] / df["Fwd P/E"]
    df["Fwd_ROA"] = df["ROA"] * df["Fwd_Adj_E"]
    df = df[df["EPS this Y"].map(lambda x: x > 0)]
    df = df[df["P/E"].map(lambda x: x < 50)]
    df = df[df["Fwd P/E"] < df["P/E"]]
    df["ASSET_TO_MK"] = 1 / (df["ROA"] * 0.01 * df["P/E"])
    df["EQUITY_TO_MK"] = 1 / (df["ROE"] * 0.01 * df["P/E"])
    df["LIABILITY_TO_MK"] = df["ASSET_TO_MK"] - df["EQUITY_TO_MK"]
    df["PROFIT_VALUE"] = df["Fwd P/E"] * (1 + df["LIABILITY_TO_MK"])
    df["PROFIT_VALUE_RANK"] = df["PROFIT_VALUE"].rank()
    df["FWD_ROA_RANK"] = df["Fwd_ROA"].rank(ascending=False)
    df["RANKSUM"] = df["PROFIT_VALUE_RANK"] + df["FWD_ROA_RANK"]
    df["AMT"] = df["Avg Volume"] * df["Price"]
    df["RANKSUM_RANK"] = df["RANKSUM"].rank(method="first")
    df = df.sort_values(by="RANKSUM_RANK").head(entry_cnt * 8)
    df["AMT_RANK"] = df["AMT"].rank(method="first")
    value_universe = df.sort_values(by="AMT_RANK").head(entry_cnt)
    return (value_universe[
                ["Ticker", "P/E", "Fwd P/E", "PROFIT_VALUE", "RANKSUM", "AMT", "Price", "ROE", "ROA",
                 "Fwd_ROA", "P/B", "Avg Volume", "Volume",
                 "Sector", 'Target Price']], df)
