import pandas as pd
import requests
import time
import logging

# current template : midcap+ and ipo years >= 10
finviz_url_template = "https://finviz.com/screener.ashx?v=152&f=cap_smallover,fa_pe_u50,geo_usa,ind_stocksonly,ipodate_more10,sh_curvol_o50,sh_price_o2&ft=4&o=-marketcap&r={}&c=0,1,2,3,4,6,7,8,11,16,17,21,39,52,53,54,57,58,62,63,65,67,69,70"

finviz_numeric_cols = ['Market Cap',
                       'P/E',
                       'Fwd P/E',
                       'P/B',
                       'EPS',
                       'EPS this Y',
                       'Sales past 5Y', 'Gross M', 'SMA20', 'SMA50', 'SMA200',
                       '52W High', '52W Low', 'Recom',
                       'Avg Volume',
                       'Price',
                       'Volume',
                       'Target Price', ]


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


def build_universe(url_template, num_cols, page_i_max=None) -> pd.DataFrame:
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


def test_finviz_features():
    actual = finviz_features(finviz_url_template)
    logging.info(actual.dtypes)
    actual_numeric = convert_numeric(actual, finviz_numeric_cols)
    logging.info(actual_numeric.dtypes)
    multiple_pages = build_universe(finviz_url_template, finviz_numeric_cols, 60)
    logging.info(multiple_pages.dtypes)
    logging.info(multiple_pages.head(5).to_dict("records"))
    logging.info(multiple_pages.shape)
    logging.info(multiple_pages.Company.unique())
