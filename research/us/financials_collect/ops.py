# shares outstanding
import pandas as pd
import json
import requests

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}


def get_eps_ttm(ticker):
    htmltext = requests.get("https://ycharts.com/companies/{ticker}/eps_ttm".format(ticker=ticker.upper()),
                            headers=headers).text
    dfs = pd.read_html(htmltext)
    dfs = dfs[:2]
    dfs_list = []
    for df in dfs:
        temp = df
        temp.columns = ["DATE", "EPS_TTM"]
        dfs_list.append(temp)

    alldf = pd.concat(dfs_list, ignore_index=True)
    alldf["TICKER"] = ticker
    return alldf


def get_eps_forecast_from_yahoo(ticker):
    url_template = "https://finance.yahoo.com/quote/{}/analysis?p={}".format(str(ticker), str(ticker))
    htmltext = requests.get(url_template, headers=headers).text
    dfs = pd.read_html(htmltext)
    eps_df = dfs[0]
    return {
        "forecast_eps_current_year": [v for k, v in
                         eps_df[eps_df["Earnings Estimate"].map(lambda x: x == "Avg. Estimate")].to_dict("records")[
                             0].items() if "Next Year" in k][0],
        "forecast_eps_next_year": [v for k, v in
                                      eps_df[eps_df["Earnings Estimate"].map(lambda x: x == "Avg. Estimate")].to_dict("records")[
                                          0].items() if "Current Year" in k][0],
        "analyst_count": int([v for k, v in
                              eps_df[eps_df["Earnings Estimate"].map(lambda x: x == "No. of Analysts")].to_dict(
                                  "records")[0].items() if "Current Year" in k][0])
    }
