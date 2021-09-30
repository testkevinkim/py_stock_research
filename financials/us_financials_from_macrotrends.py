import pandas as pd
import requests
import time
import json
from bs4 import BeautifulSoup
import logging

statement_dict = {"balance": "balance-sheet", "income": "income-statement", "cash": "cash-flows-statement"}
requests_header = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest"
}


def read_financial(ticker, statement="balance-sheet"):
    # balance-sheet, income-statement, cash-flows-statement
    import requests
    url = "https://www.macrotrends.net/stocks/charts/{}/valhi/{}?freq=Q".format(ticker, statement)
    header = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest"
    }

    r = requests.get(url, headers=header)
    new_url = r.url + "?freq=Q"
    r = requests.get(new_url, headers=header)
    state_dict = json.loads(r.text.split("originalData = ")[1].split(";\r\n\r\n\r\n")[0])

    def get_account_name(state_dict_item):
        html = state_dict_item["field_name"]
        parsed_html = BeautifulSoup(html)
        return parsed_html.body.find('a').text if parsed_html.body.find('a') else parsed_html.body.find('span').text

    result_list = []
    for s in state_dict:
        account = get_account_name(s)
        del s["field_name"]
        del s["popup_icon"]
        for k, v in s.items():
            try:
                result_list.append(
                    {"TICKER": ticker, "ACCOUNT": account, "VALUE": float(v) if v != "" else 0.0, "DATE": k,
                     "REPORT": statement})
            except Exception as e:
                print(k, v)
                raise ValueError
    return pd.DataFrame(result_list)


def test_read_financial():
    test_ticker = "AAPL"
    actual_income = read_financial(test_ticker, statement_dict["income"])
    logging.info("{} - {}".format(test_ticker, statement_dict["income"]))
    logging.info(actual_income.dtypes)
    logging.info(actual_income.head().to_string())
    logging.info(actual_income.ACCOUNT.unique())


pe_pb_ratio_dict = {
    "pb": {"columns": ["DATE", "PB_PRICE", "BPS", "PBR"], "convert_col": "BPS", "url_post_fix": "price-book",
           "price_col": "PB_PRICE"},
    "pe": {"columns": ["DATE", "PE_PRICE", "EPS", "PER"], "convert_col": "EPS", "url_post_fix": "pe-ratio",
           "price_col": "PE_PRICE"}}


def get_px(ticker, ratio_dict=pe_pb_ratio_dict):
    keys = [k for k, v in ratio_dict.items()]
    results = {}
    for k in keys:  # pb, pe
        url = "https://www.macrotrends.net/stocks/charts/{}/valhi/{}".format(ticker, ratio_dict[k]["url_post_fix"])
        dfs = pd.read_html((requests.get(url, headers=requests_header)).text)
        result = dfs[0]
        result.columns = ratio_dict[k]["columns"]
        current_price = list(result[ratio_dict[k]["price_col"]])[0]
        result = result.dropna()
        result[ratio_dict[k]["convert_col"]] = (result[ratio_dict[k]["convert_col"]]
                                                .apply(lambda x: float(x.replace("$", "")))
                                                )
        results[k] = result
    # comb
    comb = pd.merge(results["pb"], results["pe"], on="DATE", how="inner")
    comb['TICKER'] = ticker
    comb["CURRENT_PRICE"] = current_price
    return comb


def test_get_px():
    test_ticker = "AAPL"
    actual = get_px(test_ticker, pe_pb_ratio_dict)
    logging.info(actual.dtypes)
    logging.info(actual.head().to_string())
    logging.info(actual.shape)
