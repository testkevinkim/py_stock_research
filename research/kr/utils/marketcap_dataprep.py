import pandas as pd
import requests
import logging
import time


def get_ticker_name(parsed_str):
    return [x.split('</a></td>\n\t\t\t\t\t<td class="number">')[0] for x in
            parsed_str.split('href=')[1].split('main.naver?code=')[1].split('" class="tltle">')]


def get_current_marketcap_shares(url):
    name_ticker_feature = None
    temp = pd.read_html(url, encoding='euc-kr')[1].iloc[1:, [1, 2, 6, 7, 8, 9, 10, 11]]
    temp.columns = ["NAME", "PRICE", "MARKETCAP", "SHARES", "FOREIGN", "VOLUME", "PER", "ROE"]
    feature = temp[temp["NAME"].map(lambda x: pd.notnull(x))]
    # logging.info(feature)
    # logging.info(feature.shape)
    # logging.info(feature.dtypes)
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


def test_get_curren_marketcap_shares():
    test_url = "https://finance.naver.com/sise/sise_market_sum.naver?sosok=0&page=1"
    actual = get_current_marketcap_shares(test_url)
    logging.info(actual.head())


def get_all_marketcap_shares(max_page=None):
    urls = {"kospi": "https://finance.naver.com/sise/sise_market_sum.naver?sosok=0&page={}",
            "kosdaq": "https://finance.naver.com/sise/sise_market_sum.naver?sosok=1&page={}"}
    result = []
    for k, v in urls.items():
        counter = 1
        op_url = v.format(str(counter))
        temp = get_current_marketcap_shares(op_url)
        continue_flag = True
        while temp is not None and temp.shape[0] >= 10 and continue_flag:
            logging.info("{} page = {}".format(k, str(counter)))
            logging.info("url = {}".format(v.format(str(counter))))
            temp["MARKET"] = k
            result.append(temp)
            counter += 1
            temp = get_current_marketcap_shares(v.format(str(counter)))
            time.sleep(0.1)
            if max_page:
                if counter >= max_page:
                    continue_flag = False
                    logging.info("early stop because of max page")

    return pd.concat(result, ignore_index=True)


def test_get_all_marketcap_shares():
    actual = get_all_marketcap_shares(2)
    logging.info(actual.dtypes)
    logging.info(actual.head())
    logging.info(len(actual.TICKER.unique()))
