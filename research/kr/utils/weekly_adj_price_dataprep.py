import pandas as pd
import requests
import time
import logging


def get_history(ticker, data_count, interval="day"):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    url_template = "https://fchart.stock.naver.com/sise.nhn?symbol={ticker}&timeframe={interval}&count={datacount}&requestType=0"
    df = None
    try:
        url = url_template.format(ticker=ticker, interval=interval, datacount=str(data_count))
        res = requests.get(url, headers=headers)
        # data parser
        text = [x.replace('"', '') for x in res.text.split("item data=")[1:]]
        text = [x.replace(' />\n\t\t\t\n\t\t\t\t<', '') for x in text]
        text = [x.replace(' />\n\t\t\t\n\t\n\n\n\t</chartdata>\n</protocol>\n', '') for x in text]
        text_split = [x.split("|") for x in text]
        cols = ["DATE", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]
        df = pd.DataFrame(text_split, columns=cols)
        df['DATE'] = df['DATE'].apply(lambda x: "-".join([x[:4], x[4:6], x[6:]]))
        num_cols = cols[1:]
        df[num_cols] = df[num_cols].apply(pd.to_numeric, errors='coerce')
        df['TICKER'] = ticker
    except Exception as e:
        print("{} has error at history download".format(ticker))
        print(str(e))
    time.sleep(0.5)
    return df


def test_get_history():
    test_ticker = "005930"
    test_datacount = 10
    test_interval = "week"
    actual = get_history(test_ticker, test_datacount, test_interval)
    logging.info(("history size", actual.shape[0]))
    logging.info("\n" + actual.head().to_string())


def all_history(ticker_list, datacount, interval="day"):
    if "." in ticker_list[0]:
        # parsing from yahoo ticker format such as 005930.KS -> just 005930 (6 digit string)
        ticker_list = [str(x.split(".")[0]).zfill(6) for x in ticker_list]
    dfs = [get_history(x, datacount, interval) for x in ticker_list]
    dfs_valid = [x for x in dfs if x is not None]
    df_combined = pd.concat(dfs_valid, ignore_index=True)
    return df_combined


def test_all_history():
    test_tickers = ["005930", "000660"]
    test_interval = "week"
    test_datacount = 5
    actual = all_history(test_tickers, test_datacount, test_interval)
    logging.info(("all history", actual.shape[0], actual.dtypes, actual.TICKER.unique()))
