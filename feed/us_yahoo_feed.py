import requests
import logging


def yahoo_feed(ticker_list, feed_keys):
    tickers_str = ",".join(ticker_list)
    yqry_template = "https://query1.finance.yahoo.com/v7/finance/quote?symbols={tickers}".format(
        tickers=tickers_str)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    res = requests.get(url=yqry_template, headers=headers)
    res_json = res.json()
    list_dict = res_json['quoteResponse']['result']
    feed = []
    for item in list_dict:
        temp = {k: v for k, v in item.items() if k in feed_keys}
        feed.append(temp)
    return feed


def test_yahoo_feed():
    test_tickers = ["AAPL", "F"]
    actual = yahoo_feed(test_tickers, ["bid", "ask", "bidSize", "askSize", "tradeable", "symbol", "marketState"])
    logging.info(actual)
    pass
