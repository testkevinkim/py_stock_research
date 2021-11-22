# kosdqa 150 (but 200 tickers) universe

# download past 12 month concensus EPS

# historical price : adj and unadj

# marketcap, EPS change 1 month (FY1, FY2), amt, face

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
import pandas as pd
import requests
import logging
from research.kr.utils import marketcap_dataprep, unadj_day_price_dataprep


def get_ticker_name(parsed_str):
    return [x.split('</a></td>\n\t\t\t\t\t<td class="number">')[0] for x in
            parsed_str.split('href=')[1].split('main.naver?code=')[1].split('" class="tltle">')]


def get_current_marketcap_shares(url):
    name_ticker_feature = None
    temp = pd.read_html(url, encoding='euc-kr')[1].iloc[1:, [1, 2, 6, 7, 8, 9, 10, 11]]
    temp.columns = ["NAME", "PRICE", "MARKETCAP", "SHARES", "FOREIGN", "VOLUME", "PER", "ROE"]
    feature = temp[temp["NAME"].map(lambda x: pd.notnull(x))]
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


def get_kosdaq150_universe(max_page=1):
    urls = "https://finance.naver.com/sise/sise_market_sum.naver?sosok=1&page={}"
    result = []
    counter = 0
    while counter < max_page:
        counter += 1
        op_url = urls.format(str(counter))
        temp = get_current_marketcap_shares(op_url)
        result.append(temp)

    return pd.concat(result, ignore_index=True)


def get_past_12_month_consensus(ticker, fy):
    options = webdriver.ChromeOptions()
    options.headless = True
    # service = Service("/Users/KXK2ZO/Desktop/Delete/selenium/chromedriver")
    driver = webdriver.Chrome(executable_path="/Users/KXK2ZO/Desktop/Delete/selenium/chromedriver",
                              options=options)
    target_url_template = "https://comp.fnguide.com/SVO2/common/chartListPopup2.asp?oid=div2_img&cid=07_02&gicode=A{ticker}&filter=D&term=Y&etc=FY{fy}&etc2=4&titleTxt=undefined&dateTxt=undefined&unitTxt="
    target_url = target_url_template.format(ticker=str(ticker).zfill(6), fy=str(fy))
    driver.get(target_url)
    driver.implicitly_wait(1)
    table = driver.find_elements(By.XPATH, '//*[@id="chartDataGrid"]/table')
    result = table[0].text.split('\n')
    yrmo = [x for x in result if "/" in x]
    consensus = [x.split(" ")[-1] for x in result if "." in x and " " in x]
    comb = pd.DataFrame(zip(yrmo, consensus), columns=["YRMO", "CONSENSUS"])
    comb["YRMO"] = comb["YRMO"].map(lambda x: int(x.replace("/", "")))
    comb["CONSENSUS"] = comb["CONSENSUS"].map(lambda x: float(x.replace(",", "")))
    comb["FY"] = fy
    comb["TICKER"] = str(ticker).zfill(6)
    return comb


def get_all_consensus(universe, fy):
    logging.info(fy)
    results = []
    failed = []
    for i, t in enumerate(universe):

        try:
            temp = get_past_12_month_consensus(t, fy)
            if temp.shape[0] > 0:
                results.append(temp)
                logging.info("{}, {} consensus downloaded".format(str(i), t))
            else:
                logging.info("{}, {} consensus skipped because # of row = 0".format(str(i), t))
        except Exception as e:
            failed.append(t)
            logging.info(" {} skipped".format(t))
            logging.error(e, exc_info=True)
    logging.info("failed count = {}".format(str(len(failed))))
    return pd.concat(results, ignore_index=True)


# def test_universe():
#     universe = get_kosdaq150_universe(6)
#     universe.to_json("/Users/KXK2ZO/minor_research/R/jupyter_R/kr_study/data/kosdaq150_eps_consensus_universe.json")
#
#
# def test_get_all_consensus():
#     test_universe = list(get_kosdaq150_universe(6).TICKER.unique())
#     logging.info(len(test_universe))
#     actual = get_all_consensus(test_universe, 2)
#     actual.to_json(
#         "/Users/KXK2ZO/minor_research/R/jupyter_R/kr_study/data/kosdaq150_eps_consensus_past_12_month_fy2.json")
#     logging.info(len(actual.TICKER.unique()))
#     logging.info(actual.shape[0])
#     logging.info(actual.head())
#     pass


# def test_get_past_12_month_consensus():
#     actual = get_past_12_month_consensus("005930", 1)
#     logging.info(actual.head())
#     logging.info(actual.dtypes)
#     pass


# def test_get_kosdaq150_universe():
#     actual = get_kosdaq150_universe(4)
#     logging.info(len(actual.TICKER.unique()))
#     logging.info(actual.shape)
#     logging.info(actual.head())


# def test_price_download():
#     test_universe = list(get_kosdaq150_universe(6).TICKER.unique())
#     price_datacount = 1000
#     prices, fails = unadj_day_price_dataprep.get_adj_unadj_day_price(test_universe, price_datacount)
#     logging.info(len(prices.TICKER.unique()))
#     logging.info((prices.DATE.min(), prices.DATE.max()))
#     logging.info(prices.head())
#     logging.info(prices.tail())
#     prices.to_json("/Users/KXK2ZO/minor_research/R/jupyter_R/kr_study/data/kosdaq150_eps_consensus_price.json")

def get_wein_gigan(ticker, page_n):
    url_template = "https://finance.naver.com/item/frgn.naver?code={}&page={}"
    url = url_template.format(str(ticker), str(page_n))
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

    htmltext = requests.get(url, headers=headers).text
    dfs = pd.read_html(htmltext)
    df = dfs[2]
    df.columns = ["DATE", "CLOSE", "CHANGE", "CHANGE_PERC", "VOLUME", "GIGAN", "WEIN", "HOLDING", "WEIN_PORTION"]
    df = df[df["DATE"].map(lambda x: not pd.isna(x))]
    df = df[["DATE", "CLOSE", "VOLUME", "GIGAN", "WEIN"]]
    df["TICKER"] = ticker
    return df


def test_get_wein_gigan():
    test_universe = list(get_kosdaq150_universe(6).TICKER.unique())
    logging.info(len(test_universe))
    test_pages = range(12, 53)
    # test_universe = ["377300", "089860"]
    # logging.info(test_universe)

    for i, t in enumerate(test_universe):
        gigan_wein_dfs = []
        min_date = None
        for j, p in enumerate(test_pages):
            try:
                temp = get_wein_gigan(t, p)
                if min_date == temp.DATE.min():
                    logging.info("ticker={}, page={} no more new data- stop".format(str(t), str(p)))
                    break
                gigan_wein_dfs.append(temp)
                logging.info("ticker={}, page={} captured".format(str(t), str(p)))
                min_date = temp.DATE.min()
            except Exception as e:
                logging.info("ticker={}, page={} skipped".format(str(t), str(p)))
                logging.error(e, exc_info=True)
        gigan_wein = pd.concat(gigan_wein_dfs, ignore_index=True)
        logging.info((gigan_wein.DATE.min(), gigan_wein.DATE.max(), gigan_wein.shape[0]))
        logging.info(gigan_wein.groupby("TICKER").agg(MINDATE=("DATE", "min")).reset_index())
        gigan_wein.to_json(
            "/Users/KXK2ZO/minor_research/R/jupyter_R/kr_study/data/gigan_wein_long/gigan_wein_{}.json".format(str(t)))
        logging.info("{}, ticker={} saved".format(str(i), str(t)))
    pass
