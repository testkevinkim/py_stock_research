# universe: ticker, shares, marketcap = shares * adj price
# price : adj and unadj - from universe
# financial - from universe

import pandas as pd
import logging
import os
from research.kr.utils import marketcap_dataprep, unadj_day_price_dataprep, financial_per_share_dataprep
from research.kr.config import config
from rank_selection_main import utils, config as rank_selection_main_config

root_path = config.root_path
logger = utils.init_logger(os.path.join(root_path, "kr_research_data_prep.log"))
universe_path = os.path.join(root_path, "universe.parquet")
price_path = os.path.join(root_path, "price.parquet")
financials_path = os.path.join(root_path, "financials.parquet")

max_page = 3
price_datacount = 2000  # days
if max_page:
    logging.info("current universe max page = {}, this should be None for all universe".format(str(max_page)))

universe = marketcap_dataprep.get_all_marketcap_shares(max_page)
universe.to_parquet(universe_path)
logging.info("market shares downloaded, total size = {}".format(str(universe.shape[0])))

tickers = list(universe.TICKER.unique())
logging.info("total tickers for price = {}".format(str(len(tickers))))
prices, fails = unadj_day_price_dataprep.get_adj_unadj_day_price(tickers, price_datacount)
prices.to_parquet(price_path)
logging.info("price downloaded")

financials = financial_per_share_dataprep.per_share_download(tickers)
logging.info("financials total ticker count = {}".format(str(len(financials.TICKER.unique()))))
logging.info("financials downloaded")
financials.to_parquet(financials_path)

min_ticker_count = min(len(universe.TICKER.unique()), len(prices.TICKER.unique()), len(financials.TICKER.unique()))
utils.send_status_email(
    "kr - universe, shares, price, financials downloaded - total ticker count ={}".format(str(min_ticker_count)),
    cred_dict=utils.read_json_to_dict(rank_selection_main_config.email_path))
