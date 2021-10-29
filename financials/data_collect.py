import pandas as pd
from financials import universe as universe_page
from financials import us_financials_from_macrotrends
import logging
import os
import time


def download_shares_outstanding(universe_var):
    universe_list = universe_var.universe_str.split(",")
    logging.info(universe_list)

    shares = []
    failed = []
    root_path = "/Users/KXK2ZO/minor_research/R/jupyter_R/us_study/data/"
    universe_len = len(universe_list)
    logging.info(("universe size ", universe_len))
    for i, t in enumerate(universe_list):
        if i % 100 == 1:
            time.sleep(1)
        else:
            try:
                temp = us_financials_from_macrotrends.get_shares_outstanding(t)
                shares.append(temp)
                logging.info((i, universe_len - i, t, temp.shape[0]))
            except Exception as e:
                logging.error(e, exc_info=True)
                failed.append(t)

    all_shares = pd.concat(shares, ignore_index=True)
    # all_shares.to_json(os.path.join(root_path, "us_shares_outstanding.json"))
    # all_shares.to_csv(os.path.join(root_path, "us_shares_outstanding.csv"))
    logging.info((len(all_shares.TICKER.unique()), "saved"))
    pass


def test_download_shares_outstanding():
    download_shares_outstanding(universe_page)

# python -m pytest /Users/KXK2ZO/py_stock_research/financials/data_collect.py::test_download_shares_outstanding --log-cli-level=INFO