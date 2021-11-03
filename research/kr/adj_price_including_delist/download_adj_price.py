from rank_selection_main import utils
import pandas as pd
import os
import logging

root_path = "/Users/KXK2ZO/minor_research/R/jupyter_R/kr_study/data/backtest_data/"
universe = [str(x).zfill(6) for x in list(pd.read_csv(os.path.join(root_path, "kr_all_universe.csv")).TICKER.unique())]

def test_show_universe():
    logging.info(("universe cnt", len(universe)))

# python -m pytest research/kr/adj_price_including_delist/download_adj_price.py::test_show_universe --log-cli-level=INFO

def get_adj_price(universe_list):
    history = utils.kr_download_history(ticker_list=universe_list, pdays=3000)
    write_path = os.path.join(root_path, "kr_adj_price.json")
    history.to_json(write_path)
    logging.info("saved to {}".format(write_path))
    return list(history.TICKER.unique())

def test_get_adj_price():
    result = get_adj_price(universe)
    assert len(result) > 1000

# python -m pytest research/kr/adj_price_including_delist/download_adj_price.py::test_get_adj_price --log-cli-level=INFO
# history = utils.kr_download_history(ticker_list=universe, pdays=3000, print_log=True)
# write_path = os.path.join(root_path, "kr_adj_price.json")
# history.to_json(write_path)
