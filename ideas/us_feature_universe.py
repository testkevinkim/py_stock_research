from universe.us_universe_from_finviz import build_universe, finviz_url_template, finviz_numeric_cols
import pandas as pd
from datetime import datetime
import os
from universe.us_bad_stocks import get_bad_stocks
import logging

input_root_path = "/Users/KXK2ZO/minor_research/R/jupyter_R/us_study/data"


def get_us_features(root_path, page_max=None):
    """
    from finviz, download universe with features
    downloaded universe would be saved as json file in main operation

    :param root_path:
    :param page_max:
    :return:
    """
    start_time = datetime.utcnow()
    universe = build_universe(finviz_url_template, finviz_numeric_cols, page_max)
    logging.info("universe size before bad stocks = {}".format(len(universe.Ticker.unique())))
    bad_stocks = get_bad_stocks()
    logging.info("bad stocks cnt = {}".format(str(len(bad_stocks))))
    good_universe = universe[universe["Ticker"].map(lambda x: x not in bad_stocks)]
    logging.info("universe size after bad stocks = {}".format(str(len(good_universe.Ticker.unique()))))
    good_universe.to_json(os.path.join(root_path, "us_universe.json"))
    logging.info("download run time: {}".format(str(datetime.utcnow() - start_time)))
    return good_universe


def test_get_us_features():
    test_input_root_path = "/Users/KXK2ZO/minor_research/R/jupyter_R/us_study/data/test"
    test_page_max = 40
    actual = get_us_features(test_input_root_path, test_page_max)
    assert actual.shape[0] >= test_page_max * 0.5
