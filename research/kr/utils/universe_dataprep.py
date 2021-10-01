import pandas as pd
import logging
from datetime import datetime


def get_universe():
    start_time = datetime.now()
    url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13'
    universe = pd.read_html(url)[0]
    universe = universe.iloc[:, [1]]
    universe.columns = ['ticker']
    universe_list = [str(x).zfill(6) for x in list(universe.ticker.unique())]
    end_time = datetime.now()
    run_time = end_time - start_time
    logging.info("run time : {}".format(str(run_time)))
    return universe_list


def test_get_universe():
    actual = get_universe()
    logging.info(len(actual))
    logging.info(actual[:10])
