import sys
import platform

if "darwin" in platform.system().lower():
    # mac
    sys.path.append("/Users/KXK2ZO/py_stock_research")
else:  # linux
    sys.path.append("/home/py_stock_research")

from research.us.data_collect import config, get_candidate
from rank_selection_main import utils

if __name__ == "__main__":
    logger = utils.init_logger(config.log_path)
    get_candidate.main(config)
