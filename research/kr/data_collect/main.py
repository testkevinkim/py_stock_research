import sys
import platform

if "darwin" in platform.system().lower():
    # mac
    sys.path.append("/Users/KXK2ZO/py_stock_research")
else:  # linux
    sys.path.append("/home/py_stock_research")

from research.kr.data_collect import config, ops
from rank_selection_main import utils

email_cred = utils.read_json_to_dict(config.email_path)
config.email_cred = email_cred

if __name__ == "__main__":
    logger = utils.init_logger(config.log_path)
    utils.is_market_open(config.override, config.tz_name, config.ex_name)
    ops.main(config)
