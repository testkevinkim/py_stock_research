import sys
import platform

if "darwin" in platform.system().lower():
    # mac
    sys.path.append("/Users/KXK2ZO/py_stock_research")
else:  # linux
    sys.path.append("/home/py_stock_research")

from research.ca.data_collect import config, ops
from rank_selection_main import utils

email_cred = utils.read_json_to_dict(config.email_path)
config.email_cred = email_cred

if __name__ == "__main__":
    logger = utils.init_logger(config.log_path)
    ops.main(config)
