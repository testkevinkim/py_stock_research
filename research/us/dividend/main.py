import logging
import sys
import platform

if "darwin" in platform.system().lower():
    # mac
    sys.path.append("/Users/KXK2ZO/py_stock_research")
else:  # linux
    sys.path.append("/home/py_stock_research")

from research.us.dividend import ops, config
from rank_selection_main import utils

email_cred = utils.read_json_to_dict(config.email_path)
config.email_cred = email_cred

if __name__ == "__main__":
    logger = utils.init_logger(config.log_path)
    try:
        universe = list(set(ops.download_current_dividend_info().TICKER.unique()))
        current_date = utils.local_date(config.tz_name)
        target = ops.get_dividend_target_price(universe, config.target_yield, config.min_history_year)
        current_price = ops.get_current_price(list(set(target.TICKER.unique())))
        target_price = target.merge(current_price, on="TICKER", how="inner")
        target_price["DATE"] = current_date
        target_price["PRICE_LOC_PERC"] = (target_price["CURRENT_PRICE"] / target_price["TARGET_PRICE"] - 1) * 100
        target_price = target_price.sort_values(by="PRICE_LOC_PERC")
        utils.send_email_with_df("us dividend target price report - lower PRICE_LOC_PERC is closer to target price",
                                 config.email_cred, target_price)
        all_target_price = ops.save_entry(config.reco_path, target_price)
    except Exception as e:
        logging.error(e, exc_info=True)
        utils.send_status_email("us dividend target price report - has error, it should be trigger every month",
                                config.email_cred)
