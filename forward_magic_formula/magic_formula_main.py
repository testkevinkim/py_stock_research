import sys
import platform

if "darwin" in platform.system().lower():
    # mac
    sys.path.append("/Users/KXK2ZO/py_stock_research")
else:  # linux
    sys.path.append("/home/py_stock_research")

from forward_magic_formula import config
from universe import us_magic_formula_universe
from rank_selection_main import utils
import os
import logging
import pandas as pd
from forward_magic_formula import ops

email_cred = utils.read_json_to_dict(config.email_path)


def main(override=False, universe_max_page=None):
    try:
        if utils.is_market_open(override, tz_name=utils.tz_dict["US"], ex_name="nyse"):
            logging.info("main start - market is open today")
            all_universe = us_magic_formula_universe.build_magic_formula_universe(
                us_magic_formula_universe.finviz_url_template,
                us_magic_formula_universe.finviz_numeric_cols, universe_max_page)
            selected, all_info = us_magic_formula_universe.get_magic_candidates(all_universe, config.entry_cnt)
            all_info["DATE"] = utils.local_date(config.tz_name)
            all_full = utils.save_entry(config.entry_path, all_info)
            logging.info(("all dates", sorted(list(all_full.DATE.unique()))))
            logging.info(selected)

            if len(all_full.DATE.unique()) > 1:
                report = ops.get_report_ready(selected, config.tz_name, config.exit_ndays)
                utils.send_email_with_df("forward magic formula report", email_cred, report)
            else:
                logging.info("too short entry to build report")

            utils.send_email_with_df("forward magic formula candidates - {}".format(str(config.entry_cnt)), email_cred,
                                     selected)
            logging.info("email sent")
        else:
            logging.info("market closed")
    except Exception as e:
        logging.error(e, exc_info=True)
        utils.send_status_email("forward magic formula has error - check finviz url", email_cred)
    pass


if __name__ == "__main__":
    logger = utils.init_logger(config.log_path)
    main(config.override, config.universe_max_page)
    logging.info("main finished")
