import sys
import platform

if "darwin" in platform.system().lower():
    # mac
    sys.path.append("/Users/KXK2ZO/py_stock_research")
else:  # linux
    sys.path.append("/home/py_stock_research")

# check market open
# entry: ideas/us_feature_universe.py -> ideas/us_rank_selection.py
# evaluation -> report -> email
import feed.us_yahoo_feed
from rank_selection_main import utils, config
from ideas import us_feature_universe, us_rank_selection
from feed import us_yahoo_history
from evaluation import us_gain_report
import os
import logging
import pandas as pd

email_cred = utils.read_json_to_dict(config.email_path)


def do_entry(entry_count, universe_path, entry_path, entry_date, entry_time, universe_max_page=None):
    """
    every day, find new rank-selection result from new universe result which is populated from finviz

    :param entry_count:
    :param universe_path:
    :param entry_path:
    :param entry_date:
    :param entry_time:
    :param universe_max_page:
    :return:
    """
    # entry at close
    entry_universe = us_feature_universe.build_universe(us_feature_universe.finviz_url_template,
                                                        us_feature_universe.finviz_numeric_cols,
                                                        universe_max_page)
    entry_universe.to_json(universe_path)
    candidates = us_rank_selection.rank_selection(universe_path=universe_path, result_path=entry_path,
                                                  col_match_dict=us_rank_selection.input_col_match_dict,
                                                  conditions=us_rank_selection.input_conditions,
                                                  entry_cnt=entry_count,
                                                  today_date=entry_date, today_time=entry_time)
    return candidates


def do_exit(entry, start_date, end_date, exit_ndays, fee_perc, history_path):
    """
    download history of entry universe from yahoo finance
    calculated gain

    :param entry:
    :param start_date:
    :param end_date:
    :param exit_ndays:
    :param fee_perc:
    :param history_path:
    :return:
    """
    report = None
    min_entry_date_count = exit_ndays + 10
    entry_date_count = len(entry.DATE.unique())
    if entry_date_count > min_entry_date_count:
        entry_tickers = list(entry.TICKER.unique())
        history = us_yahoo_history.get_yahoo_history(entry_tickers, start_date, end_date)
        history.to_json(history_path)
        logging.info("history saved to {}".format(history_path))
        report, entry_exit = us_gain_report.gain_report(entry, history, exit_ndays, fee_perc)
        logging.info("report generated")
    else:
        logging.info("report NOT generated, because entry size is too short")
        logging.info("entry date count = {}".format(str(entry_date_count)))
    return report


def main(override=False, universe_max_page=None):
    """
    do entry -> do exit
    build aggregation report from exit result
    then send performance aggregation (CAGR, MDD, etc) via email

    :param override:
    :param universe_max_page:
    :return:
    """
    if utils.is_market_open(override, tz_name=utils.tz_dict["US"], ex_name="nyse"):
        logging.info("main start - market is open today")
        today_date = utils.local_date(utils.tz_dict["US"])
        today_time = utils.local_time(utils.tz_dict["US"])
        # entry
        today_candidates = do_entry(config.entry_count, config.universe_path, config.entry_path,
                                    today_date, today_time, universe_max_page)
        # report
        whole_entry = pd.read_json(config.entry_path, convert_dates=False)
        logging.info("entry nrow= {}".format(str(whole_entry.shape[0])))
        logging.info("entry date count ={}".format(str(len(whole_entry.DATE.unique()))))
        logging.info("entry ticker count ={}".format(str(len(whole_entry.TICKER.unique()))))
        whole_entry_dates = sorted(list(whole_entry.DATE.unique()))
        start_date = whole_entry_dates[0]
        end_date = whole_entry_dates[-1]
        logging.info("start date: {}, end date: {} in history download".format(start_date, end_date))
        report_raw = do_exit(entry=whole_entry, start_date=start_date, end_date=end_date,
                             exit_ndays=config.exit_ndays,
                             fee_perc=config.fee_perc, history_path=config.history_path)
        if report_raw:
            report = us_gain_report.agg_performance(report_raw)
            if not override:
                utils.send_email_with_df("us_rank_selection ideas result", email_cred, report)
                logging.info("email report sent")
            else:
                logging.info("skip email report")
        else:
            logging.info("entry is too short to generate report")
            skip_report_subject = "us_rank_selection ideas: entry is too short to generate report - {} days"
            utils.send_status_email(skip_report_subject.format(str(len(whole_entry_dates))), email_cred,
                                    "weekend or holiday")
    else:
        utils.send_status_email("us_rank_selection ideas skipped - weekend or holiday", email_cred,
                                "weekend or holiday")


def test_main():
    main(override=True, universe_max_page=20)
    logging.info("main test finished")


# build report
def test_do_entry():
    test_max_page = 20
    new_candidates = do_entry(config.entry_count, config.test_universe_path, config.test_entry_path,
                              us_rank_selection.input_today_date, us_rank_selection.input_today_time,
                              test_max_page)
    assert os.path.exists(config.test_universe_path)
    assert os.path.exists(config.test_entry_path)
    logging.info(new_candidates.shape[0])
    logging.info(new_candidates.head())


def test_do_exit():
    sample_history = us_yahoo_history.get_yahoo_history(["AAPL"], start_date="2021-07-01", end_date="2021-09-01")
    dates = [str(pd.to_datetime(x))[:10] for x in sorted(list(sample_history.DATE.unique()))][:15]
    logging.info("dates")
    logging.info(dates)

    test_entry_count = 5
    test_max_page = 20
    test_exit_ndays = 3
    test_fee_perc = 0.0

    if os.path.exists(config.test_entry_path):
        os.remove(config.test_entry_path)
        logging.info("existing test entry removed")

    for d in dates:
        logging.info("new entry appended - {}".format(d))
        new_candidates = do_entry(test_entry_count, config.test_universe_path, config.test_entry_path,
                                  d, "00:00:00", test_max_page)

    test_entry = pd.read_json(config.test_entry_path, convert_dates=False)
    report_raw = do_exit(test_entry, min(dates), dates[-1], test_exit_ndays, test_fee_perc,
                         config.test_history_path)
    report_raw.to_json(config.test_report_path)
    report = us_gain_report.agg_performance(report_raw)
    logging.info("report saved to  {}".format(config.test_report_path))
    logging.info(report.head())
    logging.info(report.dtypes)
    utils.send_email_with_df("us_rank_selection ideas result - exit_ndays ={}".format(str(config.exit_ndays)),
                             email_cred, report)
    logging.info("email report sent")


if __name__ == "__main__":
    logger = utils.init_logger(config.log_path)
    main()
    logging.info("main finished")
