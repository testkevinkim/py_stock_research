import pandas as pd
from feed import us_yahoo_history
from rank_selection_main import utils
import logging


def get_report_ready(entry: pd.DataFrame, tz_name, exit_ndays):
    if "Ticker" in entry.columns:
        entry = entry.rename(columns={"Ticker": "TICKER"})
    entry_universe = list(entry.TICKER.unique())
    entry_start_date = entry.DATE.min()
    history = us_yahoo_history.get_yahoo_history(entry_universe, entry_start_date,
                                                 utils.local_date(tz_name))
    history_reduced = history.copy(True)[["TICKER", "DATE", "OPEN"]]
    entry_reduced = entry[["TICKER", "DATE"]]
    history_rename = history.copy(True)[["TICKER", "DATE", "CLOSE"]].rename(columns={"CLOSE": "ENTRY_PRICE"})
    history_entry = history_rename.merge(entry_reduced, on=["TICKER", "DATE"], how="inner")
    report = utils.build_gain_report(history_entry, history_reduced, exit_ndays)
    logging.info(("report size", report.shape))
    return report


def test_get_report_ready():
    test_entry = pd.DataFrame(
        [("AAPL", "2021-11-01", 148.96), ("AAPL", "2021-11-02", 150.02), ("AAPL", "2021-11-03", 151.49)],
        columns=["Ticker", "DATE", "CLOSE"])
    test_tz_name = "US/Eastern"
    test_exit_ndays = 1
    report = get_report_ready(test_entry, test_tz_name, test_exit_ndays)
    logging.info(report)
