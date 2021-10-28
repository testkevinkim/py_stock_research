import os
import platform

# path
if "darwin" in platform.system().lower():
    # mac
    root_path = "/Users/KXK2ZO/py_stock_research_test"
else:  # linux
    root_path = "/home/py_stock_research_test"

from rank_selection_main import utils

override = False # must be False for prod
tz_name = "Asia/Seoul"
ex_name = "krx"
log_path = os.path.join(root_path, "{}kr_bid_ask_collect.log".format("test_" if override else ""))
email_path = os.path.join(root_path, "gmail.json")
universe_max_page = 1 if override else None

if override:
    first_snapshot_time = utils.local_time(tz_name)
    snapshot_interval_seconds = 5
else:
    first_snapshot_time = "14:20:00"
    snapshot_interval_seconds = 60 * 60  # 1 hour


# bid ask capture after 2nd snapshot ends + 20 minutes delay

entry_path = os.path.join(root_path, "{}kr_bid_ask_collect_entry.json".format("test_" if override else ""))
report_path = os.path.join(root_path, "{}kr_bid_ask_collect_report.json".format("test_" if override else ""))

entry_cnt = 20
exit_ndays = 4
first_exclusion_cnt = 5
