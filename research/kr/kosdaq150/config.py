import os
import platform

# path
if "darwin" in platform.system().lower():
    # mac
    root_path = "/Users/KXK2ZO/py_stock_research_test"
else:  # linux
    root_path = "/home/py_stock_research_test"

from rank_selection_main import utils

override = True # must be False for prod
tz_name = "Asia/Seoul"
ex_name = "krx"
log_path = os.path.join(root_path, "{}kr_eps_change.log".format("test_" if override else ""))
email_path = os.path.join(root_path, "gmail.json")
universe_max_page = 6 if override else 6
eps_fy = 2
esp_only_flag = True

if override:
    first_snapshot_time = utils.local_time(tz_name)
else:
    first_snapshot_time = "15:10:00"

entry_path = os.path.join(root_path, "{}kr_eps_change_entry.json".format("test_" if override else ""))
report_path = os.path.join(root_path, "{}kr_eps_change_report.json".format("test_" if override else ""))
history_path = os.path.join(root_path, "{}kr_eps_change_history.json".format("test_" if override else ""))

entry_cnt_per_idea = 10
exit_ndays = 10
fee_perc = 0.004
min_amt = 2
min_face = 1000