import os
import platform

# path
if "darwin" in platform.system().lower():
    # mac
    root_path = "/Users/KXK2ZO/py_stock_research_test"
else:  # linux
    root_path = "/home/py_stock_research_test"

override = False
ex_name = "nyse"
test_universe = None  # ["FB", "AAPL", "MMM"]
tz_name = "US/Eastern"
log_path = os.path.join(root_path, "{}us_bid_ask_collect.log".format("test_" if override else ""))
email_path = os.path.join(root_path, "gmail.json")

first_capture_time = "16:00:00"
second_capture_time = "17:00:00"

yaml_token_path = os.path.join(root_path, "token.yaml")
access_token_path = os.path.join(root_path, "access_token.yml")

entry_path = os.path.join(root_path, "{}us_bid_ask_collect_entry.json".format("test_" if override else ""))
report_path = os.path.join(root_path, "{}us_bid_ask_collect_report.json".format("test_" if override else ""))

entry_cnt = 50
report_entry_cnt = 5
exit_ndays = 1
