import os
import platform

# path
if "darwin" in platform.system().lower():
    # mac
    root_path = "/Users/KXK2ZO/py_stock_research_test"
else:  # linux
    root_path = "/home/py_stock_research_test"

override = False
ex_name = "tsx"
test_universe = None  # ["FB", "AAPL", "MMM"]
tz_name = "US/Eastern"
log_path = os.path.join(root_path, "{}ca_bid_ask_collect.log".format("test_" if override else ""))
email_path = os.path.join(root_path, "gmail.json")

first_capture_time = "14:50:00"
second_capture_time = "15:50:00"

yaml_token_path = os.path.join(root_path, "token.yaml")
access_token_path = os.path.join(root_path, "access_token.yml")

entry_path = os.path.join(root_path, "{}ca_bid_ask_collect_entry.json".format("test_" if override else ""))
report_path = os.path.join(root_path, "{}ca_bid_ask_collect_report.json".format("test_" if override else ""))
history_path = os.path.join(root_path, "{}ca_bid_ask_collect_history.json".format("test_" if override else ""))

entry_cnt = 100
report_entry_cnt = 10
exit_ndays = 1
fee_perc = 0.001

min_price = 2
min_amt = 1
top_n = 500
post_market_flag = False
