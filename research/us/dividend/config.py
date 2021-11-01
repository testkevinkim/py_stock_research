import os
import platform

# path
if "darwin" in platform.system().lower():
    # mac
    root_path = "/Users/KXK2ZO/py_stock_research_test"
else:  # linux
    root_path = "/home/py_stock_research_test"

override = False # should be False for prod
ex_name = "nyse"
tz_name = "US/Eastern"
log_path = os.path.join(root_path, "{}us_dividend_target.log".format("test_" if override else ""))
email_path = os.path.join(root_path, "gmail.json")
reco_path = os.path.join(root_path, "{}us_dividend_reco.json".format("test_" if override else ""))
target_yield = 0.2
min_history_year = 10