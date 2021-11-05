import os
import platform

# path
if "darwin" in platform.system().lower():
    # mac
    root_path = "/Users/KXK2ZO/py_stock_research_test"
else:  # linux
    root_path = "/home/py_stock_research_test"

override = False  # should be False in prod
ex_name = "nyse"
exit_ndays = 20
universe_max_page = 100 if override else None
tz_name = "US/Eastern"
log_path = os.path.join(root_path, "{}us_forward_magic_formula.log".format("test_" if override else ""))
email_path = os.path.join(root_path, "gmail.json")
entry_path = os.path.join(root_path, "{}us_forward_magic_formula_entry.json".format("test_" if override else ""))
entry_cnt = 10
