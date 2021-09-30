import os
import platform

# path
if "darwin" in platform.system().lower():
    # mac
    root_path = "/Users/KXK2ZO/py_stock_research_test"
else:  # linux
    root_path = "/home/py_stock_research_test"

log_path = os.path.join(root_path, "log.log")
email_path = os.path.join(root_path, "gmail.json")
universe_path = os.path.join(root_path, "universe.json")
entry_path = os.path.join(root_path, "entry.json")
history_path = os.path.join(root_path, "history.json")
report_path = os.path.join(root_path, "report.json")

test_entry_path = os.path.join(root_path, "test_entry.json")
test_universe_path = os.path.join(root_path, "test_universe.json")
test_history_path = os.path.join(root_path, "test_history.json")
test_report_path = os.path.join(root_path, "test_report.json")

exit_ndays = 10
entry_count = 10
fee_perc = 0.001
