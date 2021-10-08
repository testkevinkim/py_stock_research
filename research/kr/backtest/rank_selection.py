import pandas as pd
from research.kr.config import config
import os

root_path = config.root_path
price_path = os.path.join(root_path, "price.parquet")
financials_path = os.path.join(root_path, "financials.parquet")
universe_path = os.path.join(root_path, "universe.parquet")
