import pandas as pd
import os
from datetime import datetime
import pytz
import logging


est_tz = pytz.timezone("EST")
input_today_date = datetime.now(est_tz).strftime("%Y-%m-%d")
input_today_time = datetime.now(est_tz).strftime("%H:%M:%S")
input_entry_cnt = 10
input_root_path = "/Users/KXK2ZO/minor_research/R/jupyter_R/us_study/data"
input_universe_path = os.path.join(input_root_path, "us_universe.json")
input_write_path = os.path.join(input_root_path, "us_idea_candidates.json")
input_col_match_dict = {"FACE": "Price",
                        "DIS": "SMA50",
                        "PBR": "P/B",
                        "AMT": "Amount",
                        }
input_conditions = {"face_down_dis_up": [("FACE", 2, "DOWN"), ("DIS", 1, "UP")],
                    "pbr_down_face_down": [("PBR", 2, "DOWN"), ("FACE", 1, "DOWN")],
                    "face_down_pbr_down_dis_up": [("FACE", 4, "DOWN"), ("PBR", 2, "DOWN"), ("DIS", 1, "UP")],
                    "pbr_down_face_down_dis_up": [("PBR", 4, "DOWN"), ("FACE", 2, "DOWN"), ("DIS", 1, "UP")],
                    "face_down_pbr_down": [("FACE", 2, "DOWN"), ("PBR", 1, "DOWN")],
                    "face_down_amt_down": [("FACE", 2, "DOWN"), ("AMT", 1, "DOWN")],
                    "face_down_dis_up_pbr_down": [("FACE", 4, "DOWN"), ("DIS", 2, "UP"), ("PBR", 1, "DOWN")],
                    "face_down_amt_down_dis_up": [("FACE", 4, "DOWN"), ("AMT", 2, "DOWN"), ("DIS", 1, "UP")],
                    "pbr_down_dis_up_face_down": [("PBR", 4, "DOWN"), ("DIS", 2, "UP"), ("FACE", 1, "DOWN")],
                    "face_down_dis_up_amt_down": [("FACE", 4, "DOWN"), ("DIS", 2, "UP"), ("AMT", 1, "DOWN")]}


def rank_selection(universe_path, result_path, col_match_dict, conditions, entry_cnt, today_date, today_time):
    def selection(dt, col_name, multiple, entry_count, direction="DOWN"):
        dt = dt.copy()
        temp = dt.sort_values(by=col_name)
        if direction == "DOWN":
            return temp.head(entry_count * multiple)
        else:
            return temp.tail(entry_count * multiple)

    df = pd.read_json(universe_path)
    df["Amount"] = df["Price"] * df["Volume"]

    candidates = []
    for k, v in conditions.items():
        temp_df = df.copy()
        for vi in v:
            temp_df = selection(temp_df, col_match_dict[vi[0]], vi[1], entry_cnt, vi[2])
            temp_df["IDEA"] = k
        candidates.append(temp_df[["IDEA", "Ticker", "Price"]].rename(columns={"Ticker": "TICKER", "Price": "PRICE"}))

    candidates_df = pd.concat(candidates, ignore_index=True)
    candidates_df["DATE"] = today_date
    candidates_df["TIME"] = today_time

    candidates_path = result_path
    if os.path.exists(candidates_path):
        current_df = pd.read_json(candidates_path, convert_dates=False)
        candidates_df_comb = pd.concat([current_df, candidates_df], ignore_index=True)
    else:
        candidates_df_comb = candidates_df
    candidates_df_comb.to_json(candidates_path)
    logging.info("new candidates saved to {}".format(candidates_path))
    return candidates_df # new candidates


def test_rank_selection():
    test_write_path = os.path.join("/Users/KXK2ZO/minor_research/R/jupyter_R/us_study/data/test",
                                   "us_idea_candidates.json")
    test_universe_path = os.path.join("/Users/KXK2ZO/minor_research/R/jupyter_R/us_study/data", "us_universe.json")
    if os.path.exists(test_write_path):
        os.remove(test_write_path)
    actual = rank_selection(test_universe_path, test_write_path, input_col_match_dict, input_conditions,
                            input_entry_cnt, input_today_date, input_today_time)
    logging.info(actual.dtypes)
    logging.info("result size : {}".format(str(actual.shape[0])))
    assert actual.shape[0] == input_entry_cnt * len(input_conditions)
