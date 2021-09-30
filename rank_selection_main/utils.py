from datetime import datetime, timedelta
import time
from functools import reduce
import smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.utils import formatdate
import logging
from urllib.request import urlopen, Request
from typing import List, Dict
import os
from pytz import timezone
import json
import urllib
import requests
import pandas as pd
import numpy as np
import robin_stocks as r
import sys
import copy

tz_dict = dict(Korea="Asia/Seoul",
               Japan="Asia/Tokyo",
               Hongkong="Asia/Hong_Kong",
               Canada="US/Eastern",
               US="US/Eastern")


# us date time
def local_date(tz_name="Asia/Seoul"):
    """

    :param tz_name: Asia/Seoul Asia/Tokyo Asia/Hong_Kong US/Eastern
    :return:
    """
    today_str = datetime.now(timezone(tz_name)).strftime("%Y-%m-%d")
    # today_str = time.strftime("%Y-%m-%d")
    return today_str


def local_time(tz_name="Asia/Seoul"):
    time_str = datetime.now(timezone(tz_name)).strftime("%H:%M:%S")
    # time_str = time.strftime("%H:%M:%S")
    return time_str


def today_is_weekend(tz_name="Asia/Seoul"):
    weekdaysno = datetime.now(timezone(tz_name)).strftime("%w")
    if weekdaysno == '0' or weekdaysno == '6':
        return True
    else:
        return False


def today_is_holiday(tz_name="Asia/Seoul", ex_name="krx"):  # ex_name = krx jpx tsx hkex nyse
    # krx jpx tsx hkex nyse
    today = datetime.now(timezone(tz_name)).strftime("%B %d, %Y")
    this_year = datetime.now(timezone(tz_name)).strftime("%Y")
    url = "https://www.tradinghours.com/exchanges/{}/market-holidays/{}".format(ex_name, this_year)
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest"
    }
    r = requests.get(url, headers=headers, verify=False)
    calendar = pd.read_html(r.text)[0]
    holidays = list(calendar['Date'])
    selected = today in holidays
    return selected


def is_market_open(override=False, tz_name="Asia/Seoul", ex_name="krx"):
    if today_is_weekend(tz_name) or today_is_holiday(tz_name, ex_name):  # either weekend or holiday
        market_open = False
    else:
        market_open = True
    if override:
        market_open = override
    return market_open


def sleepby(seconds):
    time.sleep(seconds)
    pass


def send_status_email(subject, cred_dict, text="nothing"):
    status = "sent"
    try:
        send_from = "kevin.keunyoung.kim@gmail.com"
        send_to = "kevin.ky.kim@hotmail.com"
        msg = MIMEMultipart()
        msg['From'] = send_from
        msg['To'] = send_to
        msg['Date'] = formatdate(localtime=True)
        msg['Subject'] = subject
        server = "smtp.gmail.com"
        port = 465
        passwd = cred_dict['email_password']
        msg.attach(MIMEText(text))
        smtp = ssl.create_default_context()
        with smtplib.SMTP_SSL(host=server, port=port, context=smtp) as server:
            server.login(user=send_from, password=passwd)
            server.sendmail(from_addr=send_from, to_addrs=send_to, msg=msg.as_string())
        server.close()
        print("status email sent")
    except Exception as e:
        print("status email not sent because {}".format(str(e)))
        status = "not set"
    return status


def send_email_with_df(subject, cred_dict, df):
    status = "sent"
    try:
        send_from = "kevin.keunyoung.kim@gmail.com"
        send_to = "kevin.ky.kim@hotmail.com"
        msg = MIMEMultipart()
        msg['From'] = send_from
        msg['To'] = send_to
        msg['Date'] = formatdate(localtime=True)
        msg['Subject'] = subject
        server = "smtp.gmail.com"
        port = 465
        passwd = cred_dict['email_password']

        html = """\
        <html>
          <head></head>
          <body>
            {0}
          </body>
        </html>
        """.format(df.to_html())

        part1 = MIMEText(html, 'html')
        msg.attach(part1)

        smtp = ssl.create_default_context()
        with smtplib.SMTP_SSL(host=server, port=port, context=smtp) as server:
            server.login(user=send_from, password=passwd)
            server.sendmail(from_addr=send_from, to_addrs=send_to, msg=msg.as_string())
        server.close()
        print("email with dataframe sent")
    except Exception as e:
        print("email with dataframe not sent because {}".format(str(e)))
        status = "not sent"
    return status


def read_json_to_dict(json_path):
    with open(json_path) as f:
        dict = json.load(f)
    return dict


def init_logger(log_file_path: str):
    # logging config
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s:%(levelname)s:%(name)s:%(message)s',
                        filename=log_file_path,
                        filemode='a')  # append
    logger = logging.getLogger(__name__)
    return logger
