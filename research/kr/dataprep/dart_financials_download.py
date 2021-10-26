# activate .dart_fss virtual environment -> . .dart_fss/bin/activate
# install this at the beginning -> pip install dart-fss

import dart_fss as dart
import logging

api_key = '800a1197f66d1eb9b5d3ad2e2026121ac0e7035a'

def get_fs(api_key):
    dart.set_api_key(api_key)
    corp_list = dart.get_corp_list()
    fs = corp_list.find_by_stock_code("005930").extract_fs(bgn_de="20200101")
    result = fs.show("fs")
    return result

def test_get_fs():
    actual = get_fs(api_key)
    logging.info(actual)

    # no running in headless linux
    # fake browser -> having issue in corp laptop because of security reason
