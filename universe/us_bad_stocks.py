import pandas as pd
import yahooquery as yq
import logging


def get_symbol(query, preferred_exchange='AMS'):
    try:
        data = yq.search(query)
    except ValueError:  # Will catch JSONDecodeError
        print(query)
    else:
        quotes = data['quotes']
        if len(quotes) == 0:
            return 'No Symbol Found'

        symbol = quotes[0]['symbol']
        for quote in quotes:
            if quote['exchange'] == preferred_exchange:
                symbol = quote['symbol']
                break
        return symbol


def get_bad_stocks():
    nasdaq_non_compliant_url = "https://listingcenter.nasdaq.com/noncompliantcompanylist.aspx"
    nasdaq_warning_url = "https://listingcenter.nasdaq.com/IssuersPendingSuspensionDelisting.aspx"

    nasdaq_non_compliant_names = pd.read_html(nasdaq_non_compliant_url)[5].Deficiency.to_list()
    nasdaq_non_compliant_tickers = [get_symbol(x) for x in nasdaq_non_compliant_names]
    nasdaq_warning_tickers = pd.read_html(nasdaq_warning_url)[5].Symbol.to_list()
    bad_stocks = list(set(nasdaq_non_compliant_tickers + nasdaq_warning_tickers))
    logging.info("total bad stocks = {}".format(str(len(bad_stocks))))
    logging.info(
        "bad stocks do not include NYSE bad stocks -> check here:  https://www.nyse.com/regulation/noncompliant-issuers/")
    return bad_stocks


def test_get_bad_stocks():
    actual = get_bad_stocks()
    logging.info("bad stock count")
    logging.info(len(actual))
    pass
