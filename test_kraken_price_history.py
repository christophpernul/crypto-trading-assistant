import requests
import json
import pandas as pd


def get_prices_currency_pair(currency_pair: str, start_date: str, interval: int) -> dict:
    """
    Performs an API call to kraken: https://docs.kraken.com/rest/#tag/Market-Data/operation/getOHLCData
    Retrieves crypto prices for parameters
    :param currency_pair: format: {currency-id}{currency-id} --> currency-id (three digit identifier, e.g. BTC)
    :param start_date: string for the start-date of the call
    :param interval: see docu of kraken API
    :return: dict containing price data
    """
    base_endpoint = "https://api.kraken.com/0/public/OHLC"
    try:
        start_datetime = pd.to_datetime(start_date, format="%Y-%m-%d").strftime("%s")
    except:
        print("Wrong date-string format! Use %Y-%m-%d")

    endpoint = base_endpoint + f"?pair={currency_pair}" + f"&since={start_datetime}" + f"&interval={interval}"
    r = requests.get(endpoint)
    assert r.status_code == 200, f"Error in API call: {r.content}"
    return json.loads(r.content)


def initial_fetch(currency_pair: str):
    """
    Kraken only allows certain intervals (1 day, 1 week, 1 month) and the since-parameter seems not to work as expected.
    In order to retrieve all historical data from my first trade we need to find a solution to go back to the past with
    only the allowed 720 datapoints.
    This functions fetches daily data from 2018-01-04 onwards. It could be that this is only possible by running it on 2022-10-03.
    We use an interval of 1 week and iterate over 7 days to retrieve all historical data.
    :param currency_pair:
    :return:
    """
    datestring_my_first_trx = "2022-01-01"
    interval = 10080 # 1 week

    data = get_prices_currency_pair(currency_pair, datestring_my_first_trx, interval)

currency_pair = "BTCEUR"

datestring_my_first_trx = "2018-01-02"
interval = 10080 # 1 week
data = get_prices_currency_pair(currency_pair, datestring_my_first_trx, interval)


df = pd.DataFrame(data["result"]["XXBTZEUR"], columns=["datetime",
                                                       "price_open",
                                                       "price_high",
                                                       "price_low",
                                                       "price_close",
                                                       "volume_weighted_avg_price",
                                                       "volume",
                                                       "count",
                                                       ]
                  )
df["datetime"] = df["datetime"].apply(lambda time: pd.to_datetime(time, unit="s"))