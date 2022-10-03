import pandas as pd
import os

def load_data(crypto_path: str = "/home/chris/Dropbox/Finance/data/crypto/exported/",
              fname: str = "crypto_orders.csv") -> pd.DataFrame:
    df = pd.read_csv(os.path.join(crypto_path, fname))
    df = df.assign(date=pd.to_datetime(df.date))
    return df

def save_table(df: pd.DataFrame,
               filename: str,
               export_path: str = "/home/chris/Dropbox/Finance/data/crypto/exported/"):
    df.to_csv(os.path.join(export_path, filename), sep=",", index=False)


def create_trade_fee_table(all_trades: pd.DataFrame) -> pd.DataFrame:
    fees = all_trades[(all_trades.type == "trade") &
                      (all_trades.fee != 0.)
                     ][["date",
                        "date_string",
                        "ordertxid",
                        "fee",
                        "fee_currency"
                        ]].copy().rename(columns={"fee_currency": "currency"}
                                         ).reset_index(drop=True)
    fees = fees.assign(fee = fees.fee * -1)
    return fees

def create_crypto_buy_table(all_trades: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates all trades, where crypto-currencies where bought from either EUR or another crypto currency.
    :param all_trades: Dataframe of all crypto trades, deposits and withdrawals
    :return:
    """
    # Get all trades, where crypto-currencies where bought
    buy_trades = all_trades[(all_trades.type == "trade") &
                            (all_trades.amount > 0) &
                            (all_trades.currency != "EUR")
                            ].drop(["type",
                                    "fee",
                                    "fee_currency"
                                    ], axis=1)
    # Get all trades, where some currency was sold. This needs to be matched with above buys
    # to get infos about sold currency
    sell_matching = all_trades[(all_trades.type == "trade") &
                               (all_trades.amount < 0)
                               ].drop(["type",
                                       "fee",
                                       "fee_currency"
                                       ], axis=1)

    buys = buy_trades.merge(sell_matching,
                            how="inner",
                            left_on="ordertxid",
                            right_on="ordertxid",
                            suffixes=["_buy", "_sell"]
                            ).drop(["exchange_sell",
                                    "date_string_sell",
                                    "conversion_rate_received_spent_sell",
                                    "date_sell",
                                    "amount_sell",
                                    "txid_sell",
                                    "txid_buy"
                                    ], axis=1).rename(columns={"date_buy": "date",
                                                               "date_string_buy": "date_string",
                                                               "exchange_buy": "exchange",
                                                               "conversion_rate_received_spent_buy": "conversion_rate_received_spent"
                                                               }
                                                      )
    # Assigns a sell_order for every currency ordered by date (oldest first)
    buys["buy_order"] = buys.groupby("currency_buy")["date"].rank(method="first", ascending=True)
    buys["amount_sell"] = buys["amount_buy"] * buys["conversion_rate_received_spent"]

    # Calculate the cumsum of amount bought for every trading-pair sorted by date
    buys.loc[:, ("amount_buy_cumsum")] = buys.sort_values("date").groupby(["currency_buy",
                                                                           "currency_sell"
                                                                           ]
                                                                          )["amount_buy"].cumsum()
    # Calculate the cumsum of amount sold for every trading-pair sorted by date
    buys.loc[:, ("amount_sell_cumsum")] = buys.sort_values("date").groupby(["currency_sell",
                                                                            "currency_buy"
                                                                            ]
                                                                           )["amount_sell"].cumsum()
    return buys

def create_crypto_sell_table(all_trades: pd.DataFrame) -> pd.DataFrame:
    # Get all trades, where a crypto-currency was sold
    sell_trades = all_trades[(all_trades.type == "trade") &
                             (all_trades.amount < 0) &
                             (all_trades.currency != "EUR")
                            ].drop(["type",
                                    "fee",
                                    "fee_currency"
                                    ], axis=1)
    # Get all trades, where a crypto-currency was bought. This needs to be matched with above sells
    # to get infos about bought currency
    buy_matching = all_trades[(all_trades.type == "trade") &
                              (all_trades.amount > 0)
                            ].drop(["type",
                                    "fee",
                                    "fee_currency"
                                    ], axis=1)

    sells = sell_trades.merge(buy_matching,
                              how="inner",
                              left_on="ordertxid",
                              right_on="ordertxid",
                              suffixes=["_sell", "_buy"]
                              ).drop(["exchange_buy",
                                      "date_string_buy",
                                      "conversion_rate_received_spent_buy",
                                      "date_buy",
                                      "amount_buy",
                                      "txid_buy",
                                      "txid_sell"
                                      ], axis=1).rename(columns={"date_sell": "date",
                                                                 "date_string_sell": "date_string",
                                                                 "exchange_sell": "exchange",
                                                                 "conversion_rate_received_spent_sell": "conversion_rate_received_spent"
                                                                 }
                                                        )
    sells = sells.assign(amount_sell=sells.amount_sell * -1)
    # Assigns a sell_order for every currency ordered by date (oldest first)
    sells["sell_order"] = sells.groupby("currency_sell")["date"].rank(method="first", ascending=True)
    return sells