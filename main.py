import crypto_trading_lib as cl

df = cl.load_data()

fees = cl.create_trade_fee_table(df).sort_values(by=["date"])
cl.save_table(fees, "crypto_fees.csv")

buys = cl.create_crypto_buy_table(df).sort_values(by=["date", "currency_buy", "buy_order", "currency_sell"])
cl.save_table(buys, "crypto_buy_table.csv")

sells = cl.create_crypto_sell_table(df).sort_values(by=["date", "currency_sell", "sell_order", "currency_buy"])
cl.save_table(sells, "crypto_sell_table.csv")