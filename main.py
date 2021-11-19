import crypto_trading_lib as cl

df = cl.load_data()

fees = cl.create_trade_fee_table(df)
cl.save_table(fees, "crypto_fees.csv")

buys = cl.create_crypto_buy_table(df)
cl.save_table(buys, "crypto_buy_table.csv")

sells = cl.create_crypto_sell_table(df)
cl.save_table(sells, "crypto_sell_table.csv")