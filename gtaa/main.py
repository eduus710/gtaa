import db
import ticker
import portfolio

DB_SCHEMA = 'gtaa'

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    mydb = db.mysql_db(DB_SCHEMA)

    # download & update prices
    tickers = ticker.get_ticker_list(mydb)
    ticker.update_all_prices(mydb, tickers)

    portfolios = portfolio.get_portfolios(mydb)
    print(portfolios)

    for pf in portfolios:
        is_active = pf[6]
        if is_active:
            portfolio.backtest(mydb, pf)