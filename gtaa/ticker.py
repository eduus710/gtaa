import pandas as pd
import yfinance as yf
from datetime import *
from dateutil.relativedelta import *
from gtaa import db


def download(ticker, period='max'):
    """ download daily historical ticker prices using yfinance

    args:
    ticker - stock ticker
    period - historical period ['1m'.'3m',...'10y','max'

    returns:
    pandas dataframe of daily prices, indexed by (ticker,trade_date)
    """

    df = yf.download(
        tickers=ticker,
        period=period,
        interval='1d',
        group_by='ticker'
    )
    df.reset_index(inplace=True)
    df = df.rename(columns={
        'Date': 'trade_date',
        'Open': 'open_price',
        'High': 'high_price',
        'Low': 'low_price',
        'Close': 'close_price',
        'Adj Close': 'adj_close_price',
        'Volume': 'volume'
    })

    # enrich with trading day and reverse trading day
    # reverse trading day only really has value for end-of-month days
    # seems a bit hackish, but it works
    df.set_index(['trade_date'], inplace=True)
    df['day_of_month'] = df.index.day
    df['trade_day'] = df.groupby(pd.Grouper(freq='M'))['day_of_month'].rank()
    df['rev_trade_day'] = df.groupby(pd.Grouper(freq='M'))['day_of_month'].rank(ascending=False)
    df.drop(columns=['day_of_month'], inplace=True)

    # clean up data types
    df = df.astype({'volume': 'int32',
                    'trade_day': 'int32',
                    'rev_trade_day': 'int32'
                    })

    # reindex again by (ticker,trade_date)
    df.reset_index(inplace=True)
    df['ticker'] = ticker
    df.set_index(['ticker', 'trade_date'], inplace=True)
    df.sort_index(inplace=True)
    return df


# todo try sqlalchemy

def update_prices(mydb, df):
    """ updates a single ticker
    assumes index is ticker+trade_date, sorted in ascending trade_date order
    """
    first = df.iloc[0]
    ticker = first.name[0]
    td = first.name[1].date()
    db.execute(mydb._db_cur, 'delete from gtaa.ticker_price where ticker=%s and trade_date >= %s', (ticker, td))

    p_cols = ['ticker_sql', 'trade_date_sql2', 'open_price', 'high_price', 'low_price', 'close_price',
              'adj_close_price', 'volume', 'trade_day', 'rev_trade_day']
    df['ticker_sql'] = df.index.get_level_values('ticker')
    df['trade_date_sql1'] = df.index.get_level_values('trade_date')
    df['trade_date_sql2'] = df['trade_date_sql1'].dt.date
    params = df[p_cols].values.tolist()

    df.drop(columns=['ticker_sql', 'trade_date_sql1', 'trade_date_sql2'], inplace=True)
    db.execute_many(mydb._db_cur,
                    "insert into gtaa.ticker_price values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    params)


def get_ticker_list(mydb):
    mydb.query('select ticker, last_trade_date from gtaa.ticker_list')
    results = mydb._db_cur.fetchall()
    return results


def update_all_prices(mydb, ticker_list):
    """ download subset of historical prices based on last time downloaded
    NOTE: this does not work well with adjusted_close; subsequent dividends can
    change older adjusted prices.

    """
    for (ticker, last_trade_date) in ticker_list:
        rdelta = relativedelta(datetime.now(), last_trade_date)
        if rdelta.years >= 10:
            pd = 'max'
        elif rdelta.years >= 5:
            pd = '10y'
        elif rdelta.years >= 2:
            pd = '5y'
        elif rdelta.years >= 1:
            pd = '2y'
        elif rdelta.months >= 6:
            pd = '1y'
        elif rdelta.months >= 3:
            pd = '6mo'
        elif rdelta.months >= 1:
            pd = '3mo'
        else:
            pd = '1mo'

        df = download(ticker, period=pd)
        update_prices(mydb, df)


def get_prices_sma(mydb, tickers):
    sql = ('select ticker,trade_date,rev_trade_day,'
           'adj_close_price,'
           'sma_20d,sma_50d,sma_200d '
           'from gtaa.ticker_sma where ticker=%s')
    df = None

    for ticker in tickers:
        mydb.query(sql, params=(ticker,))
        results = mydb._db_cur.fetchall()
        ticker_df = pd.DataFrame(results,
                                 columns=['ticker', 'trade_date', 'rev_trade_day', 'adj_close_price',
                                          'sma_20d', 'sma_50d', 'sma_200d']
                                 )
        if df is None:
            df = ticker_df
        else:
            df = df.append(ticker_df)
    df.reset_index(inplace=True)
    df.set_index(['ticker', 'trade_date'], inplace=True)
    df.drop(columns=['index'], inplace=True)
    df.sort_index(inplace=True)
    return (df)


def get_close_price(mydb, portfolio_id, trade_date):
    sql = ('select ticker, adj_close_price '
           'from gtaa.portfolio_close_price where portfolio_id=%s and trade_date=%s')
    mydb.query(sql, params=(portfolio_id, trade_date))
    results = mydb._db_cur.fetchall()
    return dict(results)


def add_rolling_returns(df, prefix, periods):
    """ enrich dataframe of historical prices with set of rolling returns (using adj_close)
    assumes df indexed by ticker,trade_date

    parameters:
    df - dataframe of daily historical prices; must be sorted by ticker+trade_date
    prefix - column prefix; column label of form '[prefix]_[period]d'
    periods - list of rolling return periods in days

    returns:
    enriched dataframe
    """

    # note: need an extra day to compute the return for the period
    for period in periods:
        df[f'{prefix}_{period}d'] = df.adj_close_price.rolling(period + 1).apply(lambda x: (x[-1] - x[0]) / x[0])
    return df


def add_highs(df, prefix, periods):
    """ enrich dataframe of historical prices with a set of highs
    assumes df indexed by ticker+trade_date

    parameters:
    df - dataframe of daily historical prices; must be sorted by ticker+trade_date
    prefix - column prefix; column label will be of form '[prefix]_[period]d'
    periods - list of rolling return periods in days

    returns:
    enriched dataframe
    """
    for period in periods:
        df[f'{prefix}_{period}d'] = df.adj_close_price.rolling(period).max()
    return df


def add_gtaa(df, gtaa):
    """ enrich dataframe of historical prices + returns with gtaa measures
    gtaa measure is a sum of returns: e.g. pct_21d + pct_63d + pct_121d

    parameters:
    df - dataframe of daily historical prices and returns
    gtaa - dictionary of gtaa labels and a list of return column labels to sum

    returns:
    enriched dataframe
    """
    for gtaa_label, returns in gtaa.items():
        gtaa_val = 0
        for ret_label in returns:
            gtaa_val += df[ret_label]
        df[gtaa_label] = gtaa_val
    return df
