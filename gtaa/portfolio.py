import pandas as pd
import rule_engine
from gtaa import ticker

def get_portfolios(mydb):
    sql = ('select portfolio_id,portfolio_name,ticker_group_id,'
           'trade_day,ticker_count,default_ticker,is_active '
           'from gtaa.portfolio')

    mydb.query(sql)
    results = mydb._db_cur.fetchall()
    return results


def get_portfolio_rules(mydb, portfolio_id):
    sql = ('select rule_id, rule_text '
           'from gtaa.portfolio_rule '
           'where portfolio_id=%s')

    mydb.query(sql, (portfolio_id,))
    results = mydb._db_cur.fetchall()
    return results


def get_tickers(mydb, portfolio_id):
    sql = ('select tg.ticker from gtaa.portfolio p '
           'join gtaa.ticker_group tg on tg.group_id=p.ticker_group_id '
           'where p.portfolio_id=%s')
    mydb.query(sql, (portfolio_id,))
    results = mydb._db_cur.fetchall()
    return [r[0] for r in results]


def calculate_performance(pf, close_prices):
    notional = 0
    perf = {}
    for tk, (old_value, shares, old_price) in pf.items():
        close_price = close_prices[tk]
        new_value = close_price * shares
        gnl = new_value - old_value
        notional += new_value
        perf[tk] = (old_value, shares, gnl, close_price)
    return notional, perf


def check_rules(rules, row):
    """ check row against a set of RuleEngine rules

    args:
    rules - set of rules
    row - row to test against rules

    returns:
    true if any rule matches, false otherwise
    """

    row_dict = row.to_dict()
    for r in rules:
        if r.matches(row_dict):
            return True
    return False


def rebalance(tickers, notional, n, rules, default_ticker, default_close_price):
    tickers.reset_index(inplace=True)
    tickers.set_index(['ticker'], inplace=True)
    portfolio = {}
    balance = notional
    #      weights = [x/tot for x in range(n,0,-1)]
    weights = [1 / n] * n

    tickers = tickers.sort_values(['pct_gtaa'], ascending=False)
    for tk, row in tickers.iterrows():
        if balance < 0.01:
            continue
        if len(weights) == 0:
            continue
        if pd.isna(row['pct_gtaa']):
            continue
        if check_rules(rules, row):
            continue
        allocation = notional * weights.pop(0)
        portfolio[tk] = (allocation, allocation / row.adj_close_price, row.adj_close_price)
        balance -= allocation

    # allocate balance to default ticker
    if default_ticker:
        portfolio[default_ticker] = (balance, balance / default_close_price, default_close_price)
    return portfolio


def backtest(mydb, pf):
    portfolio_id = pf[0]
    trade_day = pf[3]
    n = pf[4]
    default_ticker = pf[5]

    tickers = get_tickers(mydb, portfolio_id)
    # todo slow
    df = ticker.get_prices_sma(mydb, tickers)
    # todo slow
    df = ticker.add_rolling_returns(df, 'pct', [21, 63, 126, 252])
    df = ticker.add_highs(df, 'high', [252, 760, 1260])
    df = ticker.add_gtaa(df, {'pct_gtaa': ['pct_21d', 'pct_63d', 'pct_126d', 'pct_252d']})

    # reset index to trade_date+ticker
    df.reset_index(inplace=True)
    df.set_index(['trade_date', 'ticker'], inplace=True)
    df.sort_index(inplace=True)

    rules = get_portfolio_rules(mydb, portfolio_id)
    rules = [rule_engine.Rule(r[1]) for r in rules]  # rule text to Rules

    td_field = 'trade_day' if trade_day > 0 else 'rev_trade_day'
    td_prices = df.query(f'{td_field} == {abs(trade_day)}')

    notional = 100000
    p = None
    perf_hist = []
    for trade_date, df in td_prices.query("trade_date > '2009-12-27'").groupby(level=0):
        close_prices = ticker.get_close_price(mydb, portfolio_id, trade_date.date())
        default_close_price = close_prices.get(default_ticker, 0)

        if p is not None:
            notional, perf = calculate_performance(p, close_prices)
            perf_hist.append(perf)
            # print(trade_date, notional, perf)

        p = rebalance(df,
                      notional=notional,
                      n=n,
                      rules=rules,
                      default_ticker=default_ticker,
                      default_close_price=default_close_price)
        print(trade_date, notional, p)

    tot_gnl = 0
    tk_perf = {}
    for perf in perf_hist:
        print(perf)
        for tk, (value, shares, gnl, price) in perf.items():
            tk_perf[tk] = tk_perf.get(tk, 0) + gnl
            tot_gnl += gnl
    print(tot_gnl)
    print(tk_perf)
