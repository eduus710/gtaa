import pandas as pd

from gtaa import ticker


class TestTicker:
    def test_add_rolling_returns1(self):
        data = {'2001-01-01': [100],
                '2001-01-02': [101],
                '2001-01-03': [102],
                '2001-01-04': [103],
                '2001-01-05': [104],
                '2001-01-06': [105]}
        df = pd.DataFrame.from_dict(data, orient='index', columns=['adj_close_price'])
        df = ticker.add_rolling_returns(df, 'pct', [1, 2, 3, 4, 5])
        assert (df.loc['2001-01-02'].pct_1d == .01)
        assert (df.loc['2001-01-06'].pct_5d == .05)

    def test_add_highs1(self):
        data = {'2001-01-01': [100],
                '2001-01-02': [101],
                '2001-01-03': [102],
                '2001-01-04': [101],
                '2001-01-05': [100]}
        df = pd.DataFrame.from_dict(data, orient='index', columns=['adj_close_price'])
        df = ticker.add_highs(df, 'high', [3, 4, 5])
        assert (df.loc['2001-01-03'].high_3d == 102)
        assert (df.loc['2001-01-05'].high_5d == 102)

    def test_add_gtaa1(self):
        data = {'2001-01-01': [.05, .10],
                '2001-01-02': [.04, .09],
                '2001-01-03': [.06, .11],
                '2001-01-04': [.03, .08],
                '2001-01-05': [.07, .12]}
        df = pd.DataFrame.from_dict(data, orient='index', columns=['pct_21d', 'pct_63d'])
        df = ticker.add_gtaa(df, {'gtaa': ['pct_21d', 'pct_63d']})
        assert (df.loc['2001-01-05'].gtaa == 0.19)
