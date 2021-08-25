from unittest import TestCase
import pandas as pd
import rule_engine

from gtaa import portfolio


class TestPortfolio(TestCase):
    def test_check_rules1(self):
        """ no test triggered
        """
        rules = [rule_engine.Rule('adj_close_price < sma_200d or pct_63d < 0')]
        row = pd.Series({'adj_close_price': 100,
                         'sma_200d': 95,
                         'pct_63d': 5.0})
        assert not portfolio.check_rules(rules, row)

    def test_check_rules2(self):
        """ 1 test triggered
        """
        rules = [rule_engine.Rule('adj_close_price < sma_200d or pct_63d < 0')]
        row = pd.Series({'adj_close_price': 95,
                         'sma_200d': 100,
                         'pct_63d': 5.0})
        assert portfolio.check_rules(rules, row)
