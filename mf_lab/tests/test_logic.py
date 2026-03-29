import unittest
import pandas as pd
import numpy as np
from mf_lab.logic import detect_integrity_issues

class TestMFLabLogic(unittest.TestCase):
    def test_detect_integrity_issues(self):
        # Create mock data
        dates = pd.date_range(start='2020-01-01', periods=300)

        # Benchmark returns (random normal)
        np.random.seed(42)
        b_ret = np.random.normal(0.0005, 0.01, 300)
        benchmark_df = pd.DataFrame({'ret': b_ret}, index=dates)

        # Fund returns (slightly better than benchmark)
        f_ret = b_ret * 1.1 + 0.0001
        fund_df = pd.DataFrame({'date': dates, 'ret': f_ret})

        # Run function
        result = detect_integrity_issues(fund_df, benchmark_df, "Large Cap")

        # Assertions
        self.assertIsNotNone(result)
        self.assertIn('alpha', result)
        self.assertIn('beta', result)
        self.assertIn('sortino', result)
        self.assertIn('upside', result)
        self.assertIn('downside', result)

        # Sanity checks
        self.assertGreater(result['beta'], 0.9) # Should be close to 1.1
        self.assertGreater(result['alpha'], 0) # Should be positive
        self.assertEqual(result['drift'], "✅ Stable") # Should be stable as beta 1.1 < 1.15

    def test_insufficient_data(self):
        dates = pd.date_range(start='2024-01-01', periods=50)
        b_ret = np.random.normal(0.0005, 0.01, 50)
        benchmark_df = pd.DataFrame({'ret': b_ret}, index=dates)
        fund_df = pd.DataFrame({'date': dates, 'ret': b_ret})

        result = detect_integrity_issues(fund_df, benchmark_df, "Large Cap")
        self.assertIsNone(result)

if __name__ == '__main__':
    unittest.main()
