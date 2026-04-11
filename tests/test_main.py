import unittest

import pandas as pd

from main import _build_sector_pulse_records


class TestMainHelpers(unittest.TestCase):
    def test_build_sector_pulse_records_uses_avg_score_for_thesis(self):
        df = pd.DataFrame([
            {"Sector": "Banks", "Velocity": 1.8, "Above_EMA200": 1.0, "Score": 82.0},
            {"Sector": "Banks", "Velocity": 0.8, "Above_EMA200": 1.0, "Score": 78.0},
            {"Sector": "IT", "Velocity": -1.2, "Above_EMA200": 0.2, "Score": 30.0},
        ])

        records = _build_sector_pulse_records(df)

        self.assertEqual(len(records), 2)

        banks = next(record for record in records if record["Sector"] == "Banks")
        it = next(record for record in records if record["Sector"] == "IT")

        self.assertEqual(banks["Avg_Score"], 80.0)
        self.assertEqual(banks["Thesis"], "🐂 Bullish Accumulation")
        self.assertEqual(banks["On_the_Rise"], "🔥 YES")

        self.assertEqual(it["Avg_Score"], 30.0)
        self.assertEqual(it["Thesis"], "❄️ Structural Weakness")
        self.assertEqual(it["On_the_Fall"], "❄️ YES")

    def test_build_sector_pulse_records_handles_missing_columns(self):
        df = pd.DataFrame([{"Sector": "Banks", "Velocity": 1.2}])
        self.assertEqual(_build_sector_pulse_records(df), [])


if __name__ == "__main__":
    unittest.main()
