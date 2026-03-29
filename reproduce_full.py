
try:
    from fortress_config import TICKER_GROUPS
    print("Fortress Config imported successfully in main.")
except ImportError as e:
    print(f"Main Import failed: {e}")

import commodities.ui
print("Commodities UI imported.")
