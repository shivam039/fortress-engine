# engine/pandas_ta.py
# Compatibility shim to map pandas_ta to pandas_ta_classic for Python 3.9
try:
    from pandas_ta_classic import *
    from pandas_ta_classic import __version__
    import pandas_ta_classic as _ta
    
    # Ensure the .ta accessor works on dataframes if possible
    # pandas_ta_classic should already handle this on import
except ImportError:
    pass
