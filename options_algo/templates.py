# options_algo/templates.py

STRATEGY_TEMPLATES = {
    "920_Straddle": [
        {"leg": 1, "type": "SELL", "option": "CE", "strike": "ATM", "qty_mult": 1},
        {"leg": 2, "type": "SELL", "option": "PE", "strike": "ATM", "qty_mult": 1}
    ],
    "Iron_Condor": [
        {"leg": 1, "type": "SELL", "option": "CE", "strike": "OTM_20_Delta", "qty_mult": 1},
        {"leg": 2, "type": "SELL", "option": "PE", "strike": "OTM_20_Delta", "qty_mult": 1},
        {"leg": 3, "type": "BUY", "option": "CE", "strike": "OTM_Wings", "qty_mult": 1},
        {"leg": 4, "type": "BUY", "option": "PE", "strike": "OTM_Wings", "qty_mult": 1}
    ],
    "Conversion_Arb": [
        {"leg": 1, "type": "BUY", "option": "CE", "strike": "ATM", "qty_mult": 1},
        {"leg": 2, "type": "SELL", "option": "PE", "strike": "ATM", "qty_mult": 1},
        {"leg": 3, "type": "SELL", "option": "STOCK", "strike": "SPOT", "qty_mult": 1}
    ],
    "Butterfly_Spread": [
        {"leg": 1, "type": "BUY", "option": "CE", "strike": "ITM", "qty_mult": 1},
        {"leg": 2, "type": "SELL", "option": "CE", "strike": "ATM", "qty_mult": 2},
        {"leg": 3, "type": "BUY", "option": "CE", "strike": "OTM", "qty_mult": 1}
    ],
    "The_Collar": [
        {"leg": 1, "type": "BUY", "option": "STOCK", "strike": "SPOT", "qty_mult": 1},
        {"leg": 2, "type": "BUY", "option": "PE", "strike": "OTM", "qty_mult": 1},
        {"leg": 3, "type": "SELL", "option": "CE", "strike": "OTM", "qty_mult": 1}
    ],
    "Ratio_Spread_2_1": [
        {"leg": 1, "type": "BUY", "option": "CE", "strike": "ATM", "qty_mult": 1},
        {"leg": 2, "type": "SELL", "option": "CE", "strike": "OTM", "qty_mult": 2}
    ],
    "Iron_Fly": [
        {"leg": 1, "type": "SELL", "option": "CE", "strike": "ATM", "qty_mult": 1},
        {"leg": 2, "type": "SELL", "option": "PE", "strike": "ATM", "qty_mult": 1},
        {"leg": 3, "type": "BUY", "option": "CE", "strike": "OTM_Hedge", "qty_mult": 1},
        {"leg": 4, "type": "BUY", "option": "PE", "strike": "OTM_Hedge", "qty_mult": 1}
    ],
    "Calendar_Spread": [
        {"leg": 1, "type": "SELL", "option": "CE", "expiry": "CURRENT", "strike": "ATM", "qty_mult": 1},
        {"leg": 2, "type": "BUY", "option": "CE", "expiry": "NEXT", "strike": "ATM", "qty_mult": 1}
    ],
    "Bull_Call_Spread": [
        {"leg": 1, "type": "BUY", "option": "CE", "strike": "ATM", "qty_mult": 1},
        {"leg": 2, "type": "SELL", "option": "CE", "strike": "OTM", "qty_mult": 1}
    ],
    "Bear_Put_Spread": [
        {"leg": 1, "type": "BUY", "option": "PE", "strike": "ATM", "qty_mult": 1},
        {"leg": 2, "type": "SELL", "option": "PE", "strike": "OTM", "qty_mult": 1}
    ],
    "Gamma_Scalp": [
        {"leg": 1, "type": "BUY", "option": "CE", "strike": "ATM", "qty_mult": 1},
        {"leg": 2, "type": "BUY", "option": "PE", "strike": "ATM", "qty_mult": 1}
    ],
    "Long_Vega_Hedge": [
        {"leg": 1, "type": "BUY", "option": "CE", "expiry": "NEXT", "strike": "OTM", "qty_mult": 1},
        {"leg": 2, "type": "BUY", "option": "PE", "expiry": "NEXT", "strike": "OTM", "qty_mult": 1}
    ]
}
