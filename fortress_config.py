# fortress_config.py
# Complete ticker database + sector mapping

TICKER_GROUPS = {
    "Nifty 50": [
        "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "ICICIBANK.NS", "INFY.NS", "BHARTIARTL.NS", 
        "SBIN.NS", "ITC.NS", "LICI.NS", "HINDUNILVR.NS", "LT.NS", "BAJFINANCE.NS", "MARUTI.NS", 
        "SUNPHARMA.NS", "ADANIENT.NS", "KOTAKBANK.NS", "TITAN.NS", "ULTRACEMCO.NS", "AXISBANK.NS", 
        "NTPC.NS", "ONGC.NS", "ADANIPORTS.NS", "ASIANPAINT.NS", "COALINDIA.NS", "JSWSTEEL.NS", 
        "BAJAJ-AUTO.NS", "NESTLEIND.NS", "GRASIM.NS", "HINDALCO.NS", "POWERGRID.NS", 
        "ADANIPOWER.NS", "WIPRO.NS", "EICHERMOT.NS", "SBILIFE.NS", "TATAMOTORS.NS", 
        "BPCL.NS", "DRREDDY.NS", "HCLTECH.NS", "JIOFIN.NS", "TECHM.NS", "BRITANNIA.NS", 
        "TATAPOWER.NS", "BAJAJFINSV.NS", "INDUSINDBK.NS", "SHRIRAMFIN.NS", "TVSMOTOR.NS", 
        "APOLLOHOSP.NS", "CIPLA.NS", "BEL.NS", "TRENT.NS"
    ],
    "Nifty Next 50": [
        "ADANIENSOL.NS", "ADANIGREEN.NS", "AMBUJACEM.NS", "DMART.NS", "BAJAJHLDNG.NS", 
        "BANKBARODA.NS", "BHEL.NS", "BOSCHLTD.NS", "CANBK.NS", "CHOLAFIN.NS", "COLPAL.NS", 
        "DABUR.NS", "DLF.NS", "GAIL.NS", "GODREJCP.NS", "HAL.NS", "HAVELLS.NS", "HZL.NS", 
        "ICICILOMB.NS", "ICICIPRULI.NS", "IOC.NS", "IRCTC.NS", "IRFC.NS", "JINDALSTEL.NS", 
        "JSWENERGY.NS", "LTIM.NS", "LUPIN.NS", "MARICO.NS", "MRF.NS", "MUTHOOTFIN.NS", 
        "NAUKRI.NS", "PFC.NS", "PIDILITIND.NS", "PNB.NS", "RECLTD.NS", "SAMVARDHANA.NS", 
        "SHREECEM.NS", "SIEMENS.NS", "TATACOMM.NS", "TATAELXSI.NS", "TATAMTRDVR.NS", 
        "TORNTPHARM.NS", "UNITDSPR.NS", "VBL.NS", "VEDL.NS", "ZOMATO.NS", "ZYDUSLIFE.NS", 
        "ABB.NS", "TIINDIA.NS", "POLYCAB.NS"
    ],
    "Nifty Midcap 150": [
        "ABBOTINDIA.NS", "ABCAPITAL.NS", "ACC.NS", "ADANITOTAL.NS", "AIAENG.NS", "AJANTPHARM.NS", 
        "ALKEM.NS", "APARINDS.NS", "APLAPOLLO.NS", "APOLLOTYRE.NS", "ASHOKLEY.NS", "ASTRAL.NS", 
        "AUROPHARMA.NS", "AVANTIFEED.NS", "BALKRISIND.NS", "BANDHANBNK.NS", "BANKINDIA.NS", 
        "BATAINDIA.NS", "BEL.NS", "BERGEPAINT.NS", "BHARATFORG.NS", "BIOCON.NS", "BLUESTARCO.NS", 
        "BSE.NS", "CESC.NS", "CGPOWER.NS", "CHAMBLFERT.NS", "CHOLAHLDNG.NS", "COFORGE.NS", 
        "CONCOR.NS", "COROMANDEL.NS", "CREDITACC.NS", "CROMPTON.NS", "CUMMINSIND.NS", 
        "CYIENT.NS", "DEEPAKNTR.NS", "DELHIVERY.NS", "DEVYANI.NS", "DIXON.NS", "EASEMYTRIP.NS", 
        "EDELWEISS.NS", "EICHERMOT.NS", "EMAMILTD.NS", "ENDURANCE.NS", "ESCORTS.NS", 
        "EXIDEIND.NS", "FEDERALBNK.NS", "FORTIS.NS", "GICRE.NS", "GLENMARK.NS", "GMRINFRA.NS", 
        "GODREJIND.NS", "GODREJPROP.NS", "GRANULES.NS", "GUJGASLTD.NS", "HAPPSTMNDS.NS", 
        "HDFCAMC.NS", "HFCL.NS", "HINDCOPPER.NS", "HINDPETRO.NS", "HUDCO.NS", "IDBI.NS", 
        "IDFCFIRSTB.NS", "IEX.NS", "IGL.NS", "INDHOTEL.NS", "INDIAMART.NS", "INDIANB.NS", 
        "INDIGO.NS", "IPCALAB.NS", "IRB.NS", "ITDCEM.NS", "JBCHEPHARM.NS", "JKCEMENT.NS", 
        "JSL.NS", "JSWINFRA.NS", "JUBLFOOD.NS", "KALYANKJIL.NS", "KEI.NS", "KPITTECH.NS", 
        "KPRMILL.NS", "L&TFH.NS", "LAURUSLABS.NS", "LICHSGFIN.NS", "LINDEINDIA.NS", 
        "LLOYDSME.NS", "LUPIN.NS", "MAHABANK.NS", "MAHINDCIE.NS", "MANAPPURAM.NS", 
        "MANKIND.NS", "MARICO.NS", "MAXHEALTH.NS", "MAZDOCK.NS", "METROPOLIS.NS", 
        "MFSL.NS", "MGL.NS", "MOTILALOFS.NS", "MPHASIS.NS", "MRPL.NS", "MUTHOOTFIN.NS", 
        "NATCOPHARM.NS", "NATIONALUM.NS", "NAVINFLUOR.NS", "NBCC.NS", "NHPC.NS", 
        "NLCINDIA.NS", "NMDC.NS", "NYKAA.NS", "OBEROIRLTY.NS", "OFSS.NS", "OIL.NS", 
        "PAGEIND.NS", "PATANJALI.NS", "PAYTM.NS", "PERSISTENT.NS", "PETRONET.NS", 
        "PHOENIXLTD.NS", "PIIND.NS", "PNBHOUSING.NS", "POLYMED.NS", "POONAWALLA.NS", 
        "PRESTIGE.NS", "PVRINOX.NS", "QUESS.NS", "RADICO.NS", "RAILTEL.NS", 
        "RAJESHEXPO.NS", "RAMCOCEM.NS", "RATNAMANI.NS", "RBLBANK.NS", "RECLTD.NS", 
        "RELAXO.NS", "RVNL.NS", "SAIL.NS", "SCHAEFFLER.NS", "SHREECEM.NS", "SJVN.NS", 
        "SKFINDIA.NS", "SOLARINDS.NS", "SONACOMS.NS", "SRF.NS", "STLTECH.NS", "SUNTV.NS", 
        "SUPREMEIND.NS", "SUZLON.NS", "SWIGGY.NS", "SYNGENE.NS", "TATACHEM.NS", 
        "TATATECH.NS", "TRIDENT.NS", "UCOBANK.NS", "UNIONBANK.NS", "UPL.NS", 
        "VGUARD.NS", "VI.NS", "VOLTAS.NS", "WHIRLPOOL.NS", "YESBANK.NS", "ZEEL.NS"
    ],
    "Nifty Smallcap 250": [
        "ABSLAMC.NS", "ACE.NS", "AEGISLOG.NS", "AETHER.NS", "AFFLE.NS", "ALKYLAMINE.NS",
        "ALLCARGO.NS", "ALOKINDS.NS", "AMBER.NS", "ANANTRAJ.NS", "ANGELONE.NS", "ANURAS.NS",
        "ARE&M.NS", "ARVIND.NS", "ASAHIINDIA.NS", "ASTERDM.NS", "ASTRAZEN.NS", "AUBANK.NS",
        "AVAS.NS", "AWL.NS", "AZAD.NS", "BAJAJELEC.NS", "BALAMINES.NS", "BALRAMCHIN.NS",
        "BANSALWIRE.NS", "BCG.NS", "BEML.NS", "BIKAJI.NS", "BIRLACORPN.NS", "BLS.NS",
        "BLUEJET.NS", "BORORENEW.NS", "BRIGADE.NS", "BSOFT.NS", "CAMS.NS", "CAMPUS.NS",
        "CASTROLIND.NS", "CEATLTD.NS", "CENTRALBK.NS", "CENTURYPLY.NS", "CENTURYTEX.NS",
        "CERA.NS", "CHALET.NS", "CHENNPETRO.NS", "CIEINDIA.NS", "CLEAN.NS", "COCHINSHIP.NS",
        "COSMOFIRST.NS", "CRAFTSMAN.NS", "DATA-PATTERNS.NS", "DATAPATTNS.NS", "DBL.NS",
        "DCMSHRIRAM.NS", "DEEPAKFERT.NS", "DOMS.NS", "ECLERX.NS", "EIDPARRY.NS", "ELGIEQUIP.NS",
        "ENGINERSIN.NS", "EQUITASBNK.NS", "ERIS.NS", "ESAFSFB.NS", "FDC.NS", "FIVESTAR.NS",
        "FLUOROCHEM.NS", "FSL.NS", "GABRIEL.NS", "GANESHHOUC.NS", "GARTEN.NS", "GATEWAY.NS",
        "GENUSPOWER.NS", "GEOJITFSL.NS", "GLS.NS", "GNA.NS", "GNFC.NS", "GOCOLORS.NS",
        "GPIL.NS", "GPPL.NS", "GRAVITA.NS", "GRSE.NS", "GSFC.NS", "GULFOILLUB.NS",
        "HAPPYFORGE.NS", "HBLPOWER.NS", "HCC.NS", "HEG.NS", "HERITGFOOD.NS", "HGINFRA.NS",
        "HIKAL.NS", "HINDWAREAP.NS", "HOMEFIRST.NS", "HONASA.NS", "IOPL.NS", "IRB.NS",
        "IRCON.NS", "ITI.NS", "ITDC.NS", "J&KBANK.NS", "JAGRAN.NS", "JAIBALAJI.NS",
        "JINDALSAW.NS", "JKPAPER.NS", "JWL.NS", "JYOTICNC.NS", "KAYNES.NS", "KEC.NS",
        "KIMS.NS", "Kirloskar.NS", "KNRCON.NS", "KRBL.NS", "KSB.NS", "LATENTVIEW.NS",
        "LEMONTREE.NS", "LXCHEM.NS", "MAHLOG.NS", "MAPMYINDIA.NS", "MASTEK.NS", "MHRIL.NS",
        "MINDACORP.NS", "MMTC.NS", "MSTCLTD.NS", "MTARTECH.NS", "NCC.NS", "NETWEB.NS",
        "NETWORK18.NS", "NOCIL.NS", "NURECA.NS", "ORCHPHARMA.NS", "ORIENTELEC.NS", "PARAS.NS",
        "PCBL.NS", "PPLPHARMA.NS", "PRICOLLTD.NS", "PRUDENT.NS", "PUNJABCHEM.NS", "RAIN.NS",
        "RAYMOND.NS", "RCF.NS", "RELIANCEP.NS", "RHIM.NS", "RITES.NS", "ROLEXTURBO.NS",
        "ROSSARI.NS", "ROUTE.NS", "RTNINDIA.NS", "SANOFI.NS", "SAPPHIRE.NS", "SARDAEN.NS",
        "SDBL.NS", "SHARDACROP.NS", "SHOPERSTOP.NS", "SHYAMMETL.NS", "SIGACHI.NS", "SOUTHBANK.NS",
        "SPARC.NS", "STERTOOLS.NS", "SUBROS.NS", "SUNTECK.NS", "SUPRIYA.NS", "TATAINVEST.NS",
        "TDPOWERSYS.NS", "TEJASNET.NS", "TEXRAIL.NS", "THOMASCOOK.NS", "TV18BRDCST.NS",
        "UJJIVANSFB.NS", "USHAMART.NS", "UTIAMC.NS", "VAIBHAVGBL.NS", "VAKRANGEE.NS",
        "VARROC.NS", "VENKEYS.NS", "VESUVIUS.NS", "VIJAYA.NS", "VIPIND.NS", "VSTIND.NS",
        "WELCORP.NS", "WELSPUNLIV.NS", "WESTLIFE.NS", "WOCKPHARMA.NS", "ZENTEC.NS"
    ]
}

SECTOR_MAP = {
    "HDFCBANK.NS": "Banking", "ICICIBANK.NS": "Banking", "SBIN.NS": "Banking", "KOTAKBANK.NS": "Banking", 
    "AXISBANK.NS": "Banking", "INDUSINDBK.NS": "Banking", "BANKBARODA.NS": "Banking", "CANBK.NS": "Banking", 
    "PNB.NS": "Banking", "BAJFINANCE.NS": "NBFC", "BAJAJFINSV.NS": "NBFC", "CHOLAFIN.NS": "NBFC",
    "SHRIRAMFIN.NS": "NBFC", "MUTHOOTFIN.NS": "NBFC", "IDFCFIRSTB.NS": "Banking", "TCS.NS": "IT", 
    "INFY.NS": "IT", "WIPRO.NS": "IT", "HCLTECH.NS": "IT", "TECHM.NS": "IT", "LTIM.NS": "IT", 
    "MPHASIS.NS": "IT", "PERSISTENT.NS": "IT", "COFORGE.NS": "IT", "TATAELXSI.NS": "IT",
    "RELIANCE.NS": "Energy", "ONGC.NS": "Energy", "BPCL.NS": "Energy", "IOC.NS": "Energy",
    "ADANIPOWER.NS": "Energy", "TATAPOWER.NS": "Energy", "NTPC.NS": "Energy", "POWERGRID.NS": "Energy",
    "JSWSTEEL.NS": "Metals", "HINDALCO.NS": "Metals", "VEDL.NS": "Metals", "JINDALSTEL.NS": "Metals",
    "MARUTI.NS": "Auto", "TATAMOTORS.NS": "Auto", "BAJAJ-AUTO.NS": "Auto", "EICHERMOT.NS": "Auto",
    "TVSMOTOR.NS": "Auto", "SUNPHARMA.NS": "Pharma", "DRREDDY.NS": "Pharma", "CIPLA.NS": "Pharma",
    "HINDUNILVR.NS": "FMCG", "ITC.NS": "FMCG", "NESTLEIND.NS": "FMCG", "BRITANNIA.NS": "FMCG",
    "LT.NS": "Infra", "ADANIPORTS.NS": "Infra", "BEL.NS": "Defense", "HAL.NS": "Defense",
    "TRENT.NS": "Retail", "ZOMATO.NS": "Retail", "NYKAA.NS": "Retail"
}

# Extended for Nifty Smallcap 250 sector mapping
try:
    from utils.helpers import load_sector_map
    _smallcap_map = load_sector_map("smallcap_sectors.csv")
    SECTOR_MAP.update(_smallcap_map)
except Exception as e:
    print(f"Warning: Could not load Smallcap sectors: {e}")

# ^NIFTYJR is delisted/unreliable on Yahoo. Using ^CNX100 (Nifty 100) as proxy or fallback.
INDEX_BENCHMARKS = {
    "Nifty 50": "^NSEI",
    "Nifty Next 50": "^CNX100",
    "Nifty Midcap 150": "^NSMIDCP",
    "Nifty Smallcap 250": "^CNXSC"
}

NIFTY_SYMBOL = "^NSEI"

# --- Stock Scanner thresholds/config ---
SMALLCAP_LIQUIDITY_MIN_CR = 2.0
SECTOR_ROTATION_BONUS_POINTS = 10
TOP_SECTOR_COUNT = 3

# --- COMMODITIES INTELLIGENCE CONFIG ---
COMMODITY_TICKERS = ['GC=F', 'SI=F', 'CL=F', 'NG=F', 'HG=F']

COMMODITY_SPECS = {
    "Gold": {
        "global": "GC=F",
        "local": None, # "GOLD.MC" Delisted/Expired
        "import_duty": 0.15,
        "unit": "10g",
        "conversion_factor": 10.0 / 31.1035 # Troy oz to 10g
    },
    "Crude Oil": {
        "global": "CL=F",
        "local": None, # "CRUDEOIL.MC" Delisted/Expired
        "import_duty": 0.00,
        "unit": "barrel",
        "conversion_factor": 1.0 # Barrel to Barrel
    },
    "Silver": {
        "global": "SI=F",
        "local": None, # "SILVER.MC" Delisted/Expired
        "import_duty": 0.15,
        "unit": "1kg",
        "conversion_factor": 1000.0 / 31.1035 # Troy oz to kg
    },
    "Natural Gas": {
        "global": "NG=F",
        "local": None, # "NATURALGAS.MC" Delisted/Expired
        "import_duty": 0.00,
        "unit": "mmbtu",
        "conversion_factor": 1.0 # MMBtu to MMBtu
    }
}

COMMODITY_CONSTANTS = {
    "WAREHOUSING_COST_PCT_MONTHLY": 0.001, # 0.1% per month
    "ARB_YIELD_THRESHOLD": 10.0, # 10% annualized
    "DEFAULT_WINDOW": 20,
    "CORRELATION_THRESHOLD": 0.8,
    "VOLATILITY_LOOKBACK": 14
}

# --- Cross-module universes ---
MF_SCHEMES = [
    "120503",  # SBI Small Cap
    "120716",  # HDFC Flexi Cap
    "118834",  # ICICI Pru Bluechip
    "125497",  # Axis Small Cap
    "120503",  # keep known active codes
]

OPTIONS_UNDERLYINGS = ["^NSEI", "^NSEBANK", "NIFTYBEES.NS", "BANKBEES.NS"]

COMMODITIES_TICKERS = ["GC=F", "SI=F", "CL=F", "HG=F", "INR=X"]

SMALLCAP_LIQUIDITY_MIN_CR = 2.0
