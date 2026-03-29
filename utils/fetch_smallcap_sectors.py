import yfinance as yf
import pandas as pd
import time
import os
import sys

# Add parent directory to sys.path to import fortress_config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from fortress_config import TICKER_GROUPS

def fetch_sectors():
    tickers = TICKER_GROUPS.get("Nifty Smallcap 250", [])
    data = []

    print(f"Fetching sectors for {len(tickers)} tickers...")

    for i, ticker in enumerate(tickers):
        try:
            info = yf.Ticker(ticker).info
            sector = info.get("sector", "Unknown")
            industry = info.get("industry", "Unknown")

            # Map Yahoo sectors to project sectors
            mapped_sector = map_sector(sector, industry, ticker)

            data.append({"Symbol": ticker, "Sector": mapped_sector, "Raw_Sector": sector, "Raw_Industry": industry})
            print(f"[{i+1}/{len(tickers)}] {ticker}: {mapped_sector} ({sector} - {industry})")

            # Sleep to be polite
            time.sleep(0.5)

        except Exception as e:
            print(f"Error fetching {ticker}: {e}")
            data.append({"Symbol": ticker, "Sector": "General", "Raw_Sector": "Error", "Raw_Industry": "Error"})

    df = pd.DataFrame(data)
    output_path = os.path.join(os.path.dirname(__file__), "smallcap_sectors.csv")
    df.to_csv(output_path, index=False)
    print(f"Saved sector map to {output_path}")

def map_sector(sector, industry, ticker):
    sector = str(sector)
    industry = str(industry)

    # Project sectors: Banking, NBFC, IT, Energy, Metals, Auto, Pharma, FMCG, Infra, Defense, Retail
    # Extended sectors: Chemicals, Textiles, Realty, Media, Consumer Durables, Capital Goods, Services, Healthcare

    if "Bank" in industry or "Bank" in sector:
        return "Banking"

    if "Financial" in sector or "Credit" in industry or "Asset Management" in industry or "Capital Markets" in industry:
        return "NBFC"

    if "Technology" in sector or "Software" in industry or "IT Services" in industry:
        return "IT"

    if "Energy" in sector or "Oil" in industry or "Gas" in industry or "Coal" in industry:
        return "Energy"

    if "Basic Materials" in sector:
        if "Steel" in industry or "Metal" in industry or "Mining" in industry or "Aluminum" in industry or "Copper" in industry or "Gold" in industry:
            return "Metals"
        if "Chemical" in industry:
            return "Chemicals"
        if "Paper" in industry:
            return "Paper" # New or General? Let's use Paper.
        if "Construction Materials" in industry:
            return "Infra" # Cement usually goes to Infra or Materials. Let's map to Infra for now as likely Cement.

    if "Consumer Cyclical" in sector:
        if "Auto" in industry:
            return "Auto"
        if "Textile" in industry or "Apparel" in industry or "Footwear" in industry:
            return "Textiles"
        if "Retail" in industry or "Department Stores" in industry:
            return "Retail"
        if "Home Improvement" in industry or "Furnishings" in industry:
            return "Consumer Durables"
        if "Travel" in industry or "Lodging" in industry or "Restaurants" in industry:
            return "Services"
        if "Media" in industry or "Entertainment" in industry:
            return "Media"

    if "Healthcare" in sector:
        if "Drug Manufacturers" in industry or "Biotechnology" in industry:
            return "Pharma"
        if "Healthcare" in industry or "Medical" in industry:
            return "Healthcare"

    if "Consumer Defensive" in sector:
        if "Beverages" in industry or "Food" in industry or "Household" in industry or "Personal" in industry or "Tobacco" in industry:
            return "FMCG"
        if "Education" in industry:
             return "Services"

    if "Industrials" in sector:
        if "Construction" in industry or "Engineering" in industry or "Infrastructure" in industry:
            return "Infra"
        if "Defense" in industry or "Aerospace" in industry:
            return "Defense"
        if "Machinery" in industry or "Electrical Equipment" in industry:
            return "Capital Goods"
        if "Logistics" in industry or "Transport" in industry or "Shipping" in industry or "Railroads" in industry:
            return "Logistics" # New sector
        if "Business Services" in industry or "Consulting" in industry:
            return "Services"

    if "Real Estate" in sector:
        return "Realty" # New sector

    if "Communication Services" in sector:
        if "Telecom" in industry:
            return "Telecom" # New sector
        if "Media" in industry or "Entertainment" in industry or "Advertising" in industry:
            return "Media"

    if "Utilities" in sector:
        return "Energy" # Power utilities mapped to Energy

    return "General"

if __name__ == "__main__":
    fetch_sectors()
