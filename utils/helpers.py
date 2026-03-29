import csv
import os

def load_sector_map(csv_path="smallcap_sectors.csv"):
    """
    Loads sector mappings from a CSV file.
    Expected format: Symbol,Sector,...
    Returns a dictionary {Symbol: Sector}.

    Args:
        csv_path (str): Path to the CSV file. Defaults to looking for 'smallcap_sectors.csv'
                        in the same directory as this file.
    """
    sector_map = {}

    # Resolve path relative to this file if it's just a filename
    if not os.path.isabs(csv_path):
        base_dir = os.path.dirname(__file__)
        csv_path = os.path.join(base_dir, csv_path)

    if not os.path.exists(csv_path):
        # Silent fail or warning? Let's print a warning for visibility during dev
        print(f"Warning: Sector map CSV not found at {csv_path}. Smallcap sectors may be missing.")
        return {}

    try:
        with open(csv_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                symbol = row.get("Symbol")
                sector = row.get("Sector")
                if symbol and sector:
                    # Skip 'General' if we want, or keep it. The requirement says:
                    # "If a ticker is missing sector info, add a fallback entry like 'Unknown' or 'General'."
                    # So we keep it.
                    sector_map[symbol.strip()] = sector.strip()
    except Exception as e:
        print(f"Error loading sector map: {e}")

    return sector_map
