import numpy as np
import pandas as pd
from mf_lab.services.config import logger

# --- SCORING ENGINE ---

def calculate_composite_score(metrics):
    """
    Calculates the raw composite score based on the new formula.
    raw_score = (alpha_norm * 2) + (sortino_norm * 10) + (100 - downside)

    metrics: dict containing 'alpha', 'sortino', 'downside'
    """
    try:
        # 1. Normalize Components (Clipping)
        # Alpha: Clip between -20 and 20
        alpha_raw = metrics.get('alpha', 0)
        alpha_norm = np.clip(alpha_raw, -20, 20)

        # Sortino: Clip between 0 and 5
        sortino_raw = metrics.get('sortino', 0)
        sortino_norm = np.clip(sortino_raw, 0, 5)

        # Downside: Raw value (lower is better, so 100 - downside is positive score)
        downside = metrics.get('downside', 100)

        # 2. Weighted Sum
        raw_score = (alpha_norm * 2) + \
                    (sortino_norm * 10) + \
                    (100 - downside)

        return raw_score

    except Exception as e:
        logger.error(f"Score calc error: {e}")
        return 0.0

def normalize_batch_scores(results_df):
    """
    Applies Min-Max normalization (0-100) to the 'Score' column of a DataFrame.
    Ideally applied per Category or per Universe.
    """
    if results_df.empty or 'Score' not in results_df.columns:
        return results_df

    try:
        df = results_df.copy()
        min_s = df['Score'].min()
        max_s = df['Score'].max()

        if max_s != min_s:
            df['Fortress Score'] = ((df['Score'] - min_s) / (max_s - min_s)) * 100
        else:
            # If all scores are identical (e.g. 1 item), default to 50 or 100
            df['Fortress Score'] = 100.0 if len(df) == 1 else 50.0

        df['Fortress Score'] = df['Fortress Score'].round(1)

        # Update the main Score column or keep both?
        # The prompt says "flow should be: raw composite → normalize across universe → final 0–100 score"
        # So 'Score' in the final output/DB should be this 0-100 value.
        df['Score'] = df['Fortress Score']

        return df

    except Exception as e:
        logger.error(f"Normalization error: {e}")
        return results_df
