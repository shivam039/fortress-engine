import pandas as pd
from mf_lab.services.config import logger

# --- ALERT ENGINE ---

def check_integrity_rules(metrics, category):
    """
    Checks for integrity breaches (Drift) based on hard rules.
    Returns: (integrity_label, drift_status, drift_message)
    """
    try:
        beta = metrics.get('beta', 1.0)
        te = metrics.get('te', 0.0)
        cagr = metrics.get('cagr', 0.0) # Used for Debt Yield check

        # 1. Debt Logic
        is_debt = category in ["Liquid/Overnight", "Ultra Short/Low Duration", "Corporate Bond", "Gilt/Dynamic Bond"]
        if is_debt:
            if cagr < 5.75:
                return "🚨", "Critical", f"Yield {cagr:.1f}% < 5.75% (Repo Breach)"
            return "✅", "Stable", "Stable"

        # 2. Equity Logic
        # Tiered Beta Thresholds
        beta_limit = 1.15
        if category == "Flexi/Multi Cap": beta_limit = 1.20
        elif category == "Mid Cap": beta_limit = 1.25
        elif category == "Small Cap": beta_limit = 1.45

        # Tracking Error Threshold
        te_limit = 6.0 if category == "Large Cap" else 9.0

        issues = []
        severity = "Stable"
        drift_score = 0

        if beta > beta_limit:
            drift_score += 50
            issues.append(f"Beta {beta:.2f} > {beta_limit}")

        if te > te_limit:
            drift_score += 50
            issues.append(f"TE {te:.1f} > {te_limit}")

        if drift_score >= 100:
            severity = "Critical"
            label = "🚨"
            msg = f"Critical Drift: {' & '.join(issues)}"
        elif drift_score >= 50:
            severity = "Moderate"
            label = "⚠️"
            msg = f"Moderate Drift: {' & '.join(issues)}"
        else:
            label = "✅"
            msg = "Stable"

        return label, severity, msg

    except Exception as e:
        logger.error(f"Integrity check error: {e}")
        return "❓", "Unknown", "Data Error"

def generate_smart_alerts(fund_row, metrics, prev_metrics=None):
    """
    Generates rule-based alerts (Breaches, Drops, etc.)
    Returns a list of alert dictionaries.
    """
    alerts = []
    # fund_row key is lowercase 'symbol' in cron audit loop
    symbol = fund_row.get('symbol', 'Unknown')

    # 1. Critical Drift Alert
    if fund_row.get('drift_status') == 'Critical':
        alerts.append({
            "symbol": symbol,
            "alert_type": "Integrity Breach", # DB Column is 'alert_type', dataframe must match or rename
            "severity": "High",
            "message": fund_row.get('drift_message', 'Critical Drift Detected')
        })

    # 2. Max Drawdown Breach (> 20% for Large Cap? Or generic)
    # Let's say generic > 30% is High Severity
    if metrics.get('max_dd', 0) < -30:
        alerts.append({
            "symbol": symbol,
            "alert_type": "Risk Breach",
            "severity": "High",
            "message": f"Max Drawdown {metrics['max_dd']:.1f}% exceeds safety limits."
        })

    # 3. Win Rate Alert
    if metrics.get('win_rate', 100) < 40:
        alerts.append({
            "symbol": symbol,
            "alert_type": "Performance Alert",
            "severity": "Medium",
            "message": f"Win Rate {metrics['win_rate']:.0f}% indicates consistent underperformance."
        })

    # 4. Fortress Score Drop (needs previous history, skipped for single pass for now)

    return alerts

def send_telegram_alert(message):
    """
    Stub for Telegram Webhook.
    Logs to console/file for now.
    """
    logger.info(f"🔔 [TELEGRAM ALERT] {message}")
    # Implementation: requests.post(WEBHOOK_URL, json={"text": message})
