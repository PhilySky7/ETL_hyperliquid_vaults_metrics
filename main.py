import logging
from dotenv import load_dotenv
from time import perf_counter, sleep
import os
from decimal import Decimal, InvalidOperation, ROUND_DOWN, getcontext

from api_client import get_vault_addresses, fetch_details
from metrics import (
    compute_performance,
    compute_risk,
    compute_trading,
    compute_trend,
    compute_capital,
    compute_efficiency,
)
from database import get_connection, upsert_vault_data

load_dotenv()
getcontext().prec = 28
BATCH_SIZE = int(os.getenv("BATCH_SIZE"))
BATCH_SLEEP_SECONDS = float(os.getenv("BATCH_SLEEP_SECONDS"))

# Database field limits (as Decimal to avoid float rounding to 10^10)
DECIMAL_18_8_MAX = Decimal("9999999999.99999999")  # DECIMAL(18,8) maximum value
DECIMAL_18_10_MAX = Decimal("9999999999.9999999999")  # DECIMAL(18,10) maximum value

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
logger = logging.getLogger("MainScript")



def validate_decimal_value(value, field_name, max_value=DECIMAL_18_8_MAX, scale_places: int = 8):
    """
    Validate and clamp decimal values to prevent database overflow.
    
    Args:
        value: The value to validate
        field_name: Name of the field for logging
        max_value: Maximum allowed value (default for DECIMAL(18,8))
        scale_places: Number of decimal places to quantize to (8 for DECIMAL(18,8), 10 for DECIMAL(18,10))
    
    Returns:
        Clamped Decimal value within valid range and precision
    """
    if value is None:
        return Decimal("0")

    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        logger.warning(f"Field '{field_name}' has invalid value {value}, setting to 0.0")
        return Decimal("0")

    if decimal_value > max_value:
        logger.warning(f"Field '{field_name}' value {decimal_value} exceeds upper limit {max_value}, clamping to {max_value}")
        decimal_value = max_value
    elif decimal_value < -max_value:
        logger.warning(f"Field '{field_name}' value {decimal_value} exceeds lower limit {-max_value}, clamping to {-max_value}")
        decimal_value = -max_value

    # Quantize to avoid rounding up beyond the max at the database scale
    quant = Decimal(1).scaleb(-scale_places)  # e.g., 1e-8 or 1e-10
    decimal_value = decimal_value.quantize(quant, rounding=ROUND_DOWN)
    return decimal_value


def get_users_from_details(details: list):
    """Return each user from vaultDetals."""
    
    users: list[str] = []
    for detail in details:
        if not isinstance(detail, dict):
            continue
        user = detail.get("leader")
        if isinstance(user, str):
            users.append(user)
    return users

def build_vault(vault_address: str, vault_detail: dict, user_fills: list) -> dict:
    """Calculating all 30 metrics and prepare dict for upsert."""
    performance = compute_performance(vault_detail)
    risk = compute_risk(vault_detail)
    trading = compute_trading(user_fills)
    trend = compute_trend(vault_detail)
    capital = compute_capital(vault_detail)
    efficiency = compute_efficiency(details=vault_detail, fills=user_fills)

    name = vault_detail.get("name", "")
    
    # Validate all DECIMAL(18,8) fields to prevent overflow
    row = {
        "vault_address": vault_address,
        "name": name,
        "apr": validate_decimal_value(performance.get("apr", 0.0), "apr", max_value=DECIMAL_18_8_MAX, scale_places=8),
        "total_pnl_usd": performance.get("total_pnl_usd", 0.0),  # DECIMAL(38,18) - no validation needed
        "total_pnl_percent": validate_decimal_value(performance.get("total_pnl_percent", 0.0), "total_pnl_percent", max_value=DECIMAL_18_8_MAX, scale_places=8),
        "monthly_account_value_change": validate_decimal_value(performance.get("monthly_account_value_change", 0.0), "monthly_account_value_change", max_value=DECIMAL_18_8_MAX, scale_places=8),
        "weekly_account_value_change": validate_decimal_value(performance.get("weekly_account_value_change", 0.0), "weekly_account_value_change", max_value=DECIMAL_18_8_MAX, scale_places=8),
        "win_days_ratio": validate_decimal_value(performance.get("win_days_ratio", 0.0), "win_days_ratio", max_value=DECIMAL_18_8_MAX, scale_places=8),
        "max_drawdown": validate_decimal_value(risk.get("max_drawdown", 0.0), "max_drawdown", max_value=DECIMAL_18_8_MAX, scale_places=8),
        "current_drawdown": validate_decimal_value(risk.get("current_drawdown", 0.0), "current_drawdown", max_value=DECIMAL_18_8_MAX, scale_places=8),
        "daily_volatility": validate_decimal_value(risk.get("daily_volatility", 0.0), "daily_volatility", max_value=DECIMAL_18_10_MAX, scale_places=10),  # DECIMAL(18,10)
        "sharpe_ratio": validate_decimal_value(risk.get("sharpe_ratio", 0.0), "sharpe_ratio", max_value=DECIMAL_18_10_MAX, scale_places=10),  # DECIMAL(18,10)
        "average_recovery_days": validate_decimal_value(risk.get("average_recovery_days", 0.0), "average_recovery_days", max_value=DECIMAL_18_8_MAX, scale_places=8),
        "daily_volume": trading.get("daily_volume", 0.0),  # DECIMAL(38,18) - no validation needed
        "trades_per_day": validate_decimal_value(trading.get("trades_per_day", 0.0), "trades_per_day", max_value=DECIMAL_18_8_MAX, scale_places=8),
        "average_trade_size": trading.get("average_trade_size", 0.0),  # DECIMAL(38,18) - no validation needed
        "average_position_holding_time": validate_decimal_value(trading.get("average_position_holding_time", 0.0), "average_position_holding_time", max_value=DECIMAL_18_8_MAX, scale_places=8),
        "top_token_volume_share": validate_decimal_value(trading.get("top_token_volume_share", 0.0), "top_token_volume_share", max_value=DECIMAL_18_8_MAX, scale_places=8),
        "seven_day_change": validate_decimal_value(trend.get("seven_day_change", 0.0), "seven_day_change", max_value=DECIMAL_18_8_MAX, scale_places=8),
        "thirty_day_change": validate_decimal_value(trend.get("thirty_day_change", 0.0), "thirty_day_change", max_value=DECIMAL_18_8_MAX, scale_places=8),
        "momentum_score": validate_decimal_value(trend.get("momentum_score", 0.0), "momentum_score", max_value=DECIMAL_18_10_MAX, scale_places=10),  # DECIMAL(18,10)
        "days_since_ath": trend.get("days_since_ath", 0),  # INTEGER
        "consecutive_positive_days": trend.get("consecutive_positive_days", 0),  # INTEGER
        "tvl": capital.get("tvl", 0.0),  # DECIMAL(38,18) - no validation needed
        "follower_count": capital.get("follower_count", 0),  # INTEGER
        "average_investment_per_follower": capital.get("average_investment_per_follower", 0.0),  # DECIMAL(38,18) - no validation needed
        "vault_age_days": capital.get("vault_age_days", 0),  # INTEGER
        "leader_commission_rate": validate_decimal_value(capital.get("leader_commission_rate", 0.0), "leader_commission_rate", max_value=DECIMAL_18_8_MAX, scale_places=8),
        "average_pnl_per_trade": efficiency.get("average_pnl_per_trade", 0.0),  # DECIMAL(38,18) - no validation needed
        "profit_factor": validate_decimal_value(efficiency.get("profit_factor", 0.0), "profit_factor", max_value=DECIMAL_18_10_MAX, scale_places=10),  # DECIMAL(18,10)
        "return_to_drawdown_ratio": validate_decimal_value(efficiency.get("return_to_drawdown_ratio", 0.0), "return_to_drawdown_ratio", max_value=DECIMAL_18_10_MAX, scale_places=10),  # DECIMAL(18,10)
        "capital_efficiency": validate_decimal_value(efficiency.get("capital_efficiency", 0.0), "capital_efficiency", max_value=DECIMAL_18_8_MAX, scale_places=8),
    }
    return row


if __name__ == "__main__":
    logger.info("Starting ETL")
    all_addresses = get_vault_addresses()
    logger.info("Fetched %d vault addresses", len(all_addresses))
    if not all_addresses:
        raise SystemExit(0)

    try:
        conn = get_connection()
        if all_addresses:
            # processing all addresses by batch
            total_addresses = len(all_addresses)
            total_batches = (total_addresses + BATCH_SIZE - 1) // BATCH_SIZE
            
            logger.info("Processing %d addresses in %d batches of %d", 
                       total_addresses, total_batches, BATCH_SIZE)
            
            for batch_num in range(total_batches):
                start_idx = batch_num * BATCH_SIZE
                end_idx = min(start_idx + BATCH_SIZE, total_addresses)
                batch_addresses = all_addresses[start_idx:end_idx]
                
                logger.info("Processing batch %d/%d: addresses %d-%d", 
                           batch_num + 1, total_batches, start_idx + 1, end_idx)
                
                # get details for current batch
                start = perf_counter()
                details = fetch_details(
                    body_field="vaultAddress",
                    addresses=batch_addresses,
                )
                duration = perf_counter() - start
                logger.info(
                    "Fetched vaultDetails for %d vaults in %.2f seconds",
                    len(details),
                    duration,
                )

                # get users from details for current batch
                users = get_users_from_details(details)
                if users:
                    start = perf_counter()
                    user_fills = fetch_details(
                        body_field="user",
                        addresses=users,
                    )
                    duration = perf_counter() - start
                    logger.info(
                        "Fetched userFills for %d users in %.2f seconds",
                        len(user_fills),
                        duration,
                    )
                else:
                    user_fills = []
                    logger.warning("No users found in batch %d", batch_num + 1)

                for addr, detail, fill in zip(batch_addresses, details, user_fills):
                    row = build_vault(vault_address=addr, vault_detail=detail, user_fills=fill)
                    upsert_vault_data(conn, row)
                
                logger.info("Completed batch %d/%d", batch_num + 1, total_batches)
                
                if batch_num < total_batches - 1:
                    logger.info("Sleeping for %.1f seconds before next batch...", BATCH_SLEEP_SECONDS)
                    sleep(BATCH_SLEEP_SECONDS)
            
            logger.info("ETL process completed successfully")
    except Exception as e:
        logger.exception(f"Failed the process with error: {e}")
    finally:
        conn.close()
        logger.info("DB connection closed")