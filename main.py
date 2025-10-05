import logging
from dotenv import load_dotenv
from time import perf_counter, sleep

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
logger = logging.getLogger("MainScript")


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
    apr = performance.get("apr", 0.0)

    row = {
        "vault_address": vault_address,
        "name": name,
        "apr": apr,
        "total_pnl_usd": performance.get("total_pnl_usd", 0.0),
        "total_pnl_percent": performance.get("total_pnl_percent", 0.0),
        "monthly_account_value_change": performance.get("monthly_account_value_change", 0.0),
        "weekly_account_value_change": performance.get("weekly_account_value_change", 0.0),
        "win_days_ratio": performance.get("win_days_ratio", 0.0),
        "max_drawdown": risk.get("max_drawdown", 0.0),
        "current_drawdown": risk.get("current_drawdown", 0.0),
        "daily_volatility": risk.get("daily_volatility", 0.0),
        "sharpe_ratio": risk.get("sharpe_ratio", 0.0),
        "average_recovery_days": risk.get("average_recovery_days", 0.0),
        "daily_volume": trading.get("daily_volume", 0.0),
        "trades_per_day": trading.get("trades_per_day", 0.0),
        "average_trade_size": trading.get("average_trade_size", 0.0),
        "average_position_holding_time": trading.get("average_position_holding_time", 0.0),
        "top_token_volume_share": trading.get("top_token_volume_share", 0.0),
        "seven_day_change": trend.get("seven_day_change", 0.0),
        "thirty_day_change": trend.get("thirty_day_change", 0.0),
        "momentum_score": trend.get("momentum_score", 0.0),
        "days_since_ath": trend.get("days_since_ath", 0.0),
        "consecutive_positive_days": trend.get("consecutive_positive_days", 0.0),
        "tvl": capital.get("tvl", 0.0),
        "follower_count": capital.get("follower_count", 0),
        "average_investment_per_follower": capital.get("average_investment_per_follower", 0.0),
        "vault_age_days": capital.get("vault_age_days", 0),
        "leader_commission_rate": capital.get("leader_commission_rate", 0.0),
        "average_pnl_per_trade": efficiency.get("average_pnl_per_trade", 0.0),
        "profit_factor": efficiency.get("profit_factor", 0.0),
        "return_to_drawdown_ratio": efficiency.get("return_to_drawdown_ratio", 0.0),
        "capital_efficiency": efficiency.get("capital_efficiency", 0.0),
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
            start = perf_counter()
            details = fetch_details(
                body_field="vaultAddress",
                addresses=all_addresses[:5],
            )
            duration = perf_counter() - start
            logger.info(
                "Fetched vaultDetails for %d vaults in %.2f seconds",
                len(details),
                duration,
            )

            users = get_users_from_details(details)
            start = perf_counter()
            user_fills = fetch_details(
                body_field="user",
                addresses=users[:5],
            )
            duration = perf_counter() - start
            logger.info(
                "Fetched userFills for %d users in %.2f seconds",
                len(user_fills),
                duration,
            )
            for addr, detail, fill in zip(all_addresses[:5], details[:5], user_fills[:5]):      
                row = build_vault(vault_address=addr, vault_detail=detail, user_fills=fill)
                upsert_vault_data(conn, row)
                sleep(0.1)
    except Exception as e:
        logger.exception(f"Failed the process with error: {e}")
    finally:
        conn.close()
        logger.info("DB connection closed")