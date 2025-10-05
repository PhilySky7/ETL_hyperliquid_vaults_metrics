import psycopg2
import os
import pathlib
import logging
from dotenv import load_dotenv


load_dotenv()

logger = logging.getLogger("Data_Base_Layer")
logger.setLevel(logging.INFO)


def get_connection():
    """
    Создаёт и возвращает соединение с PostgreSQL на основе env переменных.
    """
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
    )


def run_migration(schema_path: str = "schema.sql") -> None:
    """
    Применяет SQL схему из файла schema.sql.
    """
    sql = pathlib.Path(schema_path).read_text(encoding="utf-8")
    conn = get_connection()
    with conn, conn.cursor() as cur:
        logger.info("Data Base connection opened")
        cur.execute(sql)
        conn.commit()
    logger.info("Schema migrated")
    

def upsert_vault_data(conn, vault_data):
    """
    Вставить или обновить данные vault используя ON CONFLICT
    
    Args:
        vault_data (dict): Словарь с данными vault
    """
    upsert_query = """
    INSERT INTO vaults (
        vault_address, name, apr, total_pnl_usd, total_pnl_percent,
        monthly_account_value_change, weekly_account_value_change, win_days_ratio,
        max_drawdown, current_drawdown, daily_volatility, sharpe_ratio,
        average_recovery_days, daily_volume, trades_per_day, average_trade_size,
        average_position_holding_time, top_token_volume_share, seven_day_change,
        thirty_day_change, momentum_score, days_since_ath, consecutive_positive_days,
        tvl, follower_count, average_investment_per_follower, vault_age_days,
        leader_commission_rate, average_pnl_per_trade, profit_factor,
        return_to_drawdown_ratio, capital_efficiency, last_updated
    ) VALUES (
        %(vault_address)s, %(name)s, %(apr)s, %(total_pnl_usd)s, %(total_pnl_percent)s,
        %(monthly_account_value_change)s, %(weekly_account_value_change)s, %(win_days_ratio)s,
        %(max_drawdown)s, %(current_drawdown)s, %(daily_volatility)s, %(sharpe_ratio)s,
        %(average_recovery_days)s, %(daily_volume)s, %(trades_per_day)s, %(average_trade_size)s,
        %(average_position_holding_time)s, %(top_token_volume_share)s, %(seven_day_change)s,
        %(thirty_day_change)s, %(momentum_score)s, %(days_since_ath)s, %(consecutive_positive_days)s,
        %(tvl)s, %(follower_count)s, %(average_investment_per_follower)s, %(vault_age_days)s,
        %(leader_commission_rate)s, %(average_pnl_per_trade)s, %(profit_factor)s,
        %(return_to_drawdown_ratio)s, %(capital_efficiency)s, NOW()
    )
    ON CONFLICT (vault_address) DO UPDATE SET
        name = EXCLUDED.name,
        apr = EXCLUDED.apr,
        total_pnl_usd = EXCLUDED.total_pnl_usd,
        total_pnl_percent = EXCLUDED.total_pnl_percent,
        monthly_account_value_change = EXCLUDED.monthly_account_value_change,
        weekly_account_value_change = EXCLUDED.weekly_account_value_change,
        win_days_ratio = EXCLUDED.win_days_ratio,
        max_drawdown = EXCLUDED.max_drawdown,
        current_drawdown = EXCLUDED.current_drawdown,
        daily_volatility = EXCLUDED.daily_volatility,
        sharpe_ratio = EXCLUDED.sharpe_ratio,
        average_recovery_days = EXCLUDED.average_recovery_days,
        daily_volume = EXCLUDED.daily_volume,
        trades_per_day = EXCLUDED.trades_per_day,
        average_trade_size = EXCLUDED.average_trade_size,
        average_position_holding_time = EXCLUDED.average_position_holding_time,
        top_token_volume_share = EXCLUDED.top_token_volume_share,
        seven_day_change = EXCLUDED.seven_day_change,
        thirty_day_change = EXCLUDED.thirty_day_change,
        momentum_score = EXCLUDED.momentum_score,
        days_since_ath = EXCLUDED.days_since_ath,
        consecutive_positive_days = EXCLUDED.consecutive_positive_days,
        tvl = EXCLUDED.tvl,
        follower_count = EXCLUDED.follower_count,
        average_investment_per_follower = EXCLUDED.average_investment_per_follower,
        vault_age_days = EXCLUDED.vault_age_days,
        leader_commission_rate = EXCLUDED.leader_commission_rate,
        average_pnl_per_trade = EXCLUDED.average_pnl_per_trade,
        profit_factor = EXCLUDED.profit_factor,
        return_to_drawdown_ratio = EXCLUDED.return_to_drawdown_ratio,
        capital_efficiency = EXCLUDED.capital_efficiency,
        last_updated = NOW();
    """
    try:
        with conn, conn.cursor() as cursor:
            cursor.execute(upsert_query, vault_data)
            conn.commit()
        logger.info(f"Vault {vault_data['vault_address']} data successfully saved")
    except Exception as e:
        logger.error(f"Error while saving vault data {vault_data.get('vault_address')}: {e}")
        conn.rollback()
        raise


if __name__ == "__main__":
    # Optional: direct migration
    run_migration()