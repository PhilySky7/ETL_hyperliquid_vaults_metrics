from typing import Any, Dict, List, Tuple, Literal
import math
import statistics
from collections import defaultdict
from datetime import datetime, timezone
import logging


logger = logging.getLogger("Metrics")
logger.setLevel(logging.INFO)

RISK_FREE_RATE_ANNUAL = 0.05  # 5% annual
DAYS_PER_YEAR = 365
DAILY_RISK_FREE = RISK_FREE_RATE_ANNUAL / DAYS_PER_YEAR
MS_PER_DAY = 24 * 60 * 60 * 1000

def _to_float(value) -> float:
    if value is None or value == '':
        logger.debug("_to_float: empty value -> 0.0")
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except Exception as exc:
            logger.error(f"{exc} cannot cast string '{value}' to float")
            return 0.0
    logger.error(f"_to_float: unexpected type {type(value)}: {value}")
    return 0.0

def get_porfolio_data(details: Dict[str, Any], period: str) -> Dict[str, Any]:
    for p_name, p_data in details.get("portfolio", []):
        if p_name == period:
            return p_data or {}
    return {}

def _series_values(bucket: Dict[str, Any], field: str) -> List[Tuple[int, float]]:
    output = []
    for ts, val in bucket.get(field, []):
        output.append((int(ts), _to_float(val)))
    return output

def pct_change(period):
        series = _series_values(period, "accountValueHistory")
        if len(series) >= 2 and series[0][1] != 0:
            return ((series[-1][1] - series[0][1]) / series[0][1]) * 100.0
        return 0.0

def compute_performance(details: Dict[str, Any]) -> Dict[str, float]:
    metrics: Dict[str, float] = {}

    all_time = get_porfolio_data(details, "allTime")
    account_history = _series_values(all_time, "accountValueHistory")

    apr = _to_float(details.get("apr", 0.0))
    metrics["apr"] = apr * 100  # доли (0..1) в проценты

    pnl_history = _series_values(all_time, "pnlHistory")
    last_pnl = _to_float(pnl_history[-1][1]) if pnl_history else 0.0
    metrics["total_pnl_usd"] = last_pnl

    try:
        if not account_history or not pnl_history:
            metrics["total_pnl_percent"] = 0.0
        else:
            current_pnl = _to_float(pnl_history[-1][1])
            start_value = _to_float(account_history[0][1])
            metrics["total_pnl_percent"] = (
                ((current_pnl - start_value) / start_value) * 100.0
                if start_value
                else 0.0
            )
    except Exception as e:
        logger.error(f"Ошибка расчета Total PnL Percent: {e}")
        metrics["total_pnl_percent"] = 0.0
        
    # monthly / weekly
    month = get_porfolio_data(details, "month")
    week = get_porfolio_data(details, "week")
    

    metrics["monthly_account_value_change"] = pct_change(month)
    metrics["weekly_account_value_change"] = pct_change(week)

    # Win Days Ratio по pnlHistory приращениям
    win_days = 0
    total_days = len(pnl_history) - 1
    if len(pnl_history) >= 2 and total_days != 0:
        for i in range(1, len(pnl_history)):
            if pnl_history[i][1] > pnl_history[i-1][1]:
                win_days += 1
        metrics["win_days_ratio"] = (win_days / total_days) * 100.0
    else:
        metrics["win_days_ratio"] = 0.0
    return metrics

def drawdown_stats(
    account_history: List[Tuple[int, float]],
    drawdown_type: Literal["max", "current"],
) -> float:
    """
    Max Drawdown - максимальная просадка от пика.
    Current Drawdown - текущая просадка от пика.
    """
    if not account_history:
        logger.warning("drawdown_stats: empty account_history -> 0.0")
        return 0.0
    match drawdown_type:
        case "max":
            peak = account_history[0][1]
            max_dd = 0.0
            for _, val in account_history:
                if val > peak:
                    peak = val
                if peak != 0.0:
                    dd = (peak - val) / peak
                    max_dd = max(max_dd, dd)
                else:

                    continue
            drawdown = max_dd
        case "current":
            values = [_to_float(item[1]) for item in account_history]
            peak = max(values)
            current = values[-1]
            if peak != 0.0:
                drawdown = (peak - current) / peak
            else:
                return 0.0
    return drawdown * 100.0

def compute_risk(details: Dict[str, Any]) -> Dict[str, float]:
    metrics: Dict[str, float] = {}

    all_time = get_porfolio_data(details, "allTime")
    account_history = _series_values(all_time, "accountValueHistory")

    max_dd = drawdown_stats(
        account_history=account_history,
        drawdown_type="max"
    )
    cur_dd = drawdown_stats(
        account_history=account_history,
        drawdown_type="current"
    )
    metrics["max_drawdown"] = max_dd
    metrics["current_drawdown"] = cur_dd

    # daily volatility
    daily = []
    for i in range(1, len(account_history)):
        prev = account_history[i-1][1]
        cur = account_history[i][1]
        if prev != 0:
            daily_return = (cur - prev) / prev
            daily.append(daily_return)
        else:
            continue
    vol = statistics.stdev(daily) if len(daily) >= 2 else 0.0 # or pstdev
    metrics["daily_volatility"] = vol
    
    avg_return = statistics.mean(daily) if daily else 0.0
    excess_return = avg_return - DAILY_RISK_FREE
    metrics["sharpe_ratio"] = (excess_return / vol) * math.sqrt(365.0) if vol > 0 else 0.0
    
    # recovery days 10%+
    recovery_periods = []
    peak = -1.0
    dd_start_ts = None
    for ts, value in account_history:
        if value > peak:
            peak = value
            if dd_start_ts is not None:
                # восстановление к новому пику
                days = (ts - dd_start_ts) / (MS_PER_DAY)
                if days > 0:
                    recovery_periods.append(days)
                dd_start_ts = None
        else:
            if peak != 0.0:
                drawdown = (peak - value) / peak
            else:
                drawdown = 0.0
            if peak > 0.0 and drawdown >= 0.10 and dd_start_ts is None:
                dd_start_ts = ts
    metrics["average_recovery_days"] = statistics.mean(recovery_periods) if recovery_periods else 0.0
    return metrics

def compute_trading(fills: List[Dict[str, Any]]) -> Dict[str, float]:
    if not fills:
        return {
            'daily_volume': 0.0,
            'trades_per_day': 0.0,
            'average_trade_size': 0.0,
            'average_position_holding_time': 0.0,
            'top_token_volume_share': 0.0,
        }

    daily_volumes = defaultdict(float)
    daily_trades = defaultdict(int)
    token_volume = defaultdict(float)
    coins = defaultdict(lambda: {"opens": [], "closes": []})

    for fill in fills:
        if not isinstance(fill, dict):
            logger.warning(f"Некорректный fill: {fill} ({type(fill)}), пропускается!")
            continue
        # Daily Volume, Trades Per Day, Avg Trade Size (1, 2, 3)
        px = _to_float(fill.get("px", 0))
        sz = _to_float(fill.get("sz", 0))
        timestamp = int(fill.get("time", 0))
        date = datetime.fromtimestamp(timestamp * 0.001, tz=timezone.utc).date()
        vol = px * sz
        
        daily_volumes[date] += vol
        daily_trades[date] += 1

        # Average Position Holding Time & Top Token Volume Share (4, 5)
        coin = fill.get("coin", "")
        direction = fill.get("dir", "")

        fill_data = {
            'time': timestamp,
            'sz': sz,
            'px': px,
        }
                
        if 'Open' in direction:
            coins[coin]['opens'].append(fill_data)
        elif 'Close' in direction:
            coins[coin]['closes'].append(fill_data)

        token_volume[coin] += vol

    # 1 - 3
    days = max(len(daily_volumes), 1)
    total_volume = sum(daily_volumes.values())
    avg_volume = total_volume / days

    total_trades = sum(daily_trades.values())
    avg_trades_day = total_trades / days
    avg_trade_size = (total_volume / total_trades) if total_trades else 0.0

    # 4 -5
    top_share = 0.0
    if token_volume:
        top_volume = max(token_volume.values())
        total_token_volume = sum(token_volume.values())
        top_share = (top_volume / total_token_volume) * 100.0 if total_token_volume else 0.0

    def average_position_holding_time(coins) -> list:
        """
        Average Position Holding Time - среднее время удержания позиции
        Использует FIFO для сопоставления открывающих и закрывающих сделок
        """
        holding_times = []
        for _, trades in coins.items():
            opens = sorted(trades['opens'], key=lambda x: x['time'])
            closes = sorted(trades['closes'], key=lambda x: x['time'])
            
            # FIFO matching
            open_idx = 0
            open_time = 0
            remaining_open_sz = 0.0
            
            for close in closes:
                close_sz = close['sz']
                close_time = close['time']
                
                while close_sz > 0 and open_idx < len(opens):
                    if remaining_open_sz == 0:
                        remaining_open_sz = opens[open_idx]['sz']
                        open_time = opens[open_idx]['time']
                        open_idx += 1
                    
                    matched_sz = min(remaining_open_sz, close_sz)
                    
                    hold_time_ms = close_time - open_time
                    if hold_time_ms > 0:
                        hold_time_hours = hold_time_ms * 24 / MS_PER_DAY  # ms -> hours
                        holding_times.append(hold_time_hours)
                    
                    remaining_open_sz -= matched_sz
                    close_sz -= matched_sz
        return holding_times
    
    holding_hours = average_position_holding_time(coins=coins)
    avg_hold = statistics.mean(holding_hours) if holding_hours else 0.0

    return {
        "daily_volume": avg_volume,
        "trades_per_day": avg_trades_day,
        "average_trade_size": avg_trade_size,
        "average_position_holding_time": avg_hold,
        "top_token_volume_share": top_share,
    }

def compute_trend(details: Dict[str, Any]) -> Dict[str, float]:
    metrics: Dict[str, float] = {}

    all_time = get_porfolio_data(details, "allTime")
    account_history = _series_values(all_time, "accountValueHistory")

    week = get_porfolio_data(details, "week")
    month = get_porfolio_data(details, "month")

    # в процентах
    metrics["seven_day_change"] = pct_change(week)
    metrics["thirty_day_change"] = pct_change(month)
    
    # momentum на согласованной базе: 7d / std(дневн. доходностей за 7д)
    
    daily = []
    for i in range(1, min(8, len(account_history))):
        previous = account_history[-i-1][1]
        current = account_history[-i][1]
        if previous != 0:
            daily_return = (current - previous) / previous
            daily.append(daily_return)
        else:
            continue
    # в долях
    vol_7d = statistics.pstdev(daily) if len(daily) >= 2 else 0.0

    # вычисление в долях
    metrics["momentum_score"] = (
        metrics["seven_day_change"] * 0.01 / vol_7d
        if vol_7d > 0 else 0.0
    )
    
    # days since ATH
    if account_history:
        max_idx = max(
            range(len(account_history)),
            key=lambda i: account_history[i][1],
        )
        ath_ts = account_history[max_idx][0]
        current_ts = account_history[-1][0]
        metrics["days_since_ath"] = int(
            (current_ts - ath_ts) / MS_PER_DAY
        )
    else:
        metrics["days_since_ath"] = 0
    
    # consecutive positive days по pnlHistory
    pnl_history = _series_values(all_time, "pnlHistory")
    counter_positive = 0
    for i in range(len(pnl_history)-1, 0, -1):
        if (pnl_history[i][1] - pnl_history[i-1][1]) > 0:
            counter_positive += 1
        else:
            break
    metrics["consecutive_positive_days"] = counter_positive
    return metrics

def compute_capital(details: Dict[str, Any]) -> Dict[str, float]:
    metrics: Dict[str, float] = {}

    all_time = get_porfolio_data(details, "allTime")
    account_history = _series_values(all_time, "accountValueHistory")

    # no access to vaultSummaries to parse tvl and createTimeMillis
    # fallback:
    metrics["tvl"] = account_history[-1][1]

    followers = details.get("followers", [])
    metrics["follower_count"] = len(followers)
    
    if metrics["follower_count"]:
        metrics["average_investment_per_follower"] = metrics["tvl"] / metrics["follower_count"]
    else:
        metrics["average_investment_per_follower"] = 0.0
    
    if len(account_history) > 0:
        create_ms = account_history[0][0]
    else:
        create_ms = 0.0
    
    now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    vault_age_days = int((now_ms - create_ms) / MS_PER_DAY) if create_ms else 0.0
    metrics["vault_age_days"] = vault_age_days

    metrics["leader_commission_rate"] = _to_float(details.get("leaderCommission", 0))
    return metrics

def compute_efficiency(details: Dict[str, Any], fills: List[Dict[str, Any]]) -> Dict[str, float]:
    all_time = get_porfolio_data(details, "allTime")
    account_history = _series_values(all_time, "accountValueHistory")
    
    # Average PnL Per Trade
    pnls = []
    if fills:
        for fill in fills:
            if not isinstance(fill, dict):
                logger.warning(f"Некорректный fill: {fill} ({type(fill)}), пропускается!")
                continue
            closed_pnl = fill.get("closedPnl")
            if closed_pnl is not None:
                closed_pnl = _to_float(closed_pnl)
                pnls.append(closed_pnl)

    total_pnl = sum(pnls)
    avg_pnl_trade = (total_pnl / len(pnls)) if pnls else 0.0

    total_profit = sum(p for p in pnls if p > 0)
    total_loss = sum(-p for p in pnls if p < 0)
    profit_factor = (total_profit / total_loss) if total_loss > 0 else 0.0
    
    apr = _to_float(details.get("apr", 0.0))
    risk = compute_risk(details)
    max_dd = risk.get("max_drawdown", 0.0)
    return_to_drawdown_ratio = (apr / (max_dd * 0.01)) if max_dd > 0 else 0.0
    
    if account_history:
        avg_tvl = statistics.mean([item[1] for item in account_history])
    else:
        avg_tvl = 0.0
    
    capital_eff = ((total_pnl / avg_tvl) * 100.0) if avg_tvl > 0 else 0.0
    return {
        "average_pnl_per_trade": avg_pnl_trade,
        "profit_factor": profit_factor,
        "return_to_drawdown_ratio": return_to_drawdown_ratio,
        "capital_efficiency": capital_eff,
    }
