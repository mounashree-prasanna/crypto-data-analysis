with price_data as (
    select 
        date(time_period_start) as trading_day,
        asset_pair,
        price_open,
        price_high,
        price_low,
        price_close
    from {{ ref('crypto_asset_info') }}
),

volume_data as (
    select 
        date(time_period_start) as trading_day,
        asset_pair,
        volume_traded,
        trades_count
    from {{ ref('crypto_volume_trades') }}
)

select 
    p.trading_day,
    p.asset_pair,
    
    -- Price metrics
    p.price_open,
    p.price_close,
    p.price_high,
    p.price_low,
    round(((p.price_close - p.price_open) / p.price_open) * 100, 2) as price_change_pct,
    round((p.price_high - p.price_low), 6) as price_volatility,

    -- Volume metrics
    v.volume_traded,
    v.trades_count,
    round(v.volume_traded / nullif(v.trades_count, 0), 6) as avg_volume_per_trade

from price_data p   
join volume_data v 
  on p.trading_day = v.trading_day 
 and p.asset_pair = v.asset_pair
order by p.trading_day
