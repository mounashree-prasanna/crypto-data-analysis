SELECT
        time_period_start,
        time_period_end,
        asset_pair,
        price_open,
        price_high,
        price_low,
        price_close
FROM {{ source('raw', 'crypto_asset_info') }}