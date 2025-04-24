SELECT
        time_period_start,
        volume_traded,
        trades_count,
        asset_pair
FROM {{ source('raw', 'crypto_volume_trades') }}