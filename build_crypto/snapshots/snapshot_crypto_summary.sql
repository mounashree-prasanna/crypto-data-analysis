{% snapshot snapshot_crypto_summary %}

{{
    config(
        target_schema='analytics',
        unique_key='trading_day || asset_pair',
        strategy='check',
        check_cols=['price_open', 'price_close', 'price_high', 'price_low', 'volume_traded', 'trades_count']
    )
}}

select * 
from {{ ref('crypto_summary') }}

{% endsnapshot %}
