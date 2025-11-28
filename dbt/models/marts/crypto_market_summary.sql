{{
    config(
        materialized='table'
    )
}}

WITH crypto_data AS (
    SELECT * FROM {{ ref('stg_crypto_data') }}
),

ranked_cryptos AS (
    SELECT
        crypto_id,
        symbol,
        crypto_name,
        current_price,
        market_cap,
        market_cap_rank,
        total_volume,
        price_change_percentage_24h,
        market_cap_change_percentage_24h,

        -- Volatility indicator
        CASE
            WHEN ABS(price_change_percentage_24h) > 10 THEN 'High'
            WHEN ABS(price_change_percentage_24h) > 5 THEN 'Medium'
            ELSE 'Low'
        END AS volatility_24h,

        -- Market cap category
        CASE
            WHEN market_cap > 10000000000 THEN 'Large Cap'
            WHEN market_cap > 1000000000 THEN 'Mid Cap'
            ELSE 'Small Cap'
        END AS market_cap_category,

        -- Performance indicator
        CASE
            WHEN price_change_percentage_24h > 5 THEN 'Strong Gain'
            WHEN price_change_percentage_24h > 0 THEN 'Gain'
            WHEN price_change_percentage_24h > -5 THEN 'Loss'
            ELSE 'Strong Loss'
        END AS performance_24h,

        last_updated,
        extracted_at

    FROM crypto_data
    WHERE market_cap_rank IS NOT NULL
)

SELECT
    crypto_id,
    symbol,
    crypto_name,
    current_price,
    market_cap,
    market_cap_rank,
    total_volume,
    ROUND(price_change_percentage_24h::NUMERIC, 2) AS price_change_24h_pct,
    ROUND(market_cap_change_percentage_24h::NUMERIC, 2) AS market_cap_change_24h_pct,
    volatility_24h,
    market_cap_category,
    performance_24h,
    ROUND((total_volume::NUMERIC / market_cap::NUMERIC * 100), 2) AS volume_to_mcap_ratio,
    last_updated,
    extracted_at
FROM ranked_cryptos
ORDER BY market_cap_rank
