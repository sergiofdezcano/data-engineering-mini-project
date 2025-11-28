{{
    config(
        materialized='view'
    )
}}

WITH source_data AS (
    SELECT
        id,
        symbol,
        name,
        current_price,
        market_cap,
        market_cap_rank,
        fully_diluted_valuation,
        total_volume,
        high_24h,
        low_24h,
        price_change_24h,
        price_change_percentage_24h,
        market_cap_change_24h,
        market_cap_change_percentage_24h,
        circulating_supply,
        total_supply,
        max_supply,
        ath,
        ath_change_percentage,
        ath_date,
        atl,
        atl_change_percentage,
        atl_date,
        last_updated,
        extracted_at
    FROM {{ source('raw', 'raw_crypto_data') }}
)

SELECT
    id AS crypto_id,
    UPPER(symbol) AS symbol,
    name AS crypto_name,
    current_price,
    market_cap,
    market_cap_rank,
    COALESCE(fully_diluted_valuation, 0) AS fully_diluted_valuation,
    total_volume,
    high_24h,
    low_24h,
    price_change_24h,
    price_change_percentage_24h,
    market_cap_change_24h,
    market_cap_change_percentage_24h,
    circulating_supply,
    total_supply,
    max_supply,
    ath AS all_time_high,
    ath_change_percentage AS ath_change_pct,
    CAST(ath_date AS TIMESTAMP) AS ath_date,
    atl AS all_time_low,
    atl_change_percentage AS atl_change_pct,
    CAST(atl_date AS TIMESTAMP) AS atl_date,
    CAST(last_updated AS TIMESTAMP) AS last_updated,
    CAST(extracted_at AS TIMESTAMP) AS extracted_at
FROM source_data
