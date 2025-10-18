{{
  config(
    materialized = 'table',
    schema = 'supply_mart'
  )
}}

-- Dimension: dim_location (Order Location)
SELECT
    -- สร้าง Surrogate Key (location_key) โดยใช้ข้อมูลภูมิศาสตร์ทั้งหมด
    {{ dbt_utils.surrogate_key(['order_city', 'order_state', 'order_country', 'order_region', 'market', 'latitude', 'longitude']) }} AS location_key,
    
    order_city,
    order_state,
    order_country,
    order_region,
    market,
    latitude,
    longitude
    
FROM (
    -- เลือกข้อมูลภูมิศาสตร์ที่ไม่ซ้ำกัน
    SELECT DISTINCT
        order_city,
        order_state,
        order_country,
        order_region,
        market,
        latitude,
        longitude
    FROM {{ ref('stg_supply_chain') }}
) AS unique_locations