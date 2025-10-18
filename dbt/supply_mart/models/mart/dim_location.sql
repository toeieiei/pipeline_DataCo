{{
  config(
    materialized = 'table',
    schema = 'supply_mart'
  )
}}

-- Dimension: dim_location (Order Location)
SELECT
    -- แก้ไข: ใช้ PostgreSQL MD5 Hash แทน dbt_utils.surrogate_key
    -- เชื่อมคอลัมน์ทั้งหมดด้วยตัวคั่น '|' ก่อน hash
    md5(
        coalesce(order_city, '') || '|' ||
        coalesce(order_state, '') || '|' ||
        coalesce(order_country, '') || '|' ||
        coalesce(order_region, '') || '|' ||
        coalesce(market, '') || '|' ||
        coalesce(cast(latitude as text), '') || '|' ||
        coalesce(cast(longitude as text), '')
    ) AS location_key,
    
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
    FROM core_core.supply_chain -- อ้างอิงตาราง Core ที่สร้างสำเร็จแล้ว
) AS unique_locations