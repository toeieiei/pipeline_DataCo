{{
  config(
    materialized = 'table',
    schema = 'supply_mart'
  )
}}

-- Dimension: dim_shipping
SELECT
    -- สร้าง Surrogate Key (shipping_key)
    {{ dbt_utils.surrogate_key(['shipping_mode', 'delivery_status']) }} AS shipping_key,
    
    shipping_mode,
    delivery_status
    
FROM (
    -- เลือกข้อมูลการจัดส่งที่ไม่ซ้ำกัน
    SELECT DISTINCT
        shipping_mode,
        delivery_status
    FROM {{ ref('stg_supply_chain') }}
) AS unique_shipping