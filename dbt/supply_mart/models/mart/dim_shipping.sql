{{
  config(
    materialized = 'table',
    schema = 'supply_mart'
  )
}}

-- Dimension: dim_shipping
SELECT
    -- แก้ไข: ใช้ PostgreSQL MD5 Hash แทน dbt_utils.surrogate_key
    -- เชื่อม 2 คอลัมน์เข้าด้วยกันเพื่อสร้าง key
    md5(
        coalesce(shipping_mode, '') || '|' || 
        coalesce(delivery_status, '')
    ) AS shipping_key,
    
    shipping_mode,
    delivery_status
    
FROM (
    -- เลือกข้อมูลการจัดส่งที่ไม่ซ้ำกัน
    SELECT DISTINCT
        shipping_mode,
        delivery_status
    FROM core_core.supply_chain -- อ้างอิงตรงไปยัง Table ที่สร้างสำเร็จแล้ว
) AS unique_shipping