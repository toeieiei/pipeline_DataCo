{{
  config(
    materialized = 'table',
    schema = 'supply_mart'
  )
}}

-- Dimension: dim_product
SELECT
    -- แก้ไข: ใช้ PostgreSQL MD5 Hash แทน dbt_utils.surrogate_key
    -- product_key ใช้ product_card_id เป็น Business Key
    md5(cast(product_card_id as text)) AS product_key,
    
    product_card_id,
    product_name,
    product_price,
    category_name,
    department_name
    
FROM (
    -- เลือกข้อมูลสินค้าที่ไม่ซ้ำกัน
    SELECT DISTINCT
        product_card_id,
        product_name,
        product_price,
        category_name,
        department_name
    FROM core_core.supply_chain -- อ้างอิงตรงไปยัง Table ที่สร้างสำเร็จแล้ว
    WHERE product_card_id IS NOT NULL 
) AS unique_products