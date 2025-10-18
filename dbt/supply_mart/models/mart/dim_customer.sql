{{
  config(
    materialized = 'table',
    schema = 'supply_mart'
  )
}}

-- Dimension: dim_customer
SELECT
    -- แก้ไข: ใช้ PostgreSQL MD5 Hash แทน dbt_utils.surrogate_key
    md5(cast(customer_id as text)) AS customer_key, 
    -- ถ้า customer_key ใช้หลายคอลัมน์ ควรต่อสตริงด้วย || '|' ||
    
    customer_id,
    customer_fname,
    customer_lname,
    customer_segment,
    customer_email
    
FROM (
    -- เลือกข้อมูลลูกค้าที่ไม่ซ้ำกัน
    SELECT DISTINCT
        customer_id,
        customer_fname,
        customer_lname,
        customer_segment,
        customer_email
    FROM core_core.supply_chain -- อ้างอิงตรงไปยัง Table ที่สร้างสำเร็จแล้ว
    WHERE customer_id IS NOT NULL 
) AS unique_customers