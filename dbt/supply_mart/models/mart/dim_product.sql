{{
  config(
    materialized = 'table',
    schema = 'supply_mart'
  )
}}

-- Dimension: dim_product
SELECT
    -- สร้าง Surrogate Key (product_key)
    {{ dbt_utils.surrogate_key(['product_card_id']) }} AS product_key,
    
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
    FROM {{ ref('stg_supply_chain') }}
    WHERE product_card_id IS NOT NULL -- รับประกันว่ามี Business Key
) AS unique_products