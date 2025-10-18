{{
  config(
    materialized = 'table',
    schema = 'supply_mart'
  )
}}

-- Dimension: dim_customer
SELECT
    -- สร้าง Surrogate Key (customer_key)
    {{ dbt_utils.surrogate_key(['customer_id']) }} AS customer_key,
    
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
    FROM {{ ref('stg_supply_chain') }}
    WHERE customer_id IS NOT NULL -- รับประกันว่ามี Business Key
) AS unique_customers