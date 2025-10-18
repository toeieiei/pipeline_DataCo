{{
  config(
    materialized = 'table',
    schema = 'supply_mart'
  )
}}

-- Dimension: dim_date (เฉพาะวันที่ที่ปรากฏในข้อมูล)
WITH all_dates AS (
    SELECT order_datetime AS full_datetime FROM {{ ref('stg_supply_chain') }}
    UNION
    SELECT shipping_datetime AS full_datetime FROM {{ ref('stg_supply_chain') }}
),

unique_dates AS (
    SELECT DISTINCT
        CAST(full_datetime AS DATE) AS full_date
    FROM all_dates
    WHERE full_datetime IS NOT NULL
)

SELECT
    -- สร้าง Surrogate Key (date_key) จากวันที่
    {{ dbt_utils.surrogate_key(['full_date']) }} AS date_key,
    
    full_date,
    EXTRACT(DAY FROM full_date) AS day_of_month,
    EXTRACT(DOW FROM full_date) AS day_of_week,
    TO_CHAR(full_date, 'Day') AS day_name,
    EXTRACT(MONTH FROM full_date) AS month_number,
    TO_CHAR(full_date, 'Month') AS month_name,
    EXTRACT(QUARTER FROM full_date) AS quarter,
    EXTRACT(YEAR FROM full_date) AS year
    
FROM unique_dates
ORDER BY full_date