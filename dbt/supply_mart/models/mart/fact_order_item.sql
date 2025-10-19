{{ config(
    materialized = 'table', 
    schema = 'mart_supply_mart'
) }}

WITH
-- 🚨 FIX 1: อ้างอิงข้อมูลดิบโดยใช้ {{ source }} 
--    (แก้ปัญหา 'stg_supply_chain' not found)
stg_raw AS (
    SELECT * FROM {{ source('supply_chain_source', 'supply_chain') }}
),

-- 🚨 FIX 2: กรองข้อมูลซ้ำซ้อน (Deduplication)
--    ใช้เฉพาะ 'order_item_id' เป็น Key หลัก (เพื่อให้ได้ 180,519 แถว)
stg_deduped AS (
    SELECT *
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER(
                PARTITION BY order_item_id
                ORDER BY order_datetime NULLS LAST, shipping_datetime NULLS LAST
            ) AS rn
        FROM stg_raw
    ) AS t
    WHERE rn = 1
),

-- ----------------------------------------------------------------
-- CTEs สำหรับ Dimension Lookups (โค้ดส่วนนี้ของคุณดีอยู่แล้ว)
-- ----------------------------------------------------------------
dc AS (
    SELECT customer_id, MAX(customer_key) AS customer_key
    FROM {{ ref('dim_customer') }}
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id
),
dp AS (
    SELECT product_card_id, MAX(product_key) AS product_key
    FROM {{ ref('dim_product') }}
    WHERE product_card_id IS NOT NULL
    GROUP BY product_card_id
),
dl AS (
    SELECT
        COALESCE(order_city,'') AS order_city,
        COALESCE(order_state,'') AS order_state,
        COALESCE(order_country,'') AS order_country,
        COALESCE(order_region,'') AS order_region,
        COALESCE(market,'') AS market,
        COALESCE(latitude,0)::numeric(9,6) AS latitude,
        COALESCE(longitude,0)::numeric(9,6) AS longitude,
        MAX(location_key) AS location_key
    FROM {{ ref('dim_location') }}
    GROUP BY 1,2,3,4,5,6,7
),
ds AS (
    SELECT
        COALESCE(shipping_mode,'') AS shipping_mode,
        COALESCE(delivery_status,'') AS delivery_status,
        MAX(shipping_key) AS shipping_key
    FROM {{ ref('dim_shipping') }}
    GROUP BY 1,2
),
dd AS (
    SELECT full_date AS date_day, MAX(date_key) AS date_key
    FROM {{ ref('dim_date') }}
    GROUP BY full_date
),

-- ----------------------------------------------------------------
-- Join ตารางทั้งหมดเข้าด้วยกัน
-- ----------------------------------------------------------------
joined AS (
    SELECT
      -- 🚨 FIX 3: ใช้ 'order_item_id' เป็น Primary Key (ตรงกับ schema.yml)
      --    (เราไม่จำเป็นต้องสร้าง 'order_item_key' ด้วย md5 อีก)
        stg.order_item_id, 
        stg.order_id,

      -- Foreign Keys
        dc.customer_key,
        dp.product_key,
        dl.location_key,
        ds.shipping_key,
        ddo.date_key AS order_date_key,
        dds.date_key AS shipping_date_key,

      -- Degenerate Dimensions
        stg.order_status,
        stg.transaction_type,

      -- Measures
        stg.order_item_quantity,
        stg.sales,
        stg.order_item_total,
        stg.order_profit_per_order,
        stg.benefit_per_order,
        stg.order_item_discount,
        stg.order_item_discount_rate,
        stg.late_delivery_risk,
        stg.days_for_shipment_scheduled,
        stg.days_for_shipping_real

    -- ⬅️ ดึงจาก 'stg_deduped' (180,519 แถว)
    FROM stg_deduped AS stg
    LEFT JOIN dc ON stg.customer_id = dc.customer_id
    LEFT JOIN dp ON stg.product_card_id = dp.product_card_id
    LEFT JOIN dl
        ON COALESCE(stg.order_city,'') = dl.order_city
        AND COALESCE(stg.order_state,'') = dl.order_state
        AND COALESCE(stg.order_country,'') = dl.order_country
        AND COALESCE(stg.order_region,'') = dl.order_region
        AND COALESCE(stg.market,'') = dl.market
        AND COALESCE(stg.latitude,0)::numeric(9,6) = dl.latitude
        AND COALESCE(stg.longitude,0)::numeric(9,6) = dl.longitude
    LEFT JOIN ds
        ON COALESCE(stg.shipping_mode,'') = ds.shipping_mode
        AND COALESCE(stg.delivery_status,'') = ds.delivery_status
    LEFT JOIN dd ddo ON CAST(stg.order_datetime AS DATE) = ddo.date_day
    LEFT JOIN dd dds ON CAST(stg.shipping_datetime AS DATE) = dds.date_day
)

-- 🚨 FIX 4: ลบ CTE 'final' ที่กรองซ้ำซ้อน (rn2) ทิ้ง
--    เพราะ 'stg_deduped' (rn) ได้กรองข้อมูลให้ unique แล้ว (180,519 แถว)
--    การ Join กับ Dim ที่ Unique จะไม่ทำให้แถวเพิ่มขึ้น

SELECT * FROM joined