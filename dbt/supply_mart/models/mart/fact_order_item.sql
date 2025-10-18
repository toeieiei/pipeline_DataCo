{{
  config(
    materialized = 'table',
    schema = 'supply_mart'
  )
}}

-- Fact Table: fct_order_items
WITH stg AS (
    SELECT * FROM {{ ref('stg_supply_chain') }}
),

-- นำเข้า Keys จาก Dimension Tables
dim_customer AS (
    SELECT customer_key, customer_id FROM {{ ref('dim_customer') }}
),
dim_product AS (
    SELECT product_key, product_card_id FROM {{ ref('dim_product') }}
),
dim_location AS (
    SELECT location_key, order_city, order_state, order_country, order_region, market, latitude, longitude FROM {{ ref('dim_location') }}
),
dim_shipping AS (
    SELECT shipping_key, shipping_mode, delivery_status FROM {{ ref('dim_shipping') }}
),
dim_date AS (
    SELECT date_key, full_date FROM {{ ref('dim_date') }}
),

final AS (
    SELECT
        -- Primary Key (PK) และ Business Key
        {{ dbt_utils.surrogate_key(['stg.order_item_id', 'stg.order_id']) }} AS order_item_key,
        stg.order_item_id,
        stg.order_id,
        
        -- Foreign Keys (FKs)
        dc.customer_key,
        dp.product_key,
        dl.location_key,
        ds.shipping_key,
        
        -- Date Keys
        dod.date_key AS order_date_key,
        dsd.date_key AS shipping_date_key,
        
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
        
    FROM stg
    -- JOIN Dimension Keys
    LEFT JOIN dim_customer dc ON stg.customer_id = dc.customer_id
    LEFT JOIN dim_product dp ON stg.product_card_id = dp.product_card_id
    
    -- JOIN Location Key (ใช้หลายคอลัมน์เพื่อหา Key)
    LEFT JOIN dim_location dl 
        ON stg.order_city = dl.order_city
        AND stg.order_state = dl.order_state
        AND stg.order_country = dl.order_country
        AND stg.order_region = dl.order_region
        AND stg.market = dl.market
    
    -- JOIN Shipping Key
    LEFT JOIN dim_shipping ds
        ON stg.shipping_mode = ds.shipping_mode
        AND stg.delivery_status = ds.delivery_status

    -- JOIN Date Keys (ต้อง JOIN กับ dim_date สองครั้ง)
    LEFT JOIN dim_date dod 
        ON CAST(stg.order_datetime AS DATE) = dod.full_date
    LEFT JOIN dim_date dsd 
        ON CAST(stg.shipping_datetime AS DATE) = dsd.full_date
)

SELECT * FROM final