{{
  config(
    materialized = 'table',
    schema = 'supply_mart'
  )
}}

-- Fact Table: fct_order_items
WITH stg AS (
    -- อ้างอิง Core Model โดยตรง (จากโปรเจกต์ supply_chain)
    SELECT * FROM core_core.supply_chain
),

-- นำเข้า Keys จาก Dimension Tables (แก้ไข Schema เป็น mart_supply_mart)
dim_customer AS (
    SELECT customer_key, customer_id FROM mart_supply_mart.dim_customer
),
dim_product AS (
    SELECT product_key, product_card_id FROM mart_supply_mart.dim_product
),
dim_location AS (
    SELECT location_key, order_city, order_state, order_country, order_region, market, latitude, longitude FROM mart_supply_mart.dim_location
),
dim_shipping AS (
    SELECT shipping_key, shipping_mode, delivery_status FROM mart_supply_mart.dim_shipping
),
dim_date AS (
    SELECT date_key, full_date FROM mart_supply_mart.dim_date
),

final AS (
    SELECT
        -- Primary Key (PK) และ Business Key (แก้ไข: ใช้ MD5 Hash)
        md5(
            cast(stg.order_item_id as text) || '|' || 
            cast(stg.order_id as text)
        ) AS order_item_key,
        
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
    
    -- JOIN Location Key (ใช้ COALESCE เพื่อจัดการ NULL)
    LEFT JOIN dim_location dl 
        ON coalesce(stg.order_city, '') = coalesce(dl.order_city, '')
        AND coalesce(stg.order_state, '') = coalesce(dl.order_state, '')
        AND coalesce(stg.order_country, '') = coalesce(dl.order_country, '')
        AND coalesce(stg.order_region, '') = coalesce(dl.order_region, '')
        AND coalesce(stg.market, '') = coalesce(dl.market, '')
        
    -- JOIN Shipping Key (ใช้ COALESCE เพื่อจัดการ NULL)
    LEFT JOIN dim_shipping ds
        ON coalesce(stg.shipping_mode, '') = coalesce(ds.shipping_mode, '')
        AND coalesce(stg.delivery_status, '') = coalesce(ds.delivery_status, '')

    -- JOIN Date Keys 
    LEFT JOIN dim_date dod 
        ON CAST(stg.order_datetime AS DATE) = dod.full_date
    LEFT JOIN dim_date dsd 
        ON CAST(stg.shipping_datetime AS DATE) = dsd.full_date
)

SELECT * FROM final