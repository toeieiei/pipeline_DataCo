{{ config(materialized='table', schema='core') }}

with src as (
  -- อ้างอิงตาราง source ที่ถูกต้องแล้ว
  select * from {{ source('supply_chain_source', 'stg_supply_chain') }}
),
clean as (
  select
    -- 1. CUSTOMER & SEGMENTATION
    Customer_Id                               as raw_customer_id,
    nullif(Customer_Fname, '')                as raw_customer_fname,
    nullif(Customer_Lname, '')                as raw_customer_lname,
    nullif(Customer_Segment, '')              as raw_customer_segment,
    nullif(Customer_Email, '')                as raw_customer_email,

    -- 2. PRODUCT DETAILS
    Product_Card_Id                           as raw_product_card_id,
    nullif(Product_Name, '')                  as raw_product_name,
    nullif(Product_Price, '')                 as raw_product_price,
    nullif(Category_Name, '')                 as raw_category_name,
    nullif(Department_Name, '')               as raw_department_name,

    -- 3. LOCATION & GEOGRAPHY
    nullif(Order_City, '')                    as raw_order_city,
    nullif(Order_State, '')                   as raw_order_state,
    nullif(Order_Country, '')                 as raw_order_country,
    nullif(Order_Region, '')                  as raw_order_region,
    nullif(Market, '')                        as raw_market,
    nullif(Latitude, '')                      as raw_latitude,
    nullif(Longitude, '')                     as raw_longitude,
    nullif(Order_Zipcode, '')                 as raw_order_zipcode,

    -- 4. SHIPPING & DELIVERY
    nullif(Shipping_Mode, '')                 as raw_shipping_mode,
    nullif(Delivery_Status, '')               as raw_delivery_status,
    nullif(Late_delivery_risk, '')            as raw_late_delivery_risk,
    nullif(Type, '')                          as raw_transaction_type,

    -- 5. DATE COLUMNS (แก้ไขชื่อคอลัมน์จากวงเล็บเป็น underscore)
    nullif(order_date_DateOrders, '')         as raw_order_dt_str,
    nullif(shipping_date_DateOrders, '')      as raw_ship_dt_str,

    -- 6. ORDER ITEM & FINANCIALS
    nullif(Order_Item_Id, '')                 as raw_order_item_id,
    nullif(Order_Id, '')                      as raw_order_id,
    nullif(Order_Item_Quantity, '')           as raw_order_item_quantity,
    nullif(Sales, '')                         as raw_sales,
    nullif(Order_Item_Total, '')              as raw_order_item_total,
    nullif(Order_Profit_Per_Order, '')        as raw_order_profit_per_order,
    nullif(Benefit_per_order, '')             as raw_benefit_per_order,
    nullif(Order_Item_Discount, '')           as raw_order_item_discount,
    nullif(Order_Item_Discount_Rate, '')       as raw_order_item_discount_rate,
    nullif(Days_for_shipment_scheduled, '')   as raw_days_for_shipment_scheduled,
    nullif(Days_for_shipping_real, '')        as raw_days_for_shipping_real,
    nullif(Order_Status, '')                  as raw_order_status
    
  from src
  where Customer_Id is not null
    and Order_Item_Id is not null
    and order_date_DateOrders is not null
)
select
  -- CASTING (ใช้ชื่อ raw_column ที่ถูกต้องแล้ว)
  cast(raw_customer_id as int)                       as customer_id,
  raw_customer_fname                                 as customer_fname,
  raw_customer_lname                                 as customer_lname,
  raw_customer_segment                               as customer_segment,
  raw_customer_email                                 as customer_email,

  cast(raw_product_card_id as int)                   as product_card_id,
  raw_product_name                                   as product_name,
  cast(raw_product_price as numeric(10,2))           as product_price,
  raw_category_name                                  as category_name,
  raw_department_name                                as department_name,

  raw_order_city                                     as order_city,
  raw_order_state                                    as order_state,
  raw_order_country                                  as order_country,
  raw_order_region                                   as order_region,
  raw_market                                         as market,
  cast(raw_latitude as numeric(9,6))                 as latitude,
  cast(raw_longitude as numeric(9,6))                as longitude,
  cast(raw_order_zipcode as text)                    as order_zipcode,

  raw_shipping_mode                                  as shipping_mode,
  raw_delivery_status                                as delivery_status,
  cast(raw_late_delivery_risk as int)                as late_delivery_risk,
  raw_transaction_type                               as transaction_type,

  -- การแปลงวันที่: ต้องมั่นใจว่า format ใน data string ตรงกับที่ postgres คาดหวัง
  cast(raw_order_dt_str as timestamp)                as order_datetime,
  cast(raw_ship_dt_str as timestamp)                 as shipping_datetime,

  cast(raw_order_item_id as int)                     as order_item_id,
  cast(raw_order_id as int)                          as order_id,
  cast(raw_order_item_quantity as int)               as order_item_quantity,
  cast(raw_sales as numeric(15,4))                   as sales,
  cast(raw_order_item_total as numeric(15,4))        as order_item_total,
  cast(raw_order_profit_per_order as numeric(15,4))  as order_profit_per_order,
  cast(raw_benefit_per_order as numeric(15,4))       as benefit_per_order,
  cast(raw_order_item_discount as numeric(15,4))     as order_item_discount,
  
  case
    when raw_order_item_discount_rate like '%\%%'
      then replace(raw_order_item_discount_rate, '%', '')::numeric(7,4)/100
    else cast(raw_order_item_discount_rate as numeric(7,4))
  end                                                as order_item_discount_rate,

  cast(raw_days_for_shipment_scheduled as int)       as days_for_shipment_scheduled,
  cast(raw_days_for_shipping_real as int)            as days_for_shipping_real,
  raw_order_status                                   as order_status
from clean