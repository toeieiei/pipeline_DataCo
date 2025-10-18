{{ config(materialized='view', schema='core') }}

with src as (
  select * from {{ source('supply_chain_source', 'DataCoSupplyChainDataset') }}
),
clean as (
  select
    "Customer Id"                             as raw_customer_id,
    nullif("Customer Fname", '')              as raw_customer_fname,
    nullif("Customer Lname", '')              as raw_customer_lname,
    nullif("Customer Segment", '')            as raw_customer_segment,
    nullif("Customer Email", '')              as raw_customer_email,

    "Product Card Id"                         as raw_product_card_id,
    nullif("Product Name", '')                as raw_product_name,
    nullif("Product Price", '')               as raw_product_price,
    nullif("Category Name", '')               as raw_category_name,
    nullif("Department Name", '')             as raw_department_name,

    nullif("Order City", '')                  as raw_order_city,
    nullif("Order State", '')                 as raw_order_state,
    nullif("Order Country", '')               as raw_order_country,
    nullif("Order Region", '')                as raw_order_region,
    nullif("Market", '')                      as raw_market,
    nullif("Latitude", '')                    as raw_latitude,
    nullif("Longitude", '')                   as raw_longitude,
    nullif("Order Zipcode", '')               as raw_order_zipcode,

    nullif("Shipping Mode", '')               as raw_shipping_mode,
    nullif("Delivery Status", '')             as raw_delivery_status,
    nullif("Late_delivery_risk", '')          as raw_late_delivery_risk,
    nullif("Type", '')                        as raw_transaction_type,

    nullif("order date (DateOrders)", '')     as raw_order_dt_str,
    nullif("shipping date (DateOrders)", '')  as raw_ship_dt_str,

    nullif("Order Item Id", '')               as raw_order_item_id,
    nullif("Order Id", '')                    as raw_order_id,
    nullif("Order Item Quantity", '')         as raw_order_item_quantity,
    nullif("Sales", '')                       as raw_sales,
    nullif("Order Item Total", '')            as raw_order_item_total,
    nullif("Order Profit Per Order", '')      as raw_order_profit_per_order,
    nullif("Benefit per order", '')           as raw_benefit_per_order,
    nullif("Order Item Discount", '')         as raw_order_item_discount,
    nullif("Order Item Discount Rate", '')    as raw_order_item_discount_rate,
    nullif("Days for shipment (scheduled)", '') as raw_days_for_shipment_scheduled,
    nullif("Days for shipping (real)", '')      as raw_days_for_shipping_real,
    nullif("Order Status", '')                as raw_order_status
  from src
  where "Customer Id" is not null
    and "Order Item Id" is not null
    and "order date (DateOrders)" is not null
)
select
  cast(raw_customer_id as int)                        as customer_id,
  raw_customer_fname                                  as customer_fname,
  raw_customer_lname                                  as customer_lname,
  raw_customer_segment                                as customer_segment,
  raw_customer_email                                  as customer_email,

  cast(raw_product_card_id as int)                    as product_card_id,
  raw_product_name                                    as product_name,
  cast(raw_product_price as numeric(10,2))            as product_price,
  raw_category_name                                   as category_name,
  raw_department_name                                 as department_name,

  raw_order_city                                      as order_city,
  raw_order_state                                     as order_state,
  raw_order_country                                   as order_country,
  raw_order_region                                    as order_region,
  raw_market                                          as market,
  cast(raw_latitude as numeric(9,6))                  as latitude,
  cast(raw_longitude as numeric(9,6))                 as longitude,
  cast(raw_order_zipcode as text)                     as order_zipcode,

  raw_shipping_mode                                   as shipping_mode,
  raw_delivery_status                                 as delivery_status,
  cast(raw_late_delivery_risk as int)                 as late_delivery_risk,
  raw_transaction_type                                as transaction_type,

  -- assume 'YYYY-MM-DD HH:MI:SS', ปรับ format หากจำเป็น
  cast(raw_order_dt_str as timestamp)                 as order_datetime,
  cast(raw_ship_dt_str as timestamp)                  as shipping_datetime,

  cast(raw_order_item_id as int)                      as order_item_id,
  cast(raw_order_id as int)                           as order_id,
  cast(raw_order_item_quantity as int)                as order_item_quantity,
  cast(raw_sales as numeric(15,4))                    as sales,
  cast(raw_order_item_total as numeric(15,4))         as order_item_total,
  cast(raw_order_profit_per_order as numeric(15,4))   as order_profit_per_order,
  cast(raw_benefit_per_order as numeric(15,4))        as benefit_per_order,
  cast(raw_order_item_discount as numeric(15,4))      as order_item_discount,
  case
    when raw_order_item_discount_rate like '%\%%'
      then replace(raw_order_item_discount_rate, '%', '')::numeric(7,4)/100
    else cast(raw_order_item_discount_rate as numeric(7,4))
  end                                                 as order_item_discount_rate,
  cast(raw_days_for_shipment_scheduled as int)        as days_for_shipment_scheduled,
  cast(raw_days_for_shipping_real as int)             as days_for_shipping_real,
  raw_order_status                                    as order_status
from clean;
