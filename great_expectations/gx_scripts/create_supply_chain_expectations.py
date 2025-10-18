# file: create_supply_chain_expectations.py
import great_expectations as gx
from datetime import date

# 1. สร้าง Data Context
context = gx.get_context()

# 2. ดึง Datasource และ Asset ที่มีอยู่แล้ว
datasource = context.datasources["pg_datasource"]
asset = datasource.get_asset("stg_supply_chain_asset")
batch_request = asset.build_batch_request()

# 3. สร้างหรืออัปเดต Expectation Suite
expectation_suite_name = "stg_supply_chain_suite"
context.add_or_update_expectation_suite(expectation_suite_name=expectation_suite_name)
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)

print(f"Creating Expectations for suite: '{expectation_suite_name}'...")

# --- ชุดกฎการตรวจสอบข้อมูล (Expectations) ---

# ตรวจสอบ Primary Key และ Foreign Keys
validator.expect_column_values_to_be_unique(column="order_item_id")
validator.expect_column_values_to_not_be_null(column="order_item_id")
validator.expect_column_values_to_not_be_null(column="order_id")
validator.expect_column_values_to_not_be_null(column="customer_id")

# ตรวจสอบข้อมูลทั่วไป
validator.expect_column_values_to_be_in_set(
    column="order_status",
    value_set=['COMPLETE', 'PENDING_PAYMENT', 'PROCESSING', 'PENDING', 'CLOSED', 'ON_HOLD', 'SUSPECTED_FRAUD', 'CANCELED', 'PAYMENT_REVIEW']
)
validator.expect_column_values_to_be_between(column="order_date_dateorders", max_value=date.today().isoformat(), parse_strings_as_datetimes=True)

# ตรวจสอบข้อมูลตัวเลขทางการเงินและจำนวน
validator.expect_column_values_to_be_between(column="order_item_quantity", min_value=1)
validator.expect_column_values_to_be_between(column="product_price", min_value=0)
validator.expect_column_values_to_be_between(column="sales", min_value=0)

# ตรวจสอบค่าพิกัด Latitude และ Longitude
validator.expect_column_values_to_be_between(column="latitude", min_value=-90, max_value=90)
validator.expect_column_values_to_be_between(column="longitude", min_value=-180, max_value=180)

# ตรวจสอบความยาวของชื่อสินค้า
validator.expect_column_value_lengths_to_be_between(column="product_name", min_value=5, max_value=100)

# ตรวจสอบวันที่จัดส่งต้องไม่ก่อนวันสั่งซื้อ
validator.expect_column_pair_values_A_to_be_greater_than_B(
    column_A="shipping_date_dateorders",
    column_B="order_date_dateorders",
    or_equal=True
)

# ตรวจสอบค่าที่เป็นไปได้ต่างๆ
validator.expect_column_values_to_be_between(column="order_item_discount_rate", min_value=0, max_value=1)
validator.expect_column_values_to_be_in_set(column="type", value_set=['DEBIT', 'PAYMENT', 'TRANSFER', 'CASH'])
validator.expect_column_values_to_be_in_set(column="shipping_mode", value_set=['First Class', 'Same Day', 'Second Class', 'Standard Class'])
validator.expect_column_values_to_be_in_set(column="delivery_status", value_set=['Advance shipping', 'Late delivery', 'Shipping canceled', 'Shipping on time'])
validator.expect_column_values_to_be_in_set(column="market", value_set=['Pacific Asia', 'USCA', 'Africa', 'Europe', 'LATAM'])
validator.expect_column_values_to_be_in_set(column="customer_segment", value_set=['Consumer', 'Corporate', 'Home Office'])
validator.expect_column_values_to_be_in_set(column="late_delivery_risk", value_set=[0, 1])
validator.expect_column_values_to_be_in_set(column="product_status", value_set=[0, 1])

# ตรวจสอบค่าตัวเลขต้องไม่ติดลบ
validator.expect_column_values_to_be_between(column="days_for_shipment_scheduled", min_value=0)
validator.expect_column_values_to_be_between(column="days_for_shipping_real", min_value=0)
validator.expect_column_values_to_be_between(column="order_item_product_price", min_value=0)
validator.expect_column_values_to_be_between(column="order_item_total", min_value=0)
validator.expect_column_values_to_be_between(column="order_item_discount", min_value=0)
validator.expect_column_values_to_be_between(column="sales_per_customer", min_value=0)

# ตรวจสอบความยาวขั้นต่ำของคำอธิบายสินค้า
validator.expect_column_value_lengths_to_be_between(column="product_description", min_value=100)

# ตรวจสอบคอลัมน์ต่างๆ ต้องไม่เป็นค่าว่าง (Not Null)
columns_to_be_not_null = [
    "customer_city", "category_id", "department_id", "product_card_id", "product_category_id",
    "order_customer_id", "order_item_cardprod_id", "order_region", "category_name", "_airbyte_ab_id",
    "_airbyte_emitted_at", "order_city", "customer_fname", "department_name", "_airbyte_normalized_at",
    "_airbyte_stg_supply_chain_hashid", "order_state", "order_country", "customer_state",
    "customer_country", "customer_street", "customer_lname", "customer_email", "customer_zipcode",
    "benefit_per_order", "order_profit_per_order", "order_item_profit_ratio", "product_image",
    "customer_password"
]
for col in columns_to_be_not_null:
    validator.expect_column_values_to_not_be_null(column=col)

# 4. บันทึก Expectation Suite
validator.save_expectation_suite(discard_failed_expectations=False)

print(f"✅ Expectation suite '{expectation_suite_name}' is ready.")