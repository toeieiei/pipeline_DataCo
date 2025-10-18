# file: create_analytics_expectations.py
import great_expectations as gx

# 1. รับ Data Context
context = gx.get_context()

# 2. ดึง Datasource ที่สร้างไว้
datasource = context.datasources["pg_datasource"]

# --- สร้าง Expectations สำหรับ `dim_customer` ---
asset_customer = datasource.get_asset("dim_customer")
batch_request_customer = asset_customer.build_batch_request()
suite_name_customer = "validate_dim_customer"
context.add_or_update_expectation_suite(expectation_suite_name=suite_name_customer)
validator_customer = context.get_validator(
    batch_request=batch_request_customer,
    expectation_suite_name=suite_name_customer,
)

print(f"Creating Expectations for suite: '{suite_name_customer}'...")
validator_customer.expect_column_values_to_not_be_null("customer_key")
validator_customer.expect_column_values_to_be_unique("customer_key")
validator_customer.expect_column_values_to_not_be_null("customer_id")
validator_customer.expect_column_values_to_match_regex(
    "customer_email", r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
)
validator_customer.expect_column_values_to_be_of_type("customer_id", "INTEGER")
validator_customer.save_expectation_suite(discard_failed_expectations=False)

# --- สร้าง Expectations สำหรับ `dim_product` ---
asset_product = datasource.get_asset("dim_product")
suite_name_product = "validate_dim_product"
context.add_or_update_expectation_suite(suite_name_product)
validator_product = context.get_validator(
    batch_request=asset_product.build_batch_request(),
    expectation_suite_name=suite_name_product,
)

print(f"Creating Expectations for suite: '{suite_name_product}'...")
validator_product.expect_column_values_to_not_be_null("product_key")
validator_product.expect_column_values_to_be_unique("product_key")
validator_product.expect_column_values_to_not_be_null("product_card_id")
validator_product.expect_column_values_to_be_between("product_price", min_value=0)
validator_product.expect_column_values_to_be_of_type("product_price", "NUMERIC")
validator_product.save_expectation_suite(discard_failed_expectations=False)


# --- สร้าง Expectations สำหรับ `dim_location` ---
asset_location = datasource.get_asset("dim_location")
suite_name_location = "validate_dim_location"
context.add_or_update_expectation_suite(suite_name_location)
validator_location = context.get_validator(
    batch_request=asset_location.build_batch_request(),
    expectation_suite_name=suite_name_location,
)
print(f"Creating Expectations for suite: '{suite_name_location}'...")
validator_location.expect_column_values_to_not_be_null("location_key")
validator_location.expect_column_values_to_be_unique("location_key")
validator_location.expect_column_values_to_be_of_type("latitude", "FLOAT")
validator_location.expect_column_values_to_be_of_type("longitude", "FLOAT")
validator_location.save_expectation_suite(discard_failed_expectations=False)


# --- สร้าง Expectations สำหรับ `dim_date` ---
asset_date = datasource.get_asset("dim_date")
suite_name_date = "validate_dim_date"
context.add_or_update_expectation_suite(suite_name_date)
validator_date = context.get_validator(
    batch_request=asset_date.build_batch_request(),
    expectation_suite_name=suite_name_date,
)
print(f"Creating Expectations for suite: '{suite_name_date}'...")
validator_date.expect_column_values_to_not_be_null("date_key")
validator_date.expect_column_values_to_be_unique("date_key")
validator_date.expect_column_values_to_be_of_type("full_date", "DATE")
validator_date.save_expectation_suite(discard_failed_expectations=False)


# --- สร้าง Expectations สำหรับ `fct_order_items` ---
asset_fact = datasource.get_asset("fct_order_items")
suite_name_fact = "validate_fct_order_items"
context.add_or_update_expectation_suite(suite_name_fact)
validator_fact = context.get_validator(
    batch_request=asset_fact.build_batch_request(),
    expectation_suite_name=suite_name_fact,
)

print(f"Creating Expectations for suite: '{suite_name_fact}'...")
validator_fact.expect_column_values_to_not_be_null("order_item_key")
validator_fact.expect_column_values_to_be_unique("order_item_key")
validator_fact.expect_column_values_to_not_be_null("order_item_id")
for fk_col in ["order_date_key", "customer_key", "product_key", "location_key", "shipping_key"]:
    validator_fact.expect_column_values_to_not_be_null(fk_col)

validator_fact.expect_column_values_to_be_in_set(
    "order_status",
    ['COMPLETE', 'PENDING_PAYMENT', 'PROCESSING', 'PENDING', 'CLOSED', 'ON_HOLD', 'SUSPECTED_FRAUD', 'CANCELED', 'PAYMENT_REVIEW']
)
validator_fact.expect_column_values_to_be_in_set("late_delivery_risk", [0, 1])

# ✅ แก้ไข: แยกคอลัมน์ที่สามารถติดลบได้
non_negative_numeric_cols = [
    "order_item_quantity", "sales", "order_item_total",
    "order_item_discount", "days_for_shipment_scheduled", "days_for_shipping_real"
]
for col in non_negative_numeric_cols:
    validator_fact.expect_column_values_to_be_between(col, min_value=0)

# ✅ ยอมให้ profit และ benefit ติดลบได้ แต่ต้องไม่เป็นค่าว่าง
validator_fact.expect_column_values_to_not_be_null("order_profit_per_order")
validator_fact.expect_column_values_to_not_be_null("benefit_per_order")

validator_fact.expect_column_values_to_be_between("order_item_discount_rate", min_value=0.0, max_value=1.0)


# --- ตรวจสอบชนิดข้อมูลสำหรับ fct_order_items ---
print(f"Adding Type Checks for suite: '{suite_name_fact}'...")
numeric_type_cols = [
    "sales", "order_item_total", "order_profit_per_order", "benefit_per_order",
    "order_item_discount", "order_item_discount_rate"
]
for col in numeric_type_cols:
    validator_fact.expect_column_values_to_be_of_type(col, "NUMERIC")

integer_type_cols = [
    "order_item_quantity", "late_delivery_risk", "days_for_shipment_scheduled", "days_for_shipping_real"
]
for col in integer_type_cols:
    validator_fact.expect_column_values_to_be_of_type(col, "INTEGER")

validator_fact.save_expectation_suite(discard_failed_expectations=False)

print(f"\n✅ All Analytics Expectation suites are ready.")