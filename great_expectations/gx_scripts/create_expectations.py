import great_expectations as gx
from great_expectations.checkpoint import Checkpoint
from datetime import date

# 1. สร้าง Data Context
context = gx.get_context()

# 2. เชื่อมต่อกับข้อมูลในตารางเดียว
PG_CONNECTION_STRING = "postgresql+psycopg2://dpuser:dppass@dp_postgres:5432/dp_course"
pg_datasource = context.sources.add_or_update_postgres(
    name="pg_datasource", connection_string=PG_CONNECTION_STRING
)

# --- ปรับแก้ตรงนี้: ให้ asset ชี้ไปที่ตาราง stg_supply_chain ตารางเดียว ---
# (กรุณาเปลี่ยน "schema_name" หาก schema ของคุณไม่ใช่ "core")
stg_asset = pg_datasource.add_table_asset(
    name="stg_supply_chain_asset", schema_name="airbyte_raw", table_name="stg_supply_chain"
)
batch_request = stg_asset.build_batch_request()


# 3. สร้าง Expectation Suite ชุดเดียวสำหรับตารางนี้
expectation_suite_name = "stg_supply_chain_suite"
context.add_or_update_expectation_suite(expectation_suite_name=expectation_suite_name)
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)

# --- เพิ่มเงื่อนไขการตรวจสอบทั้งหมดใน suite เดียว ---
print(f"Creating Expectations for suite: '{expectation_suite_name}'...")

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

# ละติจูดต้องอยู่ระหว่าง -90 ถึง 90
validator.expect_column_values_to_be_between(column="latitude", min_value=-90, max_value=90)
# ลองจิจูดต้องอยู่ระหว่าง -180 ถึง 180
validator.expect_column_values_to_be_between(column="longitude", min_value=-180, max_value=180)

# ชื่อสินค้าควรมีความยาวระหว่าง 5 ถึง 100 ตัวอักษร
validator.expect_column_value_lengths_to_be_between(column="product_name", min_value=5, max_value=100)

validator.expect_column_pair_values_A_to_be_greater_than_B(
    column_A="shipping_date_dateorders",
    column_B="order_date_dateorders",
    or_equal=True  # อนุญาตให้เป็นวันเดียวกันได้
)

# อัตราส่วนลดควรเป็นค่าระหว่าง 0.0 ถึง 1.0
validator.expect_column_values_to_be_between(column="order_item_discount_rate", min_value=0, max_value=1)

# ประเภทการจ่ายเงินควรมีแค่ DEBIT, CREDIT, TRANSFER, CASH
validator.expect_column_values_to_be_in_set(column="type", value_set=['DEBIT', 'PAYMENT', 'TRANSFER', 'CASH'])

# ประเภทการจัดส่ง 'First Class', 'Same Day', 'Second Class', 'Standard Class'
validator.expect_column_values_to_be_in_set(
    column="shipping_mode", 
    value_set=['First Class', 'Same Day', 'Second Class', 'Standard Class']
    )

# ประเภทสถานะการจัดส่ง 'Advance shipping', 'Late delivery', 'Shipping canceled', 'Shipping on time'
validator.expect_column_values_to_be_in_set(
    column="delivery_status", 
    value_set=['Advance shipping', 'Late delivery', 'Shipping canceled', 'Shipping on time']
    )

validator.expect_column_values_to_be_in_set(
    column="market",
    value_set=['Pacific Asia', 'USCA', 'Africa', 'Europe', 'LATAM'] # ใส่ค่าทั้งหมดที่คุณพบ
)

validator.expect_column_values_to_be_in_set(
    column="customer_segment",
    value_set=['Consumer', 'Corporate', 'Home Office'] # ใส่ค่าทั้งหมดที่คุณพบ
)

validator.expect_column_values_to_be_in_set(column="late_delivery_risk", value_set=[0, 1])
validator.expect_column_values_to_be_in_set(column="product_status", value_set=[0, 1])
# ตรวจสอบว่าค่าต้องไม่ติดลบ
validator.expect_column_values_to_be_between(column="days_for_shipment_scheduled", min_value=0)
validator.expect_column_values_to_be_between(column="days_for_shipping_real", min_value=0)
validator.expect_column_values_to_be_between(column="order_item_product_price", min_value=0)
validator.expect_column_values_to_be_between(column="order_item_total", min_value=0)
validator.expect_column_values_to_be_between(column="order_item_discount", min_value=0)
validator.expect_column_values_to_be_between(column="sales_per_customer", min_value=0)
validator.expect_column_value_lengths_to_be_between(column="product_description", min_value=100)

validator.expect_column_values_to_not_be_null(column="customer_city")
validator.expect_column_values_to_not_be_null(column="category_id")
validator.expect_column_values_to_not_be_null(column="department_id")
validator.expect_column_values_to_not_be_null(column="product_card_id")
validator.expect_column_values_to_not_be_null(column="product_category_id")
validator.expect_column_values_to_not_be_null(column="order_customer_id")
validator.expect_column_values_to_not_be_null(column="order_item_cardprod_id")
validator.expect_column_values_to_not_be_null(column="order_region")
validator.expect_column_values_to_not_be_null(column="category_name")
validator.expect_column_values_to_not_be_null(column="_airbyte_ab_id")
validator.expect_column_values_to_not_be_null(column="_airbyte_emitted_at")
validator.expect_column_values_to_not_be_null(column="order_city")
validator.expect_column_values_to_not_be_null(column="customer_fname")
validator.expect_column_values_to_not_be_null(column="department_name")
validator.expect_column_values_to_not_be_null(column="_airbyte_normalized_at")
validator.expect_column_values_to_not_be_null(column="_airbyte_stg_supply_chain_hashid")
validator.expect_column_values_to_not_be_null(column="order_state")
validator.expect_column_values_to_not_be_null(column="order_country")
validator.expect_column_values_to_not_be_null(column="customer_state")
validator.expect_column_values_to_not_be_null(column="customer_country")
validator.expect_column_values_to_not_be_null(column="customer_street")
validator.expect_column_values_to_not_be_null(column="customer_lname")
validator.expect_column_values_to_not_be_null(column="customer_email")
validator.expect_column_values_to_not_be_null(column="customer_zipcode")
validator.expect_column_values_to_not_be_null(column="benefit_per_order")
validator.expect_column_values_to_not_be_null(column="order_profit_per_order")
validator.expect_column_values_to_not_be_null(column="order_item_profit_ratio")
validator.expect_column_values_to_not_be_null(column="product_image")
validator.expect_column_values_to_not_be_null(column="customer_password")


# บันทึก Expectation Suite
validator.save_expectation_suite(discard_failed_expectations=False)
print("Expectation Suite saved.")


# 4. สร้าง Checkpoint ที่เรียกใช้ validation ชุดเดียว
checkpoint = Checkpoint(
    name="stg_supply_chain_checkpoint",
    run_name_template="%Y%m%d-%H%M%S-stg_supply_chain_checkpoint",
    data_context=context,
    validations = [
        {
            "batch_request": batch_request,
            "expectation_suite_name": expectation_suite_name,
        }
    ],
    action_list=[
        {
            "name": "store_validation_result",
            "action": {"class_name": "StoreValidationResultAction"},
        },
        {"name": "update_data_docs", "action": {"class_name": "UpdateDataDocsAction"}},
    ],
)
context.add_or_update_checkpoint(checkpoint=checkpoint)
print("\nCheckpoint 'stg_supply_chain_checkpoint' is created and ready to run.")