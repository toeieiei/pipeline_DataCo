# file: add_datasource.py
import great_expectations as gx

# 1. สร้าง Data Context
context = gx.get_context()

# 2. กำหนดค่าการเชื่อมต่อ
PG_CONNECTION_STRING = "postgresql+psycopg2://dpuser:dppass@dp_postgres:5432/dp_course"

# 3. เพิ่มหรืออัปเดต Datasource
datasource = context.sources.add_or_update_postgres(
    name="pg_datasource", connection_string=PG_CONNECTION_STRING
)

# 4. เพิ่ม Table Asset สำหรับ Staging
datasource.add_table_asset(
    name="stg_supply_chain_asset", 
    schema_name="airbyte_raw", 
    table_name="stg_supply_chain"
)

# --- เพิ่ม: Asset สำหรับตารางใน Analytics Schema ---
print("Adding assets for Analytics schema...")

# # Asset สำหรับตาราง Fact
# datasource.add_table_asset(
#     name="fct_order_items",
#     schema_name="Analytics", 
#     table_name="fct_order_items"
# )

# # Asset สำหรับตาราง Dimension ทั้งหมด
# datasource.add_table_asset(name="dim_customer", schema_name="Analytics", table_name="dim_customer")
# datasource.add_table_asset(name="dim_product", schema_name="Analytics", table_name="dim_product")
# datasource.add_table_asset(name="dim_location", schema_name="Analytics", table_name="dim_location")
# datasource.add_table_asset(name="dim_shipping", schema_name="Analytics", table_name="dim_shipping")
# datasource.add_table_asset(name="dim_date", schema_name="Analytics", table_name="dim_date")


print("✅ Datasource and all assets are ready.")