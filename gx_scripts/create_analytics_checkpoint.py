# file: create_analytics_checkpoint.py
import great_expectations as gx

context = gx.get_context()

# ดึง datasource ที่มีอยู่แล้ว
ds = context.datasources["pg_datasource"]

# --- ดึง Asset ทั้งหมดที่ต้องการตรวจสอบ ---
asset_customer = ds.get_asset("dim_customer")
asset_product = ds.get_asset("dim_product")
asset_location = ds.get_asset("dim_location")
asset_date = ds.get_asset("dim_date")         
asset_fact = ds.get_asset("fact_order_item")

checkpoint_name = 'Analytics_checkpoint'

# --- สร้าง Checkpoint ที่มีการ Validation ครบทุกชุด ---
checkpoint = context.add_or_update_checkpoint(
    name=checkpoint_name,
    validations=[
        {
            "batch_request": asset_customer.build_batch_request(),
            "expectation_suite_name": "validate_dim_customer",
        },
        {
            "batch_request": asset_product.build_batch_request(),
            "expectation_suite_name": "validate_dim_product",
        },
        # ✅ เพิ่ม: Validation สำหรับ dim_location
        {
            "batch_request": asset_location.build_batch_request(),
            "expectation_suite_name": "validate_dim_location",
        },
        # ✅ เพิ่ม: Validation สำหรับ dim_date
        {
            "batch_request": asset_date.build_batch_request(),
            "expectation_suite_name": "validate_dim_date",
        },
        {
            "batch_request": asset_fact.build_batch_request(),
            "expectation_suite_name": "validate_fct_order_items",
        }
    ],
)

print(f"✅ Checkpoint '{checkpoint_name}' is ready and covers all dimension/fact tables.")

# รันทดสอบทันที
print("\nRunning checkpoint...")
result = context.run_checkpoint(checkpoint_name)

# --- แสดงผลลัพธ์ละเอียดขึ้น ---
print(f"\nCheckpoint run overall success: {result['success']}")

for run_name, run_result in result['run_results'].items():
    if not run_result.get("validation_result"):
        print(f"\n--- Could not find validation results for {run_name} ---")
        continue

    validation_result = run_result['validation_result']
    suite_name = validation_result['meta']['expectation_suite_name']
    print(f"\n--- Results for suite: {suite_name} ---")
    
    for exp in validation_result['results']:
        status = "✅" if exp['success'] else "❌"
        # แสดง kwargs ถ้ามี ไม่เช่นนั้นแสดงสตริงว่าง
        kwargs_str = exp['expectation_config'].get('kwargs', {})
        print(f"{status} {exp['expectation_config']['expectation_type']}: {kwargs_str}")