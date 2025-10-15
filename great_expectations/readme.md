## 🔧 สิ่งที่ทำ
1. แก้ไขไฟล์ **docker-compose.yml** ของ GX  
   - เพิ่มค่า `POSTGRES_DB=dp_course`  
   - เพิ่ม profile เป็น **lab4** (เหมือนของมาส)

2. แก้ไข **Data Type** ใน PostgreSQL ดังนี้

    ```sql
    -- กลุ่มวันที่และเวลา
    ALTER COLUMN order_date_dateorders TYPE TIMESTAMP USING order_date_dateorders::timestamp,
    ALTER COLUMN shipping_date_dateorders TYPE TIMESTAMP USING shipping_date_dateorders::timestamp,

    -- กลุ่มตัวเลขทศนิยม
    ALTER COLUMN sales TYPE NUMERIC(10, 5) USING sales::numeric,
    ALTER COLUMN product_price TYPE NUMERIC(10, 5) USING product_price::numeric,
    ALTER COLUMN latitude TYPE DOUBLE PRECISION USING latitude::double precision,
    ALTER COLUMN longitude TYPE DOUBLE PRECISION USING longitude::double precision,
    ALTER COLUMN order_item_discount_rate TYPE NUMERIC(3, 2) USING order_item_discount_rate::numeric,
    ALTER COLUMN order_item_total TYPE NUMERIC(10, 5) USING order_item_total::numeric,
    ALTER COLUMN order_item_discount TYPE NUMERIC(10, 5) USING order_item_discount::numeric,
    ALTER COLUMN sales_per_customer TYPE NUMERIC(10, 5) USING sales_per_customer::numeric,
    ALTER COLUMN benefit_per_order TYPE NUMERIC(10, 5) USING benefit_per_order::numeric,
    ALTER COLUMN order_profit_per_order TYPE NUMERIC(10, 5) USING order_profit_per_order::numeric,
    ALTER COLUMN order_item_profit_ratio TYPE NUMERIC(5, 4) USING order_item_profit_ratio::numeric,
    ALTER COLUMN order_item_product_price TYPE NUMERIC(10, 5) USING order_item_product_price::numeric,

    -- กลุ่มตัวเลขจำนวนเต็ม
    ALTER COLUMN order_item_quantity TYPE INTEGER USING order_item_quantity::integer,
    ALTER COLUMN late_delivery_risk TYPE SMALLINT USING late_delivery_risk::smallint,
    ALTER COLUMN product_status TYPE SMALLINT USING product_status::smallint,
    ALTER COLUMN days_for_shipment_scheduled TYPE INTEGER USING days_for_shipment_scheduled::integer,
    ALTER COLUMN days_for_shipping_real TYPE INTEGER USING days_for_shipping_real::integer;

## 🔧 ขั้นตอนการใช้งาน

# 1. เปิดระบบด้วย Docker Compose
docker compose --profile lab4 up -d

# 2. เข้าไปในคอนเทนเนอร์ GX
docker exec -it dp_gx bash

# 3. สร้างโปรเจกต์ Great Expectations
great_expectations init

# 4. วางสคริปต์ Python สำหรับสร้าง Expectation
# (เช่น วางไว้ที่ gx_scripts/create_expectations.py)

# 5. รันสคริปต์สร้าง Expectation
python gx_scripts/create_expectations.py

# 6. รัน Checkpoint เพื่อตรวจสอบข้อมูล
great_expectations checkpoint run stg_supply_chain_checkpoint
