from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta
import pendulum
import pandas as pd
from sqlalchemy import create_engine, text
from airflow.operators.email import EmailOperator

local_tz = pendulum.timezone("Asia/Bangkok")

default_args = {
    "owner": "DE-team",
    "email": ["bi@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}

def ingest_csv_to_postgres():
    csv_path = "/opt/airflow/dags/data/DataCoSupplyChainDataset.csv"
    df = pd.read_csv(csv_path)

    engine = create_engine("postgresql+psycopg2://dpuser:dppass@dp_postgres:5432/dp_course")

    df.to_sql(
    "datacosupplychaindataset_tmp",
    con=engine,
    schema="staging",
    if_exists="replace",
    index=False
    )
    
def fix_data_types_in_staging():
    """
    Connects to Postgres and runs ALTER TABLE 
    to fix data types in the staging table.
    """
    engine = create_engine("postgresql+psycopg2://dpuser:dppass@dp_postgres:5432/dp_course")
    

    sql_command = text("""
    ALTER TABLE staging.datacosupplychaindataset_tmp
    ALTER COLUMN "order date (DateOrders)" TYPE TIMESTAMP USING "order date (DateOrders)"::timestamp,
    ALTER COLUMN "shipping date (DateOrders)" TYPE TIMESTAMP USING "shipping date (DateOrders)"::timestamp,
    ALTER COLUMN "Sales" TYPE NUMERIC(10, 5) USING "Sales"::numeric,
    ALTER COLUMN "Product Price" TYPE NUMERIC(10, 5) USING "Product Price"::numeric,
    ALTER COLUMN "Latitude" TYPE DOUBLE PRECISION USING "Latitude"::double precision,
    ALTER COLUMN "Longitude" TYPE DOUBLE PRECISION USING "Longitude"::double precision,
    ALTER COLUMN "Order Item Discount Rate" TYPE NUMERIC(3, 2) USING "Order Item Discount Rate"::numeric,
    ALTER COLUMN "Order Item Total" TYPE NUMERIC(10, 5) USING "Order Item Total"::numeric,
    ALTER COLUMN "Order Item Discount" TYPE NUMERIC(10, 5) USING "Order Item Discount"::numeric,
    ALTER COLUMN "Sales per customer" TYPE NUMERIC(10, 5) USING "Sales per customer"::numeric,
    ALTER COLUMN "Benefit per order" TYPE NUMERIC(10, 5) USING "Benefit per order"::numeric,
    ALTER COLUMN "Order Profit Per Order" TYPE NUMERIC(10, 5) USING "Order Profit Per Order"::numeric,
    ALTER COLUMN "Order Item Profit Ratio" TYPE NUMERIC(5, 4) USING "Order Item Profit Ratio"::numeric,
    ALTER COLUMN "Order Item Product Price" TYPE NUMERIC(10, 5) USING "Order Item Product Price"::numeric,
    ALTER COLUMN "Order Item Quantity" TYPE INTEGER USING "Order Item Quantity"::integer,
    ALTER COLUMN "Late_delivery_risk" TYPE SMALLINT USING "Late_delivery_risk"::smallint,
    ALTER COLUMN "Product Status" TYPE SMALLINT USING "Product Status"::smallint,
    ALTER COLUMN "Days for shipment (scheduled)" TYPE INTEGER USING "Days for shipment (scheduled)"::integer,
    ALTER COLUMN "Days for shipping (real)" TYPE INTEGER USING "Days for shipping (real)"::integer;
    """)
    
    try:
        with engine.begin() as conn: 
            conn.execute(sql_command)
        
        print("Successfully fixed data types in staging.")
    except Exception as e:
        print(f"Error fixing data types: {e}")
        raise

with DAG(
    dag_id="dataco_supply_chain_pipeline",
    start_date=pendulum.datetime(2025, 9, 1, tz=local_tz),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["dataco"],
) as dag:

    # 1) Ingestion (Strict)
    ingest_csv = PythonOperator(
    task_id="ingest_csv",
    python_callable=ingest_csv_to_postgres,
    )
    
    validate_staging = BashOperator(
        task_id="validate_staging",
        bash_command=(
        "cd /workspace/gx && "
        "great_expectations checkpoint run stg_supply_chain_checkpoint"
        )
    ) 

    fix_staging_types = PythonOperator(
            task_id="fix_staging_types",
            python_callable=fix_data_types_in_staging,
        )  

    dbt_run_core = BashOperator(
        task_id="dbt_run_core",
        bash_command="cd /usr/app/supply_chain_core && dbt run --select core",
    )
    validate_core = BashOperator(
        task_id="validate_core",
        bash_command="cd /usr/app/supply_chain_core && dbt test --select core",
    )
    dbt_run_mart = BashOperator(
        task_id="dbt_run_mart",
        bash_command="cd /usr/app/supply_chain_mart && dbt run --select mart",
    )
    validate_mart = BashOperator(
        task_id="validate_mart",
        bash_command="cd /usr/app/supply_chain_mart && dbt test --select mart",
    )
    
    # 4) Publish (รอแค่ strict layer ผ่าน)
    # publish = EmailOperator(
    #     task_id="publish",
    #     to="bi@example.com", # ผู้รับ
    #     subject="Retailco Pipeline - Data Marts Ready",
    #     html_content="""
    #     <h3>✅ Data Marts Build Completed</h3>
    #     <p>Your marts (Sales & Customer) are now ready in PostgreSQL
    #     and can be accessed from Metabase.</p>
    #     <p>- fct_sales<br>- fct_customer_orders</p>
    #     <p>Timestamp: {{ ds }}</p>
    #     """,
    # )
    
    ingest_csv >> fix_staging_types >> validate_staging >> dbt_run_core >> validate_core >> dbt_run_mart >> validate_mart
