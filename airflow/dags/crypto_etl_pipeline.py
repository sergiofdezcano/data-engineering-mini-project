"""
Cryptocurrency ETL Pipeline DAG

This DAG orchestrates the extraction, loading, and transformation of crypto data:
1. Extract data from CoinGecko API
2. Load raw data to PostgreSQL
3. Transform data using dbt
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Import extraction functions
import sys
sys.path.insert(0, '/opt/airflow/scripts')
from extract_data import extract_crypto_data, load_to_postgres


# Default arguments for the DAG
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'crypto_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for cryptocurrency market data',
    schedule_interval='0 */6 * * *',  # Run every 6 hours
    start_date=days_ago(1),
    catchup=False,
    tags=['crypto', 'etl', 'data-engineering'],
)


def extract_task():
    """Extract cryptocurrency data from API"""
    print("Starting extraction...")
    filepath = extract_crypto_data(output_dir="/opt/airflow/data/raw")
    print(f"Extraction completed: {filepath}")
    return filepath


def load_task(**context):
    """Load extracted data to PostgreSQL"""
    # Get filepath from previous task
    ti = context['ti']
    filepath = ti.xcom_pull(task_ids='extract_data')

    print(f"Loading data from: {filepath}")
    load_to_postgres(filepath, table_name='raw_crypto_data')
    print("Data loaded successfully")


# Task 1: Extract data from API
extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_task,
    dag=dag,
)

# Task 2: Load data to PostgreSQL
load_data = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_task,
    provide_context=True,
    dag=dag,
)

# Task 3: Run dbt to transform data
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir .',
    dag=dag,
)

# Task 4: Run dbt tests
dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='cd /opt/airflow/dbt && dbt test --profiles-dir .',
    dag=dag,
)

# Task 5: Generate dbt documentation
dbt_docs = BashOperator(
    task_id='dbt_docs_generate',
    bash_command='cd /opt/airflow/dbt && dbt docs generate --profiles-dir .',
    dag=dag,
)

# Define task dependencies
extract_data >> load_data >> dbt_run >> dbt_test >> dbt_docs

# Alternative: Task dependencies can also be defined like this:
# extract_data.set_downstream(load_data)
# load_data.set_downstream(dbt_run)
# dbt_run.set_downstream(dbt_test)
# dbt_test.set_downstream(dbt_docs)
