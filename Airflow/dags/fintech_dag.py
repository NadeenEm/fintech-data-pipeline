from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from _functions.cleaning import extract_clean, extract_states, combine_sources, encoding, load_to_db

# Define the DAG
default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "retries": 0,
}

with DAG(
    dag_id="fintech_dag",
    schedule_interval="@once",  # Adjust as needed
    default_args=default_args,
    tags=["pipeline", "etl", "fintech"],
) as dag:

    # Task 1: Extract and clean main fintech data
    extract_clean_task = PythonOperator(
        task_id="extract_clean",
        python_callable=extract_clean,
        op_kwargs={
            "filename": "/opt/airflow/data/fintech_data.csv",
            "output_path": "/opt/airflow/data/fintech_clean.parquet",
        },
    )

    # Task 2: Extract states data
    extract_states_task = PythonOperator(
        task_id="extract_states",
        python_callable=extract_states,
        op_kwargs={
            "filename": "/opt/airflow/data/states.csv",
            "output_path": "/opt/airflow/data/fintech_states.parquet",
        },
    )

    # Task 3: Combine the cleaned fintech data and states data
    combine_sources_task = PythonOperator(
        task_id="combine_data",
        python_callable=combine_sources,
        op_kwargs={
            "filename1": "/opt/airflow/data/fintech_clean.parquet",
            "filename2": "/opt/airflow/data/fintech_states.parquet",
            "output_path": "/opt/airflow/data/fintech_combined.parquet",
        },
    )

    # Task 4: Encode the combined data
    encoding_task = PythonOperator(
        task_id="encoding_data",
        python_callable=encoding,
        op_kwargs={
            "filename": "/opt/airflow/data/fintech_combined.parquet",
            "output_path": "/opt/airflow/data/fintech_encoded.parquet",
        },
    )

    # Task 5: Load the encoded data into the PostgreSQL database
    load_to_postgres_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_db,
        op_kwargs={
            "filename": "/opt/airflow/data/fintech_combined.parquet",
            "table_name": "fintech_data",
            "postgres_opt": {
                "user": "root",
                "password": "root",
                "host": "pgdatabase",
                "port": 5432,
                "db": "data_engineering",
            },
        },
    )

    # Define task dependencies to match the graph structure
    [extract_clean_task, extract_states_task] >> combine_sources_task >> encoding_task >> load_to_postgres_task
