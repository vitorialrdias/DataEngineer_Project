from __future__ import annotations

from configs import MAX_PAGES, DELAY_SECONDS, S3_RAW_PREFIX, MINIO_CONN_ID, BUCKET_NAME, POSTGRES_CONN_ID, S3_STAGED_PREFIX, TABLE_NAME

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator # type: ignore

# Importa as funções de cada etapa
from extract_data import run_extraction_and_upload 
from transform_data import run_transformation
from load_datas import run_load


# Definição do DAG
with DAG(
    dag_id="etl_data_pipeline",  # <-- ARGUMENTO OBRIGATÓRIO 1: O ID ÚNICO
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),  # <-- ARGUMENTO OBRIGATÓRIO 2
    schedule=None,
    catchup=False,
    tags=["data_engineer", "etl"],
) as dag:
# ----------------------------------------------------------------------
    
    extract_task = PythonOperator(
        task_id="extract_data_to_minio",
        python_callable=run_extraction_and_upload, 
        op_kwargs={
            "max_pages": MAX_PAGES,
            "delay_seconds": DELAY_SECONDS,
            "s3": S3_RAW_PREFIX,
            "minio": MINIO_CONN_ID,
            "bucket": BUCKET_NAME
        }  
    )

    transform_task = PythonOperator(
        task_id='transform_data_to_staged',
        python_callable=run_transformation,
        # Isso permite que a função run_transformation receba 'ti' (Task Instance)
        provide_context=True, 
        op_kwargs={
            "s3_raw": S3_RAW_PREFIX,
            "s3_staged": S3_STAGED_PREFIX,
            "minio": MINIO_CONN_ID,
            "bucket": BUCKET_NAME
        }  
    )

    load_task = PythonOperator(
        task_id="load_data_to_postgres",
        python_callable=run_load,
        op_kwargs={
            "table": TABLE_NAME,
            "minio": MINIO_CONN_ID,
            "postgres": POSTGRES_CONN_ID,
            "bucket_name": BUCKET_NAME,
            "s3_staged": S3_STAGED_PREFIX
        }
    )

    extract_task >> transform_task >> load_task