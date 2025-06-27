
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dbt_vault_pipeline',
    default_args=default_args,
    description='dbT Data Vault 2.0 Pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['dbt', 'vault', 'data_warehouse']
)

# dbT tasks
dbt_deps = BashOperator(
    task_id='dbt_deps',
    bash_command='cd /path/to/dbt/project && dbt deps',
    dag=dag
)

dbt_run_staging = BashOperator(
    task_id='dbt_run_staging',
    bash_command='cd /path/to/dbt/project && dbt run --models staging',
    dag=dag
)

dbt_run_vault = BashOperator(
    task_id='dbt_run_vault',
    bash_command='cd /path/to/dbt/project && dbt run --models vault',
    dag=dag
)

dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='cd /path/to/dbt/project && dbt test',
    dag=dag
)

dbt_docs_generate = BashOperator(
    task_id='dbt_docs_generate',
    bash_command='cd /path/to/dbt/project && dbt docs generate',
    dag=dag
)

# Task dependencies
dbt_deps >> dbt_run_staging >> dbt_run_vault >> dbt_test >> dbt_docs_generate
