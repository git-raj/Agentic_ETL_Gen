import pandas as pd

def generate_airflow_dag(etl_script, metadata, llm=None, use_agentic=False):
    print('Generating Airflow DAG...')
    watermark_column = ''
    if isinstance(metadata, pd.DataFrame) and 'Watermark Column' in metadata.columns:
        non_empty = metadata['Watermark Column'].dropna()
        if not non_empty.empty:
            watermark_column = str(non_empty.iloc[0]).strip()

    prompt_addition = f"""
MANDATORY:
1. This ETL uses a watermark column `{watermark_column}` for incremental loading.
2. DO NOT omit the following:
   - Fetch `last_watermark` using `Variable.get("last_watermark")`
   - Set `os.environ["WATERMARK"] = watermark`
   - After ETL, read max({watermark_column}) from the output and call `Variable.set("last_watermark", value)`
   - Use a separate Airflow task named `update_watermark`
3. Ensure the DAG includes this logic as a part of the execution chain.
""" if watermark_column else ""

    if use_agentic and llm is not None:
        prompt = f"""
You are an expert in building production-grade Apache Airflow DAGs.
Using the following ETL script path and metadata, generate a Python DAG that:
- Uses PythonOperator for ETL logic
- Includes optional pre/post hooks if provided
- Supports dynamic watermarking if configured
- Includes retry logic, owner, scheduling, and logging
- Adds a task to update the watermark after ETL completion

ETL Script Path:
{etl_script}

Metadata:
{metadata.to_json(indent=2)}

{prompt_addition}

Return only a valid Python Airflow DAG script.
"""
        try:
            response = llm.invoke([{"role": "user", "content": prompt}])
            return response.content.strip()
        except Exception as e:
            print(f"LLM DAG generation failed, falling back to template. Error: {e}")

    # Fallback to template logic
    dag_name = metadata.get('dag_name', 'etl_dag')
    schedule_interval = metadata.get('Load Frequency', '@daily')
    retry_delay_minutes = 5
    retries = 1
    start_date = '2023, 1, 1'

    error_handling = metadata.get('Error Handling Logic', 'log and continue')
    pre_script = metadata.get('Pre/Post Load Script', '').split(';')[0].strip()
    post_script = metadata.get('Pre/Post Load Script', '').split(';')[1].strip() if ';' in metadata.get('Pre/Post Load Script', '') else ''

    etl_owner = metadata.get('ETL Job Owner', 'data_engineering')
    dependencies = metadata.get('Dependencies', '')

    watermark_handling = f"""
    from airflow.models import Variable
    watermark = Variable.get("last_watermark", default_var="1970-01-01")
    os.environ["WATERMARK"] = watermark
    """ if watermark_column else ""

    update_watermark_function = f"""
def update_watermark():
    import pandas as pd
    from airflow.models import Variable
    try:
        df = pd.read_parquet("/path/to/output")  # Update this path to your ETL output
        new_value = df['{watermark_column}'].max()
        if new_value:
            Variable.set("last_watermark", str(new_value))
    except Exception as e:
        print("Failed to update watermark:", e)
""" if watermark_column else ""

    airflow_dag_template = f'''
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.dirname("{etl_script}"))

{f"from airflow.models import Variable" if watermark_column else ''}

{update_watermark_function.strip()}

def pre_etl():
    {f"exec(open('{pre_script}').read())" if pre_script else 'pass'}

def execute_etl_script():
    {watermark_handling.strip()}
    with open("{etl_script}", 'r') as file:
        exec(file.read())

def post_etl():
    {f"exec(open('{post_script}').read())" if post_script else 'pass'}

default_args = {{
    'owner': '{etl_owner}',
    'depends_on_past': False,
    'start_date': datetime({start_date}),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': {retries},
    'retry_delay': timedelta(minutes={retry_delay_minutes}),
}}

dag = DAG(
    '{dag_name}',
    default_args=default_args,
    description='Automated ETL DAG with metadata-driven configs',
    schedule_interval='{schedule_interval}',
    catchup=False
)

pre_task = PythonOperator(
    task_id='pre_etl',
    python_callable=pre_etl,
    dag=dag
)

etl_task = PythonOperator(
    task_id='run_etl',
    python_callable=execute_etl_script,
    dag=dag
)

{f"update_task = PythonOperator(\n    task_id='update_watermark',\n    python_callable=update_watermark,\n    dag=dag\n)" if watermark_column else ''}

post_task = PythonOperator(
    task_id='post_etl',
    python_callable=post_etl,
    dag=dag
)

{f"pre_task >> etl_task >> update_task >> post_task" if watermark_column else "pre_task >> etl_task >> post_task"}
'''

    return airflow_dag_template
