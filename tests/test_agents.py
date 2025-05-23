import os
import sys
import pytest
import pandas as pd
from unittest.mock import Mock, patch

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.agents import Agents, CustomAgent
from src.llm_utils import get_llm_instance

class MockLLM:
    def invoke(self, messages):
        class MockResponse:
            def __init__(self, content):
                self.content = content
        return MockResponse("""
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def execute_etl_script():
    print("Executing ETL script")

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
    'mock_etl_dag',
    default_args=default_args,
    description='Automated ETL DAG',
    schedule_interval='@daily',
    catchup=False
)

etl_task = PythonOperator(
    task_id='execute_etl_job',
    python_callable=execute_etl_script,
    dag=dag
)
""")

def create_mock_metadata():
    """Create a mock metadata DataFrame for testing"""
    return pd.DataFrame({
        'Source Column': ['id', 'name', 'value'],
        'Target Column': ['user_id', 'user_name', 'user_value'],
        'Data Type': ['int', 'string', 'float']
    })

def test_airflow_agent_initialization():
    """Test initialization of Airflow agent"""
    mock_llm = MockLLM()
    agents = Agents(llm=mock_llm)
    
    assert hasattr(agents, 'airflow_agent'), "Airflow agent should be initialized"
    assert agents.airflow_agent.role == "Airflow Developer", "Airflow agent should have correct role"

def test_airflow_dag_generation_llm_mode():
    """Test Airflow DAG generation using LLM mode"""
    mock_llm = MockLLM()
    agents = Agents(llm=mock_llm)
    
    # Prepare mock metadata and ETL script path
    source_metadata = create_mock_metadata()
    etl_script_path = "/tmp/mock_etl_script.py"
    
    # Generate Airflow DAG
    results = agents.run(
        source_metadata_df=source_metadata,
        target_metadata_df=source_metadata,
        etl_script=etl_script_path,
        airflow_generation_mode='llm'
    )
    
    # Verify Airflow DAG generation
    assert 'airflow_dag' in results, "Airflow DAG should be generated"
    airflow_dag = results['airflow_dag']
    
    # Check basic Airflow DAG characteristics
    assert 'import airflow' in airflow_dag, "Airflow import should be present"
    assert 'DAG(' in airflow_dag, "DAG definition should be present"
    assert 'PythonOperator' in airflow_dag, "PythonOperator should be used"

def test_airflow_dag_generation_template_mode():
    """Test Airflow DAG generation using template mode"""
    mock_llm = MockLLM()
    agents = Agents(llm=mock_llm)
    
    # Prepare mock metadata and ETL script path
    source_metadata = create_mock_metadata()
    etl_script_path = "/tmp/mock_etl_script.py"
    
    # Generate Airflow DAG
    results = agents.run(
        source_metadata_df=source_metadata,
        target_metadata_df=source_metadata,
        etl_script=etl_script_path,
        airflow_generation_mode='template'
    )
    
    # Verify Airflow DAG generation
    assert 'airflow_dag' in results, "Airflow DAG should be generated"
    airflow_dag = results['airflow_dag']
    
    # Check basic Airflow DAG characteristics
    assert 'import airflow' in airflow_dag, "Airflow import should be present"
    assert 'DAG(' in airflow_dag, "DAG definition should be present"
    assert 'PythonOperator' in airflow_dag, "PythonOperator should be used"
    assert etl_script_path in airflow_dag, "ETL script path should be in the DAG"

def test_airflow_agent_error_handling():
    """Test error handling in Airflow DAG generation"""
    class ErrorLLM:
        def invoke(self, messages):
            raise Exception("LLM generation failed")
    
    mock_llm = ErrorLLM()
    agents = Agents(llm=mock_llm)
    
    # Prepare mock metadata and ETL script path
    source_metadata = create_mock_metadata()
    etl_script_path = "/tmp/mock_etl_script.py"
    
    # Generate Airflow DAG
    results = agents.run(
        source_metadata_df=source_metadata,
        target_metadata_df=source_metadata,
        etl_script=etl_script_path,
        airflow_generation_mode='llm'
    )
    
    # Verify fallback to template generation
    assert 'airflow_dag' in results, "Airflow DAG should still be generated"
    airflow_dag = results['airflow_dag']
    
    # Check basic Airflow DAG characteristics
    assert 'import airflow' in airflow_dag, "Airflow import should be present"
    assert 'DAG(' in airflow_dag, "DAG definition should be present"
    assert 'PythonOperator' in airflow_dag, "PythonOperator should be used"
