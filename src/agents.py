import os
import textwrap
from crewai import Agent
from src.dq_tasks import generate_dq_tests
from src.etl_tasks import generate_etl_code
from src.lineage_tasks import generate_lineage_json
from src.airflow_task import generate_airflow_dag
from src.dbt_vault_tasks import generate_dbt_vault_models, generate_dbt_project_files
from src.dbt_test_tasks import generate_dbt_tests, generate_dbt_sources_yml
from src.code_validator import CodeValidator

class CustomAgent:
    def __init__(self, role, goal, backstory, llm):
        self.role = role
        self.goal = goal
        self.backstory = backstory
        self.llm = llm
        self.task_fn = self._get_task_function()

    def _get_task_function(self):
        return {
            "ETL Developer": generate_etl_code,
            "Data Quality Analyst": generate_dq_tests,
            "Metadata Steward": generate_lineage_json,
            "Airflow Developer": self._generate_airflow_dag,
            "dbT Vault Developer": generate_dbt_vault_models,
            "dbT Test Developer": generate_dbt_tests
        }.get(self.role)

    def _generate_airflow_dag(self, etl_script, metadata, llm, use_agentic=False, generation_mode='llm'):
        if generation_mode == 'llm':
            return generate_airflow_dag(etl_script, metadata, llm, use_agentic=True)
        else:
            return generate_airflow_dag(etl_script, metadata)

#     def _llm_airflow_generation(self, etl_script, metadata, llm):
#         prompt = f"""
# You are an expert Airflow DAG developer. Create an Apache Airflow DAG script to execute the following ETL script:

# ETL Script:
# {etl_script}

# Metadata Context:
# {metadata.to_json()}

# Requirements:
# 1. Create a DAG with appropriate scheduling
# 2. Include error handling and logging
# 3. Use PythonOperator to execute the ETL script
# 4. Add retry mechanisms
# 5. Implement basic monitoring

# Return only the complete Airflow DAG Python script.
# """
#         try:
#             response = llm.invoke([{"role": "user", "content": prompt}])
#             return response.content.strip()
#         except Exception as e:
#             print(f"LLM Airflow Generation Error: {e}")
#             return generate_airflow_dag(etl_script, metadata)


    def execute_task(self, **kwargs):
        if not self.task_fn:
            raise ValueError(f"No task defined for role: {self.role}")

        common_args = {
            "llm": self.llm,
            "use_agentic": kwargs.get("use_agentic", False)
        }

        if self.role == "ETL Developer":
            return self.task_fn(
                source_metadata=kwargs["metadata"],
                target_metadata=kwargs["target_metadata"],
                mapping_metadata=kwargs.get("mapping_metadata"),
                dq_metadata=kwargs.get("dq_metadata"),
                target_platform=kwargs["target_platform"],
                **common_args
            )
        elif self.role == "Airflow Developer":
            return self.task_fn(
                etl_script=kwargs["etl_script"],
                metadata=kwargs["metadata"],
                generation_mode=kwargs.get("generation_mode", "llm"),
                **common_args
            )
        elif self.role == "Data Quality Analyst":
            return self.task_fn(metadata=kwargs["metadata"], **common_args)
        elif self.role == "Metadata Steward":
            return self.task_fn(mapping_metadata=kwargs["metadata"], **common_args)
        elif self.role == "dbT Vault Developer":
            return self.task_fn(
                source_metadata=kwargs["metadata"],
                target_metadata=kwargs["target_metadata"],
                mapping_metadata=kwargs.get("mapping_metadata"),
                **common_args
            )
        elif self.role == "dbT Test Developer":
            return self.task_fn(metadata=kwargs["metadata"], **common_args)

class Agents:
    def __init__(self, llm):
        self.etl_agent = CustomAgent(
            role="ETL Developer",
            goal="Generate well formatted and inline documented ETL code using PySpark for a given platform.",
            backstory="Senior experienced PySpark developer who is expert in building pipelines across Databricks, EMR, AWS Glue and Snowflake Snowpark.",
            llm=llm
        )
        self.dq_agent = CustomAgent(
            role="Data Quality Analyst",
            goal="Create comprehensive data quality test scripts using Great Expectations and PyTest.",
            backstory="Skilled in implementing DQ frameworks and validations at large enterprises.",
            llm=llm
        )
        self.lineage_agent = CustomAgent(
            role="Metadata Steward",
            goal="Produce Collibra-compatible lineage JSON from the metadata spreadsheet.",
            backstory="Specialist in metadata governance, Collibra, and enterprise data catalog integration.",
            llm=llm
        )
        self.airflow_agent = CustomAgent(
            role="Airflow Developer",
            goal="Generate Apache Airflow DAG scripts to orchestrate ETL jobs with robust error handling and monitoring.",
            backstory="Expert in creating scalable and maintainable Airflow pipelines for enterprise data workflows.",
            llm=llm
        )
        self.dbt_vault_agent = CustomAgent(
            role="dbT Vault Developer",
            goal="Generate Data Vault 2.0 models using dbT for modern data warehousing.",
            backstory="Expert in Data Vault methodology and dbT framework, specializing in creating scalable and maintainable data vault structures.",
            llm=llm
        )
        self.dbt_test_agent = CustomAgent(
            role="dbT Test Developer",
            goal="Create comprehensive dbT-native data quality tests and validation frameworks.",
            backstory="Specialist in dbT testing patterns, data quality validation, and test-driven data development.",
            llm=llm
        )

    def run(
        self,
        source_metadata_df=None,
        target_metadata_df=None,
        mapping_metadata_df=None,
        etl_metadata_df=None,
        dq_rules_df=None,
        target_platform="Databricks",
        use_agentic=False,
        etl_script=None,
        airflow_generation_mode='llm',
        generate_etl=True,
        generate_dq=True,
        generate_lineage=True,
        generate_airflow=True
    ):
        outputs = {}

        # Check if dbT Raw Vault platform is selected
        is_dbt_vault = target_platform == "dbT Raw Vault"

        if generate_etl and source_metadata_df is not None and target_metadata_df is not None:
            if is_dbt_vault:
                # Use dbT Vault agent for dbT Raw Vault platform
                vault_models = self.dbt_vault_agent.execute_task(
                    metadata=source_metadata_df,
                    target_metadata=target_metadata_df,
                    mapping_metadata=mapping_metadata_df,
                    use_agentic=use_agentic
                )
                
                # Generate dbT project files
                project_files = generate_dbt_project_files()
                
                # Validate dbT models if agentic generation is enabled
                if use_agentic:
                    validator = CodeValidator()
                    validated_models = {}
                    for model_name, model_sql in vault_models.items():
                        validation_errors = validator.validate_code(model_sql, "dbt")
                        if validation_errors:
                            print(f"⚠️ Validation warnings for {model_name}: {validation_errors}")
                            # For now, we'll keep the model but log warnings
                            # Future enhancement: implement retry logic with LLM feedback
                        validated_models[model_name] = model_sql
                    vault_models = validated_models
                
                # Combine vault models and project files
                outputs["etl"] = {
                    "vault_models": vault_models,
                    "project_files": project_files
                }
            else:
                # Use regular ETL agent for other platforms
                etl_code = self.etl_agent.execute_task(
                    metadata=source_metadata_df,
                    target_metadata=target_metadata_df,
                    mapping_metadata=mapping_metadata_df,
                    dq_metadata=dq_rules_df,
                    target_platform=target_platform,
                    use_agentic=use_agentic
                )
                
                # Validate Python ETL code if agentic generation is enabled
                if use_agentic and isinstance(etl_code, str):
                    validator = CodeValidator()
                    validation_errors = validator.validate_code(etl_code, "python")
                    if validation_errors:
                        print(f"⚠️ Validation warnings for ETL code: {validation_errors}")
                        # Future enhancement: implement retry logic with LLM feedback
                
                outputs["etl"] = etl_code

        if generate_dq and target_metadata_df is not None:
            if is_dbt_vault:
                # Use dbT Test agent for dbT Raw Vault platform
                outputs["dq"] = self.dbt_test_agent.execute_task(
                    metadata=target_metadata_df,
                    use_agentic=use_agentic
                )
                
                # Also generate sources.yml
                sources_yml = generate_dbt_sources_yml(source_metadata_df)
                if sources_yml:
                    outputs["sources_yml"] = sources_yml
                
                # Validate dbT test files if agentic generation is enabled
                if use_agentic and isinstance(outputs["dq"], dict):
                    validator = CodeValidator()
                    validated_dq_files = {}
                    for file_name, file_content in outputs["dq"].items():
                        file_type = "sql" if file_name.endswith(".sql") else "dbt" # Assuming schema.yml is 'dbt' for basic checks
                        validation_errors = validator.validate_code(file_content, file_type)
                        if validation_errors:
                            print(f"⚠️ Validation warnings for DQ file {file_name}: {validation_errors}")
                        validated_dq_files[file_name] = file_content
                    outputs["dq"] = validated_dq_files
            else:
                # Use regular DQ agent for other platforms
                dq_code = self.dq_agent.execute_task(
                    metadata=target_metadata_df,
                    use_agentic=use_agentic
                )
                
                # Validate Python DQ code if agentic generation is enabled
                if use_agentic and isinstance(dq_code, str):
                    validator = CodeValidator()
                    validation_errors = validator.validate_code(dq_code, "python")
                    if validation_errors:
                        print(f"⚠️ Validation warnings for DQ code: {validation_errors}")
                outputs["dq"] = dq_code

        if generate_airflow:
            if is_dbt_vault:
                # Generate dbT-specific Airflow DAG
                airflow_dag_code = self._generate_dbt_airflow_dag(
                    etl_metadata_df, use_agentic
                )
                # Validate dbT Airflow DAG if agentic generation is enabled
                if use_agentic and isinstance(airflow_dag_code, str):
                    validator = CodeValidator()
                    validation_errors = validator.validate_code(airflow_dag_code, "python")
                    if validation_errors:
                        print(f"⚠️ Validation warnings for dbT Airflow DAG: {validation_errors}")
                outputs["airflow_dag"] = airflow_dag_code
            elif etl_script:
                # Generate regular Airflow DAG
                airflow_dag_code = self.airflow_agent.execute_task(
                    etl_script=etl_script,
                    metadata=etl_metadata_df,
                    generation_mode=airflow_generation_mode,
                    use_agentic=use_agentic
                )
                # Validate regular Airflow DAG if agentic generation is enabled
                if use_agentic and isinstance(airflow_dag_code, str):
                    validator = CodeValidator()
                    validation_errors = validator.validate_code(airflow_dag_code, "python")
                    if validation_errors:
                        print(f"⚠️ Validation warnings for Airflow DAG: {validation_errors}")
                outputs["airflow_dag"] = airflow_dag_code

        if generate_lineage and mapping_metadata_df is not None:
            outputs["lineage"] = self.lineage_agent.execute_task(
                metadata=mapping_metadata_df,
                use_agentic=use_agentic
            )

        return outputs

    def _generate_dbt_airflow_dag(self, metadata, use_agentic=False):
        """Generate dbT-specific Airflow DAG"""
        dag_template = '''
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
'''
        return dag_template
