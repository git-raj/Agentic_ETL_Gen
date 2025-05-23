import os
import textwrap
from crewai import Agent
from src.dq_tasks import generate_dq_tests
from src.etl_tasks import generate_etl_code
from src.lineage_tasks import generate_lineage_json
from src.airflow_task import generate_airflow_dag

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
            "Airflow Developer": self._generate_airflow_dag
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

class Agents:
    def __init__(self, llm):
        self.etl_agent = CustomAgent(
            role="ETL Developer",
            goal="Generate well formatted and inline documented ETL code using PySpark for a given platform.",
            backstory="Senior experienced PySpark developer who is expert in building pipelines across Databricks, EMR, and AWS Glue.",
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

        if generate_etl and source_metadata_df is not None and target_metadata_df is not None:
            outputs["etl"] = self.etl_agent.execute_task(
                metadata=source_metadata_df,
                target_metadata=target_metadata_df,
                mapping_metadata=mapping_metadata_df,
                dq_metadata=dq_rules_df,
                target_platform=target_platform,
                use_agentic=use_agentic
            )

        if generate_airflow and etl_script:
            outputs["airflow_dag"] = self.airflow_agent.execute_task(
                etl_script=etl_script,
                metadata=etl_metadata_df,
                generation_mode=airflow_generation_mode,
                use_agentic=use_agentic
            )

        if generate_lineage and mapping_metadata_df is not None:
            outputs["lineage"] = self.lineage_agent.execute_task(
                metadata=mapping_metadata_df,
                use_agentic=use_agentic
            )

        if generate_dq and target_metadata_df is not None:
            outputs["dq"] = self.dq_agent.execute_task(
                metadata=target_metadata_df,
                use_agentic=use_agentic
            )

        return outputs
