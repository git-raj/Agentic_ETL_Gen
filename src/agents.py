from typing import Any
import os
import pandas as pd
from llm_utils import get_llm_response, parse_llm_function_call
from etl_tasks import generate_etl_code
from lineage_tasks import generate_lineage_json
from dq_tasks import generate_data_quality_tests
from crewai import Crew, Agent, Task
from dotenv import load_dotenv

load_dotenv()

class Agents:
    def __init__(self, llm_option: str, all_metadata_dfs: dict, source_metadata_df: pd.DataFrame, target_metadata_df: pd.DataFrame, mapping_metadata_df: pd.DataFrame, etl_metadata_df: pd.DataFrame, data_quality_rules_df: pd.DataFrame, security_compliance_df: pd.DataFrame, target_platform: str):
        self.llm_option = llm_option
        self.all_metadata_dfs = all_metadata_dfs
        self.source_metadata_df = source_metadata_df
        self.target_metadata_df = target_metadata_df
        self.mapping_metadata_df = mapping_metadata_df
        self.etl_metadata_df = etl_metadata_df
        self.data_quality_rules_df = data_quality_rules_df
        self.security_compliance_df = security_compliance_df
        self.target_platform = target_platform

        if self.llm_option == "Google Gemini":
            llm_model = "models/gemini-pro"
        else:
            llm_model = os.getenv("OPENAI_LLM_MODEL", "gpt-3.5-turbo")

        self.technical_lead = Agent(
            role='Technical Lead',
            goal='Oversee the entire ETL process and ensure high-quality code generation, validation, and QA.',
            backstory="You are an experienced technical lead with a deep understanding of data modeling, database design, and ETL processes. You are responsible for orchestrating the work of other agents to ensure the successful completion of the ETL project.",
            verbose=True,
            llm={"model": llm_model},
            allow_delegation=True
        )

        self.architect_agent = Agent(
            role='Data Architect',
            goal='Analyze metadata and infer schema and keys, and potential relationships.',
            backstory="You are an experienced data architect with a deep understanding of data modeling and database design.",
            verbose=True,
            llm={"model": llm_model}
        )
        self.etl_agent = Agent(
            role='ETL Developer',
            goal='Generate platform-specific PySpark code.',
            backstory="You are a skilled ETL developer with expertise in PySpark and various data platforms.",
            verbose=True,
            llm={"model": llm_model}
        )
        self.lineage_agent = Agent(
            role='Metadata Steward',
            goal='Create lineage JSON compliant with Collibra spec.',
            backstory="You are a meticulous metadata steward with a strong understanding of data lineage and Collibra's specifications.",
            verbose=True,
            llm={"model": llm_model}
        )
        self.dq_agent = Agent(
            role='QA/Data Quality',
            goal='Write Great Expectations + PyTest-based validation tests.',
            backstory="You are a detail-oriented QA engineer with expertise in data quality testing and Great Expectations.",
            verbose=True,
            llm={"model": llm_model}
        )

        self.architect_task = Task(
            description=f"""Analyze the provided metadata from the following sheets: {list(self.all_metadata_dfs.keys())}.
            Infer the schema, keys, and potential relationships between the tables.
            Detail any missing critical metadata.
            Provide a comprehensive summary report.
            Target Platform: {self.target_platform}""",
            agent=self.architect_agent,
            expected_output=""
        )

        self.etl_task = Task(
            description=f"""Generate platform-specific PySpark code based on the provided metadata.
            Source Metadata: {self.source_metadata_df}
            Target Metadata: {self.target_metadata_df}
            Target Platform: {self.target_platform}""",
            agent=self.etl_agent,
            expected_output=""
        )

        self.lineage_task = Task(
            description=f"""Create lineage JSON compliant with Collibra specifications based on the provided mapping metadata.
            Mapping Metadata: {self.mapping_metadata_df}""",
            agent=self.lineage_agent,
            expected_output=""
        )

        self.dq_task = Task(
            description=f"""Write Great Expectations + PyTest-based validation tests based on the provided data quality rules.
            Data Quality Rules: {self.data_quality_rules_df}""",
            agent=self.dq_agent,
            expected_output=""
        )

        self.crew = Crew(
            agents=[self.technical_lead, self.architect_agent, self.etl_agent, self.lineage_agent, self.dq_agent],
            tasks=[self.architect_task, self.etl_task, self.lineage_task, self.dq_task],
            verbose=True
        )

    def run(self):
        result = self.crew.kickoff()
        return result
