import os
from crewai import Agent
from src.dq_tasks import generate_dq_tests
from src.etl_tasks import generate_etl_code
from src.lineage_tasks import generate_lineage_json

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
            "Metadata Steward": generate_lineage_json
        }.get(self.role)

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
                target_platform=kwargs["target_platform"],
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
            goal="Generate ETL code using PySpark for a given platform.",
            backstory="Experienced PySpark developer who has built pipelines across Databricks, EMR, and Glue.",
            llm=llm
        )

        self.dq_agent = CustomAgent(
            role="Data Quality Analyst",
            goal="Create data quality test scripts using Great Expectations and PyTest.",
            backstory="Skilled in implementing DQ frameworks and validations at large enterprises.",
            llm=llm
        )

        self.lineage_agent = CustomAgent(
            role="Metadata Steward",
            goal="Produce Collibra-compatible lineage JSON from the metadata spreadsheet.",
            backstory="Specialist in metadata governance, Collibra, and enterprise data catalog integration.",
            llm=llm
        )

    def run(self, source_metadata_df=None, target_metadata_df=None, mapping_metadata_df=None, target_platform="Databricks", use_agentic=False):
        outputs = {}

        if source_metadata_df is not None and target_metadata_df is not None:
            outputs["etl"] = self.etl_agent.execute_task(
                metadata=source_metadata_df,
                target_metadata=target_metadata_df,
                target_platform=target_platform,
                use_agentic=use_agentic
            )

        if mapping_metadata_df is not None:
            outputs["lineage"] = self.lineage_agent.execute_task(
                metadata=mapping_metadata_df,
                use_agentic=use_agentic
            )

        if target_metadata_df is not None:
            outputs["dq"] = self.dq_agent.execute_task(
                metadata=target_metadata_df,
                use_agentic=use_agentic
            )

        return outputs
