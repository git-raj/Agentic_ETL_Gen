from typing import Any
import os
import pandas as pd

class CustomAgent:
    def __init__(self, role: str, goal: str, backstory: str, verbose: bool, llm_option: str):
        self.role = role
        self.goal = goal
        self.backstory = backstory
        self.verbose = verbose
        self.llm_option = llm_option

    def execute_task(self, **kwargs: Any) -> str:
        """
        Executes a task based on the agent's role and the provided metadata.
        """
        if self.role == "ETL Developer":
            from etl_tasks import generate_etl_code
            return generate_etl_code(
                source_metadata=kwargs["metadata"],
                target_metadata=kwargs["target_metadata"],
                target_platform=kwargs["target_platform"],
                llm_option=self.llm_option
            )

        elif self.role == "Metadata Steward":
            from lineage_tasks import generate_lineage_json
            return generate_lineage_json(
                mapping_metadata=kwargs["metadata"],
                llm_option=self.llm_option
            )

        elif self.role == "QA/Data Quality":
            from dq_tasks import generate_data_quality_tests
            return generate_data_quality_tests(
                metadata=kwargs["metadata"],
                llm_option=self.llm_option
            )

        elif self.role == "Data Architect":
            metadata = kwargs["metadata"]
            target_metadata = kwargs.get("target_metadata")
            mapping_metadata = kwargs.get("mapping_metadata")

            summary = []
            summary.append("=== Data Architect Report ===\n")

            # Analyze primary keys
            if 'Primary Key?' in metadata.columns:
                pk_info = metadata[metadata['Primary Key?'].str.lower() == 'yes']
                summary.append("Primary Keys Found:\n")
                for table in pk_info['Source Table Name'].unique():
                    cols = pk_info[pk_info['Source Table Name'] == table]['Source Column Name'].tolist()
                    summary.append(f"- Table: {table}, Primary Keys: {', '.join(cols)}")
            else:
                summary.append("No 'Primary Key?' column found in metadata\n")

            # Check nullable keys
            if 'Nullable' in metadata.columns and 'Primary Key?' in metadata.columns:
                nullable_pks = metadata[(metadata['Primary Key?'].str.lower() == 'yes') & (metadata['Nullable'].str.lower() == 'yes')]
                if not nullable_pks.empty:
                    summary.append("\nWarning: Primary Keys marked nullable:\n")
                    for _, row in nullable_pks.iterrows():
                        summary.append(f"- {row['Source Table Name']} -> {row['Source Column Name']}")

            # Infer potential relationships
            if mapping_metadata is not None and 'Join Conditions' in mapping_metadata.columns:
                joins = mapping_metadata['Join Conditions'].dropna()
                summary.append("\nPotential Foreign Key Joins:\n")
                for join in joins:
                    summary.append(f"- {join}")

            # Metadata completeness check
            required_columns = ['Source System Name', 'Source Table Name', 'Source Column Name', 'Data Type']
            missing_columns = [col for col in required_columns if col not in metadata.columns]
            if missing_columns:
                summary.append(f"\nMissing required metadata columns: {', '.join(missing_columns)}")

            return "\n".join(summary)

        else:
            return f"No handler implemented for role: {self.role}"

class Agents:
    def __init__(self, llm_option: str):
        self.architect_agent = CustomAgent(
            role='Data Architect',
            goal='Analyze metadata and infer schema and keys',
            backstory="You are an experienced data architect with a deep understanding of data modeling and database design.",
            verbose=True,
            llm_option=llm_option
        )
        self.etl_agent = CustomAgent(
            role='ETL Developer',
            goal='Generate platform-specific PySpark code',
            backstory="You are a skilled ETL developer with expertise in PySpark and various data platforms.",
            verbose=True,
            llm_option=llm_option
        )
        self.lineage_agent = CustomAgent(
            role='Metadata Steward',
            goal='Create lineage JSON compliant with Collibra spec',
            backstory="You are a meticulous metadata steward with a strong understanding of data lineage and Collibra's specifications.",
            verbose=True,
            llm_option=llm_option
        )
        self.dq_agent = CustomAgent(
            role='QA/Data Quality',
            goal='Write Great Expectations + PyTest-based validation tests',
            backstory="You are a detail-oriented QA engineer with expertise in data quality testing and Great Expectations.",
            verbose=True,
            llm_option=llm_option
        )
