**Architecture & Design Document: CrewAI-Powered ETL Automation Platform**

---

### 🌐 Overview

This solution is a modular, extensible Streamlit web application that enables teams to automate ETL code generation, data lineage creation, and data quality testing by ingesting a metadata spreadsheet. It uses CrewAI agents with distinct roles to simulate subject-matter experts collaborating on data engineering tasks.

---

### ⚖️ Core Components

#### 1. **Streamlit UI**

* Provides a web interface for users to:

  * Upload ETL metadata spreadsheets
  * Select the target platform (Databricks, EMR, or AWS Glue)
  * Trigger agent tasks manually
* Organizes outputs into timestamped folders under `./output/YYYYMMDD_HHMMSS/`

#### 2. **CrewAI Agents**

Each agent is instantiated via CrewAI and assigned specific responsibilities:

| Agent          | Role             | Responsibilities                                         |
| -------------- | ---------------- | -------------------------------------------------------- |
| ArchitectAgent | Data Architect   | Analyze metadata and infer schema and keys               |
| ETLAgent       | ETL Developer    | Generate platform-specific PySpark code                  |
| LineageAgent   | Metadata Steward | Create lineage JSON compliant with Collibra spec         |
| DQAgent        | QA/Data Quality  | Write Great Expectations + PyTest-based validation tests |

#### 3. **Generated Artifacts**

| Folder       | Output File    | Description                                          |
| ------------ | -------------- | ---------------------------------------------------- |
| `/etl/`      | `etl_job.py`   | PySpark script for ETL based on spreadsheet rules    |
| `/metadata/` | `lineage.json` | JSON lineage graph using Collibra's single-file spec |
| `/tests/`    | `test_dq.py`   | PyTest-based test suite with Great Expectations      |

---

### 🔧 Implementation Details

#### PySpark Code Generation

* Abstracts environment-specific logic (Databricks, EMR, Glue)
* Generates Spark session and minimal ETL logic using PySpark API

#### Collibra Lineage JSON

* Conforms to the [Collibra single-file lineage spec](https://productresources.collibra.com/docs/collibra/latest/Content/CollibraDataLineage/CustomTechnicalLineage/ref_custom-lineage-json-file.htm)
* Includes:

  * `codeGraph` with `nodes` and `edges`
  * `sourceProperties` and `targetProperties`
  * `script` with code format and placeholder content

#### Data Quality Tests

* Parses "Data Quality Rules" from spreadsheet
* Generates:

  * Great Expectations validations
  * PyTest test methods
  * A `load_data()` fixture for test data
  * `__main__` runner block for manual test execution

---

### ⚡ Extensibility

* Supports future JS-based UI enhancement
* Can integrate with Airflow/dbt pipelines or Collibra API
* Additional CrewAI agents can be added (e.g., ComplianceAgent, MDMPlanner)

---

### ✅ Usage Summary

1. Upload a spreadsheet with required sheets:

   * Source Metadata
   * Target Metadata
   * Data Quality Rules
2. Choose the PySpark platform target
3. Trigger agents one-by-one to generate outputs
4. Review and execute generated code manually or in pipelines

---

### 📄 Future Enhancements

## Security Considerations
- The API key is stored in the `.env` file, which is not a secure way to store sensitive information.
- It is highly recommended to use a more secure method to store and manage API keys, such as:
    - **Environment variables:** Set the API key as an environment variable on the system where the application is running.
    - **Secrets management system:** Use a dedicated secrets management system like HashiCorp Vault or AWS Secrets Manager to store and manage the API key.

* Drag-and-drop spreadsheet builder
* Previews of ETL DAGs and lineage graphs
* Real-time GPT-guided metadata curation
* Containerization and CI/CD pipeline integration
