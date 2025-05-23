**How to Use the ETL Metadata Spreadsheet for Data Engineering Projects**

This guide helps business analysts, data modelers, engineers, and LLM agents use the metadata spreadsheet effectively for automated ETL code generation, data quality validation, and lineage reporting.

---

### 📄 Spreadsheet Overview

The spreadsheet contains these key tabs:

---

### 1. **Source Metadata**
Describes the structure of source systems.

| Column             | Purpose                                                   |
|--------------------|-----------------------------------------------------------|
| Source System Name | Source DB or app name                                     |
| Source Table Name  | Source table or file name                                 |
| Source Column Name | Actual field name in source                               |
| Data Type          | Source column data type                                   |
| Nullable           | Whether NULL is allowed                                   |
| Primary Key?       | Flags primary key columns                                 |
| Default Value      | Default/fallback values                                   |
| Comments           | Business notes or transformations                         |

---

### 2. **Target Metadata**
Captures intended structure in the data warehouse.

| Column                    | Purpose                                                 |
|---------------------------|---------------------------------------------------------|
| Target System             | E.g., Snowflake                                         |
| Target Schema             | Namespace for tables                                    |
| Target Table Name         | Target table name                                       |
| Target Column Name        | Final column name                                       |
| Target Data Type          | Column data type                                        |
| Transformation Logic      | Description of logic to convert source to target        |
| Is Derived?               | If derived from other columns                           |
| Load Order / Priority     | Load sequence planning                                  |
| Partition Key?            | Whether this column is used for partitioning            |
| Hash Key or Business Key? | For vault modeling or key generation                    |

---

### 3. **Mapping Metadata**
Provides detailed logic for transformation.

| Column                     | Purpose                                                  |
|----------------------------|----------------------------------------------------------|
| Mapping Rule ID            | Unique ID for traceability                               |
| Business Rule / Expression | Expression for derived/calculated fields                 |
| Lookup Table Used          | If logic involves a lookup from another table            |
| Filter Conditions          | Row-level filters or predicates                          |
| Join Conditions            | Logic to join with other sources                         |
| Aggregation Rule           | If aggregation is needed (e.g., SUM, MAX, GROUP BY)      |

---

### 4. **ETL Metadata**
ETL execution configuration.

| Column                   | Purpose                                                |
|--------------------------|--------------------------------------------------------|
| Load Frequency           | Daily, weekly, monthly                                 |
| Incremental or Full Load | How records are reloaded                               |
| Watermark Column         | Used for incremental loads                             |
| Source File Pattern      | Wildcard patterns for S3/files                          |
| Error Handling Logic     | Quarantine/log/drop strategy                           |
| Pre/Post Load Script     | Scripts to run before or after job                     |
| ETL Job Owner            | Owner or SME contact                                   |
| Dependencies             | Job/table dependencies                                 |

---

### 5. **Data Quality Rules**
Column-level validation logic.

| Column                | Purpose                                                         |
|-----------------------|-----------------------------------------------------------------|
| Validations           | Not Null, Range(min,max), Regex(pattern)                        |
| Reject Handling       | Action if validation fails (quarantine/log/discard)             |
| Record Count Expected | Optional row count threshold                                    |

---

### 6. **Security & Compliance**
Captures governance intent.

| Column              | Purpose                                   |
|---------------------|-------------------------------------------|
| PII/PHI Flag        | Mark sensitive data                       |
| Masking Required    | Whether to mask the value in outputs      |
| Encryption Required | Whether encryption is required            |

---

### 🔄 Workflow Summary

1. **Business Analyst**: Starts with Source + Mapping + Rules
2. **Data Modeler**: Fills Target Metadata + ETL configuration
3. **ETL Developer**: Uses all sheets to generate logic + testing
4. **LLM Agent**: Automatically reads and transforms the metadata
5. **QA/Data Governance**: Reviews DQ and compliance logic

---

### ✅ Best Practices

- Use consistent table/column names across tabs
- Keep expressions readable for LLMs (avoid vague references)
- Validate sheet format before uploading
- Use version control (e.g., `v2`, `draft`, `final`) in filename
- Prefer separate records for each transformation rule in Mapping tab

---

This sheet is your contract between humans and AI agents for building production-grade ETL jobs.
