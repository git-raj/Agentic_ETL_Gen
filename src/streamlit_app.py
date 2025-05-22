import streamlit as st
import pandas as pd
import os
from datetime import datetime
from pathlib import Path
from crewai import Crew
import json

from src.etl_utils import *
from src.config import initialize_agents_and_tasks # create_architect_agent, create_dq_agent, create_etl_agent, create_lineage_agent (no longer needed directly due to initialize_agents_and_tasks)
from crewai import Task

from langchain_community.chat_models import ChatOpenAI
from langchain_google_genai import ChatGoogleGenerativeAI

from dotenv import load_dotenv

load_dotenv() # Ensure .env variables are loaded

llm_provider = os.getenv("LLM_PROVIDER", "gemini").lower()
llm_model = os.getenv("LLM_MODEL")
llm_api_key = os.getenv("LLM_API_KEY")

if llm_api_key is None:
    # Use ValueError for more specific error type
    raise ValueError("LLM API Key is missing. Please set it in your .env file or environment variables.")

# Load appropriate LLM
if llm_provider == "openai":
    if not llm_model:
        raise ValueError("LLM_MODEL is not set for OpenAI provider.")
    llm = ChatOpenAI( # Corrected: Instantiating the class
        model_name=llm_model, # Corrected: parameter name for OpenAI
        temperature=0.7,
        openai_api_key=llm_api_key
    )

elif llm_provider == "gemini":
    if not llm_model:
        raise ValueError("LLM_MODEL is not set for Gemini provider.")
    
    # Strip "models/" prefix if present, as ChatGoogleGenerativeAI/LiteLLM expects the base model name.
    if llm_model.startswith("models/"):
        llm_model = llm_model.split("/")[-1]
        print(f"Stripped 'models/' prefix, using model name: {llm_model} for Gemini")

    llm = ChatGoogleGenerativeAI( # Corrected: Instantiating the class
        model=llm_model, # Corrected: parameter name for Gemini
        temperature=0.7,
        google_api_key=llm_api_key
    )

else:
    raise ValueError(f"Unsupported LLM_PROVIDER: {llm_provider}. Must be 'openai' or 'gemini'")

print(f"LLM_PROVIDER: {llm_provider}, LLM_MODEL: {llm_model}")

# Initialize agents using the configured llm
architect_agent, dq_agent, etl_agent, lineage_agent = initialize_agents_and_tasks(llm)

task_architect = Task(
    description="Analyze the metadata spreadsheet to identify table structures, keys, and dependencies. Output a summary of the schema.",
    agent=architect_agent
)

task_dq = Task(
    description="Based on the 'Data Quality Rules' sheet in the provided metadata, create data quality test scripts using Great Expectations and PyTest. The tests should reflect the rules specified (e.g., Not Null, Range checks).",
    agent=dq_agent
)

task_etl = Task(
    description="Generate PySpark ETL code based on the metadata mapping (e.g., 'Source Metadata' and 'Target Metadata' sheets) for the specified platform. The code should include SparkSession initialization appropriate for the target and basic transformation logic as suggested by the metadata.",
    agent=etl_agent
)

task_lineage = Task(
    description="Produce a Collibra-compatible lineage JSON from the 'Source Metadata' and 'Target Metadata' sheets in the metadata spreadsheet. The JSON should represent column-level lineage from source to target.",
    agent=lineage_agent
)

# Define the crew
etl_crew = Crew(
    agents=[architect_agent, etl_agent, lineage_agent, dq_agent],
    tasks=[task_architect, task_etl, task_lineage, task_dq],
    verbose=True
)

# Agent task functions
def run_etl_crew(dfs_input, target_platform): # Renamed for clarity
    # Convert DataFrames to dictionaries of records
    dfs_dict_records = {}
    for sheet_name, df_sheet in dfs_input.items():
        dfs_dict_records[sheet_name] = df_sheet.to_dict(orient='records')
    
    # --- MODIFICATION START ---
    # Remove 'original_dfs' as it contains non-serializable DataFrame objects.
    # Agents will use 'metadata_sheets_data' which is a dictionary of lists of dictionaries (records).
    crew_inputs = {
        'metadata_sheets_data': dfs_dict_records,
        'target_platform': target_platform,
        'llm_provider': llm_provider
    }
    # --- MODIFICATION END ---

    try:
        # Temporarily log the inputs to verify structure if needed (can be removed later)
        # import json
        # st.text_area("Crew Inputs (JSON serializable part)", json.dumps(crew_inputs, indent=2), height=200)

        st.write("Kicking off ETL Crew...") # Simplified log message

        result = etl_crew.kickoff(inputs=crew_inputs)
        return result
    except Exception as e:
        st.error(f"An error occurred during ETL crew execution: {e}")
        import traceback
        st.error(traceback.format_exc())
        return None


# Streamlit UI setup
st.title("🚀 CrewAI-Powered ETL Automation")

# Upload spreadsheet
uploaded_file = st.file_uploader("Upload ETL Metadata Spreadsheet (XLSX)", type=["xlsx"])

# Select target runtime
target_system = st.selectbox("Select Runtime Target for PySpark Code", ["Databricks", "EMR", "Glue"])

if uploaded_file:
    # Reading all sheets into a dictionary of DataFrames
    excel_file_sheets = pd.read_excel(uploaded_file, sheet_name=None)
    st.success(f"Spreadsheet '{uploaded_file.name}' loaded successfully. Sheets found: {', '.join(excel_file_sheets.keys())}")

    # Display sheet names and preview first one
    sheet_names = list(excel_file_sheets.keys())
    if sheet_names:
        selected_sheet = st.selectbox("Select Sheet to Preview", sheet_names)
        if selected_sheet in excel_file_sheets:
            st.dataframe(excel_file_sheets[selected_sheet].head())
    else:
        st.warning("No sheets found in the uploaded Excel file.")

    # Prepare output directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_root = Path("./output") / timestamp
    try:
        output_root.mkdir(parents=True, exist_ok=True)
        st.info(f"Output will be saved in: {output_root.resolve()}")
    except Exception as e:
        st.error(f"Could not create output directory: {e}")
        # Potentially disable run button or handle gracefully

    if st.button("✨ Run ETL Automation Crew ✨"):
        if not excel_file_sheets:
            st.error("Cannot run ETL crew: No sheets loaded from the Excel file.")
        else:
            with st.spinner("ETL Crew is processing... This may take a few moments... 🕵️‍♂️⚙️👨‍💻📜"):
                # Pass the dictionary of DataFrames and the target system
                etl_result = run_etl_crew(excel_file_sheets, target_system)
            
            st.subheader("Crew Execution Result:")
            if etl_result:
                st.text_area("Full Crew Output", value=str(etl_result), height=300)
                st.success("✅ ETL crew finished processing!")

                # Potentially save results to files here based on etl_result structure
                # For example, if etl_result contains keys like 'schema_summary', 'dq_scripts', 'pyspark_code', 'lineage_json'
                try:
                    if isinstance(etl_result, str) and etl_result.startswith("Error"): # Simple error check
                         st.error(f"Crew execution resulted in an error: {etl_result}")
                    # This part depends on how CrewAI structures the final output from multiple tasks
                    # Assuming etl_result is a string concatenation of task outputs for now.
                    # A more structured output (e.g., a dictionary) would be better.
                    # For example:
                    # task_outputs = etl_crew.kickoff(...)
                    # if task_outputs.get('task_architect_output'):
                    #    with open(output_root / "schema_summary.txt", "w") as f:
                    #        f.write(task_outputs['task_architect_output'])
                    #    st.download_button("Download Schema Summary", task_outputs['task_architect_output'], "schema_summary.txt")

                except Exception as e:
                    st.error(f"Error processing or saving crew results: {e}")

            else:
                st.error("ETL crew execution failed or returned no result.")
