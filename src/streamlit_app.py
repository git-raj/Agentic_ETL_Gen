import streamlit as st
import pandas as pd
import os
import json
from dotenv import load_dotenv
from agents import Agents
from llm_utils import get_llm_instance

def main():
    st.set_page_config(page_title="ETL Automation Studio", layout="wide")
    st.title("🚀 ETL Automation Platform")

    load_dotenv(dotenv_path=".env")

    _, right_col = st.columns([0.01, 1.99])

    with st.sidebar:
        st.header("🧰 Run Tasks")
        metadata_file = st.file_uploader("📁 Upload ETL Metadata Spreadsheet", type=["xlsx"])
        llm_option = st.selectbox("🤖 Select LLM", ["OpenAI", "Google Gemini", "Other"])
        st.session_state.llm_option = llm_option
        llm_instance = get_llm_instance(llm_option)
        st.session_state.llm_instance = llm_instance

        target_platform = st.selectbox("🎯 Select Target Platform", ["Databricks", "EMR", "AWS Glue"], index=0)
        st.session_state.target_platform = target_platform

        use_agentic = st.checkbox("🤖 Use Agentic LLM-based Generation", value=False)
        st.session_state.use_agentic = use_agentic

        st.markdown("---")
        st.subheader("✅ What to Generate")
        generate_etl = st.checkbox("Generate ETL Code")
        generate_dq = st.checkbox("Generate DQ Tests")
        generate_lineage = st.checkbox("Generate Lineage")
        generate_airflow = st.checkbox("Generate Airflow DAG")

        st.session_state.generate_etl = generate_etl
        st.session_state.generate_dq = generate_dq
        st.session_state.generate_lineage = generate_lineage
        st.session_state.generate_airflow_dag = generate_airflow

        st.session_state.airflow_generation_mode = "llm" if use_agentic else "template"

        generate_now = st.button("🛠️ Generate")

    if metadata_file:
        excel_file = pd.ExcelFile(metadata_file)
        metadata_base = os.path.splitext(metadata_file.name)[0]
        sheet_names = excel_file.sheet_names

        with right_col:
            with st.expander("📄 Sheet Preview", expanded=True):
                selected_sheet = st.selectbox("Select sheet to preview", sheet_names)
                st.dataframe(excel_file.parse(selected_sheet), use_container_width=True)

        all_metadata_dfs = {sheet: excel_file.parse(sheet) for sheet in sheet_names}

        source_metadata_df = all_metadata_dfs.get("Source Metadata", pd.DataFrame())
        target_metadata_df = all_metadata_dfs.get("Target Metadata", pd.DataFrame())
        mapping_metadata_df = all_metadata_dfs.get("Mapping Metadata", pd.DataFrame())
        etl_metadata_df = all_metadata_dfs.get("ETL Metadata", pd.DataFrame())
        dq_rules_df = all_metadata_dfs.get("Data Quality Rules", pd.DataFrame())

        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        output_dir = os.path.join(base_dir, "output")
        etl_dir = os.path.join(output_dir, "etl")
        metadata_dir = os.path.join(output_dir, "metadata")
        tests_dir = os.path.join(output_dir, "tests")
        airflow_dir = os.path.join(output_dir, "airflow")
        os.makedirs(etl_dir, exist_ok=True)
        os.makedirs(metadata_dir, exist_ok=True)
        os.makedirs(tests_dir, exist_ok=True)
        os.makedirs(airflow_dir, exist_ok=True)

        if generate_now:
            agents = Agents(llm=st.session_state.llm_instance)
            etl_script_path = os.path.join(etl_dir, f"{metadata_base}-etl_job.py")

            results = agents.run(
                source_metadata_df=source_metadata_df,
                target_metadata_df=target_metadata_df,
                mapping_metadata_df=mapping_metadata_df,
                etl_metadata_df=etl_metadata_df,
                dq_rules_df=dq_rules_df,
                target_platform=st.session_state.target_platform,
                use_agentic=st.session_state.use_agentic,
                etl_script=etl_script_path,
                airflow_generation_mode=st.session_state.airflow_generation_mode,
                generate_etl=generate_etl,
                generate_dq=generate_dq,
                generate_lineage=generate_lineage,
                generate_airflow=generate_airflow
            )

            if generate_etl and results.get("etl"):
                st.session_state.etl_path = etl_script_path
                with open(st.session_state.etl_path, "w") as f:
                    f.write(results["etl"])

            if generate_dq and results.get("dq"):
                dq_path = os.path.join(tests_dir, f"{metadata_base}-dq_tests.py")
                st.session_state.dq_generated = dq_path
                with open(dq_path, "w") as f:
                    f.write(results["dq"])

            if generate_lineage and results.get("lineage"):
                lineage_path = os.path.join(metadata_dir, f"{metadata_base}-lineage.json")
                st.session_state.lineage_generated = lineage_path
                with open(lineage_path, "w") as f:
                    f.write(results["lineage"])

            if generate_airflow and results.get("airflow_dag"):
                airflow_path = os.path.join(airflow_dir, f"{metadata_base}_dag.py")
                st.session_state.airflow_dag_path = airflow_path
                with open(airflow_path, "w") as f:
                    f.write(results["airflow_dag"])

            st.success("✅ Code generation completed.")

        st.markdown('<div id="preview-start"></div>', unsafe_allow_html=True)
        if st.session_state.get("scroll_to_output"):
            st.markdown("""
            <script>
                const output = document.getElementById('preview-start');
                if (output) {
                    output.scrollIntoView({ behavior: 'smooth' });
                }
            </script>
            """, unsafe_allow_html=True)
        st.session_state.scroll_to_output = False

        with right_col:
            if etl_path := st.session_state.get("etl_path"):
                with st.expander("🧾 Generated ETL Code", expanded=True):
                    with open(etl_path) as f:
                        content = f.read()
                        st.code(content, language="python")
                        st.download_button("⬇️ Download ETL Code", content, file_name=os.path.basename(etl_path))

            if dq_path := st.session_state.get("dq_generated"):
                with st.expander("🧪 Data Quality Tests", expanded=True):
                    with open(dq_path) as f:
                        content = f.read()
                        st.code(content, language="python")
                        st.download_button("⬇️ Download DQ Tests", content, file_name=os.path.basename(dq_path))

            if lineage_path := st.session_state.get("lineage_generated"):
                with st.expander("🧬 Lineage JSON", expanded=True):
                    with open(lineage_path) as f:
                        content = f.read()
                        st.json(json.loads(content))
                        st.download_button("⬇️ Download Lineage JSON", content, file_name=os.path.basename(lineage_path))

            if airflow_dag_path := st.session_state.get("airflow_dag_path"):
                with st.expander("🛫 Generated Airflow DAG", expanded=True):
                    with open(airflow_dag_path) as f:
                        content = f.read()
                        st.code(content, language="python")
                        st.download_button("⬇️ Download Airflow DAG", content, file_name=os.path.basename(airflow_dag_path))

            if any([
                st.session_state.get("etl_path"),
                st.session_state.get("dq_generated"),
                st.session_state.get("lineage_generated"),
                st.session_state.get("airflow_dag_path")
            ]):
                st.success(f"🎉 All outputs saved to: {output_dir}")

if __name__ == "__main__":
    main()
