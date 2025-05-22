import streamlit as st
import pandas as pd
import os
import json
from dotenv import load_dotenv
from agents import Agents

def main():
    st.set_page_config(page_title="ETL Automation Studio", layout="wide")
    st.title("🚀 ETL Automation Platform")

    load_dotenv(dotenv_path=".env")

    # Remove left_col to eliminate the gap
    _, right_col = st.columns([0.01, 1.99])

    with st.sidebar:
        st.header("🧰 Run Tasks")
        metadata_file = st.file_uploader("📁 Upload ETL Metadata Spreadsheet", type=["xlsx"])
        llm_option = st.selectbox("🤖 Select LLM", ["OpenAI", "Google Gemini"])
        target_platform = st.selectbox("🏗️ Target Platform", ["Databricks", "EMR", "AWS Glue"])

    if metadata_file:
        excel_file = pd.ExcelFile(metadata_file)
        metadata_base = os.path.splitext(metadata_file.name)[0]
        sheet_names = excel_file.sheet_names

        with right_col:
            with st.expander("📄 Sheet Preview", expanded=True):
                selected_sheet = st.selectbox("Select sheet to preview", sheet_names)
                st.dataframe(excel_file.parse(selected_sheet), use_container_width=True)

        source_metadata_df = excel_file.parse("Source Metadata")
        target_metadata_df = excel_file.parse("Target Metadata")
        mapping_metadata_df = excel_file.parse("Mapping Metadata")
        etl_metadata_df = excel_file.parse("ETL Metadata")
        data_quality_rules_df = excel_file.parse("Data Quality Rules")
        security_compliance_df = excel_file.parse("Security & Compliance")

        agents = Agents(llm_option=llm_option)

        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        output_dir = os.path.join(base_dir, "output")
        etl_dir = os.path.join(output_dir, "etl")
        metadata_dir = os.path.join(output_dir, "metadata")
        tests_dir = os.path.join(output_dir, "tests")
        os.makedirs(etl_dir, exist_ok=True)
        os.makedirs(metadata_dir, exist_ok=True)
        os.makedirs(tests_dir, exist_ok=True)

        # Sticky header UI for control panel
        with st.sidebar:
            st.header("🧰 Run Tasks")

            if st.button("🛠️ Generate ETL Code"):
                etl_code = agents.etl_agent.execute_task(
                    metadata=source_metadata_df,
                    target_metadata=target_metadata_df,
                    target_platform=target_platform
                )
                etl_file_name = f"{metadata_base}-etl_job.py"
                etl_path = os.path.join(etl_dir, etl_file_name)
                with open(etl_path, "w") as f:
                    f.write(etl_code)
                st.session_state.etl_generated = etl_path
                st.session_state.scroll_to_output = True

            if st.session_state.get("etl_generated") and st.button("✅ Generate DQ Tests"):
                dq_tests = agents.dq_agent.execute_task(
                    metadata=data_quality_rules_df,
                    llm_option=llm_option
                )
                dq_file_name = f"{metadata_base}-test_dq.py"
                dq_path = os.path.join(tests_dir, dq_file_name)
                with open(dq_path, "w") as f:
                    f.write(dq_tests)
                st.session_state.dq_generated = dq_path
                st.session_state.scroll_to_output = True

            if st.session_state.get("dq_generated") and st.button("📈 Generate Lineage JSON"):
                lineage_json = agents.lineage_agent.execute_task(
                    metadata=mapping_metadata_df,
                    llm_option=llm_option
                )
                lineage_file_name = f"{metadata_base}-lineage.json"
                lineage_path = os.path.join(metadata_dir, lineage_file_name)
                with open(lineage_path, "w") as f:
                    f.write(lineage_json)
                st.session_state.lineage_generated = lineage_path
                st.session_state.scroll_to_output = True

        # Show generated outputs
        st.markdown('<div id="preview-start"></div>', unsafe_allow_html=True)
        scroll_script = """
        <script>
            const output = document.getElementById('preview-start');
            if (output) {
                output.scrollIntoView({ behavior: 'smooth' });
            }
        </script>
        """
        if st.session_state.get("scroll_to_output"):
            st.markdown(scroll_script, unsafe_allow_html=True)
            st.session_state.scroll_to_output = False
        with right_col:
            if etl_path := st.session_state.get("etl_generated"):
                with st.expander("🧾 Generated ETL Code", expanded=True):
                    with open(etl_path) as f:
                        content = f.read()
                        st.code(content, language="python")
                        st.download_button("⬇️ Download ETL Script", content, file_name=os.path.basename(etl_path))

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

            if st.session_state.get("lineage_generated"):
                st.success(f"🎉 All outputs saved to: {output_dir}")

if __name__ == "__main__":
    main()
