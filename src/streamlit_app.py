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

    # Right pane only
    _, right_col = st.columns([0.01, 1.99])

    with st.sidebar:
        st.header("🧰 Run Tasks")
        metadata_file = st.file_uploader("📁 Upload ETL Metadata Spreadsheet", type=["xlsx"])
        llm_option = st.selectbox("🤖 Select LLM", ["OpenAI", "Google Gemini", "Other"])
        st.session_state.llm_option = llm_option
        target_platform = st.selectbox("🎯 Select Target Platform", ["Databricks", "EMR", "AWS Glue"], index=0)
        st.session_state.target_platform = target_platform

    if metadata_file:
        excel_file = pd.ExcelFile(metadata_file)
        metadata_base = os.path.splitext(metadata_file.name)[0]
        sheet_names = excel_file.sheet_names

        with right_col:
            with st.expander("📄 Sheet Preview", expanded=True):
                selected_sheet = st.selectbox("Select sheet to preview", sheet_names)
                st.dataframe(excel_file.parse(selected_sheet), use_container_width=True)

        # Parse all sheets into a dictionary of DataFrames
        all_metadata_dfs = {}
        for sheet_name in sheet_names:
            all_metadata_dfs[sheet_name] = excel_file.parse(sheet_name)

        # Retrieve specific DataFrames for convenience and for agents that directly use them
        source_metadata_df = all_metadata_dfs.get("Source Metadata", pd.DataFrame())
        target_metadata_df = all_metadata_dfs.get("Target Metadata", pd.DataFrame())
        mapping_metadata_df = all_metadata_dfs.get("Mapping Metadata", pd.DataFrame())
        etl_metadata_df = all_metadata_dfs.get("ETL Metadata", pd.DataFrame())
        data_quality_rules_df = all_metadata_dfs.get("Data Quality Rules", pd.DataFrame())
        security_compliance_df = all_metadata_dfs.get("Security & Compliance", pd.DataFrame())

        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        output_dir = os.path.join(base_dir, "output")
        etl_dir = os.path.join(output_dir, "etl")
        metadata_dir = os.path.join(output_dir, "metadata")
        tests_dir = os.path.join(output_dir, "tests")
        os.makedirs(etl_dir, exist_ok=True)
        os.makedirs(metadata_dir, exist_ok=True)
        os.makedirs(tests_dir, exist_ok=True)

        with st.sidebar:
            if st.button("🛠️ Generate ETL Code"):
                agents = Agents(
                    llm_option=st.session_state.llm_option,
                    all_metadata_dfs=all_metadata_dfs,
                    source_metadata_df=source_metadata_df,
                    target_metadata_df=target_metadata_df,
                    mapping_metadata_df=mapping_metadata_df,
                    etl_metadata_df=etl_metadata_df,
                    data_quality_rules_df=data_quality_rules_df,
                    security_compliance_df=security_compliance_df,
                    target_platform=st.session_state.target_platform,
                )
                result = agents.run()
                st.session_state.etl_generated = result

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
            if st.session_state.get("etl_generated"):
                with st.expander("🧾 Generated ETL Code", expanded=True):
                    st.write(st.session_state.etl_generated)

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
