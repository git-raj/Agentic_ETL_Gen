from langchain.chat_models import ChatOpenAI
from langchain_google_genai import ChatGoogleGenerativeAI
import os
from crewai import Agent
from dotenv import load_dotenv

# Load environment variables from .env file at module import
load_dotenv()

def create_architect_agent(llm=None):
    return Agent(
        role="Data Architect",
        goal="Analyze metadata spreadsheet and identify table structures, keys, and dependencies",
        backstory="Expert in metadata modeling and enterprise data architecture with 10+ years of experience.",
        allow_delegation=False,
        llm=llm
    )

def create_dq_agent(llm=None):
    return Agent(
        role="Data Quality Analyst",
        goal="Create data quality test scripts using Great Expectations and PyTest",
        backstory="Skilled in implementing DQ frameworks and validations at large enterprises.",
        allow_delegation=False,
        llm=llm
    )

def create_etl_agent(llm=None):
    return Agent(
        role="ETL Developer",
        goal="Generate PySpark ETL code based on metadata mapping for a given platform",
        backstory="Experienced PySpark developer who has built pipelines across Databricks, EMR, and Glue.",
        allow_delegation=False,
        llm=llm
    )

def create_lineage_agent(llm=None):
    return Agent(
        role="Metadata Steward",
        goal="Produce a Collibra-compatible lineage JSON from the metadata spreadsheet",
        backstory="Specialist in metadata governance, Collibra, and enterprise data catalog integration.",
        allow_delegation=False,
        llm=llm
    )


def initialize_agents_and_tasks(llm=None):
    if llm is None:
        llm_provider = os.getenv("LLM_PROVIDER", "gemini").lower()
        llm_model = os.getenv("LLM_MODEL")
        llm_api_key = os.getenv("LLM_API_KEY")

        if llm_provider == "openai":
            llm = ChatOpenAI(
                model_name=llm_model,
                temperature=0.7,
                openai_api_key=llm_api_key
            )
        elif llm_provider == "gemini":
            # --- MODIFICATION START ---
            # Strip "models/" prefix if present
            if llm_model and llm_model.startswith("models/"):
                llm_model = llm_model.split("/")[-1]
                # Optional: print(f"Config: Stripped 'models/' prefix, using model name: {llm_model} for Gemini")
            # --- MODIFICATION END ---
            
            llm = ChatGoogleGenerativeAI(
                model=llm_model,
                temperature=0.7,
                google_api_key=llm_api_key
            )
        else:
            raise ValueError("Unsupported LLM_PROVIDER: must be 'openai' or 'gemini'")

    architect_agent = create_architect_agent(llm)
    dq_agent = create_dq_agent(llm)
    etl_agent = create_etl_agent(llm)
    lineage_agent = create_lineage_agent(llm)

    return architect_agent, dq_agent, etl_agent, lineage_agent