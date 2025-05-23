import os
from openai import OpenAI
import google.generativeai as genai
import json
import re # Import regex module
from dotenv import load_dotenv

load_dotenv()

def get_llm_response(prompt: str, llm_option: str) -> str:
    """
    Connects to the specified LLM and gets a response.
    """
    if llm_option == "OpenAI":
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            return "Error: OPENAI_API_KEY not found in environment variables."
        client = OpenAI(api_key=api_key)
        model = os.getenv("OPENAI_LLM_MODEL", "gpt-3.5-turbo") # Default to gpt-3.5-turbo if not specified
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}]
            )
            return response.choices[0].message.content
        except Exception as e:
            return f"Error interacting with OpenAI: {e}"

    elif llm_option == "Google Gemini":
        api_key = os.getenv("GOOGLE_API_KEY")
        if not api_key:
            return "Error: GOOGLE_API_KEY not found in environment variables."
        genai.configure(api_key=api_key)
        model_name = os.getenv("GOOGLE_LLM_MODEL", "gemini-pro") # Default to gemini-pro
        try:
            model = genai.GenerativeModel(model_name)
            response = model.generate_content(prompt)
            return response.text
        except Exception as e:
            return f"Error interacting with Google Gemini: {e}"

    else:
        return "Unsupported LLM option."

def parse_llm_function_call(llm_response: str) -> dict:
    """
    Parses the LLM's response to extract function name and arguments.
    Assumes the LLM is instructed to output a JSON string, potentially within a markdown code block.
    """
    try:
        # Try to find JSON within a markdown code block first
        match = re.search(r"```json\n(.*?)```", llm_response, re.DOTALL)
        if match:
            json_str = match.group(1)
        else:
            # If no markdown block, assume the whole response is JSON
            json_str = llm_response.strip()

        # Attempt to load the JSON
        parsed_json = json.loads(json_str)

        # Basic validation for expected keys
        if "function" in parsed_json and "args" in parsed_json:
            return parsed_json
        else:
            print(f"Parsed JSON missing 'function' or 'args' key: {parsed_json}")
            return {"function": None, "args": {"message": "Parsed JSON missing required keys."}}

    except json.JSONDecodeError as e:
        print(f"JSON decoding error: {e}")
        print(f"LLM response that caused error: '{llm_response}'")
        return {"function": None, "args": {"message": f"Invalid JSON format: {e}"}}
    except Exception as e:
        print(f"An unexpected error occurred during parsing: {e}")
        return {"function": None, "args": {"message": f"Unexpected parsing error: {e}"}}
