from langchain.chat_models import ChatOpenAI
from langchain_google_genai import ChatGoogleGenerativeAI
import os
from dotenv import load_dotenv
load_dotenv(dotenv_path=".env")

def get_llm_instance(llm_option: str):
    if llm_option == "Google Gemini":
        # Use the correct supported model name for v1beta API
        model = os.getenv("GOOGLE_LLM_MODEL", "gemini-pro")
        print(f"LLM model is: {model}")
        return ChatGoogleGenerativeAI(
            model=model,
            temperature=0.7,
            google_api_key=os.getenv("GOOGLE_API_KEY")
        )

    elif llm_option == "OpenAI":
        model = os.getenv("OPENAI_LLM_MODEL", "gpt-3.5-turbo")
        return ChatOpenAI(
            model=model,
            temperature=0.7,
            api_key=os.getenv("OPENAI_API_KEY")
        )

    else:
        # Fallback using LiteLLM or mock for local/tested models
        from litellm import completion

        class LiteLLMWrapper:
            def invoke(self, messages):
                response = completion(
                    model=os.getenv("OTHER_LLM_MODEL", "gpt-3.5-turbo"),
                    messages=[{"role": "user", "content": messages[0].content}]
                )
                return type("Response", (), {
                    "content": response["choices"][0]["message"]["content"]
                })

        return LiteLLMWrapper()
