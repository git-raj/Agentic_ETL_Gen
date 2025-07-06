import subprocess
import os
import re

class CodeValidator:
    def __init__(self, llm=None):
        self.role = "Code Validator"
        self.goal = "Ensure the generated code is syntactically correct, semantically sound, and adheres to best practices."
        self.backstory = "Experienced in Python, SQL, and dbT, with expertise in code quality and validation."
        self.llm = llm  # Optional LLM for more advanced validation

    def validate_python_code(self, code):
        """Validate Python code using pylint."""
        try:
            # Create a temporary file to store the code
            with open("temp_code.py", "w") as f:
                f.write(code)

            # Run pylint
            result = subprocess.run(
                ["pylint", "temp_code.py", "--disable=all", "--enable=syntax-errors,import-error,missing-docstring,invalid-name,unused-import,wrong-import-position,trailing-whitespace,line-too-long"],
                capture_output=True,
                text=True,
                check=True,
            )
            
            # Parse pylint output
            errors = []
            for line in result.stdout.splitlines():
                if ":" in line and "temp_code.py" in line:
                    match = re.match(r"temp_code\.py:(\d+):(\d+): (\w+): (.*)", line)
                    if match:
                        line_number, column, error_code, message = match.groups()
                        errors.append({
                            "line": int(line_number),
                            "column": int(column),
                            "code": error_code,
                            "message": message.strip()
                        })
            
            return errors
        except subprocess.CalledProcessError as e:
            # Handle pylint errors
            errors = []
            for line in e.stderr.splitlines():
                if ":" in line and "temp_code.py" in line:
                    match = re.match(r"temp_code\.py:(\d+):(\d+): (\w+): (.*)", line)
                    if match:
                        line_number, column, error_code, message = match.groups()
                        errors.append({
                            "line": int(line_number),
                            "column": int(column),
                            "code": error_code,
                            "message": message.strip()
                        })
            return errors
        except Exception as e:
            return [{"message": f"Error during pylint validation: {e}"}]
        finally:
            # Clean up the temporary file
            if os.path.exists("temp_code.py"):
                os.remove("temp_code.py")

    def validate_sql_code(self, code):
        """Validate SQL code using sqlfluff."""
        try:
            # Create a temporary file to store the code
            with open("temp_code.sql", "w") as f:
                f.write(code)

            # Run sqlfluff
            result = subprocess.run(
                ["sqlfluff", "lint", "temp_code.sql", "--dialect", "ansi"],
                capture_output=True,
                text=True,
                check=True,
            )
            
            # Parse sqlfluff output
            errors = []
            for line in result.stdout.splitlines():
                if "temp_code.sql" in line and "L:" in line:
                    match = re.match(r"temp_code\.sql:(\d+):(\d+)\s+L:(\d+)\s+E:(\d+)\s+(.*)", line)
                    if match:
                        line_number, column, _, error_code, message = match.groups()
                        errors.append({
                            "line": int(line_number),
                            "column": int(column),
                            "code": error_code,
                            "message": message.strip()
                        })
            
            return errors
        except subprocess.CalledProcessError as e:
            # Handle sqlfluff errors
            errors = []
            for line in e.stderr.splitlines():
                if "temp_code.sql" in line and "L:" in line:
                    match = re.match(r"temp_code\.sql:(\d+):(\d+)\s+L:(\d+)\s+E:(\d+)\s+(.*)", line)
                    if match:
                        line_number, column, _, error_code, message = match.groups()
                        errors.append({
                            "line": int(line_number),
                            "column": int(column),
                            "code": error_code,
                            "message": message.strip()
                        })
            return errors
        except Exception as e:
            return [{"message": f"Error during sqlfluff validation: {e}"}]
        finally:
            # Clean up the temporary file
            if os.path.exists("temp_code.sql"):
                os.remove("temp_code.sql")

    def validate_dbt_model(self, code):
        """Validate dbT model code (basic checks)."""
        errors = []
        # Basic check for config
        if "{{ config(" not in code:
            errors.append({"message": "Missing config block."})
        
        # Check for required Jinja syntax
        if "{{" not in code or "%}" not in code:
            errors.append({"message": "Missing Jinja syntax."})
        
        return errors

    def validate_code(self, code, file_type):
        """Validate code based on file type."""
        if file_type == "python":
            return self.validate_python_code(code)
        elif file_type == "sql":
            return self.validate_sql_code(code)
        elif file_type == "dbt":
            return self.validate_dbt_model(code)
        else:
            return [{"message": "Unsupported file type for validation."}]
