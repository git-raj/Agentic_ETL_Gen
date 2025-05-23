# Agentic ETL Generation

This project generates ETL code, data quality tests, and lineage information using LLMs and a metadata-driven approach.

## Overview

The project consists of the following main components:

-   `src/agents.py`: Defines the agents responsible for generating ETL code, data quality tests, and lineage information.
-   `src/dq_tasks.py`: Contains the logic for generating data quality tests using Great Expectations and PyTest.
-   `src/etl_tasks.py`: Contains the logic for generating ETL code using PySpark.
-   `src/lineage_tasks.py`: Contains the logic for generating Collibra-compliant lineage JSON documents.
-   `src/llm_utils.py`: Contains utility functions for interacting with LLMs.
-   `src/streamlit_app.py`: A Streamlit application that provides a user interface for generating ETL code, data quality tests, and lineage information.
-   `tests/`: Contains unit tests for the project.

## Usage

To run the Streamlit application, execute the following command:

```bash
streamlit run src/streamlit_app.py
```

## License
Copyright (c) [2025] [Lamichhane, Saroj]. All Rights Reserved.

This software application, including all code, designs, graphics, user interfaces, and documentation (collectively, "the Software"), is the sole and exclusive property of [Lamichhane, Saroj].

Unauthorized reproduction, distribution, public display, performance, or creation of derivative works based on the Software, in whole or in part, is strictly prohibited. This includes, but is not limited to, copying, modifying, decompiling, reverse engineering, disassembling, leasing, selling, or distributing the Software or any portion thereof.

Any unauthorized use, including taking screenshots or recording the Software for public dissemination, without the express written permission of [Lamichhane, Saroj], is a violation of copyright law and is strictly forbidden.

No license, express or implied, is granted for the use of the Software for any purpose. All rights not expressly granted herein are reserved.
