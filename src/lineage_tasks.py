import json
import pandas as pd
import re

def generate_lineage_json(mapping_metadata, llm_option):
    """
    Generates a Collibra-compliant lineage JSON document based on the provided mapping metadata.
    Tries to infer source/target table and column names from the 'Business Rule / Expression' field if not explicitly defined.
    """
    print("Generating lineage JSON...")
    # print("Columns in DataFrame:", mapping_metadata.columns.tolist())
    # print("First 3 rows:", mapping_metadata.head(3).to_dict())

    nodes = []
    edges = []
    node_ids = set()

    mapping_metadata = mapping_metadata.fillna("")

    for idx, row in mapping_metadata.iterrows():
        transformation_logic = row.get('Business Rule / Expression', '').strip()

        match = re.match(r"(\w+)_id\s*=\s*(\w+)", transformation_logic)
        if match:
            target_column, source_column = match.groups()
            target_table = target_column.split('_')[0]
            source_table = source_column.split('_')[0]
        else:
            source_table = source_column = target_table = target_column = ""

        if not source_table or not source_column or not target_table or not target_column:
            continue

        source_full_name = f"{source_table}.{source_column}"
        target_full_name = f"{target_table}.{target_column}"

        if source_full_name not in node_ids:
            nodes.append({
                "id": source_full_name,
                "type": "Column",
                "name": source_column,
                "parent": {
                    "id": source_table,
                    "type": "Table",
                    "name": source_table
                }
            })
            node_ids.add(source_full_name)

        if target_full_name not in node_ids:
            nodes.append({
                "id": target_full_name,
                "type": "Column",
                "name": target_column,
                "parent": {
                    "id": target_table,
                    "type": "Table",
                    "name": target_table
                }
            })
            node_ids.add(target_full_name)

        edges.append({
            "id": f"edge_{idx}",
            "source": source_full_name,
            "target": target_full_name,
            "label": transformation_logic if transformation_logic else "direct mapping",
            "transformation": transformation_logic if transformation_logic else "direct mapping"
        })

    lineage_json = {
        "codeGraph": {
            "nodes": nodes,
            "edges": edges
        },
        "sourceProperties": [],
        "targetProperties": [],
        "script": {
            "codeFormat": "python",
            "placeholder": "# Transformation logic not provided"
        }
    }

    return json.dumps(lineage_json, indent=2)