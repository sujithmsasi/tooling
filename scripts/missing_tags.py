import pandas as pd
import json

# -----------------------------------
# CONFIG: Change these file paths
# -----------------------------------
EXCEL_PATH = "resources/missing.xlsx"        # Excel with one column of paths
JSON_SCHEMA_PATH = "resources/schema.json"  # JSON file with Spark-like schema

# -----------------------------------
# Helper Functions
# -----------------------------------

def flatten_schema(schema, parent=""):
    paths = []

    def _walk(node, path):
        # Primitive type (e.g. "string")
        if isinstance(node, str):
            paths.append(path)
            return

        node_type = node.get("type")

        # struct type
        if node_type == "struct":
            for field in node.get("fields", []):
                new_path = f"{path}/{field['name']}" if path else f"/{field['name']}"
                _walk(field["type"], new_path)

        # array type
        elif node_type == "array":
            element_type = node.get("elementType")
            array_path = f"{path}[]" if path else "/[]"
            _walk(element_type, array_path)

        # fallback for unexpected
        else:
            paths.append(path)

    _walk(schema, parent)
    return paths

def normalize_path(path):
    return path.replace("[]", "").strip().lower()

# -----------------------------------
# Load Inputs
# -----------------------------------

# Load schema JSON
with open(JSON_SCHEMA_PATH, "r") as f:
    schema = json.load(f)

# Load Excel column (assumes no header)
df = pd.read_excel(EXCEL_PATH, header=None)
excel_paths = df[0].dropna().tolist()

# -----------------------------------
# Flatten Schema and Compare
# -----------------------------------

schema_paths = flatten_schema(schema)
schema_paths_normalized = [normalize_path(p) for p in schema_paths]
excel_paths_normalized = [normalize_path(p) for p in excel_paths]

missing_paths = [original for original in excel_paths
                 if normalize_path(original) not in schema_paths_normalized]

# -----------------------------------
# Output
# -----------------------------------
print("Missing Paths:")
for path in missing_paths:
    print(path)
