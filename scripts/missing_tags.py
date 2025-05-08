import pandas as pd
import json

def flatten_schema(schema, parent=''):

    paths = []
    if schema.get('type') == 'struct':
        for field in schema.get('fields', []):
            name = field['name']
            new_path = f"{parent}/{name}" if parent else f"/{name}"
            if isinstance(field.get('type'), dict) and field['type'].get('type') == 'struct':
                # Recurse into nested struct
                paths.extend(flatten_schema(field['type'], new_path))
            else:
                # Leaf field
                paths.append(new_path)
    return paths

# -------- CONFIG --------
EXCEL_FILE_PATH = "resources/missing.xlsx"   # <-- Replace with actual path
JSON_SCHEMA_FILE_PATH = "resources/schema.json"  # <-- Replace with actual path
# ------------------------

# Load Excel paths
df = pd.read_excel(EXCEL_FILE_PATH, header=None)
excel_paths = df[0].dropna().tolist()
excel_paths = [str(p).strip() for p in excel_paths]

# Load JSON schema
with open(JSON_SCHEMA_FILE_PATH) as f:
    schema = json.load(f)

# Flatten schema
schema_paths = flatten_schema(schema)

# Compare
missing_paths = [path for path in excel_paths if path not in schema_paths]

# Show result
print("Missing Paths:")
for path in missing_paths:
    print(path)
