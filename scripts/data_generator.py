import json
from mimesis import Generic

generic = Generic('en')


def load_json_schema(path):
    with open(path, 'r') as f:
        return json.load(f)


def load_patterns(path):
    with open(path, 'r') as f:
        return json.load(f).get("patterns", {})


def find_pattern(field, patterns):
    field_lower = field.lower()
    for pattern, keywords in patterns.items():
        if any(keyword.lower() in field_lower for keyword in keywords):
            return pattern
    return "text"  # default fallback


def generate_value(pattern, max_length=35):
    generators = {
        "customer_name": lambda: generic.person.full_name(),
        "address": lambda: generic.address.address(),
        "email": lambda: generic.person.email(),
        "phone_number": lambda: generic.person.telephone(),
        "date": lambda: generic.datetime.date().isoformat(),  # 🛠 FIXED
        "ssn": lambda: generic.person.identifier(mask="###-##-####"),
        "tax_id": lambda: generic.person.identifier(mask="###-##-####"),
        "account_number": lambda: str(generic.numeric.integer_number(start=1000000000, end=9999999999)),
        "transaction_id": lambda: generic.code.custom_code(mask="???-#####"),
        "amount": lambda: round(generic.numeric.float_number(start=1, end=100000, precision=2), 2),
        "currency": lambda: generic.finance.currency_iso_code(),
        "company_name": lambda: generic.business.company(),
        "job_title": lambda: generic.person.occupation(),
        "swift_code": lambda: generic.finance.swift_bic(),
        "bank_name": lambda: generic.business.company(),
        "uuid": lambda: generic.code.uuid(),
        "zip_code": lambda: generic.address.postal_code(),
        "city": lambda: generic.address.city(),
        "country": lambda: generic.address.country(),
        "boolean": lambda: generic.random.choice([True, False]),
        "name": lambda: generic.person.full_name(),
        "text": lambda: generic.text.word(),
    }

    return generators.get(pattern, lambda: generic.text.word())()




def generate_data_from_schema(schema, patterns):
    def process_node(properties):
        data = {}
        for field, attr in properties.items():
            ftype = attr.get("type", "string")
            if ftype == "object":
                data[field] = process_node(attr.get("properties", {}))
            elif ftype == "array":
                item_props = attr.get("items", {}).get("properties", {})
                data[field] = [process_node(item_props)]
            elif ftype == "boolean":
                data[field] = generic.random.choice([True, False])
            elif ftype == "number":
                data[field] = generic.numeric.float_number(start=1, end=1000)
            elif ftype == "string":
                pattern = find_pattern(field, patterns)
                max_len = attr.get("maxLength", 35)
                data[field] = generate_value(pattern, max_len)
            else:
                data[field] = None
        return data

    result = {}
    for key, node in schema.items():
        if node.get("type") == "object":
            result[key] = process_node(node.get("properties", {}))
    return result


def main(schema_path, patterns_path):
    schema = load_json_schema(schema_path)
    patterns = load_patterns(patterns_path)
    data = generate_data_from_schema(schema, patterns)

    with open('generated_data.json', 'w') as f:
        json.dump(data, f, indent=4)
    print("✅ Data generated and saved to 'generated_data.json'")


if __name__ == "__main__":
    main('output_schema.json', 'patterns.json')
