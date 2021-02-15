import json
from bigquery_schema_generator.generate_schema import SchemaGenerator
import csv

# POC for generating schemas for Kirby ingestion pipelines
# A schema should have personal_data/non_personal_data even if it's not encrypted
# (full dumps with a 30 day retention), so that we have metadata on where we have personal data.


class PaywayDialect(csv.Dialect):
  delimiter = '^'
  quoting = csv.QUOTE_MINIMAL
  quotechar = '"'
  lineterminator = '\r\n'


generator = SchemaGenerator(
    input_format='csv',
    # infer_mode=infer_mode,
    # keep_nulls=keep_nulls,
    # quoted_values_are_strings=quoted_values_are_strings,
    # debugging_interval=debugging_interval,
    # debugging_map=debugging_map,
    # sanitize_names=True,
    # ignore_invalid_lines=ignore_invalid_lines,
    infer_mode=True,
    csv_dialect=PaywayDialect
)

schema_map, _ = generator.deduce_schema(open('products.csv', 'r'))
# print(schema)

schema = generator.flatten_schema(schema_map)
for item in schema:
    item.update({'description': 'Describe this column shortly',
                 'privacy_classification': 'personal_data/non_personal_data'})
print(schema)
json.dump(schema, open('products_schema.json', 'w'), indent=2)
# print(file=output_file)

# generator.run(input_file=open('products.csv', 'r'),
#               output_file=open('schema_payway_products.json', 'w'))
