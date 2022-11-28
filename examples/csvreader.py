#!/usr/bin/env python3
#
# Example of using SchemaGenerator as a library instead of a command line
# script. Read the CSV file named 'csvfile.csv' in the current directory, deduce
# its schema, and print it out on the stdout.
#
# This is the equivalent of:
# $ generate-schema
#   --input_format=csv
#   --infer_mode
#   --quoted_values_are_strings
#   --sanitize_names
#   < csvfile.csv

import json
import logging
import sys
from bigquery_schema_generator.generate_schema import SchemaGenerator

FILENAME = "csvfile.csv"

generator = SchemaGenerator(
    input_format='csv',
    infer_mode=True,
    quoted_values_are_strings=True,
    sanitize_names=True,
)

with open(FILENAME) as file:
    schema_map, errors = generator.deduce_schema(file)

for error in errors:
    logging.info("Problem on line %s: %s", error['line_number'], error['msg'])

schema = generator.flatten_schema(schema_map)
json.dump(schema, sys.stdout, indent=2)
print()
