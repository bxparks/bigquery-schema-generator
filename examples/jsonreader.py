#!/usr/bin/env python3
#
# Example of using SchemaGenerator programmatically instead of a command line
# script. Read the JSON-ND file named 'jsonfile.json' in the current directory,
# deduce its schema, and print it out on the stdout.
#
# This is the equivalent of:
# $ generate-schema --quoted_values_are_strings < jsonfile.json

import json
import logging
import sys
from bigquery_schema_generator.generate_schema import SchemaGenerator

FILENAME = "jsonfile.json"

generator = SchemaGenerator(
    input_format='json',
    quoted_values_are_strings=True,
)

with open(FILENAME) as file:
    schema_map, errors = generator.deduce_schema(file)

for error in errors:
    logging.info("Problem on line %s: %s", error['line_number'], error['msg'])

schema = generator.flatten_schema(schema_map)
json.dump(schema, sys.stdout, indent=2)
print()
