#!/usr/bin/env python3
#
# Example of using SchemaGenerator programmatically instead of a command line
# script. This example consumes a JSON data set that has *already* been read
# into memory as a Python array of dict.

import json
import sys
from bigquery_schema_generator.generate_schema import SchemaGenerator

generator = SchemaGenerator(input_format='dict')
input_data = [
    {
        's': 'string',
        'b': True,
    },
    {
        'd': '2021-08-18',
        'x': 3.1
    },
]
schema_map, error_logs = generator.deduce_schema(input_data)
schema = generator.flatten_schema(schema_map)
json.dump(schema, sys.stdout, indent=2)
print()
