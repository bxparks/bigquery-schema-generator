#!/usr/bin/env python3
#
# Example of using SchemaGenerator programmatically instead of a command line
# script. This example opens the CSV and JSON files and calls
# SchemaGenerator.run() to deduce the schema and print out the result to
# sys.stdout.

import sys
from bigquery_schema_generator.generate_schema import SchemaGenerator

FILENAME = "csvfile.csv"
generator = SchemaGenerator(input_format='csv')
with open(FILENAME) as file:
    generator.run(file, sys.stdout)

FILENAME = "jsonfile.json"
generator = SchemaGenerator(input_format='json')
with open(FILENAME) as file:
    generator.run(file, sys.stdout)
