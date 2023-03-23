#!/usr/bin/env python3
#
# Example of using SchemaGenerator programmatically instead of a command line
# script. This example reads a TSV (tab separated value) file by wrapping it
# with a DictReader, then feeding that into SchemaGenerator, using the
# `input_format='csvdictreader'` option. The `infer_mode=True` generates a
# schema with `mode=REQUIRED` if every row contains a value for all columns.

import csv
import json
import sys
from bigquery_schema_generator.generate_schema import SchemaGenerator

FILENAME = "tsvfile.tsv"

generator = SchemaGenerator(
    input_format='csvdictreader',
    infer_mode=True,
)
with open(FILENAME) as file:
    reader = csv.DictReader(file, delimiter='\t')
    schema_map, errors = generator.deduce_schema(reader)

schema = generator.flatten_schema(schema_map)
json.dump(schema, sys.stdout, indent=2)
print()
