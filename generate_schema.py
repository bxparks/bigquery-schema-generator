import argparse
import csv
import json

from bigquery_schema_generator.generate_schema import SchemaGenerator


# Generating schemas for Kirby ingestion pipelines

class PaywayDialect(csv.Dialect):
    delimiter = '^'
    quoting = csv.QUOTE_MINIMAL
    quotechar = '"'
    lineterminator = '\r\n'


csv_dialects = {
    "payway": PaywayDialect
}


def generate_schema(input_file, encryption_key_id, personal_columns, input_format, csv_dialect):
    generator = SchemaGenerator(
        input_format=input_format,
        infer_mode=True,
        csv_dialect=csv_dialect
    )

    schema_map, _ = generator.deduce_schema(input_file)

    schema = generator.flatten_schema(schema_map)
    for item in schema:
        item.update({'description': 'Describe this column briefly',
                     'privacy_classification': 'non_personal_data'})

        if encryption_key_id == item['name']:
            item.update({'encryption_key_id': True})

        if item['name'] in personal_columns:
            item.update({'privacy_classification': 'personal_data'})

    print(json.dumps(schema, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate a schema for Kirby')
    parser.add_argument('input', type=argparse.FileType('r'))
    parser.add_argument('--encryption_key_id', help='Column that should be key for encryption, if dataset should be encrypted')
    parser.add_argument('--personal_columns', nargs='+', help='Columns that contain personal data', default=[])
    parser.add_argument('--input_format', help='json or csv (default = csv)', default='csv')
    parser.add_argument('--csv_dialect', choices=csv_dialects.keys(), help='')
    args = parser.parse_args()

    generate_schema(input_file=args.input,
                    encryption_key_id=args.encryption_key_id,
                    personal_columns=args.personal_columns,
                    input_format=args.input_format,
                    csv_dialect=csv_dialects.get(args.csv_dialect))
