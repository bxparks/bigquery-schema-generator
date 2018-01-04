# BigQuery Schema Generator

## Summary

This script generates the BigQuery schema from the data records on the STDIN.
The BigQuery data importer uses only the first 100 lines when the schema
auto-detection feature is enabled. In contrast, this script uses all data
records to generate the schema.

Usage:
```
$ generate-schema < file.data.json > file.schema.json
```

## Background

Data can be imported into [BigQuery](https://cloud.google.com/bigquery/) using
the [bq](https://cloud.google.com/bigquery/bq-command-line-tool) command line
tool. It accepts a number of data formats including CSV or newline-delimited
JSON. The data can be loaded into an existing table or a new table can be
created during the loading process. The structure of the table is defined by
its [schema](https://cloud.google.com/bigquery/docs/schemas). The table's
schema can be defined manually or the schema can be
[auto-detected](https://cloud.google.com/bigquery/docs/schema-detect#auto-detect).

When the auto-detect feature is used, the BigQuery data importer examines only
the first 100 records of the input data. In many cases, this is sufficient
because the data records were dumped from another database and the exact schema
of the source table was known. However, for data extracted from a service
(e.g. using a REST API) the record fields could have been organically added
at later dates. In this case, the first 100 records do not contain fields which
are present in later records. The **bq load** auto-detection fails and the data
fails to load.

The **bq load** tool does not support the ability to process the entire dataset
to determine a more accurate schema. This script fills in that gap. It
processes the entire dataset given in the STDIN and outputs the BigQuery schema
in JSON format on the STDOUT. This schema file can be fed back into the **bq
load** tool to create a table that is more compatible with the data fields in
the input dataset.

## Installation

Install from [PyPI](https://pypi.python.org/pypi) repository using:
```
$ pip3 install bigquery_schema_generator
```

## Usage

The `generate_schema.py` script accepts a newline-delimited JSON data file on
the STDIN. (CSV is not supported currently.) It scans every record in the
input data file to deduce the table's schema. It prints the JSON formatted
schema file on the STDOUT. There are at least 3 ways to run this script:

1) If you installed using `pip3`, then it should have installed a small helper
script named `generate-schema` in your local `./bin` directory of your current
environment (depending on whether you are using a virtual environment).

```
$ generate-schema < file.data.json > file.schema.json
```

2) You can invoke the module directly using:
```
$ python3 -m bigquery_schema_generator.generate_schema < file.data.json > file.schema.json
```
This is essentially what the `generate-schema` command does.

3) If you retrieved this code from its [GitHub
repository](https://github.com/bxparks/bigquery-schema-generator), then you can invoke
the Python script directly:
```
$ ./generate_schema.py < file.data.json > file.schema.json
```

### Schema Output

The resulting schema file can be used in the **bq load** command using the
`--schema` flag:
```
$ bq load --source_format NEWLINE_DELIMITED_JSON \
        --schema file.schema.json \
        mydataset.mytable \
        file.data.json
```

where `mydataset.mytable` is the target table in BigQuery.

A useful flag for **bq load** is `--ignore_unknown_values`, which causes **bq load**
to ignore fields in the input data which are not defined in the schema. When
`generate_schema.py` detects an inconsistency in the definition of a particular
field in the input data, it removes the field from the schema definition.
Without the `--ignore_unknown_values`, the **bq load** fails when the
inconsistent data record is read.

After the BigQuery table is loaded, the schema can be retrieved using:

```
$ bq show --schema mydataset.mytable | python -m json.tool
```

(The `python -m json.tool` command will pretty-print the JSON formatted schema
file.) This schema file should be identical to `file.schema.json`.

### Options

The `generate_schema.py` script supports a handful of command line flags:

* `--help` Prints the usage with the list of supported flags.
* `--keep_nulls` Print the schema for null values, empty arrays or empty records.
* `--debugging_interval lines` Number of lines between heartbeat debugging messages. Default 1000.
* `--debugging_map` Print the metadata schema map for debugging purposes

#### Help

Print the built-in help strings:

```
$ generate-schema --help
```

#### Null Values

Normally when the input data file contains a field which has a null, empty
array or empty record as its value, the field is suppressed in the schema file.
This flag enables this field to be included in the schema file. In other words,
for the data file:
```
{ "s": null, "a": [], "m": {} }
```
the schema would normally be:
```
[]
```
With the ``keep_nulls``, the resulting schema file will be:
```
[
  {
    "mode": "REPEATED",
    "type": "STRING",
    "name": "a"
  },
  {
    "mode": "NULLABLE",
    "fields": [
      {
        "mode": "NULLABLE",
        "type": "STRING",
        "name": "__unknown__"
      }
    ],
    "type": "RECORD",
    "name": "d"
  },
  {
    "mode": "NULLABLE",
    "type": "STRING",
    "name": "s"
  }
]
```

Example:

```
$ generate-schema --keep_nulls < file.data.json > file.schema.json
```

#### Debugging Interval

By default, the `generate_schema.py` script prints a short progress message
every 1000 lines of input data. This interval can be changed using the
`--debugging_interval` flag.

```
$ generate-schema --debugging_interval 1000 < file.data.json > file.schema.json
```

#### Debugging Map

Instead of printing out the BigQuery schema, the `--debugging_map` prints out
the bookkeeping metadata map which is used internally to keep track of the
various fields and theirs types that was inferred using the data file. This
flag is intended to be used for debugging.

```
$ generate-schema --debugging_map < file.data.json > file.schema.json
```

## Examples

Here is an example of a single JSON data record on the STDIN:

```
$ generate-schema
{ "s": "string", "b": true, "i": 1, "x": 3.1, "t": "2017-05-22T17:10:00-07:00" }
^D
INFO:root:Processed 1 lines
[
  {
    "mode": "NULLABLE",
    "name": "b",
    "type": "BOOLEAN"
  },
  {
    "mode": "NULLABLE",
    "name": "i",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "s",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "t",
    "type": "TIMESTAMP"
  },
  {
    "mode": "NULLABLE",
    "name": "x",
    "type": "FLOAT"
  }
]
```

In most cases, the data file will be stored in a file:
```
cat > file.data.json
{ "a": [1, 2] }
{ "i": 3 }
^D

$ generate-schema < file.data.json > file.schema.json
INFO:root:Processed 2 lines

$ cat file.schema.json
[
  {
    "mode": "REPEATED",
    "name": "a",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "i",
    "type": "INTEGER"
  }
]
```

## System Requirements

This project was developed on Ubuntu 17.04 using Python 3.5. It is likely
compatible with other Python environments but I have not yet verified those.

## Author

Created by Brian T. Park (brian@xparks.net).

## License

Apache License 2.0
