# BigQuery Schema Generator

## Summary

This script generates the BigQuery schema from the newline-delimited JSON data
records on the STDIN. The BigQuery data importer (`bq load`) uses only the
first 100 lines when the schema auto-detection feature is enabled. In contrast,
this script uses all data records to generate the schema.

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

Install from [PyPI](https://pypi.python.org/pypi) repository using `pip3`.
If you want to install the package for your entire system globally, use
```
$ sudo -H pip3 install bigquery_schema_generator
```
If you are using a virtual environment (such as
[venv](https://docs.python.org/3/library/venv.html)), then you don't need
the `sudo` coommand, and you can just type:
```
$ pip3 install bigquery_schema_generator
```

A successful install should print out the following:
```
Collecting bigquery-schema-generator
Installing collected packages: bigquery-schema-generator
Successfully installed bigquery-schema-generator-0.1.4
```

The shell script `generate-schema` is installed in the same directory as
`pip3`.

### Ubuntu Linux

Under Ubuntu Linux, you should find the `generate-schema` script at
`/usr/local/bin/generate-schema`.

### MacOS

If you installed Python from
[Python Releases for Mac OS X](https://www.python.org/downloads/mac-osx/),
then `/usr/local/bin/pip3` is a symlink to
`/Library/Frameworks/Python.framework/Versions/3.6/bin/pip3`. So
`generate-schema` is installed at
`/Library/Frameworks/Python.framework/Versions/3.6/bin/generate-schema`.

The Python installer updates `$HOME/.bash_profile` to add
`/Library/Frameworks/Python.framework/Versions/3.6/bin` to the `$PATH`
environment variable. So you should be able to run the `generate-schema`
command without typing in the full path.

## Usage

The `generate_schema.py` script accepts a newline-delimited JSON data file on
the STDIN. (CSV is not supported currently.) It scans every record in the
input data file to deduce the table's schema. It prints the JSON formatted
schema file on the STDOUT. There are at least 3 ways to run this script:

**1) Shell script**

If you installed using `pip3`, then it should have installed a small helper
script named `generate-schema` in your local `./bin` directory of your current
environment (depending on whether you are using a virtual environment).

```
$ generate-schema < file.data.json > file.schema.json
```

**2) Python module**

You can invoke the module directly using:
```
$ python3 -m bigquery_schema_generator.generate_schema < file.data.json > file.schema.json
```
This is essentially what the `generate-schema` command does.

**3) Python script**

If you retrieved this code from its [GitHub
repository](https://github.com/bxparks/bigquery-schema-generator), then you can invoke
the Python script directly:
```
$ ./generate_schema.py < file.data.json > file.schema.json
```

### Schema Output

The resulting schema file can be given to the **bq load** command using the
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

### Flag Options

The `generate_schema.py` script supports a handful of command line flags:

* `--help` Prints the usage with the list of supported flags.
* `--keep_nulls` Print the schema for null values, empty arrays or empty records.
* `--debugging_interval lines` Number of lines between heartbeat debugging messages. Default 1000.
* `--debugging_map` Print the metadata schema map for debugging purposes

#### Help (`--help`)

Print the built-in help strings:

```
$ generate-schema --help
usage: generate_schema.py [-h] [--keep_nulls]
                          [--debugging_interval DEBUGGING_INTERVAL]
                          [--debugging_map]

Generate BigQuery schema.

optional arguments:
  -h, --help            show this help message and exit
  --keep_nulls          Print the schema for null values, empty arrays or
                        empty records.
  --debugging_interval DEBUGGING_INTERVAL
                        Number of lines between heartbeat debugging messages.
  --debugging_map       Print the metadata schema_map instead of the schema
                        for debugging
```

#### Keep Nulls (`--keep_nulls`)

Normally when the input data file contains a field which has a null, empty
array or empty record as its value, the field is suppressed in the schema file.
This flag enables this field to be included in the schema file.

In other words, using a data file containing just nulls and empty values:
```
$ generate_schema
{ "s": null, "a": [], "m": {} }
^D
INFO:root:Processed 1 lines
[]
```

With the `keep_nulls` flag, we get:
```
$ generate-schema --keep_nulls
{ "s": null, "a": [], "m": {} }
^D
INFO:root:Processed 1 lines
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

#### Debugging Interval (`--debugging_interval`)

By default, the `generate_schema.py` script prints a short progress message
every 1000 lines of input data. This interval can be changed using the
`--debugging_interval` flag.

```
$ generate-schema --debugging_interval 50 < file.data.json > file.schema.json
```

#### Debugging Map (`--debugging_map`)

Instead of printing out the BigQuery schema, the `--debugging_map` prints out
the bookkeeping metadata map which is used internally to keep track of the
various fields and theirs types that was inferred using the data file. This
flag is intended to be used for debugging.

```
$ generate-schema --debugging_map < file.data.json > file.schema.json
```

## Schema Types

### Supported Types

The **bq show --schema** command produces a JSON schema file that uses the
older [Legacy SQL date types](https://cloud.google.com/bigquery/data-types).
For compatibility, **generate-schema** script will also generate a schema file
using the legacy data types.

The supported types are:

* `BOOLEAN`
* `INTEGER`
* `FLOAT`
* `STRING`
* `TIMESTAMP`
* `DATE`
* `TIME`
* `RECORD`

The `generate-schema` script supports both `NULLABLE` and `REPEATED` modes of all
of the above types. The following types are _not_ supported:

* `BYTES`
* `DATETIME` (unable to distinguish from `TIMESTAMP`)

### Type Inferrence Rules

The `generate-schema` script attempt to emulate the various type conversion and
compatibility rules implemented by *bq load*:

* `INTEGER` can upgrade to `FLOAT`
  * if a field in an early record is an `INTEGER`, but a subsequent record shows
    this field to have a `FLOAT` value, the type of the field will be upgraded to
    a `FLOAT`
  * the reverse does not happen, once a field is a `FLOAT`, it will remain a
    `FLOAT`
* conflicting `TIME`, `DATE`, `TIMESTAMP` types downgrades to `STRING`
  * if a field is determined to have one type of "time" in one record, then
    subsequently a different "time" type, then the field will be assigned a
    `STRING` type
* `NULLABLE RECORD` can upgrade to a `REPEATED RECORD`
  * a field may be defined as `RECORD` (aka "Struct") type with `{ ... }`
  * if the field is subsequently read as an array with a `[{ ... }]`, the field
    is upgraded to a `REPEATED RECORD`
* a primitive type (`FLOAT`, `INTEGER`, `STRING`) cannot upgrade to a `REPEATED`
  primitive type
  * there's no technical reason why this cannot be allowed, but *bq load*
    does not support it, so we follow the rule
* a `DATETIME` field is always inferred to be a `TIMESTAMP`
  * the format of these two fields are identical (in the absence of timezone)
  * we follow the same logic as *bq load* and always infer these as `TIMESTAMP`


## Examples

Here is an example of a single JSON data record on the STDIN (the `^D` below
means typing Control-D, which indicates "end of file" under Linux and MacOS):

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
$ cat > file.data.json
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

This project was developed on Ubuntu 17.04 using Python 3.5.3. I have
tested it on:

* Ubuntu 17.04, Python 3.5.3
* Ubuntu 16.04, Python 3.5.2
* MacOS 10.13.2, [Python 3.6.4](https://www.python.org/downloads/release/python-364/)

## Author

Created by Brian T. Park (brian@xparks.net).

## License

Apache License 2.0
