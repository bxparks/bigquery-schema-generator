# BigQuery Schema Generator

This script generates the BigQuery schema from the newline-delimited data
records on the STDIN. The records can be in JSON format or CSV format. The
BigQuery data importer (`bq load`) uses only the first 100 lines when the schema
auto-detection feature is enabled. In contrast, this script uses all data
records to generate the schema.

Usage:
```
$ generate-schema < file.data.json > file.schema.json
$ generate-schema --input_format csv < file.data.csv > file.schema.json
```

Version: 1.0 (2020-04-04)

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
the [first 100 records](https://cloud.google.com/bigquery/docs/schema-detect)
of the input data. In many cases, this is sufficient
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

Install from [PyPI](https://pypi.python.org/pypi) repository using `pip3`. There
are too many ways to install packages in Python. The following are in order
highest to lowest recommendation:

1) If you are using a virtual environment (such as
[venv](https://docs.python.org/3/library/venv.html)), then use:
```
$ pip3 install bigquery_schema_generator
```

2) If you aren't using a virtual environment you can install into
your local Python directory:

```
$ pip3 install --user bigquery_schema_generator
```

3) If you want to install the package for your entire system globally, use
```
$ sudo -H pip3 install bigquery_schema_generator
```
but realize that you will be running code from PyPI as `root` so this has
security implications.

Sometimes, your Python environment gets into a complete mess and the `pip3`
command won't work. Try typing `python3 -m pip` instead.

A successful install should print out something like the following (the version
number may be different):
```
Collecting bigquery-schema-generator
Installing collected packages: bigquery-schema-generator
Successfully installed bigquery-schema-generator-0.3.2
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

The `generate_schema.py` script accepts a newline-delimited JSON or
CSV data file on the STDIN. JSON input format has been tested extensively.
CSV input format was added more recently (in v0.4) using the `--input_format
csv` flag. The support is not as robust as JSON file. For example, CSV format
supports only the comma-separator, and does not support the pipe (`|`) or tab
(`\t`) character.

Unlike `bq load`, the `generate_schema.py` script reads every record in the
input data file to deduce the table's schema. It prints the JSON formatted
schema file on the STDOUT.

There are at least 3 ways to run this script:

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

If you retrieved this code from its
[GitHub repository](https://github.com/bxparks/bigquery-schema-generator),
then you can invoke the Python script directly:
```
$ ./generate_schema.py < file.data.json > file.schema.json
```

### Using the Schema Output

The resulting schema file can be given to the **bq load** command using the
`--schema` flag:
```

$ bq load --source_format NEWLINE_DELIMITED_JSON \
    --ignore_unknown_values \
    --schema file.schema.json \
    mydataset.mytable \
    file.data.json
```
where `mydataset.mytable` is the target table in BigQuery.

For debugging purposes, here is the equivalent `bq load` command using schema
autodetection:

```
$ bq load --source_format NEWLINE_DELIMITED_JSON \
    --autodetect \
    mydataset.mytable \
    file.data.json
```

If the input file is in CSV format, the first line will be the header line which
enumerates the names of the columns. But this header line must be skipped when
importing the file into the BigQuery table. We accomplish this using
`--skip_leading_rows` flag:
```
$ bq load --source_format CSV \
    --schema file.schema.json \
    --skip_leading_rows 1 \
    mydataset.mytable \
    file.data.csv
```

Here is the equivalent `bq load` command for CSV files using autodetection:
```
$ bq load --source_format CSV \
    --autodetect \
    mydataset.mytable \
    file.data.csv
```

A useful flag for `bq load`, particularly for JSON files,  is
`--ignore_unknown_values`, which causes `bq load` to ignore fields in the input
data which are not defined in the schema. When `generate_schema.py` detects an
inconsistency in the definition of a particular field in the input data, it
removes the field from the schema definition. Without the
`--ignore_unknown_values`, the `bq load` fails when the inconsistent data record
is read.

Another useful flag during development and debugging is `--replace` which
replaces any existing BigQuery table.

After the BigQuery table is loaded, the schema can be retrieved using:

```
$ bq show --schema mydataset.mytable | python3 -m json.tool
```

(The `python -m json.tool` command will pretty-print the JSON formatted schema
file. An alternative is the [jq command](https://stedolan.github.io/jq/).)
The resulting schema file should be identical to `file.schema.json`.

### Flag Options

The `generate_schema.py` script supports a handful of command line flags
as shown by the `--help` flag below.

#### Help (`--help`)

Print the built-in help strings:

```
$ generate-schema --help
usage: generate_schema.py [-h] [--input_format INPUT_FORMAT] [--keep_nulls]
                          [--quoted_values_are_strings] [--infer_mode]
                          [--debugging_interval DEBUGGING_INTERVAL]
                          [--debugging_map] [--sanitize_names]

Generate BigQuery schema from JSON or CSV file.

optional arguments:
  -h, --help            show this help message and exit
  --input_format INPUT_FORMAT
                        Specify an alternative input format ('csv', 'json')
  --keep_nulls          Print the schema for null values, empty arrays or
                        empty records
  --quoted_values_are_strings
                        Quoted values should be interpreted as strings
  --infer_mode          Determine if mode can be 'NULLABLE' or 'REQUIRED'
  --debugging_interval DEBUGGING_INTERVAL
                        Number of lines between heartbeat debugging messages
  --debugging_map       Print the metadata schema_map instead of the schema
  --sanitize_names      Forces schema name to comply with BigQuery naming
                        standard
```

#### Input Format (`--input_format`)

Specifies the format of the input file, either `json` (default) or `csv`.

If `csv` file is specified, the `--keep_nulls` flag is automatically activated.
This is required because CSV columns are defined positionally, so the schema
file must contain all the columns specified by the CSV file, in the same
order, even if the column contains an empty value for every record.

See [Issue #26](https://github.com/bxparks/bigquery-schema-generator/issues/26)
for implementation details.

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

#### Quoted Values Are Strings (`--quoted_values_are_strings`)

By default, quoted values are inspected to determine if they can be interpreted
as `DATE`, `TIME`, `TIMESTAMP`, `BOOLEAN`, `INTEGER` or `FLOAT`. This is
consistent with the algorithm used by `bq load`. However, for the `BOOLEAN`,
`INTEGER`, or `FLOAT` types, it is sometimes more useful to interpret those as
normal strings instead. This flag disables type inference for `BOOLEAN`,
`INTEGER` and `FLOAT` types inside quoted strings.

```
$ generate-schema
{ "name": "1" }
^D
[
  {
    "mode": "NULLABLE",
    "name": "name",
    "type": "INTEGER"
  }
]

$ generate-schema --quoted_values_are_strings
{ "name": "1" }
^D
[
  {
    "mode": "NULLABLE",
    "name": "name",
    "type": "STRING"
  }
]
```

#### Infer Mode (`--infer_mode`)

Set the schema `mode` of a field to `REQUIRED` instead of the default
`NULLABLE` if the field contains a non-null or non-empty value for every data
record in the input file. This option is available only for CSV
(`--input_format csv`) files. It is theoretically possible to implement this
feature for JSON files, but too difficult to implement in practice because
fields are often completely missing from a given JSON record (instead of
explicitly being defined to be `null`).

See [Issue #28](https://github.com/bxparks/bigquery-schema-generator/issues/28)
for implementation details.

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
various fields and their types that were inferred using the data file. This
flag is intended to be used for debugging.

```
$ generate-schema --debugging_map < file.data.json > file.schema.json
```

#### Sanitize Names (`--sanitize_names`)

BigQuery column names are restricted to certain characters and length. With this
flag, column names are sanitizes so that any character outside of ASCII letters,
numbers and underscore (`[a-zA-Z0-9_]`) are converted to an underscore. (For
example "go&2#there!" is converted to "go_2_there_".) Names longer than 128
characters are truncated to 128.

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

The `generate-schema` script supports both `NULLABLE` and `REPEATED` modes of
all of the above types.

The supported format of `TIMESTAMP` is as close as practical to the
[bq load format](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#timestamp-type):
```
YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]][time zone]
```
which appears to be an extension of the
[ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601).
The difference from `bq load` is that the `[time zone]` component can be only
* `Z`
* `UTC` (same as `Z`)
* `(+|-)H[H][:M[M]]`

Note that BigQuery supports up to 6 decimal places after the integer 'second'
component. `generate-schema` follows the same restriction for compatibility. If
your input file contains more than 6 decimal places, you need to write a data
cleansing filter to fix this.

The suffix `UTC` is not standard ISO 8601 nor
[documented by Google](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#time-zones)
but the `UTC` suffix is used by `bq extract` and the web interface. (See
[Issue 19](https://github.com/bxparks/bigquery-schema-generator/issues/19).)

Timezone names from the [tz database](http://www.iana.org/time-zones) (e.g.
"America/Los_Angeles") are _not_ supported by `generate-schema`.

The following types are _not_ supported at all:

* `BYTES`
* `DATETIME` (unable to distinguish from `TIMESTAMP`)

### Type Inferrence Rules

The `generate-schema` script attempts to emulate the various type conversion and
compatibility rules implemented by **bq load**:

* `INTEGER` can upgrade to `FLOAT`
    * if a field in an early record is an `INTEGER`, but a subsequent record
      shows this field to have a `FLOAT` value, the type of the field will be
      upgraded to a `FLOAT`
    * the reverse does not happen, once a field is a `FLOAT`, it will remain a
      `FLOAT`
* conflicting `TIME`, `DATE`, `TIMESTAMP` types upgrades to `STRING`
    * if a field is determined to have one type of "time" in one record, then
      subsequently a different "time" type, then the field will be assigned a
      `STRING` type
* `NULLABLE RECORD` can upgrade to a `REPEATED RECORD`
    * a field may be defined as `RECORD` (aka "Struct") type with `{ ... }`
    * if the field is subsequently read as an array with a `[{ ... }]`, the
      field is upgraded to a `REPEATED RECORD`
* a primitive type (`FLOAT`, `INTEGER`, `STRING`) cannot upgrade to a `REPEATED`
  primitive type
    * there's no technical reason why this cannot be allowed, but **bq load**
      does not support it, so we follow its behavior
* a `DATETIME` field is always inferred to be a `TIMESTAMP`
    * the format of these two fields is identical (in the absence of timezone)
    * we follow the same logic as **bq load** and always infer these as
      `TIMESTAMP`
* `BOOLEAN`, `INTEGER`, and `FLOAT` can appear inside quoted strings
    * In other words, `"true"` (or `"True"` or `"false"`, etc) is considered a
      BOOLEAN type, `"1"` is considered an INTEGER type, and `"2.1"` is
      considered a FLOAT type. Luigi Mori (jtschichold@) added additional logic
      to replicate the type conversion logic used by `bq load` for these
      strings.
    * This type inference inside quoted strings can be disabled using the
      `--quoted_values_are_strings` flag
    * (See [Issue #22](https://github.com/bxparks/bigquery-schema-generator/issues/22) for more details.)
* `INTEGER` values overflowing a 64-bit signed integer upgrade to `FLOAT`
    * integers greater than `2^63-1` (9223372036854775807)
    * integers less than `-2^63` (-9223372036854775808)
    * (See [Issue #18](https://github.com/bxparks/bigquery-schema-generator/issues/18) for more details)

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

Here is the schema generated from a CSV input file. The first line is the header
containing the names of the columns, and the schema lists the columns in the
same order as the header:
```
$ generate-schema --input_format csv
e,b,c,d,a
1,x,true,,2.0
2,x,,,4
3,,,,
^D
INFO:root:Processed 3 lines
[
  {
    "mode": "NULLABLE",
    "name": "e",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "b",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "c",
    "type": "BOOLEAN"
  },
  {
    "mode": "NULLABLE",
    "name": "d",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "a",
    "type": "FLOAT"
  }
]
```

Here is an example of the schema generated with the `--infer_mode` flag:
```
$ generate-schema --input_format csv --infer_mode
name,surname,age
John
Michael,,
Maria,Smith,30
Joanna,Anders,21
^D
INFO:root:Processed 4 lines
[
  {
    "mode": "REQUIRED",
    "name": "name",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "surname",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "age",
    "type": "INTEGER"
  }
]
```

## Using As a Library

The `bigquery_schema_generator` module can be used as a library by an external
Python client code by creating an instance of `SchemaGenerator` and calling the
`run(input, output)` method:

```python
from bigquery_schema_generator.generate_schema import SchemaGenerator

generator = SchemaGenerator(
    input_format=input_format,
    infer_mode=infer_mode,
    keep_nulls=keep_nulls,
    quoted_values_are_strings=quoted_values_are_strings,
    debugging_interval=debugging_interval,
    debugging_map=debugging_map)
generator.run(input_file, output_file)
```

If you need to process the generated schema programmatically, use the
`deduce_schema()` method and process the resulting `schema_map` and `error_log`
data structures like this:

```python
from bigquery_schema_generator.generate_schema import SchemaGenerator
...
schema_map, error_logs = generator.deduce_schema(input_file)

for error in error_logs:
    logging.info("Problem on line %s: %s", error['line'], error['msg'])

schema = generator.flatten_schema(schema_map)
json.dump(schema, output_file, indent=2)
```

## Benchmarks

I wrote the `bigquery_schema_generator/anonymize.py` script to create an
anonymized data file `tests/testdata/anon1.data.json.gz`:
```
$ ./bigquery_schema_generator/anonymize.py < original.data.json \
    > anon1.data.json
$ gzip anon1.data.json
```
This data file is 290MB (5.6MB compressed) with 103080 data records.

Generating the schema using
```
$ bigquery_schema_generator/generate_schema.py < anon1.data.json \
    > anon1.schema.json
```
took 67s on a Dell Precision M4700 laptop with an Intel Core i7-3840QM CPU @
2.80GHz, 32GB of RAM, Ubuntu Linux 18.04, Python 3.6.7.

## System Requirements

This project was initially developed on Ubuntu 17.04 using Python 3.5.3, but it
now requires Python 3.6 or higher, I think mostly due to the use of f-strings.

I have tested it on:

* Ubuntu 18.04, Python 3.7.7
* Ubuntu 18.04, Python 3.6.7
* Ubuntu 17.10, Python 3.6.3
* MacOS 10.14.2, [Python 3.6.4](https://www.python.org/downloads/release/python-364/)
* MacOS 10.13.2, [Python 3.6.4](https://www.python.org/downloads/release/python-364/)

The GitHub Actions continuous integration pipeline validates on Python 3.6, 3.7
and 3.8.

## Changelog

See [CHANGELOG.md](CHANGELOG.md).

## Authors

* Created by Brian T. Park (brian@xparks.net).
* Type inference inside quoted strings by Luigi Mori (jtschichold@).
* Flag to disable type inference inside quoted strings by Daniel Ecer
  (de-code@).
* Support for CSV files and detection of `REQUIRED` fields by Sandor Korotkevics
  (korotkevics@).
* Better support for using `bigquery_schema_generator` as a library from an
  external Python code by StefanoG_ITA (StefanoGITA@).
* Sanitizing of column names to valid BigQuery characters and length by Jon
  Warghed (jonwarghed@).
* Bug fix in `--sanitize_names` by Riccardo M. Cefala (riccardomc@).


## License

Apache License 2.0
