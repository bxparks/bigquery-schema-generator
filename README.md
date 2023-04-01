# BigQuery Schema Generator

[![BigQuery Schema Generator CI](https://github.com/bxparks/bigquery-schema-generator/actions/workflows/pythonpackage.yml/badge.svg)](https://github.com/bxparks/bigquery-schema-generator/actions/workflows/pythonpackage.yml)

This script generates the BigQuery schema from the newline-delimited data
records on the STDIN. The records can be in JSON format or CSV format. The
BigQuery data importer (`bq load`) uses only the
[first 500 records](https://cloud.google.com/bigquery/docs/schema-detect)
when the schema auto-detection feature is enabled. In contrast, this script uses
all data records to generate the schema.

Usage:
```
$ generate-schema < file.data.json > file.schema.json
$ generate-schema --input_format csv < file.data.csv > file.schema.json
```

**Version**: 1.5.2 (2023-04-01)

**Changelog**: [CHANGELOG.md](CHANGELOG.md)

## Table of Contents

* [Background](#Background)
* [Installation](#Installation)
    * [Ubuntu Linux](#UbuntuLinux)
    * [MacOS](#MacOS)
        * [MacOS 12 (Monterey)](#MacOS12)
        * [MacOS 11 (Big Sur)](#MacOS11)
        * [MacOS 10.14 (Mojave)](#MacOS1014)
* [Usage](#Usage)
    * [Command Line](#CommandLine)
    * [Schema Output](#SchemaOutput)
    * [Command Line Flag Options](#FlagOptions)
        * [Help (`--help`)](#Help)
        * [Input Format (`--input_format`)](#InputFormat)
        * [Keep Nulls (`--keep_nulls`)](#KeepNulls)
        * [Quoted Values Are Strings(`--quoted_values_are_strings`)](#QuotedValuesAreStrings)
        * [Infer Mode (`--infer_mode`)](#InferMode)
        * [Debugging Interval (`--debugging_interval`)](#DebuggingInterval)
        * [Debugging Map (`--debugging_map`)](#DebuggingMap)
        * [Sanitize Names (`--sanitize_names`)](#SanitizedNames)
        * [Ignore Invalid Lines (`--ignore_invalid_lines`)](#IgnoreInvalidLines)
        * [Existing Schema Path (`--existing_schema_path`)](#ExistingSchemaPath)
        * [Preserve Input Sort Order
          (`--preserve_input_sort_order`)](#PreserveInputSortOrder)
    * [Using as a Library](#UsingAsLibrary)
        * [`SchemaGenerator.run()`](#SchemaGeneratorRun)
        * [`SchemaGenerator.deduce_schema()` from
          File](#SchemaGeneratorDeduceSchemaFromFile)
        * [`SchemaGenerator.deduce_schema()` from
          Dict](#SchemaGeneratorDeduceSchemaFromDict)
        * [`SchemaGenerator.deduce_schema()` from
          DictReader](#SchemaGeneratorDeduceSchemaFromCsvDictReader)
* [Schema Types](#SchemaTypes)
    * [Supported Types](#SupportedTypes)
    * [Type Inference](#TypeInference)
* [Examples](#Examples)
* [Benchmarks](#Benchmarks)
* [System Requirements](#SystemRequirements)
* [License](#License)
* [Feedback and Support](#Feedback)
* [Authors](#Authors)

<a name="Background"></a>
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
the [first 500 records](https://cloud.google.com/bigquery/docs/schema-detect)
of the input data. In many cases, this is sufficient
because the data records were dumped from another database and the exact schema
of the source table was known. However, for data extracted from a service
(e.g. using a REST API) the record fields could have been organically added
at later dates. In this case, the first 500 records do not contain fields which
are present in later records. The **bq load** auto-detection fails and the data
fails to load.

The **bq load** tool does not support the ability to process the entire dataset
to determine a more accurate schema. This script fills in that gap. It
processes the entire dataset given in the STDIN and outputs the BigQuery schema
in JSON format on the STDOUT. This schema file can be fed back into the **bq
load** tool to create a table that is more compatible with the data fields in
the input dataset.

<a name="Installation"></a>
## Installation

**Prerequisite**: You need have Python 3.6 or higher.

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
Successfully installed bigquery-schema-generator-1.1
```

The shell script `generate-schema` will be installed somewhere in your system,
depending on how your Python environment is configured. See below for
some notes for Ubuntu Linux and MacOS.

<a name="UbuntuLinux"></a>
### Ubuntu Linux (18.04, 20.04, 22.04)

After running `pip3 install bigquery_schema_generator`, the `generate-schema`
script may be installed in one the following locations:

* `/usr/bin/generate-schema`
* `/usr/local/bin/generate-schema`
* `$HOME/.local/bin/generate-schema`
* `$HOME/.virtualenvs/{your_virtual_env}/bin/generate-schema`

<a name="MacOS"></a>
### MacOS

I don't have any Macs which are able to run the latest macOS, and I don't use
them much for software development these days, but here are some notes on older
versions of macOS in case they help.

<a name="MacOS12"></a>
#### MacOS 12 (Monterey)

Python 2 or 3 is not installed by default on Monterey. If you try to run
`python3` on the command line, a dialog box asks you to install the
[Xcode](https://developer.apple.com/support/xcode/) development package. It
apparently takes over an hour at 10 MB/s.

You can instead install Python 3 using
[Homebrew](https://docs.brew.sh/Homebrew-and-Python), by installing `brew`, and
typing `$ brew install python`. Currently, it downloads Python 3.10 in about 1-2
minutes and installs the `python3` and `pip3` binaries into
`/usr/local/bin/python3` and `/usr/local/bin/pip3`. Using `brew` seems to be
easiest option, so let's assume that Python 3 was installed through that.

If you run:
```
$ pip3 install bigquery_schema_generator
```
the package will be installed at `/usr/local/lib/python3.10/site-packages/`, and
the `generate-schema` script will be installed at
`/usr/local/bin/generate-schema`.

If you use the `--user` flag:
```
$ pip3 install --user bigquery_schema_generator
```
the package will be installed at
`$HOME/Library/Python/3.10/lib/python/site-packages/`, and the `generate-schema`
script will be installed at `$HOME/Library/Python/3.10/bin/generate-schema`.

You may need to add the `$HOME/Library/Python/3.10/bin` directory to your
`$PATH` variable in your `$HOME/.bashrc` file.

<a name="MacOS11"></a>
#### MacOS 11 (Big Sur)

Python 2.7.16 is installed by default on Big Sur as `/usr/bin/python`. If you
try to run `python3` on the command line, a dialog box asks you to install
the [Xcode](https://developer.apple.com/support/xcode/) development package will
be installed, which I think installs Python 3.8 as `/usr/bin/python3` (I can't
remember, it was installed a long time ago.)

You can instead install Python 3 using
[Homebrew](https://docs.brew.sh/Homebrew-and-Python), by installing `brew`, and
typing `$ brew install python`. Currently, it downloads Python 3.10 in about 1-2
minutes and installs the `python3` and `pip3` binaries into
`/usr/local/bin/python3` and `/usr/local/bin/pip3`. Using `brew` seems to be
easiest option, so let's assume that Python 3 was installed through that.

If you run:
```
$ pip3 install bigquery_schema_generator
```
the package will be installed at `/usr/local/lib/python3.10/site-packages/`, and
the `generate-schema` script will be installed at
`/usr/local/bin/generate-schema`.

If you use the `--user` flag:
```
$ pip3 install --user bigquery_schema_generator
```
the package will be installed at
`$HOME/Library/Python/3.10/lib/python/site-packages/`, and the `generate-schema`
script will be installed at `$HOME/Library/Python/3.10/bin/generate-schema`.

You may need to add the `$HOME/Library/Python/3.10/bin` directory to your
`$PATH` variable in your `$HOME/.bashrc` file.

<a name="MacOS1014"></a>
#### MacOS 10.14 (Mojave)

This MacOS version comes with Python 2.7 only. To install Python 3, you can
install using:

1)) Downloading the [macos installer directly from
   Python.org](https://www.python.org/downloads/macos/).

The python3 binary will be located at `/usr/local/bin/python3`, and the
`/usr/local/bin/pip3` is a symlink to
`/Library/Frameworks/Python.framework/Versions/3.6/bin/pip3`.

So running

```
$ pip3 install --user bigquery_schema_generator
```

will install `generate-schema` at
`/Library/Frameworks/Python.framework/Versions/3.6/bin/generate-schema`.

The Python installer updates `$HOME/.bash_profile` to add
`/Library/Frameworks/Python.framework/Versions/3.6/bin` to the `$PATH`
environment variable. So you should be able to run the `generate-schema`
command without typing in the full path.

2)) Using [Homebrew](https://docs.brew.sh/Homebrew-and-Python).

In this environment, the `generate-schema` script will probably be installed in
`/usr/local/bin` but I'm not completely certain.

<a name="Usage"></a>
## Usage

<a name="CommandLine"></a>
### Command Line

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

<a name="SchemaOutput"></a>
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

<a name="FlagOptions"></a>
### Command Line Flag Options

The `generate_schema.py` script supports a handful of command line flags
as shown by the `--help` flag below.

<a name="Help"></a>
#### Help (`--help`)

Print the built-in help strings:

```bash
$ generate-schema --help
usage: generate-schema [-h] [--input_format INPUT_FORMAT] [--keep_nulls]
                       [--quoted_values_are_strings] [--infer_mode]
                       [--debugging_interval DEBUGGING_INTERVAL]
                       [--debugging_map] [--sanitize_names]
                       [--ignore_invalid_lines]
                       [--existing_schema_path EXISTING_SCHEMA_PATH]
                       [--preserve_input_sort_order]

Generate BigQuery schema from JSON or CSV file.

optional arguments:
  -h, --help            show this help message and exit
  --input_format INPUT_FORMAT
                        Specify an alternative input format ('csv', 'json',
                        'dict')
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
  --ignore_invalid_lines
                        Ignore lines that cannot be parsed instead of stopping
  --existing_schema_path EXISTING_SCHEMA_PATH
                        File that contains the existing BigQuery schema for a
                        table. This can be fetched with: `bq show --schema
                        <project_id>:<dataset>:<table_name>
  --preserve_input_sort_order
                        Preserve the original ordering of columns from input
                        instead of sorting alphabetically. This only impacts
                        `input_format` of json or dict

```

<a name="InputFormat"></a>
#### Input Format (`--input_format`)

Specifies the format of the input file as a string. It must be one of `json`
(default), `csv`, or `dict`:

* `json`
    * a "file-like" object containing newline-delimited JSON
* `csv`
    * a "file-like" object containing newline-delimited CSV
* `dict`
    * a `list` of Python `dict` objects corresponding to list of
      newline-delimited JSON, in other words `List[Dict[str, Any]]`
    * applies only if `SchemaGenerator` is used as a library through the
      `run()` or `deduce_schema()` method
    * useful if the input data (usually JSON) has already been read into memory
      and parsed from newline-delimited JSON into native Python dict objects.

If `csv` file is specified, the `--keep_nulls` flag is automatically activated.
This is required because CSV columns are defined positionally, so the schema
file must contain all the columns specified by the CSV file, in the same
order, even if the column contains an empty value for every record.

See [Issue #26](https://github.com/bxparks/bigquery-schema-generator/issues/26)
for implementation details.

<a name="KeepNulls"></a>
#### Keep Nulls (`--keep_nulls`)

Normally when the input data file contains a field which has a null, empty
array or empty record as its value, the field is suppressed in the schema file.
This flag enables this field to be included in the schema file.

In other words, using a data file containing just nulls and empty values:
```bash
$ generate_schema
{ "s": null, "a": [], "m": {} }
^D
INFO:root:Processed 1 lines
[]
```

With the `keep_nulls` flag, we get:
```bash
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

<a name="QuotedValuesAreStrings"></a>
#### Quoted Values Are Strings (`--quoted_values_are_strings`)

By default, quoted values are inspected to determine if they can be interpreted
as `DATE`, `TIME`, `TIMESTAMP`, `BOOLEAN`, `INTEGER` or `FLOAT`. This is
consistent with the algorithm used by `bq load`. However, for the `BOOLEAN`,
`INTEGER`, or `FLOAT` types, it is sometimes more useful to interpret those as
normal strings instead. This flag disables type inference for `BOOLEAN`,
`INTEGER` and `FLOAT` types inside quoted strings.

```bash
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

<a name="InferMode"></a>
#### Infer Mode (`--infer_mode`)

Set the schema `mode` of a field to `REQUIRED` instead of the default
`NULLABLE` if the field contains a non-null or non-empty value for every data
record in the input file. This option is available only for CSV
(`--input_format csv`) files. It is theoretically possible to implement this
feature for JSON files, but too difficult to implement in practice because
fields are often completely missing from a given JSON record (instead of
explicitly being defined to be `null`).

In addition to the above, this option, when used in conjunction with
`--existing_schema_map`, will allow fields to be relaxed from REQUIRED to
NULLABLE if they were REQUIRED in the existing schema and NULL rows are found in
the new data we are inferring a schema from. In this case it can be used with
either input_format, CSV or JSON.

See [Issue #28](https://github.com/bxparks/bigquery-schema-generator/issues/28)
for implementation details.

<a name="DebuggingInterval"></a>
#### Debugging Interval (`--debugging_interval`)

By default, the `generate_schema.py` script prints a short progress message
every 1000 lines of input data. This interval can be changed using the
`--debugging_interval` flag.

```bash
$ generate-schema --debugging_interval 50 < file.data.json > file.schema.json
```

<a name="DebuggingMap"></a>
#### Debugging Map (`--debugging_map`)

Instead of printing out the BigQuery schema, the `--debugging_map` prints out
the bookkeeping metadata map which is used internally to keep track of the
various fields and their types that were inferred using the data file. This
flag is intended to be used for debugging.

```bash
$ generate-schema --debugging_map < file.data.json > file.schema.json
```

<a name="SanitizedNames"></a>
#### Sanitize Names (`--sanitize_names`)

BigQuery column names are [restricted to certain characters and
length](https://cloud.google.com/bigquery/docs/schemas#column_names):
* it must contain only letters (a-z, A-Z), numbers (0-9), or underscores
* it must start with a letter or underscore
* the maximum length is 128 characters
* column names are case-insensitive

For CSV files, the `bq load` command seems to automatically convert invalid
column names into valid column names. This flag attempts to perform some of the
same transformations, to avoid having to scan through the input data twice to
generate the schema file. The transformations are:

* any character outside of ASCII letters, numbers and underscore
  (`[a-zA-Z0-9_]`) are converted to an underscore. For example `go&2#there!` is
  converted to `go_2_there_`;
* names longer than 128 characters are truncated to 128.

My recollection is that the `bq load` command does *not* normalize the JSON key
names. Instead it prints an error message. So the `--sanitize_names` flag is
useful mostly for CSV files. For JSON files, you'll have to do a second pass
through the data files to cleanup the column names anyway. See
[Issue #14](https://github.com/bxparks/bigquery-schema-generator/issues/14) and
[Issue #33](https://github.com/bxparks/bigquery-schema-generator/issues/33).

<a name="IgnoreInvalidLines"></a>
#### Ignore Invalid Lines (`--ignore_invalid_lines`)

By default, if an error is encountered on a particular line, processing stops
immediately with an exception. This flag causes invalid lines to be ignored and
processing continues. A list of all errors and their line numbers will be
printed on the STDERR after processing the entire file.

This flag is currently most useful for JSON files, to ignore lines which do not
parse correctly as a JSON object.

This flag is probably not useful for CSV files. CSV files are processed by the
`DictReader` class which performs its own line processing internally, including
extracting the column names from the first line of the file. If the `DictReader`
does throw an exception on a given line, we would not be able to catch it and
continue processing. Fortunately, CSV files are fairly robust, and the schema
deduction logic will handle any missing or extra columns gracefully.

Fixes
[Issue #49](https://github.com/bxparks/bigquery-schema-generator/issues/49).

<a name="ExistingSchemaPath"></a>
#### Existing Schema Path (`--existing_schema_path`)

There are cases where we would like to start from an existing BigQuery table
schema rather than starting from scratch with a new batch of data we would like
to load. In this case we can specify the path to a local file on disk that is
our existing bigquery table schema. This can be generated via the following `bq
show --schema` command:
```bash
bq show --schema <PROJECT_ID>:<DATASET_NAME>.<TABLE_NAME> > existing_table_schema.json
```

We can then run generate-schema with the additional option
```bash
--existing_schema_path existing_table_schema.json
```

There is some subtle interaction between the `--existing_schema_path` and fields
which are marked with a `mode` of `REQUIRED` in the existing schema. If the new
data contains a `null` value (either in a CSV or JSON data file), it is not
clear if the schema should be changed to `mode=NULLABLE` or whether the new data
should be ignored and the schema should remain `mode=REQUIRED`. The choice is
determined by overloading the `--infer_mode` flag:

* If `--infer_mode` is given, the new schema will be allowed to revert back to
  `NULLABLE`.
* If `--infer_mode` is not given, the offending new record will be ignored
  and the new schema will remain `REQUIRED`.

See discussion in
[PR #57](https://github.com/bxparks/bigquery-schema-generator/pull/57) for
more details.

<a name="PreserveInputSortOrder"></a>
#### Preserve Input Sort Order (`--preserve_input_sort_order`)

By default, the order of columns in the BQ schema file is sorted
lexicographically, which matched the original behavior of `bq load
--autodetect`. If the `--preserve_input_sort_order` flag is given, the columns
in the resulting schema file is not sorted, but preserves the order of
appearance in the input JSON data. For example, the following JSON data with
the `--preserve_input_sort_order` flag will produce:

```bash
$ generate-schema --preserve_input_sort_order
{ "s": "string", "i": 3, "x": 3.2, "b": true }
^D
[
  {
    "mode": "NULLABLE",
    "name": "s",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "i",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "x",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "b",
    "type": "BOOLEAN"
  }
]
```

It is possible that each JSON record line contains only a partial subset of the
total possible columns in the data set. The order of the columns in the BQ
schema will then be the order that each column was first *seen* by the
script:

```bash
$ generate-schema --preserve_input_sort_order
{ "s": "string", "i": 3 }
{ "x": 3.2, "s": "string", "i": 3 }
{ "b": true, "x": 3.2, "s": "string", "i": 3 }
^D
[
  {
    "mode": "NULLABLE",
    "name": "s",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "i",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "x",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "b",
    "type": "BOOLEAN"
  }
]
```

**Note**: In Python 3.6 (the earliest version of Python supported by this
project), the order of keys in a `dict` was the insertion-order, but this
ordering was an implementation detail, and not guaranteed. In Python 3.7, that
ordering was made permanent. So the `--preserve_input_sort_order` flag
**should** work in Python 3.6 but is not guaranteed.

See discussion in
[PR #75](https://github.com/bxparks/bigquery-schema-generator/pull/75) for
more details.

<a name="UsingAsLibrary"></a>
### Using As a Library

The `SchemaGenerator` class can be used programmatically as a library from a
larger Python application.

<a name="SchemaGeneratorRun"></a>
#### `SchemaGenerator.run()`

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
    debugging_map=debugging_map,
    sanitize_names=sanitize_names,
    ignore_invalid_lines=ignore_invalid_lines,
    preserve_input_sort_order=preserve_input_sort_order,
)

FILENAME = "..."

with open(FILENAME) as input_file:
    generator.run(input_file=input_file, output_file=output_file)
```

The `input_format` is one of `json`, `csv`, and `dict` as described in the
[Input Format](#InputFormat) section above. The `input_file` must match the
format given by this parameter.

See [generatorrun.py](examples/generatorrun.py) for an example.

<a name="SchemaGeneratorDeduceSchemaFromFile"></a>
#### `SchemaGenerator.deduce_schema()` from File

If you need to process the generated schema programmatically, create an instance
of `SchemaGenerator` using the appropriate `input_format` option, use the
`deduce_schema()` method to read in the file, then postprocess the resulting
`schema_map` and `error_log` data structures.

The following reads in a JSON file (see [jsoneader.py](examples/jsoneader.py)):

```python
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
```

The following reads a CSV file (see [csvreader.py](examples/csvreader.py)):

```python
...(same as above)...

generator = SchemaGenerator(
    input_format='csv',
    infer_mode=True,
    quoted_values_are_strings=True,
    sanitize_names=True,
)

with open(FILENAME) as file:
    schema_map, errors = generator.deduce_schema(file)

...(same as above)...
```

The `deduce_schema()` also supports starting from an existing `schema_map`
instead of starting from scratch. This is the internal version of the
`--existing_schema_path` functionality.

```python
schema_map1, errors = generator.deduce_schema(input_data=data1)
schema_map2, errors = generator.deduce_schema(
    input_data=data1, schema_map=schema_map1
)
```

The `input_data` must match the `input_format` given in the constructor. The
format is described in the [Input Format](#InputFormat) section above.

<a name="SchemaGeneratorDeduceSchemaFromDict"></a>
#### `SchemaGenerator.deduce_schema()` from Iterable of Dict

If the JSON data set has already been read into memory into an array or iterable
of Python `dict` objects, the `SchemaGenerator` can process that too using the
`input_format='dict'` option. Here is an example from
[dictreader.py](examples/dictreader.py):


```Python
import json
import logging
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
schema_map, errors = generator.deduce_schema(input_data)
schema = generator.flatten_schema(schema_map)
json.dump(schema, sys.stdout, indent=2)
print()
```

**Note**: The `input_format='dict'` option supports any `input_data` object
which acts like an iterable of `dict`. The data does not have to be loaded into
memory.

<a name="SchemaGeneratorDeduceSchemaFromCsvDictReader"></a>
#### `SchemaGenerator.deduce_schema()` from csv.DictReader

The `input_format='csvdictreader'` option is similar to `input_format='dict'`
but sort of acts like `input_format='csv'`. It supports any object that behaves
like an iterable of `dict`, but it is intended to be used with the
[csv.DictReader](https://docs.python.org/3/library/csv.html) object.

The difference between `'dict'` and `'csvdictreader'` is the assumption made
about the shape of the data. The `'csvdictreader'` option assumes that the data
is tabular like a CSV file, with every row usually containing an entry for every
column. The `'dict'` option does not make that assumption, and the data can be
more hierarchical with some rows containing partial sets of columns.

This semantic difference means that `'csvdictreader'` supports options which
apply to `'csv'` files. In particular, the `infer_mode=True` option can be used
to determine if the `mode` field can be `REQUIRED` instead of `NULLABLE` if the
script finds that all columns are defined in every row.

Here is an example from [tsvreader.py](examples/tsvreader.py) which reads a
tab-separate file (TSV):

```python
import csv
import json
import sys
from bigquery_schema_generator.generate_schema import SchemaGenerator

FILENAME = "tsvfile.tsv"

generator = SchemaGenerator(input_format='dict')
with open(FILENAME) as file:
    reader = csv.DictReader(file, delimiter='\t')
    schema_map, errors = generator.deduce_schema(reader)

schema = generator.flatten_schema(schema_map)
json.dump(schema, sys.stdout, indent=2)
print()
```

<a name="SchemaTypes"></a>
## Schema Types

<a name="SupportedTypes"></a>
### Supported Types

The `bq show --schema` command produces a JSON schema file that uses the
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

<a name="TypeInference"></a>
### Type Inference Rules

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

<a name="Examples"></a>
## Examples

Here is an example of a single JSON data record on the STDIN (the `^D` below
means typing Control-D, which indicates "end of file" under Linux and MacOS):

```bash
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
```bash
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
```bash
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
```bash
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

<a name="Benchmarks"></a>
## Benchmarks

I wrote the `bigquery_schema_generator/anonymize.py` script to create an
anonymized data file `tests/testdata/anon1.data.json.gz`:
```bash
$ ./bigquery_schema_generator/anonymize.py < original.data.json \
    > anon1.data.json
$ gzip anon1.data.json
```
This data file is 290MB (5.6MB compressed) with 103080 data records.

Generating the schema using
```bash
$ bigquery_schema_generator/generate_schema.py < anon1.data.json \
    > anon1.schema.json
```
took 67s on a Dell Precision M4700 laptop with an Intel Core i7-3840QM CPU @
2.80GHz, 32GB of RAM, Ubuntu Linux 18.04, Python 3.6.7.

<a name="SystemRequirements"></a>
## System Requirements

This project was initially developed on Ubuntu 17.04 using Python 3.5.3, but it
now requires Python 3.6 or higher, I think mostly due to the use of f-strings.

I have tested it on:

* Ubuntu 22.04, Python 3.10.6
* Ubuntu 20.04, Python 3.8.5
* Ubuntu 18.04, Python 3.7.7
* Ubuntu 18.04, Python 3.6.7
* Ubuntu 17.10, Python 3.6.3
* MacOS 12.6.2 (Monterey), Python 3.10.9
* MacOS 11.7.2 (Big Sur), Python 3.10.9
* MacOS 11.7.2 (Big Sur), Python 3.8.9
* MacOS 10.14.2 (Mojave), Python 3.6.4
* MacOS 10.13.2 (High Sierra), Python 3.6.4

The GitHub Actions continuous integration pipeline validates on Python 3.7,
3.8, 3.9, and 3.10.

The unit tests are invoked with `$ make tests` target, and depends only on the
built-in Python `unittest` package.

The coding style check is invoked using `$ make flake8` and depends on the
`flake8` package. It can be installed using `$ pip3 install --user flake8`.

<a name="License"></a>
## License

Apache License 2.0

<a name="Feedback"></a>
## Feedback and Support

If you have any questions, comments, or feature requests for this library,
please use the [GitHub
Discussions](https://github.com/bxparks/bigquery-schema-generator/discussions)
for this project. If you have bug reports, please file a ticket in [GitHub
Issues](https://github.com/bxparks/bigquery-schema-generator/issues). Feature
requests should go into Discussions first because they often have alternative
solutions which are useful to remain visible, instead of disappearing from the
default view of the Issue tracker after the ticket is closed.

Please refrain from emailing me directly unless the content is sensitive. The
problem with email is that I cannot reference the email conversation when other
people ask similar questions later.

<a name="Authors"></a>
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
* Print full path of nested JSON elements in error messages, by Austin Brogle
  (abroglesc@).
* Allow an existing schema file to be specified using `--existing_schema_path`,
  by Austin Brogle (abroglesc@) and Bozo Dragojevic (bozzzzo@).
* Allow `SchemaGenerator.deduce_schema()` to accept a list of native Python
  `dict` objects, by Zigfrid Zvezdin (ZiggerZZ@).
* Make the column order in the BQ schema file match the order of appearance in
  the JSON data file using the `--preserve_input_sort_order` flag. By Kevin
  Deggelman (kdeggelman@).
