# Kirby Schema Generator
Generates schema files that are used for describing data that is ingested by Kirby.

Description has to be updated manually after schema generation.

## Usage
A sample data file is required for analyzing which will be used when generating the final schema.
```
$ python3 generate_schema.py <sample_data_file> --encryption_key_id <key_column_name> --personal_columns <column_names> 
```
sample_data_file: path to the sample file to be analyzed
encryption_key_id: If dataset is to be encrypted the column to use for keys, usually the user id.

This will return the generated schema to stdout. To save it to a file:
```
$ python3 generate_schema.py <sample_data_file> [options] > schema.json
```


For a description on all options, use:
```
$ python3 generate_schema.py -h   
```

### Examples

```
$ python3 generate_schema.py sample_data_file.csv --encryption_key_id user_id --personal_columns name email
```


Example sample CSV file:
```
user_id, name, email, subscription_start, subscription_end
1000, Kirby Kirbysson, kirby@bonniernews.se, 2019-01-02, 2021-02-01
1001, Luigi Plumberson, luigi@bonniernews.se, 2019-03-01, 2019-04-01
...
```

Example JSON sample file:
```
TODO: @dnjo Add JSON example
```

## TODO
* Add support for nested JSON properties

