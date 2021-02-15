# Kirby Schema Generator
Generates schema files that are used for describing data that is ingested by Kirby.

## Usage
A sample data file is required for analyzing which will be used when generating the final schema.
```
$ python poc_schema_generator.py <sample_data_file> --encryption_key_id <key_column_name> --personal_columns <column_names> 
```
example_data_file can be example.csv or example.json
encryption_key_id the column to use for keys, if dataset is to be encrypted

This will return the analyzed schema to standard out. To save it to a file:
```
$ python poc_schema_generator <sample_data_file> [options] > schema.json
```


For a description on all options, use:
```
 python3 poc_schema_generator.py -h   
```

### Examples

```
 python3 poc_schema_generator.py  example_data_file.csv --encryption_key_id user_id --personal_columns name email
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

### TODO
* Add support for nested JSON properties

