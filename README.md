# Kirby Schema Generator
Generates schema files that are used for describing data that is ingested by Kirby.

## Usage
A sample data file is required for analyzing which will be used when generating the final schema.
```
$ python3 generate_schema.py <sample_data_file> --encryption_key_id <key_column_name> --personal_columns <column_names> 
```
`sample_data_file`: Path to the sample file to be analyzed.

`encryption_key_id`: If dataset is to be encrypted the column to use for keys, usually the user id.

`personal_columns`: Columns that contain personal information.

This will return the generated schema to stdout. To save it to a file:
```
$ python3 generate_schema.py <sample_data_file> [options] > schema.json
```

For a description on all options, use:
```
$ python3 generate_schema.py --help
```

### Schema column description
Column descriptions have to be added to the schema manually after it has been generated.

## Examples

### From a CSV sample file
`users.csv`:
```
user_id, name, email, subscription_start, subscription_end
1000, Kirby Kirbysson, kirby@bonniernews.se, 2019-01-02, 2021-02-01
1001, Luigi Plumberson, luigi@bonniernews.se, 2019-03-01, 2019-04-01
```

```
$ python3 generate_schema.py users.csv --encryption_key_id user_id --personal_columns name email
```

### From a JSON sample file
`users.json`:
```
{
  "user_id": 1000,
  "name": "Kirby Kirbysson",
  "email": "kirby@bonniernews.se",
  "subscription_start": "2019-01-02",
  "subscription_end": "2021-02-01"
}
{
  "user_id": 1001,
  "name": "Luigi Plumberson",
  "email": "luigi@bonniernews.se",
  "subscription_start": "2019-03-01",
  "subscription_end": "2019-04-01"
}
```

```
$ python3 generate_schema.py users.json --input_format json --encryption_key_id user_id --personal_columns name email
```

## TODO
* Add support for nested JSON properties

