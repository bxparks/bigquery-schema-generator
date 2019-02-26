#!/usr/bin/env python3
#
# Copyright 2017 Brian T. Park
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
import os
import json
from collections import OrderedDict
from bigquery_schema_generator.generate_schema import SchemaGenerator
from bigquery_schema_generator.generate_schema import sort_schema
from bigquery_schema_generator.generate_schema import is_string_type
from bigquery_schema_generator.generate_schema import convert_type
from data_reader import DataReader


class TestSchemaGenerator(unittest.TestCase):
    def test_timestamp_matcher_valid(self):
        self.assertTrue(
            SchemaGenerator.TIMESTAMP_MATCHER.match('2017-05-22T12:33:01'))
        self.assertTrue(
            SchemaGenerator.TIMESTAMP_MATCHER.match('2017-05-22 12:33:01'))
        self.assertTrue(
            SchemaGenerator.TIMESTAMP_MATCHER.match('2017-05-22 12:33:01.123'))
        self.assertTrue(
            SchemaGenerator.TIMESTAMP_MATCHER.match(
                '2017-05-22 12:33:01.123456'))
        self.assertTrue(
            SchemaGenerator.TIMESTAMP_MATCHER.match('2017-05-22T12:33:01Z'))
        self.assertTrue(
            SchemaGenerator.TIMESTAMP_MATCHER.match('2017-05-22T12:33:01 Z'))
        self.assertTrue(
            SchemaGenerator.TIMESTAMP_MATCHER.match('2017-05-22T12:33:01UTC'))
        self.assertTrue(
            SchemaGenerator.TIMESTAMP_MATCHER.match('2017-05-22 12:33:01 UTC'))
        self.assertTrue(
            SchemaGenerator.TIMESTAMP_MATCHER.match(
                '2017-05-22 12:33:01-7:00'))
        self.assertTrue(
            SchemaGenerator.TIMESTAMP_MATCHER.match(
                '2017-05-22 12:33:01-07:30'))
        self.assertTrue(
            SchemaGenerator.TIMESTAMP_MATCHER.match('2017-05-22T12:33:01-7'))
        self.assertTrue(
            SchemaGenerator.TIMESTAMP_MATCHER.match(
                '2017-05-22 12:33:01+7:00'))
        self.assertTrue(
            SchemaGenerator.TIMESTAMP_MATCHER.match('2017-5-2T1:3:1'))

    def test_timestamp_matcher_invalid(self):
        self.assertFalse(
            SchemaGenerator.TIMESTAMP_MATCHER.match(
                '2017-05-22 12:33:01-123:445'))
        self.assertFalse(
            SchemaGenerator.TIMESTAMP_MATCHER.match(
                '2017-05-22 12:33:01-0700'))
        self.assertFalse(
            SchemaGenerator.TIMESTAMP_MATCHER.match(
                '2017-05-22 12:33:01.1234567'))
        self.assertFalse(
            SchemaGenerator.TIMESTAMP_MATCHER.match('2017-05-22T12:33'))
        self.assertFalse(
            SchemaGenerator.TIMESTAMP_MATCHER.match('2017-05-22A12:33:00'))
        self.assertFalse(
            SchemaGenerator.TIMESTAMP_MATCHER.match(
                '2017-05-22T12:33:01X07:00'))
        self.assertFalse(
            SchemaGenerator.TIMESTAMP_MATCHER.match('2017-5-2A2:3:0'))
        self.assertFalse(
            SchemaGenerator.TIMESTAMP_MATCHER.match('17-05-22T12:33:01'))
        self.assertFalse(
            SchemaGenerator.TIMESTAMP_MATCHER.match('2017-05-22T12:33:01 UT'))

    def test_date_matcher_valid(self):
        self.assertTrue(SchemaGenerator.DATE_MATCHER.match('2017-05-22'))
        self.assertTrue(SchemaGenerator.DATE_MATCHER.match('2017-1-1'))

    def test_date_matcher_invalid(self):
        self.assertFalse(SchemaGenerator.DATE_MATCHER.match('17-05-22'))
        self.assertFalse(SchemaGenerator.DATE_MATCHER.match('2017-111-22'))
        self.assertFalse(SchemaGenerator.DATE_MATCHER.match('1988-00-00'))

    def test_time_matcher_valid(self):
        self.assertTrue(SchemaGenerator.TIME_MATCHER.match('12:33:01'))
        self.assertTrue(SchemaGenerator.TIME_MATCHER.match('12:33:01.123'))
        self.assertTrue(SchemaGenerator.TIME_MATCHER.match('12:33:01.123456'))
        self.assertTrue(SchemaGenerator.TIME_MATCHER.match('1:3:1'))

    def test_time_matcher_invalid(self):
        self.assertFalse(SchemaGenerator.TIME_MATCHER.match(':33:01'))
        self.assertFalse(SchemaGenerator.TIME_MATCHER.match('123:33:01'))
        self.assertFalse(
            SchemaGenerator.TIME_MATCHER.match('12:33:01.1234567'))

    def test_infer_value_type(self):
        generator = SchemaGenerator()

        # STRING and date/time
        self.assertEqual('STRING', generator.infer_value_type('abc'))
        self.assertEqual('TIME', generator.infer_value_type('12:34:56'))
        self.assertEqual('DATE', generator.infer_value_type('2018-02-08'))
        self.assertEqual('TIMESTAMP',
                         generator.infer_value_type('2018-02-08T12:34:56'))

        # BOOLEAN
        self.assertEqual('BOOLEAN', generator.infer_value_type(True))
        self.assertEqual('QBOOLEAN', generator.infer_value_type('True'))
        self.assertEqual('QBOOLEAN', generator.infer_value_type('False'))
        self.assertEqual('QBOOLEAN', generator.infer_value_type('true'))
        self.assertEqual('QBOOLEAN', generator.infer_value_type('false'))

        # INTEGER
        self.assertEqual('INTEGER', generator.infer_value_type(1))
        self.assertEqual('INTEGER',
                         generator.infer_value_type(9223372036854775807))
        self.assertEqual('INTEGER',
                         generator.infer_value_type(-9223372036854775808))
        self.assertEqual('FLOAT',
                         generator.infer_value_type(9223372036854775808))
        self.assertEqual('FLOAT',
                         generator.infer_value_type(-9223372036854775809))

        # Quoted INTEGER
        self.assertEqual('QINTEGER', generator.infer_value_type('2'))
        self.assertEqual('QINTEGER', generator.infer_value_type('-1000'))
        self.assertEqual('QINTEGER',
                         generator.infer_value_type('9223372036854775807'))
        self.assertEqual('QINTEGER',
                         generator.infer_value_type('-9223372036854775808'))
        self.assertEqual('QFLOAT',
                         generator.infer_value_type('9223372036854775808'))
        self.assertEqual('QFLOAT',
                         generator.infer_value_type('-9223372036854775809'))

        # FLOAT
        self.assertEqual('FLOAT', generator.infer_value_type(2.0))

        # Quoted FLOAT
        self.assertEqual('QFLOAT', generator.infer_value_type('3.0'))
        self.assertEqual('QFLOAT', generator.infer_value_type('-5.4'))

        # RECORD
        self.assertEqual('RECORD', generator.infer_value_type({
            'a': 1,
            'b': 2
        }))

        # Special
        self.assertEqual('__null__', generator.infer_value_type(None))
        self.assertEqual('__empty_record__', generator.infer_value_type({}))
        self.assertEqual('__empty_array__', generator.infer_value_type([]))
        self.assertEqual('__array__', generator.infer_value_type([1, 2, 3]))

    def test_quoted_values_are_strings(self):
        generator = SchemaGenerator(quoted_values_are_strings=True)
        self.assertEqual('STRING', generator.infer_value_type('abcd'))

        self.assertEqual('INTEGER', generator.infer_value_type(1))
        self.assertEqual('STRING', generator.infer_value_type('1'))

        self.assertEqual('FLOAT', generator.infer_value_type(1.0))
        self.assertEqual('STRING', generator.infer_value_type('1.0'))

        self.assertEqual('BOOLEAN', generator.infer_value_type(True))
        self.assertEqual('STRING', generator.infer_value_type('True'))

    def test_infer_bigquery_type(self):
        generator = SchemaGenerator()

        self.assertEqual(('NULLABLE', 'TIME'),
                         generator.infer_bigquery_type('12:33:01'))
        self.assertEqual(('NULLABLE', 'DATE'),
                         generator.infer_bigquery_type('2018-02-08'))
        self.assertEqual(('NULLABLE', 'TIMESTAMP'),
                         generator.infer_bigquery_type('2018-02-08T12:34:56'))
        self.assertEqual(('NULLABLE', 'STRING'),
                         generator.infer_bigquery_type('abc'))
        self.assertEqual(('NULLABLE', 'BOOLEAN'),
                         generator.infer_bigquery_type(True))
        self.assertEqual(('NULLABLE', 'INTEGER'),
                         generator.infer_bigquery_type(1))
        self.assertEqual(('NULLABLE', 'FLOAT'),
                         generator.infer_bigquery_type(2.0))
        # yapf: disable
        self.assertEqual(('NULLABLE', 'RECORD'),
                         generator.infer_bigquery_type({ 'a': 1, 'b': 2 }))
        # yapf: enable
        self.assertEqual(('NULLABLE', '__null__'),
                         generator.infer_bigquery_type(None))
        self.assertEqual(('NULLABLE', '__empty_record__'),
                         generator.infer_bigquery_type({}))
        self.assertEqual(('NULLABLE', '__empty_array__'),
                         generator.infer_bigquery_type([]))

        self.assertEqual(('REPEATED', 'TIME'),
                         generator.infer_bigquery_type(
                             ['00:00:00', '00:00:01', '00:00:02']))
        self.assertEqual(
            ('REPEATED', 'DATE'),
            generator.infer_bigquery_type(['2018-02-08', '2018-02-09']))
        self.assertEqual(('REPEATED', 'TIMESTAMP'),
                         generator.infer_bigquery_type(
                             ['2018-02-08T12:34:56', '2018-02-08T12:34:56']))
        self.assertEqual(('REPEATED', 'STRING'),
                         generator.infer_bigquery_type(['a', 'b', 'c']))
        self.assertEqual(('REPEATED', 'BOOLEAN'),
                         generator.infer_bigquery_type([True, False, True]))
        self.assertEqual(('REPEATED', 'INTEGER'),
                         generator.infer_bigquery_type([1, 2, 3]))
        self.assertEqual(('REPEATED', 'FLOAT'),
                         generator.infer_bigquery_type([1.0, 2.0]))
        # yapf: disable
        self.assertEqual(('REPEATED', 'RECORD'),
                         generator.infer_bigquery_type([
                            { 'a': 1, 'b': 2 },
                            { 'c': 3 }]))
        # yapf: enable
        self.assertEqual(('REPEATED', '__empty_record__'),
                         generator.infer_bigquery_type([{}]))

        # Cannot have arrays of nulls (REPEATED __null__)
        with self.assertRaises(Exception):
            generator.infer_bigquery_type([None])

        # Cannot have arrays of empty arrays: (REPEATED __empty_array__)
        with self.assertRaises(Exception):
            generator.infer_bigquery_type([[], []])

        # Cannot have arrays of arrays: (REPEATED __array__)
        with self.assertRaises(Exception):
            generator.infer_bigquery_type([[1, 2], [2]])

    def test_infer_array_type(self):
        generator = SchemaGenerator()

        self.assertEqual('INTEGER', generator.infer_array_type([1, 1]))
        self.assertEqual('FLOAT', generator.infer_array_type([1.0, 2.0]))
        self.assertEqual('BOOLEAN', generator.infer_array_type([True, False]))
        self.assertEqual('STRING', generator.infer_array_type(['a', 'b']))
        self.assertEqual('DATE',
                         generator.infer_array_type(
                             ['2018-02-09', '2018-02-10']))
        self.assertEqual('TIME',
                         generator.infer_array_type(['10:44:00', '10:44:01']))
        self.assertEqual('TIMESTAMP',
                         generator.infer_array_type(
                             ['2018-02-09T11:00:00', '2018-02-10T11:00:01']))
        self.assertEqual('RECORD', generator.infer_array_type([{'a': 1}]))

        # Special types are supported
        self.assertEqual('__null__', generator.infer_array_type([None]))
        self.assertEqual('__empty_record__', generator.infer_array_type([{}]))
        self.assertEqual('__empty_array__', generator.infer_array_type([[]]))

        # Mixed TIME, DATE, TIMESTAMP converts to STRING
        self.assertEqual('STRING',
                         generator.infer_array_type(['2018-02-09',
                                                     '10:44:00']))
        self.assertEqual('STRING',
                         generator.infer_array_type(
                             ['2018-02-09T11:00:00', '10:44:00']))
        self.assertEqual('STRING',
                         generator.infer_array_type(
                             ['2018-02-09', '2018-02-09T10:44:00']))
        self.assertEqual('STRING',
                         generator.infer_array_type(['time', '10:44:00']))
        self.assertEqual('STRING',
                         generator.infer_array_type(['date', '2018-02-09']))
        self.assertEqual('STRING',
                         generator.infer_array_type(
                             ['timestamp', '2018-02-09T10:44:00']))

        # Mixed FLOAT and INTEGER returns FLOAT
        self.assertEqual('FLOAT', generator.infer_array_type([1, 2.0]))
        self.assertEqual('FLOAT', generator.infer_array_type([1.0, 2]))

        # Invalid mixed arrays
        self.assertIsNone(generator.infer_array_type([None, 1]))
        self.assertIsNone(generator.infer_array_type([1, True]))
        self.assertIsNone(generator.infer_array_type([1, '2018-02-09']))
        self.assertIsNone(generator.infer_array_type(['a', 1]))
        self.assertIsNone(generator.infer_array_type(['a', []]))
        self.assertIsNone(generator.infer_array_type(['a', {}]))
        self.assertIsNone(generator.infer_array_type([{}, []]))
        self.assertIsNone(generator.infer_array_type([{'a': 1}, []]))
        self.assertIsNone(generator.infer_array_type([{'a': 1}, [2]]))
        self.assertIsNone(generator.infer_array_type([{}, [2]]))

    def test_convert_type(self):
        # no conversion needed
        self.assertEqual('BOOLEAN', convert_type('BOOLEAN', 'BOOLEAN'))
        self.assertEqual('INTEGER', convert_type('INTEGER', 'INTEGER'))
        self.assertEqual('FLOAT', convert_type('FLOAT', 'FLOAT'))
        self.assertEqual('STRING', convert_type('STRING', 'STRING'))
        self.assertEqual('DATE', convert_type('DATE', 'DATE'))
        self.assertEqual('RECORD', convert_type('RECORD', 'RECORD'))

        # quoted and unquoted versions of the same type
        self.assertEqual('BOOLEAN', convert_type('BOOLEAN', 'QBOOLEAN'))
        self.assertEqual('BOOLEAN', convert_type('QBOOLEAN', 'BOOLEAN'))
        self.assertEqual('INTEGER', convert_type('INTEGER', 'QINTEGER'))
        self.assertEqual('INTEGER', convert_type('QINTEGER', 'INTEGER'))
        self.assertEqual('FLOAT', convert_type('FLOAT', 'QFLOAT'))
        self.assertEqual('FLOAT', convert_type('QFLOAT', 'FLOAT'))

        # [Q]INTEGER and [Q]FLOAT conversions
        self.assertEqual('FLOAT', convert_type('INTEGER', 'FLOAT'))
        self.assertEqual('FLOAT', convert_type('INTEGER', 'QFLOAT'))
        self.assertEqual('FLOAT', convert_type('QINTEGER', 'FLOAT'))
        self.assertEqual('QFLOAT', convert_type('QINTEGER', 'QFLOAT'))
        self.assertEqual('FLOAT', convert_type('FLOAT', 'INTEGER'))
        self.assertEqual('FLOAT', convert_type('FLOAT', 'QINTEGER'))
        self.assertEqual('FLOAT', convert_type('QFLOAT', 'INTEGER'))
        self.assertEqual('QFLOAT', convert_type('QFLOAT', 'QINTEGER'))

        # quoted and STRING conversions
        self.assertEqual('STRING', convert_type('STRING', 'QBOOLEAN'))
        self.assertEqual('STRING', convert_type('STRING', 'QINTEGER'))
        self.assertEqual('STRING', convert_type('STRING', 'QFLOAT'))
        self.assertEqual('STRING', convert_type('QBOOLEAN', 'STRING'))
        self.assertEqual('STRING', convert_type('QINTEGER', 'STRING'))
        self.assertEqual('STRING', convert_type('QFLOAT', 'STRING'))

        # quoted and DATE conversions
        self.assertEqual('STRING', convert_type('DATE', 'QBOOLEAN'))
        self.assertEqual('STRING', convert_type('DATE', 'QINTEGER'))
        self.assertEqual('STRING', convert_type('DATE', 'QFLOAT'))
        self.assertEqual('STRING', convert_type('QBOOLEAN', 'DATE'))
        self.assertEqual('STRING', convert_type('QINTEGER', 'DATE'))
        self.assertEqual('STRING', convert_type('QFLOAT', 'DATE'))

        # quoted and TIME conversions
        self.assertEqual('STRING', convert_type('TIME', 'QBOOLEAN'))
        self.assertEqual('STRING', convert_type('TIME', 'QINTEGER'))
        self.assertEqual('STRING', convert_type('TIME', 'QFLOAT'))
        self.assertEqual('STRING', convert_type('QBOOLEAN', 'TIME'))
        self.assertEqual('STRING', convert_type('QINTEGER', 'TIME'))
        self.assertEqual('STRING', convert_type('QFLOAT', 'TIME'))

        # quoted and TIMESTAMP conversions
        self.assertEqual('STRING', convert_type('TIMESTAMP', 'QBOOLEAN'))
        self.assertEqual('STRING', convert_type('TIMESTAMP', 'QINTEGER'))
        self.assertEqual('STRING', convert_type('TIMESTAMP', 'QFLOAT'))
        self.assertEqual('STRING', convert_type('QBOOLEAN', 'TIMESTAMP'))
        self.assertEqual('STRING', convert_type('QINTEGER', 'TIMESTAMP'))
        self.assertEqual('STRING', convert_type('QFLOAT', 'TIMESTAMP'))

        # DATE, TIME, and TIMESTAMP conversions
        self.assertEqual('STRING', convert_type('DATE', 'TIME'))
        self.assertEqual('STRING', convert_type('DATE', 'TIMESTAMP'))
        self.assertEqual('STRING', convert_type('DATE', 'STRING'))
        self.assertEqual('STRING', convert_type('TIME', 'TIMESTAMP'))
        self.assertEqual('STRING', convert_type('TIME', 'STRING'))
        self.assertEqual('STRING', convert_type('TIMESTAMP', 'STRING'))

        # no conversion possible
        self.assertEqual(None, convert_type('INTEGER', 'BOOLEAN'))
        self.assertEqual(None, convert_type('QINTEGER', 'BOOLEAN'))
        self.assertEqual(None, convert_type('INTEGER', 'QBOOLEAN'))
        self.assertEqual(None, convert_type('FLOAT', 'BOOLEAN'))
        self.assertEqual(None, convert_type('QFLOAT', 'BOOLEAN'))
        self.assertEqual(None, convert_type('FLOAT', 'QBOOLEAN'))
        self.assertEqual(None, convert_type('FLOAT', 'STRING'))
        self.assertEqual(None, convert_type('STRING', 'BOOLEAN'))
        self.assertEqual(None, convert_type('BOOLEAN', 'DATE'))
        self.assertEqual(None, convert_type('BOOLEAN', 'RECORD'))

    def test_is_string_type(self):
        self.assertTrue(is_string_type('STRING'))
        self.assertTrue(is_string_type('TIMESTAMP'))
        self.assertTrue(is_string_type('DATE'))
        self.assertTrue(is_string_type('TIME'))

    def test_sort_schema(self):
        # yapf: disable
        unsorted = [{
            "mode": "REPEATED",
            "name": "a",
            "type": "STRING"
        }, {
            "fields": [{
                "mode": "NULLABLE",
                "name": "__unknown__",
                "type": "STRING"
            }],
            "mode": "NULLABLE",
            "name": "m",
            "type": "RECORD"
        }, {
            "mode": "NULLABLE",
            "name": "s",
            "type": "STRING"
        }]

        expected = [
            OrderedDict([
                ("mode", "REPEATED"),
                ("name", "a"),
                ("type", "STRING")]),
            OrderedDict([
                ("fields", [
                    OrderedDict([
                        ("mode", "NULLABLE"),
                        ("name", "__unknown__"),
                        ("type", "STRING")])
                ]),
                ("mode", "NULLABLE"),
                ("name", "m"),
                ("type", "RECORD")]),
            OrderedDict([
                ("mode", "NULLABLE"),
                ("name", "s"),
                ("type", "STRING")])
        ]
        # yapf: enable
        self.assertEqual(expected, sort_schema(unsorted))


class TestFromDataFile(unittest.TestCase):
    """Read the test case data from TESTDATA_FILE and verify that the expected
    schema matches the one produced by SchemaGenerator.deduce_schema(). Multiple
    test cases are stored in TESTDATA_FILE. The data_reader.py module knows how
    to parse that file.
    """

    TESTDATA_FILE = 'testdata.txt'

    def test(self):
        # Find the TESTDATA_FILE in the same directory as this script file.
        dir_path = os.path.dirname(os.path.realpath(__file__))
        testdata_path = os.path.join(dir_path, self.TESTDATA_FILE)

        # Read each test case (data chunk) and verify the expected schema.
        with open(testdata_path) as testdatafile:
            data_reader = DataReader(testdatafile)
            chunk_count = 0
            while True:
                chunk = data_reader.read_chunk()
                if chunk is None:
                    break
                chunk_count += 1
                self.verify_data_chunk(chunk_count, chunk)

    def verify_data_chunk(self, chunk_count, chunk):
        data_flags = chunk['data_flags']
        keep_nulls = ('keep_nulls' in data_flags)
        quoted_values_are_strings = ('quoted_values_are_strings' in data_flags)
        input_format = 'csv' if ('csv' in data_flags) else 'json'
        records = chunk['records']
        expected_errors = chunk['errors']
        expected_error_map = chunk['error_map']
        expected_schema = chunk['schema']

        print("Test chunk %s: First record: %s" % (chunk_count, records[0]))

        # Generate schema.
        generator = SchemaGenerator(
            keep_nulls=keep_nulls,
            quoted_values_are_strings=quoted_values_are_strings,
            input_format=input_format)
        schema_map, error_logs = generator.deduce_schema(records)
        schema = generator.flatten_schema(schema_map)

        # Check the schema
        expected = sort_schema(json.loads(expected_schema))
        self.assertEqual(expected, schema)

        # Check the error messages
        self.assertEqual(len(expected_errors), len(error_logs))
        self.assert_error_messages(expected_error_map, error_logs)

    def assert_error_messages(self, expected_error_map, error_logs):
        # Convert the list of errors into a map
        error_map = {}
        for error in error_logs:
            line_number = error['line']
            messages = error_map.get(line_number)
            if messages is None:
                messages = []
                error_map[line_number] = messages
            messages.append(error['msg'])

        # Check that each entry in 'error_logs' is expected. Currently checks
        # only that the number of errors matches on a per line basis.
        # TODO: Look deeper and verify that the error message strings match as
        # well.
        for line_number, messages in sorted(error_map.items()):
            expected_entry = expected_error_map.get(line_number)
            self.assertIsNotNone(expected_entry)
            expected_messages = expected_entry['msgs']
            self.assertEqual(len(expected_messages), len(messages))


if __name__ == '__main__':
    unittest.main()
