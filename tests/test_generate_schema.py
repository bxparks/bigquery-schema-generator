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
            SchemaGenerator.TIMESTAMP_MATCHER.match('2017-05-22 12:33:01.123456'))
        self.assertTrue(
            SchemaGenerator.TIMESTAMP_MATCHER.match('2017-05-22T12:33:01Z'))
        self.assertTrue(
            SchemaGenerator.TIMESTAMP_MATCHER.match('2017-05-22 12:33:01-7:00'))
        self.assertTrue(
            SchemaGenerator.TIMESTAMP_MATCHER.match('2017-05-22 12:33:01-07:30'))
        self.assertTrue(
            SchemaGenerator.TIMESTAMP_MATCHER.match('2017-05-22T12:33:01-7'))
        self.assertTrue(
            SchemaGenerator.TIMESTAMP_MATCHER.match('2017-05-22 12:33:01+7:00'))
        self.assertTrue(
            SchemaGenerator.TIMESTAMP_MATCHER.match('2017-5-2T1:3:1'))

    def test_timestamp_matcher_invalid(self):
        self.assertFalse(
            SchemaGenerator.TIMESTAMP_MATCHER.match('2017-05-22 12:33:01-123:445'))
        self.assertFalse(
            SchemaGenerator.TIMESTAMP_MATCHER.match('2017-05-22 12:33:01-0700'))
        self.assertFalse(
            SchemaGenerator.TIMESTAMP_MATCHER.match('2017-05-22 12:33:01.1234567'))
        self.assertFalse(
            SchemaGenerator.TIMESTAMP_MATCHER.match('2017-05-22T12:33'))
        self.assertFalse(
            SchemaGenerator.TIMESTAMP_MATCHER.match('2017-05-22A12:33:00'))
        self.assertFalse(
            SchemaGenerator.TIMESTAMP_MATCHER.match('2017-05-22T12:33:01X07:00'))
        self.assertFalse(
            SchemaGenerator.TIMESTAMP_MATCHER.match('2017-5-2A2:3:0'))
        self.assertFalse(
            SchemaGenerator.TIMESTAMP_MATCHER.match('17-05-22T12:33:01'))

    def test_sort_schema(self):
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
            OrderedDict([("mode", "REPEATED"), ("name", "a"),
                         ("type", "STRING")]),
            OrderedDict([("fields", [
                OrderedDict([("mode", "NULLABLE"), ("name", "__unknown__"),
                             ("type", "STRING")])
            ]), ("mode", "NULLABLE"), ("name", "m"), ("type", "RECORD")]),
            OrderedDict([("mode", "NULLABLE"), ("name", "s"),
                         ("type", "STRING")])
        ]
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
        records = chunk['records']
        expected_errors = chunk['errors']
        expected_error_map = chunk['error_map']
        expected_schema = chunk['schema']

        print("Test chunk %s: First record: %s" % (chunk_count, records[0]))

        # Generate schema.
        generator = SchemaGenerator(keep_nulls)
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

        # Check that each entry in 'error_logs' is expected.
        # Currently checks only that the number of errors matches on a per line basis.
        # TODO: Look deeper and verify that the error message strings match as well.
        for line_number, messages in sorted(error_map.items()):
            expected_entry = expected_error_map.get(line_number)
            self.assertIsNotNone(expected_entry)
            expected_messages = expected_entry['msgs']
            self.assertEqual(len(expected_messages), len(messages))


if __name__ == '__main__':
    unittest.main()
