#!/usr/bin/env python3
#
# Copyright 2018 Brian T. Park
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
"""
Anonymize a newline-delimited JSON data file. By default, both the keys and
values are anonymized to prevent any chance of extracting the original source of
the data file. To preserve the keys, use the --preserve_keys flag.

The purpose of this script is to anonymize large JSON data files for
benchmarking purposes, while preserving the structure of the JSON data file so
that the BigQuery schema of the anonymized data file is structurally identical
to the original data file (modulo the names of the keys). If the --preserve_keys
flag is used, then the BigQuery schema file from the anonymized data should
be identical to the schema file from the original dataset.

Usage: anonymize.py [-h] [flags ...] < file.data.json > file.anon.data.json
"""

import argparse
import json
import logging
import sys
from collections import OrderedDict
from bigquery_schema_generator.generate_schema import SchemaGenerator


class Anonymizer:
    """Anonymize both the key and value of a newline-delimited JSON file.
    The anon_map is used to keep track of the (key -> anon_key) mapping.

    anon_map := {
        key: anon_entry,
        ...
    }
    anon_entry := {
        'anon_key': anonymous_key,
        'anon_map': anon_map,
        'next_anon_key': next_key
    }
    """

    def __init__(self, debugging_interval=1000, preserve_keys=False):
        self.debugging_interval = debugging_interval
        self.preserve_keys = preserve_keys
        self.line_number = 0

    def log_error(self, msg):
        logging.error('line: %s; msg: %s', self.line_number, msg)

    def anonymize_file(self, file):
        """Anonymous the JSON data record one line at a time from the
        given file-like object.
        """
        anon_entry = {}
        for line in file:
            self.line_number += 1
            if self.line_number % self.debugging_interval == 0:
                logging.info("Processing line %s", self.line_number)
            json_object = json.loads(line)

            if not isinstance(json_object, dict):
                self.log_error(
                    'Top level record must be an Object but was a %s' %
                    type(json_object))
                continue

            try:
                anon_dict = self.anonymize_dict(json_object, anon_entry)
            except Exception as e:
                self.log_error(str(e))
            json.dump(anon_dict, sys.stdout)
            print()
        logging.info("Processed %s lines", self.line_number)

    def anonymize_dict(self, json_dict, anon_entry):
        """Recursively anonymize the JSON dictionary object, replacing the key
        and the value with their anonymized versions. Returns the 'anon_dict'
        with the 'anon_entry' updated.
        """
        # Add some bookkeeping variables to 'anon_entry' for a dict.
        anon_map = anon_entry.get('anon_map')
        if not anon_map:
            anon_map = {}
            anon_entry['anon_map'] = anon_map
        next_anon_key = anon_entry.get('next_anon_key')
        if not next_anon_key:
            next_anon_key = 'a'

        anon_dict = OrderedDict()

        for key, value in json_dict.items():
            child_anon_entry = anon_map.get(key)
            if not child_anon_entry:
                child_anon_entry = {}
                if self.preserve_keys:
                    child_anon_entry['anon_key'] = key
                else:
                    # Pad the anonymous key to preserve length
                    padding = max(0, len(key) - len(next_anon_key))
                    child_anon_entry['anon_key'] = \
                        next_anon_key + ('.' * padding)
                next_anon_key = increment_anon_key(next_anon_key)
                anon_map[key] = child_anon_entry

            if isinstance(value, dict):
                value = self.anonymize_dict(value, child_anon_entry)
            elif isinstance(value, list):
                value = self.anonymize_list(value, child_anon_entry)
            else:
                value = self.anonymize_value(value)

            child_anon_key = child_anon_entry['anon_key']
            anon_dict[child_anon_key] = value

        # Update the next_anon_key so that anon_entry can be reused
        # for multiple dicts, e.g. in a list or lines in a file.
        anon_entry['next_anon_key'] = next_anon_key

        return anon_dict

    def anonymize_list(self, json_list, anon_entry):
        """Anonymize the given list, calling anonymize_dict() recursively if
        necessary.
        """
        anon_list = []
        for item in json_list:
            if isinstance(item, list):
                item = self.anonymize_list(item, anon_entry)
            elif isinstance(item, dict):
                item = self.anonymize_dict(item, anon_entry)
            else:
                item = self.anonymize_value(item)
            anon_list.append(item)
        return anon_list

    def anonymize_value(self, value):
        """Anonymize the value. A string is replaced with a string of an equal
        number of '*' characters. DATE, TIME and TIMESTAMP values are replaced
        with a fixed versions of those. An integer is replaced with just a '1'.
        A float is replaced with just a '2.0'. A boolean is replaced with just a
        'True'.
        """
        if isinstance(value, str):
            if SchemaGenerator.TIMESTAMP_MATCHER.match(value):
                return '2018-07-17T09:05:00-07:00'
            elif SchemaGenerator.DATE_MATCHER.match(value):
                return '2018-07-17'
            elif SchemaGenerator.TIME_MATCHER.match(value):
                return '09:05:00'
            else:
                # Pad the anonymous string to the same length as the original
                return '*' * len(value)
        elif isinstance(value, bool):
            return True
        elif isinstance(value, int):
            return 1
        elif isinstance(value, float):
            return 2.0
        elif value is None:
            return None
        else:
            raise Exception('Unsupported node type: %s' % type(value))

    def run(self):
        self.anonymize_file(sys.stdin)


def increment_anon_key(key):
    """Increment the key in base-26 to the next key. The sequence looks like
    this: [a, ..., z, ba, bb, ..., bz, ..., baa, ...].

    Note that this is not the the Excel column label sequence. Base-26 is easier
    to generate and it's good enough for this use-case. Also note that this
    method performs NO validation, it assumes that all the digits are in the
    [a-z] range.
    """
    reversed_key = key[::-1]
    new_key = ''
    carry = 1
    for c in reversed_key:
        if carry == 0:
            new_key += c
            continue

        new_ord = ord(c) + carry
        if new_ord == ord('z') + 1:
            newc = 'a'
            carry = 1
        else:
            newc = chr(new_ord)
            carry = 0
        new_key += newc
    if carry == 1:
        new_key += 'b'
    return new_key[::-1]


def main():
    # Configure command line flags.
    parser = argparse.ArgumentParser(
        description='Anonymize newline-delimited JSON data file.')
    parser.add_argument(
        '--preserve_keys',
        help='Preserve the keys, do not anonymize them',
        action="store_true")
    parser.add_argument(
        '--debugging_interval',
        help='Number of lines between heartbeat debugging messages.',
        type=int,
        default=1000)
    args = parser.parse_args()

    # Configure logging.
    logging.basicConfig(level=logging.INFO)

    anonymizer = Anonymizer(args.debugging_interval, args.preserve_keys)
    anonymizer.run()


if __name__ == '__main__':
    main()
