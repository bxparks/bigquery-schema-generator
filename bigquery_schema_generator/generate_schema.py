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
"""
Generate the BigQuery schema of the data file given on the standard input.
Unlike the BigQuery importer which uses only the first 100 records, this script
uses all available records in the data file.

Usage: generate_schema.py [-h] [flags ...] < file.data.json > file.schema.json

* file.data.json is a newline-delimited JSON data file, one JSON object per
  line.
* file.schema.json is the schema definition of the table.
"""

from collections import OrderedDict
import argparse
import json
import logging
import re
import sys


class SchemaGenerator:
    """Reads in a list of data records and deduces the BigQuery schema
    from the records.

    Usage:
        generator = SchemaGenerator()
        schema_map, error_logs = generator.deduce_schema(records)
        schema = generator.flatten_schema(schema_map)
    """

    # Detect a TIMESTAMP field of the form
    # YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]][time zone]
    TIMESTAMP_MATCHER = re.compile(
        r'^\d{4}-\d{1,2}-\d{1,2}[T ]\d{1,2}:\d{1,2}:\d{1,2}(\.\d{1,6})?'
        r'(([+-]\d{1,2}(:\d{1,2})?)|Z)?$')

    # Detect a DATE field of the form YYYY-[M]M-[D]D.
    DATE_MATCHER = re.compile(
        r'^\d{4}-(?:[1-9]|0[1-9]|1[012])-(?:[1-9]|0[1-9]|[12][0-9]|3[01])$')

    # Detect a TIME field of the form [H]H:[M]M:[S]S[.DDDDDD]
    TIME_MATCHER = re.compile(r'^\d{1,2}:\d{1,2}:\d{1,2}(\.\d{1,6})?$')

    INTEGER_MATCHER = re.compile(r'^[-]?\d+$')
    FLOAT_MATCHER = re.compile(r'^[-]?\d+\.\d+$')

    def __init__(self,
                 keep_nulls=False,
                 debugging_interval=1000,
                 debugging_map=False):
        self.debugging_interval = debugging_interval
        self.keep_nulls = keep_nulls
        self.debugging_map = debugging_map
        self.line_number = 0
        self.error_logs = []

    def log_error(self, msg):
        self.error_logs.append({'line': self.line_number, 'msg': msg})

    def deduce_schema(self, file):
        """Loop through each newlined-delimitered JSON line of 'file' and
        deduce the BigQuery schema. The schema is returned as a recursive map
        that contains both the database schema and some additional metadata
        about each entry. It has the following form:

          schema_map := {
            key: schema_entry
          }

        The 'key' is the name of the table column.

          schema_entry := {
            'status': 'hard | soft',
            'info': {
              'name': key,
              'type': 'STRING | TIMESTAMP | DATE | TIME
                      | FLOAT | INTEGER | BOOLEAN | RECORD'
              'mode': 'NULLABLE | REPEATED',
              'fields': schema_map
            }
          }

        The status of 'hard' or 'soft' refers the reliability of the type
        inference that we made for a particular JSON element. If the element
        value is a 'null', we assume that it's a 'soft' STRING. If the element
        value is an empty [] or {}, we assume that the element type is a 'soft'
        REPEATED STRING or a 'soft' NULLABLE RECORD, respectively. When we come
        across a subsequent entry with non-null or non-empty values, we are
        able infer the type definitively, and we change the status to 'hard'.
        The status can transition from 'soft' to 'hard' but not the reverse.

        The function returns a tuple of 2 things:
          * an OrderedDict which is sorted by the 'key' of the column name
          * a list of possible errors containing a map of 'line' and 'msg'
        """
        schema_map = OrderedDict()
        for line in file:
            self.line_number += 1
            if self.line_number % self.debugging_interval == 0:
                logging.info("Processing line %s", self.line_number)
            # TODO: Add support for other input formats, like CSV?
            json_object = json.loads(line)

            # Deduce the schema from this given data record.
            try:
                if isinstance(json_object, dict):
                    self.deduce_schema_for_line(json_object, schema_map)
                else:
                    self.log_error(
                        'Top level record must be an Object but was a %s' %
                        type(json_object))
            except Exception as e:
                self.log_error(str(e))
        logging.info("Processed %s lines", self.line_number)
        return schema_map, self.error_logs

    def deduce_schema_for_line(self, json_object, schema_map):
        """Figures out the BigQuery schema for the given 'json_object' and
        updates 'schema_map' with the latest info. A 'schema_map' entry of type
        'soft' is a provisional entry that can be overwritten by a subsequent
        'soft' or 'hard' entry. If both the old and new have the same type,
        then they must be compatible.
        """
        for key, value in json_object.items():
            schema_entry = schema_map.get(key)
            try:
                new_schema_entry = self.get_schema_entry(key, value)
                merged_schema_entry = self.merge_schema_entry(
                    schema_entry, new_schema_entry)
            except Exception as e:
                self.log_error(str(e))
                continue
            schema_map[key] = merged_schema_entry

    def merge_schema_entry(self, old_schema_entry, new_schema_entry):
        """Merges the 'new_schema_entry' into the 'old_schema_entry' and return
        a merged schema entry. Recursivesly merges in sub-fields as well.

        Returns the merged schema_entry. This method assumes that the
        'old_schema_entry' is no longer used by the calling code, so it often
        modifies the old_schema_entry in-place to generate the merged
        schema_entry.
        """
        if not old_schema_entry:
            return new_schema_entry

        old_status = old_schema_entry['status']
        new_status = new_schema_entry['status']

        # new 'soft' does not clobber old 'hard'
        if old_status == 'hard' and new_status == 'soft':
            return old_schema_entry

        # new 'hard' clobbers old 'soft'
        if old_status == 'soft' and new_status == 'hard':
            return new_schema_entry

        # Verify that it's soft->soft or hard->hard
        if old_status != new_status:
            raise Exception(
                ('Unexpected schema_entry type, this should never happen: '
                 'old (%s); new (%s)') % (old_status, new_status))

        old_info = old_schema_entry['info']
        old_name = old_info['name']
        old_type = old_info['type']
        old_mode = old_info['mode']
        new_info = new_schema_entry['info']
        new_name = new_info['name']
        new_type = new_info['type']
        new_mode = new_info['mode']

        # Defensive check, names should always be the same.
        if old_name != new_name:
            raise Exception(
                'old_name (%s) != new_name(%s), should never happen' %
                (old_name, new_name))

        # Recursively merge in the subfields of a RECORD, allowing
        # NULLABLE to become REPEATED (because 'bq load' allows it).
        if old_type == 'RECORD' and new_type == 'RECORD':
            # Allow NULLABLE RECORD to be upgraded to REPEATED RECORD because
            # 'bq load' allows it.
            if old_mode == 'NULLABLE' and new_mode == 'REPEATED':
                old_info['mode'] = 'REPEATED'
                self.log_error(
                    ('Converting schema for "%s" from NULLABLE RECORD '
                     'into REPEATED RECORD') % old_name)
            elif old_mode == 'REPEATED' and new_mode == 'NULLABLE':
                # TODO: Maybe remove this warning output. It was helpful during
                # development, but maybe it's just natural.
                self.log_error(
                    'Leaving schema for "%s" as REPEATED RECORD' % old_name)

            # RECORD type needs a recursive merging of sub-fields. We merge into
            # the 'old_schema_entry' which assumes that the 'old_schema_entry'
            # can be modified in situ.
            old_fields = old_info['fields']
            new_fields = new_info['fields']
            for key, new_entry in new_fields.items():
                old_entry = old_fields.get(key)
                merged_entry = self.merge_schema_entry(old_entry, new_entry)
                old_fields[key] = merged_entry
            return old_schema_entry

        # For all other types, the old_mode must be the same as the new_mode. It
        # might seem reasonable to allow a NULLABLE {primitive_type} to be
        # upgraded to a REPEATED {primitive_type}, but currently 'bq load' does
        # not support that so we must also follow that rule.
        if old_mode != new_mode:
            raise Exception(('Mismatched mode for non-RECORD: '
                             'old=(%s,%s,%s,%s); new=(%s,%s,%s,%s)') %
                            (old_status, old_name, old_mode, old_type,
                             new_status, new_name, new_mode, new_type))

        candidate_type = convert_type(old_type, new_type)
        if not candidate_type:
            raise Exception(
                'Mismatched type: old=(%s,%s,%s,%s); new=(%s,%s,%s,%s)' %
                (old_status, old_name, old_mode, old_type, new_status,
                 new_name, new_mode, new_type))

        new_info['type'] = candidate_type
        return new_schema_entry

    def get_schema_entry(self, key, value):
        """Determines the 'schema_entry' of the JSON (key, value) pair. Calls
        deduce_schema_for_line() recursively if the value is another JSON
        object, instead of a primitive.
        """
        value_mode, value_type = self.infer_bigquery_type(value)

        # yapf: disable
        if value_type == 'RECORD':
            # recursively figure out the RECORD
            fields = OrderedDict()
            if value_mode == 'NULLABLE':
                self.deduce_schema_for_line(value, fields)
            else:
                for val in value:
                    self.deduce_schema_for_line(val, fields)
            schema_entry = OrderedDict([('status', 'hard'),
                                        ('info', OrderedDict([
                                            ('fields', fields),
                                            ('mode', value_mode),
                                            ('name', key),
                                            ('type', value_type),
                                        ]))])
        elif value_type == '__null__':
            schema_entry = OrderedDict([('status', 'soft'),
                                        ('info', OrderedDict([
                                            ('mode', 'NULLABLE'),
                                            ('name', key),
                                            ('type', 'STRING'),
                                        ]))])
        elif value_type == '__empty_array__':
            schema_entry = OrderedDict([('status', 'soft'),
                                        ('info', OrderedDict([
                                            ('mode', 'REPEATED'),
                                            ('name', key),
                                            ('type', 'STRING'),
                                        ]))])
        elif value_type == '__empty_record__':
            schema_entry = OrderedDict([('status', 'soft'),
                                        ('info', OrderedDict([
                                            ('fields', OrderedDict()),
                                            ('mode', value_mode),
                                            ('name', key),
                                            ('type', 'RECORD'),
                                        ]))])
        else:
            schema_entry = OrderedDict([('status', 'hard'),
                                        ('info', OrderedDict([
                                            ('mode', value_mode),
                                            ('name', key),
                                            ('type', value_type),
                                        ]))])
        # yapf: enable
        return schema_entry

    def infer_bigquery_type(self, node_value):
        """Determines the BigQuery (mode, type) tuple of the right hand side of
        the JSON value.
        """
        node_type = self.infer_value_type(node_value)
        if node_type != '__array__':
            return ('NULLABLE', node_type)

        # Do further process for arrays.

        # Verify that the array elements are identical types.
        array_type = self.infer_array_type(node_value)
        if not array_type:
            raise Exception(
                "All array elements must be the same compatible type: %s" %
                node_value)

        # Disallow array of special types (with '__' not supported).
        # EXCEPTION: allow (REPEATED __empty_record) ([{}]) because it is
        # allowed by 'bq load'.
        if '__' in array_type and array_type != '__empty_record__':
            raise Exception('Unsupported array element type: %s' % array_type)

        return ('REPEATED', array_type)

    def infer_value_type(self, value):
        """Infers the type of the given JSON value.
        If the value is '{}', the type '__empty_record__' is returned.
        If the value is '[]', the type '__empty_array__' is returned.
        If the value is 'null' (python None), the type '__null__' is returned.
        Note that primitive types do not have the string '__' in the returned
        value, which is a useful marker.
        """
        if isinstance(value, str):
            if self.TIMESTAMP_MATCHER.match(value):
                return 'TIMESTAMP'
            elif self.DATE_MATCHER.match(value):
                return 'DATE'
            elif self.TIME_MATCHER.match(value):
                return 'TIME'
            elif self.INTEGER_MATCHER.match(value):
                return 'QINTEGER'  # quoted integer
            elif self.FLOAT_MATCHER.match(value):
                return 'QFLOAT'  # quoted float
            elif value.lower() in ['true', 'false']:
                return 'QBOOLEAN'  # quoted boolean
            else:
                return 'STRING'
        # Python 'bool' is a subclass of 'int' so we must check it first
        elif isinstance(value, bool):
            return 'BOOLEAN'
        elif isinstance(value, int):
            return 'INTEGER'
        elif isinstance(value, float):
            return 'FLOAT'
        elif value is None:
            return '__null__'
        elif isinstance(value, dict):
            if value:
                return 'RECORD'
            else:
                return '__empty_record__'
        elif isinstance(value, list):
            if value:
                return '__array__'
            else:
                return '__empty_array__'
        else:
            raise Exception('Unsupported node type: %s' % type(value))

    def infer_array_type(self, elements):
        """Return the type of all the array elements, accounting for the same
        conversions supported by infer_bigquery_type(). In other words:

        * arrays of mixed INTEGER and FLOAT: FLOAT
        * arrays of mixed STRING, TIME, DATE, TIMESTAMP: STRING

        Returns None if the array is not homogeneous.
        """
        if not elements:
            raise Exception('Empty array, should never happen here.')

        candidate_type = ''
        for e in elements:
            etype = self.infer_value_type(e)
            if candidate_type == '':
                candidate_type = etype
                continue
            candidate_type = convert_type(candidate_type, etype)
            if not candidate_type:
                return None

        return candidate_type

    def flatten_schema(self, schema_map):
        """Converts the bookkeeping 'schema_map' into the format recognized by
        BigQuery using the same sorting order as BigQuery.
        """
        return flatten_schema_map(schema_map, self.keep_nulls)

    def run(self):
        """Read the data records from the STDIN and print out the BigQuery
        schema on the STDOUT. The error logs are printed on the STDERR.
        """
        # TODO: BigQuery is case-insensitive with regards to the 'name' of the
        # field. Verify that the 'name' is unique regardless of the case.

        schema_map, error_logs = self.deduce_schema(sys.stdin)

        for error in error_logs:
            logging.info("Problem on line %s: %s", error['line'], error['msg'])

        if self.debugging_map:
            json.dump(schema_map, sys.stdout, indent=2)
            print()
        else:
            schema = self.flatten_schema(schema_map)
            json.dump(schema, sys.stdout, indent=2)
            print()


def convert_type(atype, btype):
    """Return the compatible type between 'atype' and 'btype'. Return 'None'
    if there is no compatible type. Type conversions (in order of precedence)
    are:

    * type + type => type
    * [Q]BOOLEAN + [Q]BOOLEAN => BOOLEAN
    * [Q]INTEGER + [Q]INTEGER => INTEGER
    * [Q]FLOAT + [Q]FLOAT => FLOAT
    * QINTEGER + QFLOAT = QFLOAT
    * QFLOAT + QINTEGER = QFLOAT
    * [Q]INTEGER + [Q]FLOAT => FLOAT (except QINTEGER + QFLOAT)
    * [Q]FLOAT + [Q]INTEGER => FLOAT (except QFLOAT + QINTEGER)
    * (DATE, TIME, TIMESTAMP, QBOOLEAN, QINTEGER, QFLOAT, STRING) +
        (DATE, TIME, TIMESTAMP, QBOOLEAN, QINTEGER, QFLOAT, STRING) => STRING
    """
    # type + type => type
    if atype == btype:
        return atype

    # [Q]BOOLEAN + [Q]BOOLEAN => BOOLEAN
    if atype == 'BOOLEAN' and btype == 'QBOOLEAN':
        return 'BOOLEAN'
    if atype == 'QBOOLEAN' and btype == 'BOOLEAN':
        return 'BOOLEAN'

    # [Q]INTEGER + [Q]INTEGER => INTEGER
    if atype == 'QINTEGER' and btype == 'INTEGER':
        return 'INTEGER'
    if atype == 'INTEGER' and btype == 'QINTEGER':
        return 'INTEGER'

    # [Q]FLOAT + [Q]FLOAT => FLOAT
    if atype == 'QFLOAT' and btype == 'FLOAT':
        return 'FLOAT'
    if atype == 'FLOAT' and btype == 'QFLOAT':
        return 'FLOAT'

    # QINTEGER + QFLOAT => QFLOAT
    if atype == 'QINTEGER' and btype == 'QFLOAT':
        return 'QFLOAT'

    # QFLOAT + QINTEGER => QFLOAT
    if atype == 'QFLOAT' and btype == 'QINTEGER':
        return 'QFLOAT'

    # [Q]INTEGER + [Q]FLOAT => FLOAT (except QINTEGER + QFLOAT => QFLOAT)
    if atype == 'INTEGER' and btype == 'FLOAT':
        return 'FLOAT'
    if atype == 'INTEGER' and btype == 'QFLOAT':
        return 'FLOAT'
    if atype == 'QINTEGER' and btype == 'FLOAT':
        return 'FLOAT'

    # [Q]FLOAT + [Q]INTEGER => FLOAT (except # QFLOAT + QINTEGER => QFLOAT)
    if atype == 'FLOAT' and btype == 'INTEGER':
        return 'FLOAT'
    if atype == 'FLOAT' and btype == 'QINTEGER':
        return 'FLOAT'
    if atype == 'QFLOAT' and btype == 'INTEGER':
        return 'FLOAT'

    # All remaining combination of:
    # (DATE, TIME, TIMESTAMP, QBOOLEAN, QINTEGER, QFLOAT, STRING) +
    #   (DATE, TIME, TIMESTAMP, QBOOLEAN, QINTEGER, QFLOAT, STRING) => STRING
    if is_string_type(atype) and is_string_type(btype):
        return 'STRING'

    return None


def is_string_type(thetype):
    """Returns true if the type is one of: STRING, TIMESTAMP, DATE, or
    TIME."""
    return thetype in [
        'STRING', 'TIMESTAMP', 'DATE', 'TIME', 'QINTEGER', 'QFLOAT', 'QBOOLEAN'
    ]


def flatten_schema_map(schema_map, keep_nulls=False):
    """Converts the 'schema_map' into a more flatten version which is
    compatible with BigQuery schema. If 'keep_nulls' is True then the resulting
    schema contains entries for nulls, empty arrays or empty records in the
    data.
    """
    if not isinstance(schema_map, dict):
        raise Exception(
            "Unexpected type '%s' for schema_map" % type(schema_map))

    # Build the BigQuery schema from the internal 'schema_map'.
    schema = []
    for name, meta in sorted(schema_map.items()):
        status = meta['status']
        info = meta['info']

        # Schema entries with a status of 'soft' are caused by 'null' or
        # empty fields. Don't print those out if the 'keep_nulls' flag is
        # False.
        if status == 'soft' and not keep_nulls:
            continue

        # Copy the 'info' dictionary ordered alphabetically to match the output
        # of BigQuery.
        new_info = OrderedDict()
        for key, value in sorted(info.items()):
            if key == 'fields':
                if not value:
                    # Create a dummy attribute for an empty RECORD to make
                    # the BigQuery importer happy.
                    new_value = [
                        OrderedDict([
                            ('mode', 'NULLABLE'),
                            ('name', '__unknown__'),
                            ('type', 'STRING'),
                        ])
                    ]
                else:
                    # Recursively flatten the sub-fields of a RECORD entry.
                    new_value = flatten_schema_map(value, keep_nulls)
            elif key == 'type' and value in ['QINTEGER', 'QFLOAT', 'QBOOLEAN']:
                new_value = value[1:]
            else:
                new_value = value
            new_info[key] = new_value
        schema.append(new_info)
    return schema


def sort_schema(schema):
    """Sort the given BigQuery 'schema' and return a version that uses
    'OrderedDict' with the same sorting rules as BigQuery.
    """
    if not isinstance(schema, list):
        raise Exception('Unsupported type %s' % type(schema))

    old_sorted = sorted(schema, key=lambda x: x['name'])
    new_sorted = []
    for old_elem in old_sorted:
        if not isinstance(old_elem, dict):
            raise Exception('Unsupported type %s' % type(schema))

        new_elem = OrderedDict()
        for key, value in sorted(old_elem.items()):
            if key == 'fields':
                new_elem[key] = sort_schema(value)
            else:
                new_elem[key] = value
        new_sorted.append(new_elem)
    return new_sorted


def main():
    # Configure command line flags.
    parser = argparse.ArgumentParser(description='Generate BigQuery schema.')
    parser.add_argument(
        '--keep_nulls',
        help='Print the schema for null values, empty arrays or empty records.',
        action="store_true")
    parser.add_argument(
        '--debugging_interval',
        help='Number of lines between heartbeat debugging messages.',
        type=int,
        default=1000)
    parser.add_argument(
        '--debugging_map',
        help=
        'Print the metadata schema_map instead of the schema for debugging',
        action="store_true")
    args = parser.parse_args()

    # Configure logging.
    logging.basicConfig(level=logging.INFO)

    generator = SchemaGenerator(args.keep_nulls, args.debugging_interval,
                                args.debugging_map)
    generator.run()


if __name__ == '__main__':
    main()
