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
Parses the 'testdata.txt' date file used by the 'test_generate_schema.py'
program.

Usage:
    ./data_reader.py < testdata.txt
"""

import sys


class DataReader:
    """Reads each test data section from the given file-like object (e.g.
    STDIN). The test data file has the following structure:

        # comment
        DATA [flags]
        json_records
        ...
        ERRORS
        line: msg
        ...
        SCHEMA
        bigquery_schema
        END

        # comment
        DATA [flags]
        json_records
        ...
        ERRORS
        line: msg
        ...
        SCHEMA
        bigquery_schema
        END

        ...

    The file is composed by zero or more 'chunks'. Each chunk is composed of
    the following components:

        * a DATA section containing the newline-separated JSON data records
        * an optional ERRORS section containing the expected error messages
        * a SCHEMA section containing the expected BigQuery schema
        * comment lines start with a '#' character.

    The DATA section is required. The DATA tag line can contain a list of
    optional flags. Currently, the only flag recognized by the calling code is
    'keep_nulls', but all strings on the DATA tag line will be returned by
    read_chunk().

    The ERRORS section is optional. The format of the expected error message is:

        line_number: message

    The SCHEMA section is required. It contains the expected BigQuery schema in
    JSON format.

    A blank line between chunks is recommended for readability but is not
    necessary for correct parsing. Both comment lines and blank lines are
    ignored.

    Some rudimentary parsing checks are performed. For example, a DATA tag line
    followed by other DATA tag line will throw an exception. But the checks are
    not extensive so it is likely that many syntax errors will cause the parser
    to become very confused.

    Usage:

        data_reader = DataReader(sys.stdin)
        while True:
            chunk = data_reader.read_chunk()
            if chunk is None:
                break

            data_flags = chunk['data_flags']
            keep_nulls = ('keep_nulls' in data_flags)
            records = chunk['records']
            schema = chunk['schema']
            ...
    """

    # Recognized tags.
    # TODO: Change to a hash set to speed up the lookup if many are added.
    TAG_TOKENS = ['DATA', 'ERRORS', 'SCHEMA', 'END']

    def __init__(self, testdata_file):
        self.testdata_file = testdata_file
        self.next_line = None

    def read_chunk(self):
        """Returns a dict with the next test chunk from the data file,
        containing the following fields:
            {
                'data_flags': [data_flags],
                'data': [data lines],
                'errors': {errors},
                'schema': schema_string
            }
        Returns None if there are no more test chunks.
        """
        data_flags, records = self.read_data_section()
        if data_flags is None:
            return None
        errors = self.read_errors_section()
        error_map = self.process_errors(errors)
        schema = self.read_schema_section()
        self.read_end_marker()

        return {
            'data_flags': data_flags,
            'records': records,
            'errors': errors,
            'error_map': error_map,
            'schema': schema
        }

    def read_data_section(self):
        """Returns a tuple of ([data_flags]..., [data records])
        Returns None if there are no more test data chunks.
        """

        # First tag must be 'DATA [flags]'
        tag_line = self.read_line()
        if tag_line is None:
            return (None, None)
        (tag, data_flags) = self.parse_tag_line(tag_line)
        if tag != 'DATA':
            raise Exception(
                "Unrecoginized tag line '%s', should be DATA" % tag_line)

        # Read the DATA records until the next TAG_TOKEN.
        records = []
        while True:
            line = self.read_line()
            if line is None:
                raise Exception(
                    "Unexpected EOF, should be ERRORS or SCHEMA tag")
            (tag, _) = self.parse_tag_line(line)
            if tag in self.TAG_TOKENS:
                if tag == 'DATA':
                    raise Exception("Unexpected DATA tag")
                self.push_back(line)
                break
            records.append(line)

        return (data_flags, records)

    def read_errors_section(self):
        """Return a dictionary of errors which are expected from the parsing of
        the DATA section. The dict has the form:
            {
                'line': line,
                'msg': [ messages ...]
            }
        """

        # The 'ERRORS' section is optional.
        tag_line = self.read_line()
        if tag_line is None:
            return []
        (tag, _) = self.parse_tag_line(tag_line)
        if tag != 'ERRORS':
            self.push_back(tag_line)
            return []

        # Read the ERRORS records until the next TAG_TOKEN.
        errors = []
        while True:
            line = self.read_line()
            if line is None:
                raise Exception("Unexpected EOF, should be SCHEMA tag")
            (tag, _) = self.parse_tag_line(line)
            if tag in self.TAG_TOKENS:
                if tag == 'ERRORS':
                    raise Exception("Unexpected ERRORS tag")
                self.push_back(line)
                break
            errors.append(line)
        return errors

    def read_schema_section(self):
        """Returns the JSON string of the schema section.
        """

        # The next tag must be 'SCHEMA'
        tag_line = self.read_line()
        if tag_line is None:
            raise Exception("Unexpected EOF, should be SCHEMA tag")
        (tag, _) = self.parse_tag_line(tag_line)
        if tag != 'SCHEMA':
            raise Exception(
                "Unrecoginized tag line '%s', should be SCHEMA" % tag_line)

        # Read the SCHEMA records until the next TAG_TOKEN
        schema_lines = []
        while True:
            line = self.read_line()
            if line is None:
                break
            (tag, _) = self.parse_tag_line(line)
            if tag in self.TAG_TOKENS:
                if tag == 'SCHEMA':
                    raise Exception("Unexpected SCHEMA tag")
                self.push_back(line)
                break
            schema_lines.append(line)

        return ''.join(schema_lines)

    def read_end_marker(self):
        """Verify that the 'END' marker exists."""
        tag_line = self.read_line()
        if tag_line is None:
            raise Exception("Unexpected EOF, should be END tag")
        (tag, _) = self.parse_tag_line(tag_line)
        if tag != 'END':
            raise Exception(
                "Unrecoginized tag line '%s', should be END" % tag_line)

    def parse_tag_line(self, line):
        """Parses a potential tag line of the form 'TAG [flags...]' where
        'flags' is a list of strings separated by spaces. Returns the tuple of
        (tag, [flags]).
        """
        tokens = line.split()
        if tokens:
            return (tokens[0], tokens[1:])
        else:
            return (None, [])

    def read_line(self):
        """Return the next line, while supporting a one-line push_back().
        Comment lines begin with a '#' character and are skipped.
        Blank lines are skipped.
        Prepending and trailing whitespaces are stripped.
        Return 'None' if EOF is reached.
        """
        if self.next_line:
            line = self.next_line
            self.next_line = None
            return line

        while True:
            line = self.testdata_file.readline()
            # EOF
            if line == '':
                return None

            stripped = line.strip()

            # Blank line
            if stripped == '':
                continue

            # skip over comment lines
            if len(stripped) != 0 and stripped[0] == '#':
                continue

            return stripped

    def push_back(self, line):
        """Push back the given 'line' so that it is returned on the next call
        to 'read_line'.
        """
        self.next_line = line

    def process_errors(self, error_records):
        """Parse the error records into a dictionary.
        """
        error_map = {}
        for error in error_records:
            (line_number, message) = self.parse_error_line(error)
            error_entry = error_map.get(line_number)
            if error_entry is None:
                error_entry = {'line': line_number, 'msgs': []}
                error_map[line_number] = error_entry
            messages = error_entry['msgs']
            messages.append(message)
        return error_map

    def parse_error_line(self, line):
        """Parse the error line of the form:
            line: msg
        """
        pos = line.find(':')
        if pos < 0:
            raise Exception(
                "Error line must be of the form 'line: msg': '%s'" % line)
        line_number = int(line[0:pos])
        message = line[pos + 1:].strip()
        return (line_number, message)


def main():
    """Read the test data chunks from the STDIN and print them out. The ability
    to run this from the command line is intended mostly for testing purposes.

    Usage:
        data_reader.py < test_data.txt
    """
    data_reader = DataReader(sys.stdin)
    while True:
        chunk = data_reader.read_chunk()
        if chunk is None:
            break
        print("DATA_FLAGS: %s" % chunk['data_flags'])
        print("DATA: %s" % chunk['records'])
        print("ERRORS: %s" % chunk['errors'])
        print("ERROR_MAP: %s" % chunk['error_map'])
        print("SCHEMA: %s" % chunk['schema'])


if __name__ == '__main__':
    main()
