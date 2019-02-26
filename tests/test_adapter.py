import unittest

from bigquery_schema_generator._adapter import _CsvAdapter
from contextlib import contextmanager
import tempfile


class TestAdapter(unittest.TestCase):

    def test_it_converts_first_line_to_json_object(self):
        with self.create_temporary_file() as csv_file:
            self.prepare_csv_file(csv_file)

            adapter = _CsvAdapter(csv_file)

            self.assertDictEqual({'name': 'John', 'surname': 'Smith', 'age': '23'},
                                 adapter.to_json_object(csv_file.readline()))

    def test_it_converts_fourth_line_to_json_object(self):
        with self.create_temporary_file() as csv_file:
            self.prepare_csv_file(csv_file)

            adapter = _CsvAdapter(csv_file)
            for times in range(0, 3):
                csv_file.readline()

            self.assertDictEqual({'name': 'Joanna', 'surname': 'Anders', 'age': '21'},
                                 adapter.to_json_object(csv_file.readline()))

    @staticmethod
    def prepare_csv_file(csv_file):
        csv_file.write('name,surname,age\n'
                       'John,Smith,23\n'
                       'Michael,Johnson,27\n'
                       'Maria,Smith,30\n'
                       'Joanna,Anders,21')
        csv_file.seek(0)

    @contextmanager
    def create_temporary_file(self):
        file = tempfile.NamedTemporaryFile(mode='w+t')
        try:
            yield file
        finally:
            file.close()
