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

import unittest
from bigquery_schema_generator.anonymize import increment_anon_key


class TestAnonymizer(unittest.TestCase):
    def test_increment_anon_key(self):
        self.assertEqual('b', increment_anon_key('a'))
        self.assertEqual('ba', increment_anon_key('z'))
        self.assertEqual('baa', increment_anon_key('zz'))


if __name__ == '__main__':
    unittest.main()
