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

import csv
import json
from io import StringIO


class _CsvAdapter:
    """Converts a single CSV file to JSON format whereas row serves
    values and headers resolved at instantiation time serve keys.
    """

    @staticmethod
    def __to_list(line) -> list:
        return next(csv.reader(StringIO(str(line))))

    def __init__(self, file):
        super().__init__()
        self._headers = self.__to_list(file.readline())

    def to_json_object(self, line):
        return dict(zip(self._headers, self.__to_list(line)))


class _DefaultAdapter:

    @staticmethod
    def to_json_object(line):
        return json.loads(line)
