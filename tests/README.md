# Tests

## Unit Test for generate_schema.py

Instead of embedding the input data records and the expected schema into
the `test_generate_schema.py` file, we placed them into the `testdata.txt`
file which is parsed by the unit test program.  This has two advantages:

* we can more easily update the input and output data records, and 
* the `testdata.txt` data can be reused for versions written in other languages

The output of `test_generate_schema.py` should look something like this:

```
$ ./test_generate_schema.py
----------------------------------------------------------------------
Ran 4 tests in 0.002s

OK
Test chunk 1: First record: { "s": null, "a": [], "m": {} }
Test chunk 2: First record: { "s": null, "a": [], "m": {} }
Test chunk 3: First record: { "s": "string", "b": true, "i": 1, "x": 3.1, "t": "2017-05-22T17:10:00-07:00" }
Test chunk 4: First record: { "a": [1, 2], "r": { "r0": "r0", "r1": "r1" } }
Test chunk 5: First record: { "s": "string", "x": 3.2, "i": 3, "b": true, "a": [ "a", 1] }
Test chunk 6: First record: { "a": [1, 2] }
Test chunk 7: First record: { "r" : { "a": [1, 2] } }
Test chunk 8: First record: { "i": 1 }
Test chunk 9: First record: { "i": null }
Test chunk 10: First record: { "i": 3 }
Test chunk 11: First record: { "i": [1, 2] }
Test chunk 12: First record: { "r" : { "i": 3 } }
Test chunk 13: First record: { "r" : [{ "i": 4 }] }
```

## Unit Test for anonymize.py

The unit test for `anonymize.py` should look like this:
```
$ ./test_anonymize.py
.
----------------------------------------------------------------------
Ran 1 test in 0.000s

OK
```

## Unit Test for _adapter.py

The unit test for `_adapter.py` should look like this:
```
$ ./test_adapter.py
.
----------------------------------------------------------------------
Ran 2 tests in 0.003s

OK
```

## Running All Tests

Use the
[discovery mode](https://docs.python.org/3/library/unittest.html)
for `unittest` which runs all tests with the `test_` prefix:
```
$ python3 -m unittest
```
