# Tests

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
