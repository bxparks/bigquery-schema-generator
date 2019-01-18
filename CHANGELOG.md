# Changelog

* 0.3.1 (2019-01-18)
    * Infer integers that overflow signed 64-bits to be `FLOAT` for
      consistency with `bq load`. (Fixes #18)
    * Support 'UTC' suffix in TIMESTAMP fields, in addition to 'Z'. (Fixes #19)
* 0.3 (2018-12-17)
    * Tighten TIMESTAMP and DATE validation (thanks jtschichold@).
    * Inspect the internals of STRING values to infer BOOLEAN, INTEGER or FLOAT
      types (thanks jtschichold@).
    * Handle conversion of these string types when mixed with their non-quoted
      equivalents, matching the conversion logic followed by 'bq load'.
* 0.2.1 (2018-07-18)
    * Add `anonymizer.py` script to create anonymized data files for
      benchmarking.
    * Add benchmark numbers to README.md.
    * Add `DEVELOPER.md` file to record how to upload to PyPI.
    * Fix some minor warnings from pylint3.
* 0.2.0 (2018-02-10)
    * Add support for `DATE` and `TIME` types.
    * Update type conversion rules to be more compatible with **bq load**.
        * Allow `DATE`, `TIME` and `TIMESTAMP` to gracefully degrade to
          `STRING`.
        * Allow type conversions of elements within arrays
          (e.g. array of `INTEGER` and `FLOAT`, or array of mixed `DATE`,
          `TIME`, or `TIMESTAMP` elements).
        * Better detection of invalid values (e.g. arrays of arrays).
* 0.1.6 (2018-01-26)
    * Pass along command line arguments to `generate-schema`.
* 0.1.5 (2018-01-25)
    * Updated installation instructions for MacOS.
* 0.1.4 (2018-01-23)
    * Attempt #3 to fix exception during pip3 install.
* 0.1.3 (2018-01-23)
    * Attempt #2 to fix exception during pip3 install.
* 0.1.2 (2018-01-23)
    * Attemp to fix exception during pip3 install. Didn't work. Pulled.
* 0.1.1 (2018-01-03)
    * Install `generate-schema` script in `/usr/local/bin`
* 0.1 (2018-01-02)
    * Iniitial release to PyPI.
