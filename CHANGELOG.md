# Changelog

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
