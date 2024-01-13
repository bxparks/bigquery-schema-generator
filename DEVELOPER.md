# Developer Notes

## Installing for Local Testing

Use the `-e` option to install the `bigquery_schema_generator` package
as "symlinks" so that changes to the code are picked up immediately,
without having to run `python setup.py install`:
```
$ cd bigquery-schema-generator
$ pip3 install -e .
```

## Uploading to PyPI

### Preamble

There are a lot of instructions on the web that uses
`setup.py register` and `setup.py upload`, but as far as I can tell,
those are deprecated. The tool that seems to work for me is
[Twine](https://github.com/pypa/twine).

[PyPI](https://pypi.python.org/pypi) now supports Markdown so we no longer need
to download `pypandoc` (Python package) and `pandoc` (apt package) to convert
Markdown to RST.

Install the following packages:
```
$ sudo -H pip3 install setuptools wheel twine
```

Starting sometime in 2023, 2FA is required on PyPI. In addition, `twine` no
longer accepts a password but requires an API token. See
https://pypi.org/manage/account/token/ on how to generate a token and how to
configure a `$HOME/.pypirc` file that contains this token. The file will look
like this:

```
[distutils]
  index-servers =
    pypi
    bigquery-schema-generator

[pypi]
  username = __token__
  password = # insert user-scope or project-scoped token

[schemagenerator]
  repository = https://upload.pypi.org/legacy/
  username = __token__
  password = # insert project-scoped token
```

### Steps

1. Increment the version numbers in:
    - `version.py`
    - `README.md`
    - `CHANGELOG.md`
1. Push all changes to `develop` branch.
1. Create a GitHub pull request (PR) from `develop` into `master` branch.
1. Merge the PR into `master`.
1. Create a new Release in GitHub with the new tag label.
1. Create the dist using `python3 setup.py sdist`.
    - If `dist/` becomes too cluttered, we can remove the entire `dist/`
      directory and run `python3 setup.py sdist` again.
1. Upload to PyPI
    - `$ twine upload dist/bigquery-schema-generator-{version}.tar.gz`, or
    - `$ twine --repository schemagenerator upload
      dist/bigquery-schema-generator-{version}.tar.gz`
    - the `--repository PROJECT_NAME` flag is undocumented, seems to map to the
      `[PROJECT_NAME]` section in the config file
    - if this flag is omitted, then the `[pypi]` section appears to be used
