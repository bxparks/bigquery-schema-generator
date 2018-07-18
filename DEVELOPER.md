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

[PyPI](https://pypi.python.org/pypi) does not support Markdown, so
we use `pypandoc` and `pandoc` to convert Markdown to RST.
`pypandoc` is a thin Python wrapper around `pandoc`.

Install the following packages:
```
$ sudo apt install pandoc
$ sudo -H pip3 install setuptools wheel twine pypandoc
```

### Steps

1. Edit `setup.py` and increment the `version`.
1. Push all changes to `develop` branch.
1. Merge `develop` into `master` branch, and checkout the `master` branch.
1. Create the dist using `python3 setup.py sdist`.
1. Upload to PyPI using `twine upload dist/*`.
   (Need to enter my PyPI login creddentials).
    * If `dist/` becomes too cluttered, we can remove the entire `dist/`
      directory and run `python3 setup.py sdist` again.
1. Tag the `master` branch with the release on GitHub.
