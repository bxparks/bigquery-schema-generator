from setuptools import setup

# Slurp in the README.md. PyPI finally supports Markdown, so we no longer need
# to convert it to RST.
with open('README.md', encoding="utf-8") as f:
    long_description = f.read()

# Read the version string from bigquery_schema_generator/version.py.
# See https://packaging.python.org/guides/single-sourcing-package-version/
version = {}
with open("bigquery_schema_generator/version.py") as fp:
    exec(fp.read(), version)
version_string = version['__version__']
if not version_string:
    raise Exception("Unable to read version.py")

setup(
    name='bigquery-schema-generator',
    version=version_string,
    description='BigQuery schema generator from JSON or CSV data',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/bxparks/bigquery-schema-generator',
    author='Brian T. Park',
    author_email='brian@xparks.net',
    license='Apache 2.0',
    packages=['bigquery_schema_generator'],
    python_requires='>=3.6',
    entry_points={
        'console_scripts': [
            'generate-schema = bigquery_schema_generator.generate_schema:main'
        ]
    },
)
