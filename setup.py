from setuptools import setup

# Convert README.md to README.rst because PyPI does not support Markdown.
try:
    import pypandoc
    long_description = pypandoc.convert('README.md', 'rst', format='md')
except:  # noqa: E722
    # If unable to convert, try inserting the raw README.md file.
    try:
        with open('README.md', encoding="utf-8") as f:
            long_description = f.read()
    except:  # noqa: E722
        # If all else fails, use some reasonable string.
        long_description = 'BigQuery schema generator.'

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
    url='https://github.com/bxparks/bigquery-schema-generator',
    author='Brian T. Park',
    author_email='brian@xparks.net',
    license='Apache 2.0',
    packages=['bigquery_schema_generator'],
    python_requires='~=3.6',
    entry_points={
        'console_scripts': [
            'generate-schema = bigquery_schema_generator.generate_schema:main'
        ]
    },
)
