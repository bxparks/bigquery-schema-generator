from setuptools import setup

# Convert README.md to README.rst because PyPI does not support Markdown.
try:
    import pypandoc
    long_description = pypandoc.convert('README.md', 'rst')
except OSError:
    with open('README.md', encoding="utf-8") as f:
        long_description = f.read()

setup(name='bigquery-schema-generator',
      version='0.1.1',
      description='BigQuery schema generator',
      long_description=long_description,
      url='https://github.com/bxparks/bigquery-schema-generator',
      author='Brian T. Park',
      author_email='brian@xparks.net',
      license='Apache 2.0',
      packages=['bigquery_schema_generator'],
      scripts=['scripts/generate-schema'],
      python_requires='~=3.5')
