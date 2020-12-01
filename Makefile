.PHONY: tests flake8 all

all: flake8 tests

tests:
	python3 -m unittest

flake8:
	flake8 bigquery_schema_generator/ \
		--count \
		--ignore W503 \
		--show-source \
		--statistics \
		--max-line-length=80
	flake8 tests/ \
		--count \
		--ignore W503 \
		--show-source \
		--statistics \
		--max-line-length=80

