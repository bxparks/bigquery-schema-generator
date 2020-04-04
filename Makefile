.PHONY: tests

tests:
	python3 -m unittest

flake8:
	flake8 . \
		--count \
		--ignore W503 \
		--show-source \
		--statistics \
		--max-line-length=80
