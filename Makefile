# Shortcuts for the commands we run most. Run `make help` for the list.
PYTHON ?= .venv/bin/python
ENV ?= dev
STAGE ?= all

.PHONY: help venv lint format test run report clean-report

help:
	@echo "make venv            create .venv with runtime + dev dependencies"
	@echo "make lint            ruff lint + format check"
	@echo "make format          apply ruff fixes and formatting"
	@echo "make test            run the test suite"
	@echo "make run ENV=dev     run the pipeline (STAGE=bronze|silver|gold|all)"
	@echo "make report          build report/main.pdf"

venv:
	python3 -m venv .venv
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements-dev.txt

lint:
	$(PYTHON) -m ruff check src tests
	$(PYTHON) -m ruff format --check src tests

format:
	$(PYTHON) -m ruff format src tests
	$(PYTHON) -m ruff check --fix src tests

test:
	$(PYTHON) -m pytest

run:
	PYTHONPATH=src $(PYTHON) -m maude --env $(ENV) --stage $(STAGE)

report:
	$(MAKE) -C report

clean-report:
	$(MAKE) -C report clean
