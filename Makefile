.DEFAULT_GOAL := help

define BROWSER_PYSCRIPT
import os, webbrowser, sys
try:
	from urllib import pathname2url
except:
	from urllib.request import pathname2url

webbrowser.open("file://" + pathname2url(os.path.abspath(sys.argv[1])))
endef
export BROWSER_PYSCRIPT

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT
BROWSER := python -c "$$BROWSER_PYSCRIPT"

PYTHON := python
PYTHON_LIB := $(shell $(PYTHON) -c "from distutils.sysconfig import get_python_lib; import sys; sys.stdout.write(get_python_lib())" )


.PHONY: help
help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

.PHONY: virtualenv
virtualenv: ## create virtual environment typically used for development purposes
	virtualenv env --setuptools --prompt='(twindb_proxysql_tools)'

.PHONY: rebuild-requirements
rebuild-requirements: ## Rebuild requirements files requirements.txt and requirements_dev.txt
	pip-compile --verbose --no-index --output-file requirements.txt requirements.in
	pip-compile --verbose --no-index --output-file requirements_dev.txt requirements_dev.in

.PHONY: upgrade-requirements
upgrade-requirements: ## Upgrade requirements
	pip-compile --upgrade --verbose --no-index --output-file requirements.txt requirements.in
	pip-compile --upgrade --verbose --no-index --output-file requirements_dev.txt requirements_dev.in

.PHONY: bootstrap
bootstrap: ## bootstrap the development environment
	pip install -U "setuptools==32.3.1"
	pip install -U "pip==9.0.1"
	pip install -U "pip-tools>=1.6.0"
	pip-sync requirements.txt requirements_dev.txt
	pip install --editable .

.PHONY: clean
clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts

.PHONY: clean-build
clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

.PHONY: clean-pyc
clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

.PHONY: clean-test
clean-test: ## remove test and coverage artifacts
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/

lint: ## check style with flake8
	flake8 proxysql_tools tests

test: ## run tests quickly with the default Python
	pytest -xv --cov-report term-missing --cov=./proxysql_tools tests/unit

test-integration: ## run integration tests
	py.test tests/integration/

test-all: ## run tests on every Python version with tox (must be run in Linux with docker)
	tox

coverage: ## check code coverage quickly with the default Python
	py.test --cov-report term-missing --cov=./proxysql_tools tests/unit

.PHONY: docs
docs: ## generate Sphinx HTML documentation, including API docs
	rm -f docs/proxysql_tools.rst
	rm -f docs/modules.rst
	sphinx-apidoc -o docs/ proxysql_tools
	$(MAKE) -C docs clean
	$(MAKE) -C docs html
	$(BROWSER) docs/_build/html/index.html

servedocs: docs ## compile the docs watching for changes
	watchmedo shell-command -p '*.rst' -c '$(MAKE) -C docs html' -R -D .

install: clean ## install the package to the active Python's site-packages
	if test -z "${DESTDIR}" ; \
    	then $(PYTHON) setup.py install \
        	--prefix /usr \
        	--install-lib $(PYTHON_LIB); \
	else $(PYTHON) setup.py install \
        	--prefix /usr \
        	--install-lib $(PYTHON_LIB) \
        	--root "${DESTDIR}" ; \
	fi
