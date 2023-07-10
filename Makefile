.DEFAULT_GOAL:=help

# define the name of the virtual environment directory
VENV := .venv3.8.16

# define the profile used by the dbt
PROFILE := dwx_endpoint

.PHONY: all install_deps dev_setup functional_test test clean help

all: dev_setup test  ## Default target for dev setup and run tests.

# Required python3 already installed in the system
$(VENV)/bin/activate:
	@\
	python3 -m venv $(VENV)

install_deps: $(VENV)/bin/activate dev-requirements.txt	## Install all dependencies.
	@\
	$(VENV)/bin/python3 -m pip install --upgrade pip
	$(VENV)/bin/pip install -e . -r dev-requirements.txt

dev_setup: $(VENV)/bin/activate	 ## Install all dependencies and setup pre-commit.
	@\
	make install_deps
	$(VENV)/bin/pre-commit install

functional_test:	## Run functional tests.
	$(VENV)/bin/python3 -m pytest --profile $(PROFILE) $(TESTS)

test:  ## Run all tests.
	@make functional_test

clean: 	## Cleanup and reset development environment.
	@echo 'cleaning virtual environment...'
	rm -rf $(VENV)
	@find . -type f -name '*.pyc' -delete
	@find . -type d -name '__pycache__' -depth -delete
	@echo 'done.'

help:  ## Show this help message.
	@echo 'usage: make [target] [VENV=.venv] [PROFILE=dwx_endpoint]'
	@echo
	@echo 'targets:'
	@grep -E '^[8+a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo 'options:'
	@echo 'use VENV=<dir> to specify virtual enviroment directory'
	@echo 'use PROFILE=<profile> to use specific endpoint/warehouse'