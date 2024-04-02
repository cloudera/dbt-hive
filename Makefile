.DEFAULT_GOAL:=help

CHANGED_FILES := $(shell git ls-files --modified --other --exclude-standard)
CHANGED_FILES_IN_BRANCH := $(shell git diff --name-only $(shell git merge-base origin/main HEAD))

.PHONY: all install_deps dev_setup functional_test test clean help
.PHONY: pre-commit pre-commit-in-branch pre-commit-all

dev_setup:
	nox -s dev_setup

test:  ## Run all tests.
    ifndef PYTHON_VERSION
		nox -- $(TESTS) $(DEBUG)
    else
		nox --python=$(PYTHON_VERSION) -- $(TESTS) $(DEBUG)
    endif

pre-commit:  ## check modified and added files (compared to last commit!) with pre-commit.
	$(VENV)/bin/pre-commit run --files $(CHANGED_FILES)

pre-commit-in-branch:  ## check changed since origin/main files with pre-commit.
	$(VENV)/bin/pre-commit run --files $(CHANGED_FILES_IN_BRANCH)

pre-commit-all:  ## Check all files in working directory with pre-commit.
	$(VENV)/bin/pre-commit run --all-files

help:  ## Show this help message.
	@echo 'usage: make [target] [VENV=.venv] [PROFILE=dwx_endpoint]'
	@echo
	@echo 'targets:'
	@grep -E '^[8+a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo 'options:'
	@echo 'use VENV=<dir> to specify virtual enviroment directory'
	@echo 'use PROFILE=<profile> to use specific endpoint/warehouse'
