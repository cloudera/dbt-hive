# Contributing to `dbt-hive`

1. [About this document](#about-this-document)
2. [Getting the code](#getting-the-code)
3. [Running `dbt-hive` in development](#running-dbt-hive-in-development)
4. [Testing](#testing)
4. [Submitting a Pull Request](#submitting-a-pull-request)

## About this document
This document is a guide for anyone interested in contributing to the `dbt-hive` repository. It outlines how to create issues and submit pull requests (PRs).

This is not intended as a guide for using `dbt-hive` in a project.

We assume users have a Linux or MacOS system. You should have familiarity with:

- Python `virturalenv`s
- Python modules
- `pip`
- common command line utilities like `git`.

In addition to this guide, we highly encourage you to read the [dbt-core](https://github.com/dbt-labs/dbt-core/blob/main/CONTRIBUTING.md). Almost all information there is applicable here!

## Getting the code

 `git` is needed in order to download and modify the `dbt-hive` code. There are several ways to install Git. For MacOS, we suggest installing [Xcode](https://developer.apple.com/support/xcode/) or [Xcode Command Line Tools](https://mac.install.guide/commandlinetools/index.html).

### External contributors

If you are not a member of the `Cloudera` GitHub organization, you can contribute to `dbt-hive` by forking the `dbt-hive` repository. For more on forking, check out the [GitHub docs on forking](https://help.github.com/en/articles/fork-a-repo). In short, you will need to:

1. fork the `dbt-hive` repository
2. clone your fork locally
3. check out a new branch for your proposed changes
4. push changes to your fork
5. open a pull request of your forked repository against `cloudera/dbt-hive`

### Cloudera contributors

If you are a member of the `Cloudera` GitHub organization, you will have push access to the `dbt-hive` repo. Rather than forking `dbt-hive` to make your changes, clone the repository like normal, and check out feature branches.

## Running `dbt-hive` in development

### Installation

1. Ensure you have the Python 3.8 or higher installed on the machine.

2. Ensure you have the latest version of `pip` installed by running `pip install --upgrade pip` in terminal.

3. Either manually configure the virtual environment

    3.1 Configure and activate a `virtualenv` as described in [Setting up an environment](https://github.com/dbt-labs/dbt-core/blob/HEAD/CONTRIBUTING.md#setting-up-an-environment).

    3.2. Install `dbt-core` in the active `virtualenv`. To confirm you installed dbt correctly, run `dbt --version` and `which dbt`.

    3.3. Install `dbt-hive` and development dependencies in the active `virtualenv`. Run `pip install -e . -r dev-requirements.txt`.

    3.4. Add the pre-commit hook. Run `pre-commit install`

4. OR Use `make`, to run multiple setup or test steps in combination.

    4.1. Run `make dev_setup` to setup the virtual environment and install dependencies

    4.2. Optionally, you can specify the venv directory to use via setting `VENV` variable in the make command eg `make dev_setup VENV=.venv`


When `dbt-hive` is installed this way, any changes you make to the `dbt-hive` source code will be reflected immediately (i.e. in your next local dbt invocation against a Hive target).

## Testing

### Initial setup

`dbt-hive` contains [functional](https://github.com/cloudera/dbt-hive/tree/master/tests/functional/) tests. Functional tests require an actual Hive warehouse to test against.

- You can run functional tests "locally" by configuring a `test.env` file with appropriate `ENV` variables.

```
cp test.env.example test.env
$EDITOR test.env
```

WARNING: The parameters in your `test.env` file must link to a valid Hive instance. The `test.env` file you create is git-ignored, but please be _extra_ careful to never check in credentials or other sensitive information when developing.


### "Local" test commands
There are a few methods for running tests locally.

#### `pytest`
You may run a specific test or group of tests using `pytest` directly or `make`. Activate a Python virtualenv active with dev dependencies installed as explained in the [installation steps](#installation). Use the appropriate profile like cdh_endpoint or dwx_endpoint. Then, run tests like so:

```sh
# Note: replace $strings with valid names

# run full tests suite against an environment/endpoint
python -m pytest --profile dwx_endpoint

# using make to run full tests suites against an environment/endpoint
make test PROFILE=dwx_endpoint

# run all hive functional tests in a directory
python -m pytest tests/functional/$test_directory --profile dwx_endpoint
python -m pytest tests/functional/adapter/test_basic.py --profile dwx_endpoint

# using make run all hive functional tests in a directory
make test TESTS=tests/functional/$test_directory

# run all hive functional tests in a module
python -m pytest --profile dwx_endpoint tests/functional/$test_dir_and_filename.py
python -m pytest --profile dwx_endpoint tests/functional/adapter/test_basic.py

# run all hive functional tests in a class
python -m pytest --profile dwx_endpoint tests/functional/$test_dir_and_filename.py::$test_class_name
python -m pytest --profile dwx_endpoint tests/functional/adapter/test_basic.py::TestSimpleMaterializationsHive

# run a specific hive functional test
python -m pytest --profile dwx_endpoint tests/functional/$test_dir_and_filename.py::$test_class_name::$test__method_name
python -m pytest --profile dwx_endpoint tests/functional/adapter/test_basic.py::TestSimpleMaterializationsHive::test_base
```

To configure the pytest setting, update pytest.ini. By default, all the tests run logs are captured in `logs/<test-run>/dbt.log`

## Submitting a Pull Request

A `dbt-hive` maintainer will review your PR and will determine if it has passed regression tests. They may suggest code revisions for style and clarity, or they may request that you add unit or functional tests. These are good things! We believe that, with a little bit of help, anyone can contribute high-quality code.

Once all tests are passing and your PR has been approved, a `dbt-hive` maintainer will merge your changes into the active development branch. And that's it! Happy developing :tada:
