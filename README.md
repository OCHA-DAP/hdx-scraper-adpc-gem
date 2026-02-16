# Collector for ADPC GEM Datasets
[![Build Status](https://github.com/OCHA-DAP/hdx-scraper-adpc-gem/actions/workflows/run-python-tests.yaml/badge.svg)](https://github.com/OCHA-DAP/hdx-scraper-adpc_gem/actions/workflows/run-python-tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-adpc-gem/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-adpc-gem?branch=main)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

This script creates HDX datasets from [ADPC Gender Equality Monitor (GEM)](https://gem-servir.adpc.net/) data for countries in Southeast Asia (Cambodia, Laos, Myanmar, Thailand, Vietnam). For each country, it generates a dataset containing 7 CSV resources (Gender Inequality Index at national and subnational levels, GII dimensions and indicators split by admin level, and gender-disaggregated data) plus 2 GeoJSON boundary files (country and province). The source data is static and the files are stored locally in `src/hdx/scraper/adpc_gem/data/`. To process specific countries for testing, pass a list of ISO3 codes to the `countries` parameter (`countries="KHM,THA"`) in `__main__.py`, otherwise all countries will be processed.

## Development

### Environment

Development is currently done using Python 3.13. The environment can be created with:

```shell
    uv sync
```

This creates a .venv folder with the versions specified in the project's uv.lock file.

### Installing and running

For the script to run, you will need to have a file called
.hdx_configuration.yaml in your home directory containing your HDX key, e.g.:

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod

 You will also need to supply the universal .useragents.yaml file in your home
 directory as specified in the parameter *user_agent_config_yaml* passed to
 facade in run.py. The collector reads the key
 **hdx-scraper-adpc_gem** as specified in the parameter
 *user_agent_lookup*.

 Alternatively, you can set up environment variables: `USER_AGENT`, `HDX_KEY`,
`HDX_SITE`, `EXTRA_PARAMS`, `TEMP_DIR`, and `LOG_FILE_ONLY`.

To run, execute:

```shell
    uv run python -m hdx.scraper.adpc_gem
```

### Pre-commit

pre-commit will be installed when syncing uv. It is run every time you make a git
commit if you call it like this:

```shell
    pre-commit install
```

With pre-commit, all code is formatted according to
[ruff](https://docs.astral.sh/ruff/) guidelines.

To check if your changes pass pre-commit without committing, run:

```shell
    pre-commit run --all-files
```

## Packages

[uv](https://github.com/astral-sh/uv) is used for package management.  If
you’ve introduced a new package to the source code (i.e. anywhere in `src/`),
please add it to the `project.dependencies` section of `pyproject.toml` with
any known version constraints.

To add packages required only for testing, add them to the
`[dependency-groups]`.

Any changes to the dependencies will be automatically reflected in
`uv.lock` with `pre-commit`, but you can re-generate the files without committing by
executing:

```shell
    uv lock --upgrade
```

## Project

[uv](https://github.com/astral-sh/uv) is used for project management. The project can be
built using:

```shell
    uv build
```

Linting and syntax checking can be run with:

```shell
    uv run ruff check
```

To run the tests and view coverage, execute:

```shell
    uv run pytest
```
