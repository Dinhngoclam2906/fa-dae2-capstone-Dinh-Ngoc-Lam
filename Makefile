# Common commands. `make help` lists them. Run from the repo root.
# On Windows, `make` comes with Git for Windows' SDK, or: winget install ezwinports.make
# (or copy the command from the recipe below).

ENV      := --env-file .env
DBT      := cd transform && uv run --env-file ../.env dbt
COMPOSE  := docker compose --project-directory . -f infra/local

.DEFAULT_GOAL := help
.PHONY: help install test lint check agent cli eval dbt-build dbt-freshness memory-db airflow down

help:  ## List the commands
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

install:  ## Install all dependencies (uv)
	uv sync

test:  ## Unit tests (no network, no credentials)
	uv run pytest

lint:  ## SQLFluff over the dbt models
	cd transform && uv run sqlfluff lint models

check:  ## Pipeline health by rows: workflow runs, jobs, rows per source, vectors
	uv run $(ENV) python .claude/skills/check-pipeline/check.py --runs 6

agent:  ## Streamlit app
	uv run $(ENV) streamlit run agent/app.py

cli:  ## Terminal agent with retrieval stats
	uv run $(ENV) python agent/cli_agent.py --stats

eval:  ## Hard-question regression benchmark (costs a few cents)
	uv run $(ENV) python agent/eval/hard_eval.py run

dbt-build:  ## dbt build against BigQuery (dev target)
	$(DBT) build --profiles-dir .dbt

dbt-freshness:  ## dbt source freshness
	$(DBT) source freshness --profiles-dir .dbt

memory-db:  ## Local Postgres for agent memory (port 5433)
	$(COMPOSE)/docker-compose.yml up -d

airflow:  ## Local Airflow (legacy orchestration)
	$(COMPOSE)/docker-compose-airflow.yml up -d

down:  ## Stop every local container
	$(COMPOSE)/docker-compose-airflow.yml down
	$(COMPOSE)/docker-compose.yml down
