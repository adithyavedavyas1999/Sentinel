.DEFAULT_GOAL := help
SHELL := /bin/bash

UV ?= uv
COMPOSE ?= docker compose

.PHONY: help setup up down logs test lint fmt seed-data demo clean

help:
	@grep -E '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | \
		awk 'BEGIN{FS=":.*?## "}{printf "  %-14s %s\n", $$1, $$2}'

setup: ## create venv and install deps via uv
	$(UV) venv
	$(UV) pip install -e ".[dev]"
	@echo "done. activate with: source .venv/bin/activate"

up: ## start the compose stack
	$(COMPOSE) up -d

down: ## stop the compose stack
	$(COMPOSE) down

logs: ## tail logs from the stack
	$(COMPOSE) logs -f --tail=100

test: ## run pytest with coverage
	pytest

lint: ## ruff check + mypy
	ruff check .
	ruff format --check .
	mypy sentinel

fmt: ## auto-format with ruff
	ruff check --fix .
	ruff format .

seed-data: ## download a TLC sample into data/raw
	python scripts/seed_data.py

demo: ## materialize 3 months of bronze (TLC + weather) end-to-end
	python scripts/demo.py

incidents: ## list open incidents
	sentinel incident list

chaos: ## list available chaos scenarios
	sentinel chaos list

dbt-deps: ## install dbt packages
	cd dbt && dbt deps

dbt-build: ## run dbt build (requires duckdb file + minio data)
	cd dbt && dbt build

test-unit: ## unit tests only (fast)
	pytest -m "not integration" tests/unit

test-integration: ## integration tests (needs docker)
	pytest -m integration tests/integration

api: ## run the incident API locally on :8000
	uvicorn sentinel.api:app --reload --host 0.0.0.0 --port 8000

dashboard: ## run the streamlit dashboard locally on :8501
	streamlit run sentinel/ui/dashboard.py

clean: ## stop stack and wipe local data (destructive)
	$(COMPOSE) down -v
	rm -rf data/warehouse/*.duckdb data/raw/*
