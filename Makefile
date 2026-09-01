PYTHON ?= python3
UV ?= uv

PROFILE ?= smoke-local
BENCHMARK_CONFIG ?= benchmark/configs/benchmark-laptop-m02.yaml
ECOMMERCE_CORE_CONFIGS := \
	benchmark/configs/benchmark-laptop-m02.yaml \
	benchmark/configs/benchmark-laptop-m04.yaml \
	benchmark/configs/benchmark-laptop-m05.yaml \
	benchmark/configs/benchmark-laptop-m08.yaml \
	benchmark/configs/benchmark-laptop-m10.yaml \
	benchmark/configs/benchmark-laptop-b01.yaml
TPCH_CORE_CONFIGS := \
	benchmark/configs/benchmark-laptop-tpch-q01.yaml \
	benchmark/configs/benchmark-laptop-tpch-q03.yaml \
	benchmark/configs/benchmark-laptop-tpch-q06.yaml \
	benchmark/configs/benchmark-laptop-tpch-q12.yaml
RESEARCH_CORE_CONFIGS := $(ECOMMERCE_CORE_CONFIGS) $(TPCH_CORE_CONFIGS)

.PHONY: setup validate validate-research lint test fixture research-data \
	research-data-ecommerce research-data-tpch plan research-plan \
	compose-config build up smoke tpch-schema-check benchmark benchmark-one report run-all down \
	report-diagnostic clean-generated

setup:
	$(PYTHON) scripts/bootstrap_env.py
	$(UV) sync --all-extras --frozen

validate:
	$(UV) run lakehouse-bench validate --config benchmark/configs/smoke-m02.yaml

validate-research:
	@for config in $(RESEARCH_CORE_CONFIGS); do \
		$(UV) run lakehouse-bench validate --config "$$config" || exit $$?; \
	done

lint:
	$(UV) run ruff check analysis benchmark data pipeline scripts tests
	$(UV) run ruff format --check analysis benchmark data pipeline scripts tests
	$(UV) run mypy

test:
	$(UV) run pytest

fixture:
	$(UV) run python scripts/ensure_fixture.py

research-data: research-data-ecommerce research-data-tpch

research-data-ecommerce:
	$(UV) run python scripts/ensure_research_data.py --generate

research-data-tpch:
	$(UV) run python scripts/ensure_tpch_data.py --generate

plan: fixture
	$(UV) run lakehouse-bench plan --config benchmark/configs/smoke-m02.yaml

research-plan:
	$(UV) run python scripts/run_research_suite.py --prepare-only

compose-config:
	$(PYTHON) scripts/bootstrap_env.py
	docker compose config --quiet

build: compose-config
	docker compose build

up: setup fixture build
	docker compose up -d minio minio-init iceberg-rest spark-master spark-worker

smoke: setup lint test plan compose-config
	bash scripts/run_native_smoke.sh
	$(MAKE) tpch-schema-check

tpch-schema-check: build
	docker compose --profile tools run --rm --no-deps \
		--entrypoint /opt/spark/bin/spark-submit spark-client \
		--master local[1] /opt/lakehouse/scripts/validate_tpch_schemas_spark.py

benchmark: setup lint test compose-config
	$(UV) run python scripts/run_research_suite.py

benchmark-one: setup lint test research-data compose-config
	$(UV) run python scripts/run_research_suite.py \
		--config $(BENCHMARK_CONFIG)

report:
	$(UV) run python -m analysis.scripts.build_report results/raw --output results/reports \
		--require-publishable

report-diagnostic:
	$(UV) run python -m analysis.scripts.build_report results/raw --output results/reports

run-all:
	@if [ "$(PROFILE)" = "smoke-local" ]; then \
		$(MAKE) smoke; \
	elif [ "$(PROFILE)" = "benchmark-laptop" ]; then \
		$(MAKE) benchmark report; \
	else \
		echo "Unsupported PROFILE=$(PROFILE); use smoke-local or benchmark-laptop." >&2; \
		exit 2; \
	fi

down:
	docker compose down

clean-generated:
	@echo "Generated data and immutable artifacts are not deleted automatically."
	@echo "Remove a specific data/artifact directory only after reviewing its manifest."
