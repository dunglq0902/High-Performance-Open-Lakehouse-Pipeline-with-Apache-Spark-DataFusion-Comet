PYTHON ?= python3
UV ?= uv

PROFILE ?= smoke-local
BENCHMARK_CONFIG ?= benchmark/configs/benchmark-laptop-m02.yaml
DEMO_EXPERIMENT ?= EXP-TPCH-SF1-Q01
DEMO_PAIR ?= 1
DEMO_ENV := .artifacts/demo/spark-ui/current.env
DEMO_VIDEO ?= deliverables/video/spark-ui-comparison.mp4
VIDEO_VISUAL_REVIEW_ATTESTATION ?= 0
VIDEO_FULL_PLAYBACK_ATTESTATION ?= 0
VIDEO_VISUAL_REVIEW_FLAG = $(if $(filter 1 true yes,$(VIDEO_VISUAL_REVIEW_ATTESTATION)),--confirm-visual-review,)
VIDEO_PLAYBACK_FLAG = $(if $(filter 1 true yes,$(VIDEO_FULL_PLAYBACK_ATTESTATION)),--confirm-full-playback,)
PRESENTATION ?= deliverables/presentation/lakehouse-comet-research.pptx
PRESENTATION_VISUAL_REVIEW_ATTESTATION ?= 0
PRESENTATION_VISUAL_REVIEW_FLAG = $(if $(filter 1 true yes,$(PRESENTATION_VISUAL_REVIEW_ATTESTATION)),--confirm-visual-review,)
PRESENTATION_MANIFEST ?= $(patsubst %.pptx,%.manifest.json,$(PRESENTATION))
VIDEO_MANIFEST ?= $(patsubst %.mp4,%.manifest.json,$(DEMO_VIDEO))
RELEASE_BUNDLE ?= deliverables/release/lakehouse-comet-evidence.zip
EVIDENCE_SOURCE_COMMIT ?=
EVIDENCE_ARCHIVE_LABEL ?=
override EVIDENCE_SOURCE_COMMIT := $(value EVIDENCE_SOURCE_COMMIT)
override EVIDENCE_ARCHIVE_LABEL := $(value EVIDENCE_ARCHIVE_LABEL)
export EVIDENCE_SOURCE_COMMIT EVIDENCE_ARCHIVE_LABEL
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
	report-diagnostic demo-prepare demo-prepare-diagnostic demo-ui demo-ui-diagnostic \
	demo-ui-verify demo-ui-verify-diagnostic demo-ui-down \
	demo-video-finalize demo-video-finalize-diagnostic \
	presentation-finalize presentation-finalize-diagnostic evidence-bundle \
	verify-evidence-bundle archive-research-evidence-dry-run \
	archive-research-evidence archive-research-evidence-rollback \
	archive-partial-research-incident-dry-run archive-partial-research-incident \
	archive-partial-research-incident-rollback release clean-generated

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

archive-research-evidence-dry-run:
	@test -n "$${EVIDENCE_SOURCE_COMMIT:-}" || \
		(echo "EVIDENCE_SOURCE_COMMIT must be the 40-character commit bound to the old raw records." >&2; exit 2)
	@test -n "$${EVIDENCE_ARCHIVE_LABEL:-}" || \
		(echo "EVIDENCE_ARCHIVE_LABEL must be a unique lowercase archive label." >&2; exit 2)
	$(UV) run python scripts/archive_research_evidence.py \
		--expected-source-commit "$${EVIDENCE_SOURCE_COMMIT}" \
		--label "$${EVIDENCE_ARCHIVE_LABEL}"

archive-research-evidence:
	@test -n "$${EVIDENCE_SOURCE_COMMIT:-}" || \
		(echo "EVIDENCE_SOURCE_COMMIT must be the 40-character commit bound to the old raw records." >&2; exit 2)
	@test -n "$${EVIDENCE_ARCHIVE_LABEL:-}" || \
		(echo "EVIDENCE_ARCHIVE_LABEL must be a unique lowercase archive label." >&2; exit 2)
	$(UV) run python scripts/archive_research_evidence.py \
		--expected-source-commit "$${EVIDENCE_SOURCE_COMMIT}" \
		--label "$${EVIDENCE_ARCHIVE_LABEL}" --execute

archive-research-evidence-rollback:
	@test -n "$${EVIDENCE_SOURCE_COMMIT:-}" || \
		(echo "EVIDENCE_SOURCE_COMMIT must identify the interrupted archive." >&2; exit 2)
	@test -n "$${EVIDENCE_ARCHIVE_LABEL:-}" || \
		(echo "EVIDENCE_ARCHIVE_LABEL must identify the interrupted archive." >&2; exit 2)
	$(UV) run python scripts/archive_research_evidence.py \
		--expected-source-commit "$${EVIDENCE_SOURCE_COMMIT}" \
		--label "$${EVIDENCE_ARCHIVE_LABEL}" --rollback-staging

archive-partial-research-incident-dry-run:
	@test -n "$${EVIDENCE_SOURCE_COMMIT:-}" || \
		(echo "EVIDENCE_SOURCE_COMMIT must be the 40-character commit bound to the incident." >&2; exit 2)
	@test -n "$${EVIDENCE_ARCHIVE_LABEL:-}" || \
		(echo "EVIDENCE_ARCHIVE_LABEL must be a unique lowercase incident label." >&2; exit 2)
	$(UV) run python scripts/archive_partial_research_incident.py \
		--expected-source-commit "$${EVIDENCE_SOURCE_COMMIT}" \
		--label "$${EVIDENCE_ARCHIVE_LABEL}"

archive-partial-research-incident:
	@test -n "$${EVIDENCE_SOURCE_COMMIT:-}" || \
		(echo "EVIDENCE_SOURCE_COMMIT must be the 40-character commit bound to the incident." >&2; exit 2)
	@test -n "$${EVIDENCE_ARCHIVE_LABEL:-}" || \
		(echo "EVIDENCE_ARCHIVE_LABEL must be a unique lowercase incident label." >&2; exit 2)
	$(UV) run python scripts/archive_partial_research_incident.py \
		--expected-source-commit "$${EVIDENCE_SOURCE_COMMIT}" \
		--label "$${EVIDENCE_ARCHIVE_LABEL}" --execute

archive-partial-research-incident-rollback:
	@test -n "$${EVIDENCE_SOURCE_COMMIT:-}" || \
		(echo "EVIDENCE_SOURCE_COMMIT must identify the interrupted incident archive." >&2; exit 2)
	@test -n "$${EVIDENCE_ARCHIVE_LABEL:-}" || \
		(echo "EVIDENCE_ARCHIVE_LABEL must identify the interrupted incident archive." >&2; exit 2)
	$(UV) run python scripts/archive_partial_research_incident.py \
		--expected-source-commit "$${EVIDENCE_SOURCE_COMMIT}" \
		--label "$${EVIDENCE_ARCHIVE_LABEL}" --rollback-staging

report:
	$(UV) run python -m analysis.scripts.build_report results/raw --output results/reports \
		--require-publishable

report-diagnostic:
	$(UV) run python -m analysis.scripts.build_report results/raw --output results/reports

demo-prepare:
	$(UV) run python scripts/prepare_spark_ui_demo.py \
		--experiment $(DEMO_EXPERIMENT) --pair $(DEMO_PAIR)

demo-prepare-diagnostic:
	$(UV) run python scripts/prepare_spark_ui_demo.py \
		--experiment $(DEMO_EXPERIMENT) --pair $(DEMO_PAIR) --allow-diagnostic

demo-ui: setup demo-prepare
	docker compose --env-file .env --env-file $(DEMO_ENV) --profile demo \
		up -d --wait --wait-timeout 120 spark-history
	$(UV) run python scripts/verify_spark_ui_demo.py --wait-seconds 30
	@echo "Spark History Server: http://127.0.0.1:18080"

demo-ui-diagnostic: setup demo-prepare-diagnostic
	docker compose --env-file .env --env-file $(DEMO_ENV) --profile demo \
		up -d --wait --wait-timeout 120 spark-history
	$(UV) run python scripts/verify_spark_ui_demo.py \
		--allow-diagnostic --wait-seconds 30
	@echo "Spark History Server (diagnostic rehearsal): http://127.0.0.1:18080"

demo-ui-verify:
	$(UV) run python scripts/verify_spark_ui_demo.py --wait-seconds 30

demo-ui-verify-diagnostic:
	$(UV) run python scripts/verify_spark_ui_demo.py \
		--allow-diagnostic --wait-seconds 30

demo-ui-down:
	docker compose --profile demo stop spark-history

demo-video-finalize:
	$(UV) run python scripts/finalize_demo_video.py \
		--video $(DEMO_VIDEO) $(VIDEO_VISUAL_REVIEW_FLAG) $(VIDEO_PLAYBACK_FLAG)

demo-video-finalize-diagnostic:
	$(UV) run python scripts/finalize_demo_video.py \
		--video $(DEMO_VIDEO) --allow-diagnostic

presentation-finalize:
	$(UV) run python scripts/finalize_presentation.py \
		--presentation $(PRESENTATION) $(PRESENTATION_VISUAL_REVIEW_FLAG)

presentation-finalize-diagnostic:
	$(UV) run python scripts/finalize_presentation.py \
		--presentation $(PRESENTATION) --allow-diagnostic

evidence-bundle:
	$(UV) run python scripts/build_evidence_bundle.py \
		--output $(RELEASE_BUNDLE) \
		--presentation-manifest $(PRESENTATION_MANIFEST) \
		--video-manifest $(VIDEO_MANIFEST)

verify-evidence-bundle:
	$(UV) run python scripts/verify_evidence_bundle.py $(RELEASE_BUNDLE)

release:
	$(MAKE) report
	$(MAKE) presentation-finalize
	$(MAKE) demo-video-finalize
	$(MAKE) evidence-bundle
	$(MAKE) verify-evidence-bundle

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
