.PHONY: venv install test poll-once poll poll-bg poll-status poll-stop \
        profile profile-json fixture profile-fixture clean clean-archive \
        kafka-up kafka-down kafka-topics kafka-logs replay-sample \
        replay resolve aggregate evaluate demo demo-clean features train predict \
        venv-spark bronze silver gold lake lake-test lake-clean \
        static dim otp dbt-run dbt-test marts serve-export serve \
        dagster-ui dagster-run k8s-validate full \
        bronze-stream bronze-stream-continuous stage-gtfs dbt-snapshot

# Use the isolated venv explicitly rather than whatever `python` resolves to.
# This service pins protobuf <7.0 for compatibility with the ML stack; picking
# up a system interpreter silently changes the decode runtime, which is the
# thing DECODER_VERSION/protobuf_runtime exist to make traceable.
PY := .venv/bin/python

# The live archive. Unrecoverable: GTFS-RT has no history endpoint, so anything
# deleted here is gone permanently. No target may remove this by default.
DATA_ROOT ?= data

# Synthetic archive, kept in a SEPARATE root. It used to share `data/`, which
# meant `make fixture` -- an innocuous keyless demo -- would silently destroy a
# live archive. Separate roots make that collision impossible rather than
# merely unlikely.
FIXTURE_ROOT ?= fixture_data

# Isolated venv. Recommended: this is a standalone ingest service and has no
# reason to share an environment with mlflow/databricks/spark. Doing so is
# how a protobuf pin turns into a broken ML workspace.
venv:
	python3 -m venv .venv && .venv/bin/pip install -q -r requirements.txt
	@echo "activate with: source .venv/bin/activate"

install:
	pip install -r requirements.txt

test:
	$(PY) -m pytest tests/ -q

fixture:            ## synthetic archive, no API key needed
	rm -rf $(FIXTURE_ROOT) && $(PY) tests/make_fixture.py $(FIXTURE_ROOT)

profile-fixture:    ## profile the synthetic archive
	$(PY) profiling/profile_feed.py --data-root $(FIXTURE_ROOT)

poll-once:
	$(PY) -m ingest.poller.poller --once

poll:               ## foreground, single instance
	$(PY) -m ingest.poller.poller

poll-bg:            ## supervised background run (auto-restart, single-instance lock)
	@nohup scripts/run_poller.sh >/dev/null 2>&1 & echo "supervisor started"

poll-status:
	@scripts/run_poller.sh --status

poll-stop:
	@scripts/run_poller.sh --stop

# ---- Kafka (local, single broker, KRaft) --------------------------------

kafka-up:           ## start Kafka and create topics; waits until ready
	docker compose up -d
	@echo "waiting for topic creation..."
	@docker compose wait kafka-init >/dev/null 2>&1 || true
	@$(MAKE) --no-print-directory kafka-topics

kafka-down:         ## stop Kafka. -v also drops the log, so replays start clean
	docker compose down -v

kafka-topics:
	@docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 --describe 2>/dev/null \
		| grep -E "^Topic:" || echo "kafka not running -- try: make kafka-up"

kafka-logs:
	docker compose logs -f kafka

# ---- Pipeline -----------------------------------------------------------

replay:             ## archive -> Kafka
	$(PY) -m streaming.producer --source data/replay_sample --speed 0

resolve:            ## Kafka positions -> derived arrivals
	$(PY) -m streaming.consumer --from-beginning --idle-timeout-s 15 \
		--out outputs/arrival_events.jsonl

aggregate:          ## arrivals + predictions -> outputs/
	$(PY) -m streaming.aggregator --agency $(AGENCY) --idle-timeout-s 15

evaluate:           ## acceptance checks -> evaluation/
	$(PY) evaluation/run_acceptance_checks.py

AGENCY ?= SF

demo: kafka-up      ## THE ONE COMMAND: full pipeline from committed sample
	@rm -f outputs/arrival_events.jsonl
	$(MAKE) --no-print-directory replay
	$(MAKE) --no-print-directory resolve
	$(MAKE) --no-print-directory aggregate
	$(MAKE) --no-print-directory evaluate

# ---- Bounded AI element: ETA correction model ---------------------------

features:           ## rebuild the full feature table from the live archive
	$(PY) -m ml.build_features --agency $(AGENCY) --data-root $(DATA_ROOT)

train:              ## train + evaluate against baselines (uses shipped sample)
	$(PY) -m ml.train --features ml/data/features_sample.csv.gz

train-full:         ## train on the full local archive (not shipped)
	$(PY) -m ml.train --features ml/data/features.csv

predict:            ## demo one corrected ETA
	$(PY) -m ml.predict

# ---- Lakehouse: Spark Structured Streaming -> Delta ---------------------
#
# Deliberately a SEPARATE venv and interpreter from the ingest service. Spark 4
# needs Java 17+ and Python <=3.12; the ingest venv is 3.13 with a protobuf pin
# that exists for the ML stack. Sharing one environment is how a protobuf pin
# turns into a broken Spark install.

PY_SPARK := .venv-spark/bin/python
PY_SPARK_BIN := .venv-spark/bin
PWD := $(shell pwd)
JAVA_HOME ?= /opt/homebrew/opt/openjdk/libexec/openjdk.jdk/Contents/Home
LAKE_ROOT ?= lake
BRONZE_FILES ?= 120

venv-spark:         ## Spark/Delta venv (python3.12 + JDK 21)
	/opt/homebrew/bin/python3.12 -m venv .venv-spark && \
	  .venv-spark/bin/pip -q install --upgrade pip && \
	  .venv-spark/bin/pip -q install pyspark==4.0.0 delta-spark==4.0.0 \
	    'gtfs-realtime-bindings>=1.0' 'protobuf>=6.32,<7.0' pytest
	@echo "spark venv ready. JDK: $(JAVA_HOME)"

bronze:             ## archive -> Delta bronze (decode + explode, dumb and stable)
	JAVA_HOME=$(JAVA_HOME) $(PY_SPARK) -m spark.bronze \
	  --data-root $(DATA_ROOT) --lake-root $(LAKE_ROOT) --limit-files $(BRONZE_FILES)

silver:             ## bronze -> Delta silver (typed, deduped, flagged)
	JAVA_HOME=$(JAVA_HOME) $(PY_SPARK) -m spark.silver --lake-root $(LAKE_ROOT)

gold:               ## silver -> Delta gold fct_stop_arrival (idempotent MERGE)
	JAVA_HOME=$(JAVA_HOME) $(PY_SPARK) -m spark.gold --lake-root $(LAKE_ROOT)

lake: bronze silver gold   ## full medallion build from the live archive

lake-test:          ## lakehouse invariant tests (needs a built lake)
	JAVA_HOME=$(JAVA_HOME) $(PY_SPARK) -m pytest tests/test_lakehouse.py -q

# Drops the DERIVED lake only. The archive in $(DATA_ROOT) is untouched and
# every table here is rebuildable from it.
lake-clean:
	rm -rf $(LAKE_ROOT)

demo-clean:         ## demo from a torn-down Kafka -- use this for a live demo
	@echo "tearing down Kafka so the run starts from empty topics..."
	-@$(MAKE) --no-print-directory kafka-down
	@$(MAKE) --no-print-directory demo

# ---- Structured Streaming bronze ----------------------------------------
# Same decode as the batch job; the checkpoint replaces the hand-rolled
# processed-files table and commits atomically with the write.

bronze-stream:      ## consume the backlog, then stop (availableNow)
	JAVA_HOME=$(JAVA_HOME) $(PY_SPARK) -m spark.bronze_stream \
	  --feed vehicle_positions --data-root $(DATA_ROOT) --lake-root $(LAKE_ROOT)

bronze-stream-continuous:  ## stay up on a 120s trigger, matching the poller
	JAVA_HOME=$(JAVA_HOME) $(PY_SPARK) -m spark.bronze_stream \
	  --feed vehicle_positions --data-root $(DATA_ROOT) --lake-root $(LAKE_ROOT) \
	  --continuous

# ---- Schedule dimension + OTP -------------------------------------------

stage-gtfs:         ## extracted GTFS CSV -> Delta staging (no versioning)
	JAVA_HOME=$(JAVA_HOME) $(PY_SPARK) -m spark.stage_gtfs --lake-root $(LAKE_ROOT)

dbt-snapshot:       ## SCD2 schedule dimension, via dbt snapshot
	JAVA_HOME=$(JAVA_HOME) $(PY_SPARK_BIN)/dbt snapshot --project-dir dbt --profiles-dir dbt


static:             ## fetch + archive 511 GTFS-Static (counts against the quota)
	$(PY) -m ingest.static.fetch_static

dim:                ## GTFS-Static -> SCD2 dim_stop_schedule
	JAVA_HOME=$(JAVA_HOME) $(PY_SPARK) -m spark.dim_schedule --lake-root $(LAKE_ROOT)

otp:                ## as-of join arrivals to the schedule in force -> fct_stop_otp
	JAVA_HOME=$(JAVA_HOME) $(PY_SPARK) -m spark.fct_otp --lake-root $(LAKE_ROOT)

# ---- dbt ----------------------------------------------------------------
# Run from the repo root, never `cd dbt`: dbt chdirs into --project-dir, and
# anything resolving relative to the process CWD (Derby, relative paths) then
# lands somewhere different from the Spark jobs.

dbt-run:
	JAVA_HOME=$(JAVA_HOME) $(PY_SPARK_BIN)/dbt run --project-dir dbt --profiles-dir dbt

dbt-test:
	JAVA_HOME=$(JAVA_HOME) $(PY_SPARK_BIN)/dbt test --project-dir dbt --profiles-dir dbt

marts: dbt-run dbt-test

# ---- Serving ------------------------------------------------------------

serve-export:       ## marts -> compact SQLite for the always-on API
	JAVA_HOME=$(JAVA_HOME) $(PY_SPARK) -m serving.export_marts

serve:              ## run the dashboard + API (no Spark needed)
	$(PY) -m uvicorn serving.api:app --reload --port 8000

# ---- Orchestration ------------------------------------------------------

dagster-ui:         ## asset graph in a browser
	JAVA_HOME=$(JAVA_HOME) DAGSTER_HOME=$(PWD)/.dagster_home PYTHONPATH=$(PWD) \
	  $(PY_SPARK_BIN)/dagster dev -m orchestration.definitions

dagster-run:        ## materialise the whole graph
	JAVA_HOME=$(JAVA_HOME) DAGSTER_HOME=$(PWD)/.dagster_home PYTHONPATH=$(PWD) \
	  $(PY_SPARK_BIN)/dagster job execute -j daily_refresh -m orchestration.definitions

k8s-validate:
	$(PY) -c "import yaml,glob; [list(yaml.safe_load_all(open(f))) for f in glob.glob('k8s/*.yaml')]; print('manifests parse OK')"

# The whole lakehouse, in dependency order. Kafka path is separate (`make demo`).
full: lake stage-gtfs dbt-snapshot otp marts serve-export
	@echo "lakehouse rebuilt. `make serve` to view."

# ---- Replay sample ------------------------------------------------------

replay-sample:      ## cut the committed sample from the live archive
	$(PY) scripts/make_replay_sample.py --data-root $(DATA_ROOT) --polls 20

profile:
	$(PY) profiling/profile_feed.py --data-root $(DATA_ROOT)

profile-json:
	@mkdir -p docs
	$(PY) profiling/profile_feed.py --data-root $(DATA_ROOT) --json > docs/profile.json

# Deliberately does NOT touch $(DATA_ROOT). This target previously ran
# `rm -rf data`, which put the one irreplaceable artifact in the project behind
# the most reflexively-typed command in any Makefile.
clean:
	rm -rf $(FIXTURE_ROOT) __pycache__ .pytest_cache
	find . -name '__pycache__' -type d -prune -not -path './.venv/*' -exec rm -rf {} +

clean-archive:      ## DESTROYS the live archive. Requires CONFIRM=yes.
ifeq ($(CONFIRM),yes)
	rm -rf $(DATA_ROOT)
	@echo "archive deleted."
else
	@echo "Refusing to delete $(DATA_ROOT) -- GTFS-RT history cannot be re-fetched."
	@echo "If you are certain:  make clean-archive CONFIRM=yes"
	@exit 1
endif
