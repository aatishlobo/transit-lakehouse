.PHONY: venv install test poll-once poll poll-bg poll-status poll-stop \
        profile profile-json fixture profile-fixture clean clean-archive \
        kafka-up kafka-down kafka-topics kafka-logs replay-sample \
        replay resolve aggregate evaluate demo features train predict

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
