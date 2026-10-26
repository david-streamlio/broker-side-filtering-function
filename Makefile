# Convenience targets. Most work goes through mvn (Java) or pytest (Python); this Makefile
# is a thin layer for the deploy/ IaC smoke tests + repeatable cross-cutting actions.

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-32s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# ---------------------------------------------------------------- builds

.PHONY: package
package: ## Build all reactor NARs (selector-filter + orchestrator)
	mvn -B -pl functions/selector-filter,functions/orchestrator package -DskipTests

.PHONY: clean
clean: ## Reactor-wide mvn clean
	mvn -B clean

# ---------------------------------------------------------------- deploy smoke

.PHONY: deploy-smoke-helm
deploy-smoke-helm: package ## Kind + Pulsar Helm + Terraform apply + Python SDK round-trip
	bash deploy/terraform/pulsar-helm/smoke/kind-bootstrap.sh

.PHONY: deploy-smoke-sn-cloud
deploy-smoke-sn-cloud: ## SN Cloud smoke (requires TF_VAR_streamnative_admin_token)
	@[ -n "$$TF_VAR_streamnative_admin_token" ] || { \
	  echo "ERROR: TF_VAR_streamnative_admin_token must be set" >&2; exit 1; }
	bash deploy/terraform/sn-cloud/smoke/manual.sh

# ---------------------------------------------------------------- terraform sanity

.PHONY: tf-fmt
tf-fmt: ## terraform fmt across all of deploy/terraform/
	terraform -chdir=deploy/terraform/modules/orchestrator-topology fmt -recursive
	terraform -chdir=deploy/terraform/modules/orchestrator-function fmt -recursive
	terraform -chdir=deploy/terraform/sn-cloud fmt -recursive
	terraform -chdir=deploy/terraform/pulsar-helm fmt -recursive

.PHONY: tf-validate
tf-validate: ## terraform init + validate across all wrappers (offline-friendly except provider download)
	terraform -chdir=deploy/terraform/modules/orchestrator-function init -backend=false >/dev/null
	terraform -chdir=deploy/terraform/modules/orchestrator-function validate
	terraform -chdir=deploy/terraform/modules/orchestrator-topology init -backend=false >/dev/null
	terraform -chdir=deploy/terraform/modules/orchestrator-topology validate
	terraform -chdir=deploy/terraform/sn-cloud init -backend=false >/dev/null
	terraform -chdir=deploy/terraform/sn-cloud validate
	terraform -chdir=deploy/terraform/pulsar-helm init -backend=false >/dev/null
	terraform -chdir=deploy/terraform/pulsar-helm validate

# ---------------------------------------------------------------- combined demo
# One-command bring-up of the filtering stack + the broker-side-filter-observability
# UI (http://localhost:3200). Pinned to the v1.2.0 GA; override SELECTOR_REL /
# OBSERVABILITY_IMAGE_TAG to run a different release.

SELECTOR_REPO ?= david-streamlio/broker-side-filtering-function
SELECTOR_REL  ?= v1.2.0

.PHONY: demo-nars
demo-nars: ## Fetch the v1.2.0 orchestrator + filter NARs into demo/local/nars (local-mode prereq)
	rm -f demo/local/nars/*.nar
	gh release download $(SELECTOR_REL) --repo $(SELECTOR_REPO) --dir demo/local/nars --pattern '*.nar'

.PHONY: demo-local
demo-local: demo-nars ## Local-mode stock-quotes demo (detached; observability UI on :3200)
	docker compose -f demo/docker-compose.local.yml up --build -d
	@echo ""
	@echo "✅ Started detached (~1-2 min to warm up). UI: http://localhost:3200"
	@echo "   logs: docker compose -f demo/docker-compose.local.yml logs -f   |   stop: make demo-down"

.PHONY: demo-local-parcels
demo-local-parcels: demo-nars ## Local-mode logistics/parcels demo (detached; 3 source topics + UI)
	docker compose -f demo/docker-compose.parcels.local.yml up --build -d
	@echo ""
	@echo "✅ Started detached (~1-2 min to warm up). UI: http://localhost:3200"
	@echo "   logs: docker compose -f demo/docker-compose.parcels.local.yml logs -f   |   stop: make demo-down"

.PHONY: demo-cloud
demo-cloud: ## Cloud-mode stock-quotes demo against SN Cloud (detached; needs demo/.env + token)
	@[ -f demo/.env ] || { echo "ERROR: demo/.env not found — run demo/infra/terraform/recreate.sh (it writes demo/.env)" >&2; exit 1; }
	docker compose -f demo/docker-compose.yml up --build -d
	@echo ""
	@echo "✅ Started detached. UI: http://localhost:3200"
	@echo "   logs: docker compose -f demo/docker-compose.yml logs -f   |   stop: make demo-down"

.PHONY: demo-cloud-parcels
demo-cloud-parcels: ## Cloud-mode logistics/parcels demo against SN Cloud (detached; needs demo/.env + token)
	@[ -f demo/.env ] || { echo "ERROR: demo/.env not found — run demo/infra/terraform/recreate.sh (it writes demo/.env)" >&2; exit 1; }
	docker compose -f demo/docker-compose.parcels.yml up --build -d
	@echo ""
	@echo "✅ Started detached. UI: http://localhost:3200"
	@echo "   logs: docker compose -f demo/docker-compose.parcels.yml logs -f   |   stop: make demo-down"

.PHONY: demo-down
# `-t 30` gives multi-query-app's JVM shutdown hook time to close its consumers,
# which makes the SDK send UNREGISTER for every queryId before the container is
# killed. The orchestrator then tombstones those queries and GCs the spawned
# filter functions on the cluster — without it, docker's default 10s grace can
# SIGKILL mid-UNREGISTER and leave orphaned filters running on the broker.
demo-down: ## Tear down any running demo stack + UNREGISTER queries (graceful 30s stop; safe if not up)
	-docker compose -f demo/docker-compose.local.yml down -v -t 30
	-docker compose -f demo/docker-compose.parcels.local.yml down -v -t 30
	-docker compose -f demo/docker-compose.yml down -v -t 30
	-docker compose -f demo/docker-compose.parcels.yml down -v -t 30

.PHONY: reset-demo-cloud
# Depends on demo-down so shadowtraffic + multi-query-app are guaranteed-stopped
# before topic deletion — otherwise the still-running producer would auto-recreate
# the source topics (auto-topic-creation is on for public/default) the moment we
# delete them, undoing the reset.
reset-demo-cloud: demo-down ## Wipe spawned filters, old NAR packages, and demo topics on SN Cloud (destructive; auto-runs demo-down first)
	bash demo/infra/terraform/reset-cloud.sh

# ---------------------------------------------------------------- time-windowed read demo
# The stage scene for "Stop Scanning the Whole Topic": Apache Pulsar standalone, order traffic, and
# one order's events read three ways (demo/window/README.md). Build first: make window-demo-build.

WINDOW_DEMO_VENV    := demo/window/.venv
WINDOW_DEMO_COMPOSE := docker compose -p window-demo -f demo/window/docker-compose.yml

$(WINDOW_DEMO_VENV):
	python3 -m venv $@
	$@/bin/pip install -q pulsar-client==3.5.0

.PHONY: window-demo-build
window-demo-build: ## Build what the time-windowed read demo mounts and runs (NARs, bench clients, MCP server)
	mvn -B -pl functions/selector-filter,functions/orchestrator,bench/clients,mcp/entity-events -am package -DskipTests

.PHONY: window-demo
window-demo: $(WINDOW_DEMO_VENV) ## Time-windowed read stage demo from a clean broker (PAUSE=1 waits before each act)
	$(WINDOW_DEMO_VENV)/bin/python demo/window/window_demo.py $(if $(PAUSE),--pause)

.PHONY: window-demo-down
window-demo-down: ## Stop the time-windowed read demo's broker and delete its data
	$(WINDOW_DEMO_COMPOSE) down -v
