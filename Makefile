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
# UI (http://localhost:3200). Pinned to the v1.0.0 GA; override SELECTOR_REL /
# OBSERVABILITY_IMAGE_TAG to run a different release.

SELECTOR_REPO ?= david-streamlio/broker-side-filtering-function
SELECTOR_REL  ?= v1.0.0

.PHONY: demo-nars
demo-nars: ## Fetch the v1.0.0 orchestrator + filter NARs into demo/local/nars (local-mode prereq)
	rm -f demo/local/nars/*.nar
	gh release download $(SELECTOR_REL) --repo $(SELECTOR_REPO) --dir demo/local/nars --pattern '*.nar'

.PHONY: demo-local
demo-local: demo-nars ## Local-mode stock-quotes demo (Pulsar standalone + observability UI on :3200)
	docker compose -f demo/docker-compose.local.yml up --build

.PHONY: demo-local-parcels
demo-local-parcels: demo-nars ## Local-mode logistics/parcels demo (3 source topics + observability UI)
	docker compose -f demo/docker-compose.parcels.local.yml up --build

.PHONY: demo-cloud
demo-cloud: ## Cloud-mode stock-quotes demo against SN Cloud (needs demo/.env + token; run recreate.sh first)
	@[ -f demo/.env ] || { echo "ERROR: demo/.env not found — cp demo/.env.example demo/.env and fill it in" >&2; exit 1; }
	docker compose -f demo/docker-compose.yml up --build

.PHONY: demo-cloud-parcels
demo-cloud-parcels: ## Cloud-mode logistics/parcels demo against SN Cloud (needs demo/.env + token)
	@[ -f demo/.env ] || { echo "ERROR: demo/.env not found — cp demo/.env.example demo/.env and fill it in" >&2; exit 1; }
	docker compose -f demo/docker-compose.parcels.yml up --build

.PHONY: demo-down
demo-down: ## Tear down any running demo stack (all four compose files; safe if not up)
	-docker compose -f demo/docker-compose.local.yml down -v
	-docker compose -f demo/docker-compose.parcels.local.yml down -v
	-docker compose -f demo/docker-compose.yml down -v
	-docker compose -f demo/docker-compose.parcels.yml down -v
