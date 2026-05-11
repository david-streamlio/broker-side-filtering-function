# Orchestrator Function — implementation tracking

Plan-of-record lives at `/Users/david/.claude/plans/now-we-need-to-ancient-dragonfly.md`.
This file tracks progress against that plan.

## Locked design decisions

- Schema mismatch on existing registry → REJECT
- outputTopic + queryId are SDK-generated
- NAR distribution via Pulsar package URL pre-upload
- Command field: REGISTER (subscribe), MODIFY (set/add/update), UNREGISTER (close/delete)
- UNREGISTER on unknown queryId → idempotent
- REGISTER on existing queryId: idempotent if fields match, reject if differ
- No SDK←orchestrator response channel in v1
- No idle-function teardown in v1
- Orchestrator parallelism = 1

## Phases

### Phase 1 — Wire format + common types

- ☐ Add `protocol/filter-request.schema.json` (JSON Schema draft-07, additionalProperties:false, oneOf for inputTopics/topicsPattern on REGISTER)
- ☐ Add `common/.../FilterRequest.java` (Jackson @JsonCreator, validate() method)
- ☐ Add `common/.../ClientType.java` (enum KAFKA, PULSAR)
- ☐ Add `common/.../Command.java` (enum REGISTER, MODIFY, UNREGISTER)
- ☐ Add `common/.../FilterRequestTests.java` (serde, validation, command-specific field rules)
- ☐ `mvn -pl common test` green

### Phase 2 — SDK migration to FilterRequest

- ☐ Strip `Selectors.topicEquals/topicIn/combine/MessageBindingNames` (keep validate())
- ☐ Drop `PrivateQuery.topicConstraint()`
- ☐ Refactor `Session.publishQuery` → `publishFilterRequest(queryId, command, ...)`, separate paths per command
- ☐ New `Session.registerPrivateQuery(...)` overload accepting serializers + ClientType
- ☐ Wire `FilterableConsumerBuilder` (Pulsar) to pass ClientType.PULSAR + null serializers
- ☐ Wire `FilterableKafkaConsumerBuilder` to extract Kafka serializer FQNs + pass ClientType.KAFKA
- ☐ Update `SessionTests` for new wire shape
- ☐ Update `SelectorsTests` to cover only validate()
- ☐ Update Kafka SDK unit tests for serializer/clientType assertions
- ☐ Mark `SessionIT` and `KafkaConsumerKoPIT` `@Disabled` with "fixed in Phase 6"
- ☐ `mvn -pl common,sdk/java/pulsar,sdk/java/kafka test` green

### Phase 3 — Orchestrator skeleton (no spawn yet)

- ☐ Add `functions/orchestrator/pom.xml` + plumbing (services descriptor, logback.xml)
- ☐ Register module in reactor `pom.xml`
- ☐ Add `OrchestratorConfig.java`
- ☐ Add `TopicKeySanitizer.java` + tests
- ☐ Add `RegistryEntry.java` with `check(FilterRequest)` + `mergedWith()` + compat matrix tests
- ☐ Add `RegistryStore.java` (TableView facade, t:/q: keyspaces) + tests
- ☐ Add `OrchestratorFunction.java` initial: parse FilterRequest, validate, registry lookup, publish QueryMessage via mock-spawner
- ☐ Add `OrchestratorFunctionTests.java` (REGISTER/MODIFY/UNREGISTER, idempotency, schema mismatch, fan-out)
- ☐ `mvn -pl common,functions/orchestrator test` green
- ☐ Orchestrator NAR builds

### Phase 4 — Spawner integration

- ☐ Add `FilterFunctionSpawner.java`: `getFunctionStatus` first, `createFunction` on NotFound, leader-retry pattern, AlreadyExists → success
- ☐ Per-function control topic compaction setup via `topicPolicies().setCompactionThreshold`
- ☐ ConcurrentHashMap<String, CompletableFuture<RegistryEntry>> in-flight collapse
- ☐ Spawner unit tests with mocked admin
- ☐ `mvn -pl functions/orchestrator test` green

### Phase 5 — Orchestrator flagship IT

- ☐ Add `OrchestratorFunctionIT.java`: testcontainer setup, NAR package upload via `admin.packages().upload(...)`, orchestrator deployment
- ☐ Test: happy-path REGISTER → spawn → end-to-end filter output
- ☐ Test: REGISTER redelivery idempotency
- ☐ Test: schema-mismatch rejection
- ☐ Test: MODIFY hot reload
- ☐ Test: UNREGISTER tombstone propagation
- ☐ Test: multi-input fan-out
- ☐ Test: topicsPattern
- ☐ `mvn -pl functions/selector-filter,functions/orchestrator verify` green

### Phase 6 — SDK ITs rewired to orchestrator path

- ☐ `SessionIT.@BeforeAll` uploads filter NAR as package, deploys orchestrator, points Session.controlTopic at orchestrator's input
- ☐ Remove `@Disabled` from `SessionIT` (Phase 2 leftover)
- ☐ `KafkaConsumerKoPIT.@BeforeAll` same orchestrator-fronted setup
- ☐ Remove `@Disabled` from `KafkaConsumerKoPIT`
- ☐ Reactor `mvn -B clean verify` green

### Phase 7 — Docs + deployment artifacts

- ☐ `functions/orchestrator/deployment/orchestrator-function-config.yaml`
- ☐ `functions/orchestrator/src/main/docker/Dockerfile`
- ☐ README architecture diagram + deployment sequence
- ☐ README orchestrator userConfig reference
- ☐ README operator playbook (NAR upload, package versioning, permissions)
- ☐ README pre-1.0 clean-break note
- ☐ Final reactor `mvn -B clean verify` green

## Review (filled in after each phase)

_TBD_
