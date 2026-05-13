# TradeWise SDK adoption — publish artifacts so a real demo can consume them

## Goal

Make this repo's SDK + function NARs consumable by an external repo (StreamNative OSS
`Trade-Wise-app`) without `mvn install` on the developer's box. Specifically:

- Maven SNAPSHOTs of `selector-filter-common`, `selector-filter-client`, and
  `selector-filter-client-kafka` deployed to GitHub Packages on every push to `main`.
- The two function NARs (`selector-filter-orchestrator-<v>.nar`,
  `selector-filter-function-<v>.nar`) attached to GitHub Releases on `v*` tags so the
  consumer's `recreate.sh` can `curl` them by URL.

The downstream change in `Trade-Wise-app` (refactoring `topic-filter-service` to call
`Session.registerPrivateQuery / updatePrivateQuery / retirePrivateQuery`) is tracked
separately in that repo. This todo covers only **this repo's contribution**: PR A from
`/Users/david/.claude/plans/help-me-plan-a-sorted-koala.md`.

## Locked design decisions

- **Distribution**: GitHub Packages for Maven, GitHub Releases for NARs. Org `david-streamlio`.
- **Versioning**: SNAPSHOTs from `main`; cut a `v1.0.0-rc1` tag to validate the NAR-release
  path before promoting to `v1.0.0`.
- **Auth**: consumers configure a GitHub PAT in `~/.m2/settings.xml` with `read:packages`.
  CI uses the workflow's built-in `GITHUB_TOKEN`.
- **Scope of artifacts**:
  - Published JARs: `selector-filter-common`, `selector-filter-client`,
    `selector-filter-client-kafka` (plus any transitive parent POM Maven needs).
  - NOT published to Packages: function NARs (too large; not Maven-shaped). These go to
    Releases only.
- **No SDK code changes** — modules already exist and pass `mvn -B verify`. Only build/CI
  metadata is touched.

## Architecture

### Parent `pom.xml` change

```xml
<distributionManagement>
  <snapshotRepository>
    <id>github</id>
    <url>https://maven.pkg.github.com/david-streamlio/broker-side-filtering-function</url>
  </snapshotRepository>
  <repository>
    <id>github</id>
    <url>https://maven.pkg.github.com/david-streamlio/broker-side-filtering-function</url>
  </repository>
</distributionManagement>
```

`perf` module is excluded from deployment (benchmark, not a consumable artifact); add
`<maven.deploy.skip>true</maven.deploy.skip>` to `perf/pom.xml` if not already there.
Function NAR modules can be left deployable for now — they're small enough and harmless on
Packages; the canonical distribution stays the GitHub Release.

### `.github/workflows/publish-snapshots.yml`

Trigger: push to `main`. Permissions: `contents:read`, `packages:write`. Steps: checkout,
setup-java 17 (with server-id `github`), `mvn -B -DskipTests deploy`. Auth via
`GITHUB_TOKEN`.

### `.github/workflows/release-nars.yml`

Trigger: tag matching `v*`. Steps: checkout, setup-java 17, `mvn -B -DskipTests package`,
`softprops/action-gh-release@v2` uploading
`functions/orchestrator/target/selector-filter-orchestrator-*.nar` and
`functions/selector-filter/target/selector-filter-function-*.nar`.

### `README.md` addendum

Two short sections appended near the existing "Project layout" section:

1. **Consuming the SDK from another repo.** Maven `<repositories>` snippet pointing at
   `https://maven.pkg.github.com/david-streamlio/broker-side-filtering-function`, the three
   GAVs, and a `~/.m2/settings.xml` `<server id="github">` snippet showing where to put a
   `read:packages` PAT.

2. **SN Cloud `kafkaNamespace` caveat.** One paragraph: on SN Cloud, `kafkaNamespace` is set
   at cluster provisioning (not via the pulsar Terraform provider);
   `Session.privateTopicNamespace(...)` MUST equal that namespace's tail, or the Kafka SDK
   path will silently receive no records.

## Implementation order

1. ☐ `pom.xml` — add `<distributionManagement>`.
2. ☐ `.github/workflows/publish-snapshots.yml` — add deploy workflow.
3. ☐ `.github/workflows/release-nars.yml` — add release workflow.
4. ☐ `README.md` — append the two subsections.
5. ☐ Commit and push as a single PR. After merge:
   - Watch the publish-snapshots run end-to-end against Packages.
   - Cut tag `v1.0.0-rc1` and verify the release-nars run uploads both NARs.

## Verification

- `mvn -B -DskipTests deploy -Dmaven.deploy.skip=true` succeeds locally (sanity: parent POM
  parses; no module mistakenly tries to publish without `distributionManagement` inherited).
- After the workflow merges and runs, browse
  `https://github.com/david-streamlio/broker-side-filtering-function/packages` and confirm
  the three Maven artifacts are present at version `1.0.0-SNAPSHOT`.
- After the `v1.0.0-rc1` tag, browse the GitHub Release and confirm both NAR files are
  attached.
- From a scratch directory:
  ```bash
  cat > settings.xml <<EOF
  <settings><servers><server>
    <id>github</id><username>$GH_USER</username><password>$GH_PAT</password>
  </server></servers></settings>
  EOF
  mvn -s settings.xml dependency:get \
      -Dartifact=io.streamnative:selector-filter-client:1.0.0-SNAPSHOT \
      -DremoteRepositories=github::default::https://maven.pkg.github.com/david-streamlio/broker-side-filtering-function
  ```
  Resolves cleanly.

## Review

(To be filled in after PR merges.)
