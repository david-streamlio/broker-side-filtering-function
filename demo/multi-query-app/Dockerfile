# syntax=docker/dockerfile:1.6
#
# Two-stage: Maven build (against the parent reactor) → JRE 17 runtime.
# Mounts ~/.m2/settings.xml as a build secret so internal Maven repos work
# without baking creds into image layers.

FROM maven:3.9.9-eclipse-temurin-17 AS builder
WORKDIR /build

# Bring in the full parent reactor so the demo module can reference its parent
# pom. Copy `pom.xml` + `common/` + `sdk/` + the demo module; the rest of the
# repo is unnecessary for this image.
COPY pom.xml ./
COPY common ./common
COPY sdk ./sdk
COPY demo/multi-query-app ./demo/multi-query-app

# Warm the dep cache against the demo module only.
RUN --mount=type=secret,id=mvn-settings,target=/root/.m2/settings.xml \
    mvn -B -pl demo/multi-query-app -am -DskipTests dependency:go-offline || true

# Build the shaded jar.
RUN --mount=type=secret,id=mvn-settings,target=/root/.m2/settings.xml \
    mvn -B -pl demo/multi-query-app -am -DskipTests package

# ── Runtime ──────────────────────────────────────────────────────────────────
FROM eclipse-temurin:17-jre
WORKDIR /app

COPY --from=builder /build/demo/multi-query-app/target/multi-query-demo.jar /app/app.jar
# queries.yaml is mounted at runtime via docker-compose so operators can edit
# it without rebuilding the image.

ENV JAVA_OPTS="-XX:MaxRAMPercentage=75.0"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
