/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.streamnative.filter.perf;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

/**
 * One benchmark run's result, serialised under {@code perf/target/reports/&lt;name&gt;-&lt;sha&gt;.json}
 * per the schema in {@code tasks/todo-perf-shadowtraffic.md}. Field names follow snake_case so
 * the on-disk JSON can be aggregated across runs by a downstream regression-chart script without
 * remapping.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "benchmark", "git_sha", "ran_at", "duration_sec",
        "target_rate", "achieved_rate", "output_rate",
        "latency_ms", "broker_cpu_avg", "filter_cpu_avg", "notes"
})
final class BenchmarkResult {

    @JsonProperty("benchmark")     final String benchmark;
    @JsonProperty("git_sha")       final String gitSha;
    @JsonProperty("ran_at")        final String ranAt;
    @JsonProperty("duration_sec")  final int    durationSec;
    @JsonProperty("target_rate")   final long   targetRate;
    @JsonProperty("achieved_rate") final double achievedRate;
    @JsonProperty("output_rate")   final double outputRate;
    @JsonProperty("latency_ms")    final LatencyMs latencyMs;
    @JsonProperty("broker_cpu_avg") final Double brokerCpuAvg;
    @JsonProperty("filter_cpu_avg") final Double filterCpuAvg;
    @JsonProperty("notes")          final String notes;

    private BenchmarkResult(Builder b) {
        this.benchmark    = b.benchmark;
        this.gitSha       = b.gitSha;
        this.ranAt        = b.ranAt;
        this.durationSec  = b.durationSec;
        this.targetRate   = b.targetRate;
        this.achievedRate = b.achievedRate;
        this.outputRate   = b.outputRate;
        this.latencyMs    = b.latencyMs;
        this.brokerCpuAvg = b.brokerCpuAvg;
        this.filterCpuAvg = b.filterCpuAvg;
        this.notes        = b.notes;
    }

    static Builder builder() {
        return new Builder();
    }

    /** Latency percentiles in milliseconds. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"p50", "p95", "p99", "max"})
    static final class LatencyMs {
        @JsonProperty("p50") final double p50;
        @JsonProperty("p95") final double p95;
        @JsonProperty("p99") final double p99;
        @JsonProperty("max") final double max;

        LatencyMs(double p50, double p95, double p99, double max) {
            this.p50 = p50;
            this.p95 = p95;
            this.p99 = p99;
            this.max = max;
        }
    }

    /**
     * Writes this result as JSON under {@code <perfModuleDir>/target/reports/<bench>-<git-sha>.json}.
     * Creates the directory if absent; overwrites any existing file for the same SHA so re-runs
     * of the same commit replace earlier numbers.
     *
     * @return the written path, for logging
     */
    Path writeJson() throws IOException {
        Path moduleDir = Path.of(System.getProperty("user.dir"));
        Path reports = moduleDir.resolve("target").resolve("reports");
        Files.createDirectories(reports);
        Path out = reports.resolve(benchmark + "-" + gitSha + ".json");

        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        Files.writeString(out, mapper.writeValueAsString(this), StandardCharsets.UTF_8);
        return out;
    }

    /**
     * Best-effort short git SHA via {@code git rev-parse --short HEAD}. Returns {@code "unknown"}
     * if git is not on PATH or the process is not in a checkout (e.g. a CI tarball build).
     */
    static String shortGitSha() {
        try {
            Process p = new ProcessBuilder("git", "rev-parse", "--short", "HEAD")
                    .redirectErrorStream(true)
                    .start();
            try (BufferedReader r = new BufferedReader(
                    new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                String line = r.readLine();
                p.waitFor();
                if (line != null && !line.isBlank() && p.exitValue() == 0) {
                    return line.trim();
                }
            }
        } catch (Exception ignored) {
            // fall through
        }
        return "unknown";
    }

    static final class Builder {
        private String benchmark;
        private String gitSha = shortGitSha();
        private String ranAt = Instant.now().toString();
        private int    durationSec;
        private long   targetRate;
        private double achievedRate;
        private double outputRate;
        private LatencyMs latencyMs;
        private Double brokerCpuAvg;
        private Double filterCpuAvg;
        private String notes;

        Builder benchmark(String v)    { this.benchmark = v; return this; }
        Builder gitSha(String v)       { this.gitSha = v; return this; }
        Builder ranAt(String v)        { this.ranAt = v; return this; }
        Builder durationSec(int v)     { this.durationSec = v; return this; }
        Builder targetRate(long v)     { this.targetRate = v; return this; }
        Builder achievedRate(double v) { this.achievedRate = v; return this; }
        Builder outputRate(double v)   { this.outputRate = v; return this; }
        Builder latencyMs(LatencyMs v) { this.latencyMs = v; return this; }
        Builder brokerCpuAvg(Double v) { this.brokerCpuAvg = v; return this; }
        Builder filterCpuAvg(Double v) { this.filterCpuAvg = v; return this; }
        Builder notes(String v)        { this.notes = v; return this; }

        BenchmarkResult build() {
            if (benchmark == null) {
                throw new IllegalStateException("benchmark name is required");
            }
            return new BenchmarkResult(this);
        }
    }
}
