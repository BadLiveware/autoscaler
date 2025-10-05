## VPA External Telemetry Integration – Remaining Work Plan

### Goals Recap
- Allow each VPA to declare its telemetry source (initially Kubernetes metrics-server or Prometheus) via the CRD.
- Feed resource usage samples and OOM events from the configured source into the recommender.
- Maintain compatibility with existing defaults when telemetry config is absent.

### Current State
- CRD exposes `spec.telemetry`, supporting `source: Kubernetes|Prometheus` with Prometheus-specific options and auth.
- Generated deepcopy helpers updated accordingly.
- Metrics snapshots now carry optional OOM counters, but upstream logic ignores them.
- No runtime wiring yet between CRD telemetry settings and recommender components.
- OpenTelemetry support deferred.

### Workstreams & Tasks

1. [x] **Recommender Configuration Plumbing**
  - [x] Parse `spec.telemetry` when loading VPAs, validate per-source constraints.
  - [x] Propagate effective telemetry config into `ClusterStateFeederFactory` (or new settings struct).
  - [x] Ensure flags / global defaults continue to work when telemetry config is unset.
  - [x] **Testing:** Unit tests verify config parsing and defaults (`pkg/recommender/model/...` tests pass).
2. [x] **Metrics Source Selection**
  - [x] Extend `input/metrics.NewMetricsClient` (and feeder wiring) to instantiate the appropriate source.
  - [x] Allow per-VPA namespace scoping and query overrides defined in `PrometheusTelemetry`.
  - [x] Add error reporting / VPA conditions when telemetry source cannot be initialized.
  - [x] **Testing:** Existing metrics tests cover routing logic; validation runs in feeder tests.
3. [x] **Prometheus Runtime Client**
  - [x] Implement/adapt Prometheus query layer for resource usage aligned with expectations.
  - [x] Query cumulative OOM counters per container with configurable metric names.
  - [x] Handle label mapping, namespace filtering, authentication, and query timeouts.
  - [x] **Testing:** Unit tests for Prometheus query builder and response parsing.
4. [x] **OOM Counter Integration**
  - [x] Track previous OOM counter values to compute deltas per run.
  - [x] Invoke `RecordOOM` (or extension) when counters increase, capturing timestamp/memory if possible.
  - [x] Provide fallback to pod-status detection when counters unavailable.
  - [x] **Testing:** Tests for OOM delta tracking and spike generation logic.
5. [x] **Validation & Defaults**
  - [x] Add API validation for incompatible combinations (e.g., Prometheus source without address).
  - [x] Document precedence between CRD telemetry config and process flags; maintain backwards compatibility.
  - [x] Provide default metric names aligned with cAdvisor/kube-state metrics.
  - [x] **Testing:** Validation tests for various config combinations.
6. [x] **Observability & Error Handling**
  - [x] Emit metrics/logs for telemetry query success/failure and OOM ingestion events.
  - [x] Surface new VPA conditions for telemetry failures (ConfigUnsupported for invalid config).
  - [x] **Testing:** Verify metrics emission and condition updates in error scenarios.
7. [x] **Integration & E2E Testing**
  - [x] Integration tests with fake Prometheus server to verify full pipeline.
  - [x] E2E tests demonstrating VPA with Prometheus telemetry source.
    - [x] Prometheus deployment manifest with cadvisor scraping (RBAC includes nodes/proxy).
    - [x] Pushgateway deployment for injecting fake OOM metrics in tests.
    - [x] E2E test infrastructure updated to optionally deploy Prometheus via DEPLOY_PROMETHEUS env var.
    - [x] Added `prometheus-telemetry` suite option to `hack/run-e2e-locally.sh`.
    - [x] Tests labeled with "PrometheusRequired" for selective execution.
    - [x] Pushgateway utilities for pushing fake metrics from tests.
    - [x] OOM detection test using Pushgateway to simulate OOM counter deltas.
  - [x] Verify fallback behavior when Prometheus is unavailable.
8. [x] **Documentation & Examples**
  - [x] Update CRD docs/examples to demonstrate `spec.telemetry` usage.
  - [x] Provide sample Prometheus configuration.
  - [x] Mention OpenTelemetry as future work or Prometheus gateway workaround.

### Risks & Mitigations
- **Prometheus performance:** use efficient selectors, limit query scope/timeouts.
- **OOM counter semantics:** ensure chosen metrics are cumulative and per container; document limitations.
- **Per-VPA configuration divergence:** log clearly when telemetry differs from global defaults.

### Out of Scope
- Direct OpenTelemetry ingestion (pending future design).
- Admission controller/updater telemetry changes (focus on recommender path).

### Implementation Notes
- **OOM Counter Delta Tracking:** Fixed to use container memory request (not actual usage) when recording OOM events, matching the existing Kubernetes OOM observer behavior. This ensures recommendations increase appropriately (e.g., 250Mi → ~422Mi with 3 OOM events).
- **E2E Testing:** Pushgateway-based OOM simulation proved reliable for testing OOM detection without triggering actual container OOMs.
- **Partial Custom Queries:** The system correctly handles cases where only some queries (e.g., OOM) are customized, falling back to defaults for others.

### Completion Status
All workstreams completed and tested. E2E tests pass with both basic Prometheus metrics and OOM detection from custom counters.
