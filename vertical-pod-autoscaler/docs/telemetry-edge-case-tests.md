# VPA Prometheus Telemetry - Edge Case Test Implementation Tracker

This document tracks the implementation status of edge case tests for VPA Prometheus telemetry integration.

**Test File:** `e2e/v1/telemetry_edge_cases.go`

## Implementation Status

### Phase 1: Core Resilience (HIGH PRIORITY)

| Test | Status | Priority | Notes |
|------|--------|----------|-------|
| Prometheus unreachable fallback | ✅ E2E Test | 🔴 Critical | Verifies production resilience when Prometheus is down |
| Multiple VPAs with mixed telemetry | ✅ E2E Test | 🔴 Critical | Real-world multi-tenant scenarios |
| OOM counter resets | ✅ Unit Test | 🔴 Critical | Handles Prometheus/pod restarts |
| Decreasing OOM counters | ✅ Unit Test | 🟡 Medium | Edge case for corrupted metrics |
| VPA with no matching pods | ✅ Unit Test | 🟡 Medium | Common misconfiguration |

**Phase 1 Progress:** 5/5 (100%) ✅

---

### Phase 2: Query Variations (MEDIUM PRIORITY)

| Test | Status | Priority | Notes |
|------|--------|----------|-------|
| Partial custom queries (OOM only) | ✅ Integration Test | 🟡 Medium | Most common customization |
| Partial custom queries (CPU only) | ✅ Integration Test | 🟢 Low | Less common but should work |
| Partial custom queries (Memory only) | ✅ Integration Test | 🟢 Low | Less common but should work |
| All custom queries | ✅ Integration Test | 🟡 Medium | Power user scenario |
| Metrics with missing labels | ✅ Unit Test | 🟡 Medium | Data validation |
| Query returning no results | ✅ Unit Test | 🟢 Low | Empty result handling |
| Queries that aggregate away labels | ✅ Unit Test | 🟡 Medium | User query validation |

**Phase 2 Progress:** 7/7 (100%) ✅

---

### Phase 3: Advanced Scenarios (MEDIUM PRIORITY)

| Test | Status | Priority | Notes |
|------|--------|----------|-------|
| High frequency OOM events | ✅ Integration Test | 🟡 Medium | Stress testing |
| Stale metrics handling | ✅ Integration Test | 🟡 Medium | Prometheus scraping stopped |
| Bearer token authentication | ⏳ TODO (E2E) | 🟡 Medium | Enterprise requirement |
| Basic authentication | ⏳ TODO (E2E) | 🟢 Low | Alternative auth method |
| OOM events across pod restarts | ⏳ TODO (E2E) | 🟡 Medium | Pod churn scenarios |

**Phase 3 Progress:** 2/5 (40%)

---

### Phase 4: Production Scenarios (LOW PRIORITY)

| Test | Status | Priority | Notes |
|------|--------|----------|-------|
| Slow query timeout | ✅ Integration Test | 🟢 Low | Performance edge case |
| Insecure TLS | ⏳ TODO (E2E) | 🟢 Low | Dev/test convenience |
| UpdateMode=Initial with Prometheus | ⏳ TODO (E2E) | 🟢 Low | Integration verification |
| UpdateMode=Off with Prometheus | ⏳ TODO (E2E) | 🟢 Low | Integration verification |
| StatefulSet target | ⏳ TODO (E2E) | 🟡 Medium | StatefulSet support |
| Prometheus server restarts | ⏳ TODO (E2E) | 🟡 Medium | Infrastructure resilience |
| Concurrent VPA operations | ⏳ TODO (E2E) | 🟢 Low | Scalability testing |

**Phase 4 Progress:** 1/7 (14%)

---

### Phase 5: Error Recovery (LOW PRIORITY)

| Test | Status | Priority | Notes |
|------|--------|----------|-------|
| Invalid Prometheus data | ✅ Integration Test | 🟡 Medium | Error handling (malformed JSON, missing fields, etc.) |
| NaN/Inf values | ✅ Unit Test | 🟢 Low | Special float handling |
| VPA config hot reload | ⏳ TODO (E2E) | 🟢 Low | Config update testing |
| Kubernetes → Prometheus migration | ⏳ TODO (E2E) | 🟢 Low | Migration path |
| Prometheus → Kubernetes migration | ⏳ TODO (E2E) | 🟢 Low | Rollback path |

**Phase 5 Progress:** 2/5 (40%)

---

## Overall Progress

**E2E Tests:** 12 remaining (reduced from 29 - removed tests already covered by unit/integration tests)
**Unit Tests:** 30 implemented ✅  
**Integration Tests:** 13 implemented ✅ (4 existing + 9 new)

**E2E Completion:** 14% (2 of 14 originally planned) - Phase 1 complete ✅
**Unit Test Coverage:** 30 tests (11 existing + 13 new + 6 dual OOM)
**Integration Test Coverage:** 9 new tests covering query variations, error handling, and production scenarios

**Test Efficiency:** 15 out of 29 originally planned E2E tests (52%) are now covered by faster unit/integration tests!

### Remaining E2E Tests (12 total - require real cluster)

These tests cannot be unit/integration tested because they require real Kubernetes infrastructure:

**Phase 1: Core Resilience** ✅ Complete
~~1. Prometheus unreachable fallback~~
~~2. Multiple VPAs with different telemetry sources~~

**Phase 3: Advanced Scenarios** (3 tests)
1. Bearer token authentication
2. Basic authentication
3. OOM events across pod restarts

**Phase 4: Production Scenarios** (6 tests)
4. Insecure TLS connections
5. UpdateMode=Initial with Prometheus
6. UpdateMode=Off with Prometheus
7. StatefulSet target
8. Prometheus server restarts
9. Concurrent VPA operations

**Phase 5: Error Recovery** (3 tests)
10. VPA config hot reload
11. Kubernetes → Prometheus migration
12. Prometheus → Kubernetes migration

---

## Unit Test Coverage (NEW)

The following unit tests have been implemented to cover edge cases at a lower, faster testing level:

### Label Extraction Tests (`telemetry_edge_cases_unit_test.go`)

✅ **Implemented:**
1. `TestExtractContainerID_MissingNamespace` - Verifies error when namespace label missing
2. `TestExtractContainerID_MissingPod` - Verifies error when pod/pod_name labels missing
3. `TestExtractContainerID_MissingContainer` - Verifies error when container/name labels missing
4. `TestExtractContainerID_WithAlternativeLabels` - Tests alternative label names (pod_name, name)
5. `TestExtractContainerID_Success` - Verifies normal extraction with all labels present
6. `TestQueryPrometheus_EmptyVector` - Handles empty Prometheus query results
7. `TestQueryPrometheus_NaNValue` - Handles NaN metric values gracefully
8. `TestQueryPrometheus_InfValue` - Handles Inf metric values gracefully
9. `TestQueryPrometheus_MultipleMetricsWithSomeMissingLabels` - Skips invalid metrics, processes valid ones
10. `TestTelemetryAwareSource_NoVPAs` - Handles no VPAs without errors
11. `TestTelemetryAwareSource_VPAWithNoMatchingPods` - Handles VPA with no pods gracefully

### OOM Counter Tests (`oom_counter_edge_cases_test.go`)

✅ **Implemented:**
1. `TestOOMCounterDecreasing` - Handles decreasing counters (e.g., Prometheus restart) without negative deltas
2. `TestOOMCounterReset` - Handles counter resets and correctly detects new OOMs after reset
3. `TestOOMCounterVeryLargeDelta` - Handles large counter jumps (1000 OOMs) without hanging
4. `TestOOMCounterMaxUint64` - Handles maximum uint64 value without overflow
5. `TestOOMCounterNilValue` - Handles missing OOM counter (nil) gracefully
6. `TestOOMCounterDeltaDetection` - Validates delta calculation (existing test)
7. `TestOOMCounterWithNoInitialValue` - Validates baseline establishment (existing test)

**Run unit tests:**
```bash
cd pkg/recommender/input/metrics
go test -v -run "TestExtractContainerID|TestQueryPrometheus|TestTelemetryAwareSource" .

cd pkg/recommender/input
go test -v -run "TestOOMCounter|TestDualOOM" .
```

### Dual OOM Detection Tests (`dual_oom_detection_test.go`)

✅ **Implemented:**
1. `TestDualOOMDetection_SameEvent` - Both OOM mechanisms can operate simultaneously
2. `TestDualOOMDetection_DifferentEvents` - Different OOMs in different windows
3. `TestDualOOMDetection_KubernetesFirst` - Kubernetes event arrives before Prometheus scrape
4. `TestDualOOMDetection_E2EWithMetricsClient` - Full flow with both mechanisms active
5. `TestDualOOMDetection_ManagedRuntimeScenario` - Real-world .NET/Java app scenarios
6. `TestDualOOMDetection_RapidOOMs` - Rapid successive OOMs from both sources

**Run dual OOM tests:**
```bash
cd pkg/recommender/input
go test -v -run "TestDualOOM" .
```

---

## Integration Test Coverage (NEW)

The following integration tests use fake Prometheus servers (`httptest.NewServer`) for fast, reliable testing without requiring a real cluster:

### Query Variation Tests (`telemetry_integration_test.go`)

✅ **Implemented:**
1. `TestTelemetryIntegration_PartialCustomQuery_OOMOnly` - Custom OOM query, default CPU/Memory
2. `TestTelemetryIntegration_PartialCustomQuery_CPUOnly` - Custom CPU query, default Memory/OOM  
3. `TestTelemetryIntegration_PartialCustomQuery_MemoryOnly` - Custom Memory query, default CPU/OOM
4. `TestTelemetryIntegration_AllCustomQueries` - All three metrics use custom queries

### Advanced Scenario Tests

✅ **Implemented:**
5. `TestTelemetryIntegration_HighFrequencyOOMs` - Rapid OOM counter increments (stress test)
6. `TestTelemetryIntegration_StaleMetrics` - Handles repeated/old timestamps gracefully

### Production Scenario Tests

✅ **Implemented:**
7. `TestTelemetryIntegration_SlowQueryTimeout` - Prometheus with deliberate delays (~2s response time)

### Error Recovery Tests

✅ **Implemented:**
8. `TestTelemetryIntegration_InvalidPrometheusData` - Malformed JSON, missing fields, wrong types, invalid values (4 sub-tests)
9. `TestTelemetryIntegration_PrometheusErrorResponse` - HTTP 400, 503, 422 error codes (3 sub-tests)

**Run integration tests:**
```bash
cd pkg/recommender/input/metrics
go test -v -run "TestTelemetryIntegration" .
```

**Run new integration tests only:**
```bash
go test -v -run "TestTelemetryIntegration_(PartialCustomQuery|AllCustomQueries|HighFrequencyOOMs|StaleMetrics|SlowQueryTimeout|InvalidPrometheusData|PrometheusErrorResponse)" .
```

---

## Running the Tests

### Run all edge case tests:
```bash
./hack/run-e2e-locally.sh --keep-cluster prometheus-telemetry
```

### Run specific phase:
```bash
cd e2e
go test -v ./v1 -ginkgo.focus="Phase 1: Core Resilience" -ginkgo.label-filter="PrometheusRequired && EdgeCases"
```

### Run specific test:
```bash
go test -v ./v1 -ginkgo.focus="should fallback gracefully when Prometheus is unreachable"
```

---

## Legend

- ⏳ **TODO** - Not yet implemented (uses `ginkgo.Skip`)
- 🚧 **In Progress** - Currently being implemented
- ✅ **Done** - Implemented and passing
- ❌ **Blocked** - Blocked by dependency or issue

### Priority Levels
- 🔴 **Critical** - Must have for production readiness
- 🟡 **Medium** - Important but not blocking
- 🟢 **Low** - Nice to have

---

## Implementation Order Recommendation

1. **Start with Phase 1** - Core resilience tests are most critical
2. **Move to Phase 2** - Query variations are frequently used
3. **Add Phase 3** - Advanced scenarios for enterprise users
4. **Consider Phase 4** - Production scenarios as time allows
5. **Phase 5 last** - Error recovery is least critical

---

## Notes

- All tests are stubbed with `ginkgo.Skip()` and include detailed test plans in comments
- Remove the `ginkgo.Skip()` line when implementing each test
- Update this document's status when tests are completed
- Tests labeled with `PrometheusRequired` and `EdgeCases` for selective execution
