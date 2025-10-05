# VPA Telemetry Failure Handling - Enhancement Proposal

## Current Behavior

When Prometheus is configured as a telemetry source but becomes unreachable at runtime:

**What happens:**
- Error logged: `"Failed to fetch Prometheus metrics"`
- VPA receives no metrics
- VPA does **not** get recommendations
- Recommender continues running (no crash)

**What's missing:**
- ❌ No VPA status condition set
- ❌ No metrics exposed for alerting
- ❌ No configurable fallback behavior

## Problem

Users cannot:
1. **Monitor telemetry health** - No metrics to alert on
2. **Understand VPA state** - Status doesn't reflect telemetry failure
3. **Configure fallback** - No option for graceful degradation

This creates operational blind spots in production.

## Proposed Solution

### 1. VPA Status Condition

Add a new condition type to indicate telemetry health:

```go
// In pkg/apis/autoscaling.k8s.io/v1/types.go
const (
    // ... existing conditions ...
    
    // TelemetryUnavailable indicates the configured telemetry source is unreachable
    TelemetryUnavailable VerticalPodAutoscalerConditionType = "TelemetryUnavailable"
)
```

**Set condition when:**
- Prometheus connection fails (timeout, DNS, network error)
- Prometheus returns persistent errors (4xx, 5xx)
- Authentication failures

**Example condition:**
```yaml
conditions:
- type: TelemetryUnavailable
  status: "True"
  reason: "PrometheusUnreachable"
  message: "Failed to fetch metrics from http://prometheus:9090: connection timeout after 10s"
  lastTransitionTime: "2025-01-15T10:30:00Z"
```

### 2. Prometheus Metrics for Alerting

Expose telemetry health metrics in the recommender:

```go
// Counter for telemetry errors
vpa_telemetry_errors_total{
    vpa_namespace="default",
    vpa_name="my-app-vpa",
    telemetry_source="prometheus",
    reason="connection_failed",  // connection_failed, auth_failed, query_error, timeout
}

// Gauge for current telemetry status (0=ok, 1=failing)
vpa_telemetry_status{
    vpa_namespace="default",
    vpa_name="my-app-vpa",
    telemetry_source="prometheus",
}

// Histogram for query duration
vpa_telemetry_query_duration_seconds{
    vpa_namespace="default",
    vpa_name="my-app-vpa",
    telemetry_source="prometheus",
    query_type="cpu",  // cpu, memory, oom
}
```

**Alert examples:**
```yaml
# Alert when telemetry is failing
- alert: VPATelemetryUnavailable
  expr: vpa_telemetry_status == 1
  for: 5m
  annotations:
    summary: "VPA {{ $labels.vpa_name }} cannot fetch metrics"
    
# Alert on high error rate
- alert: VPATelemetryErrorRateHigh
  expr: rate(vpa_telemetry_errors_total[5m]) > 0.1
  for: 10m
```

### 3. Configurable Fallback Behavior (Optional)

Add optional fallback configuration:

```go
// In pkg/apis/autoscaling.k8s.io/v1/types.go
type TelemetryConfig struct {
    Source            TelemetrySource        `json:"source,omitempty"`
    Prometheus        *PrometheusTelemetry   `json:"prometheus,omitempty"`
    
    // NEW: Fallback behavior on telemetry source failure
    // +optional
    FallbackOnFailure *bool `json:"fallbackOnFailure,omitempty"`
}
```

**Behavior:**
- `fallbackOnFailure: true` → Fall back to Kubernetes metrics-server on Prometheus failure
- `fallbackOnFailure: false` → No fallback (fail-closed, current behavior)
- `fallbackOnFailure: null` (default) → No fallback (safe default)

**Example VPA:**
```yaml
apiVersion: autoscaling.k8s.io/v1
kind: VerticalPodAutoscaler
metadata:
  name: my-app-vpa
spec:
  targetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: my-app
  telemetry:
    source: Prometheus
    fallbackOnFailure: true  # Gracefully degrade to metrics-server
    prometheus:
      address: http://prometheus.monitoring.svc:9090
```

**Recommendation:** Default to `false` (fail-closed) to avoid masking infrastructure issues.

## Implementation Plan

### Phase 1: Observability (High Priority)
1. Add `TelemetryUnavailable` condition type to VPA CRD
2. Set condition in `telemetry_source.go` when fetch fails
3. Expose `vpa_telemetry_errors_total` and `vpa_telemetry_status` metrics
4. Update E2E test to verify condition is set

### Phase 2: Metrics Enhancement (Medium Priority)
5. Add `vpa_telemetry_query_duration_seconds` histogram
6. Add per-query-type error tracking
7. Document metrics and alerting best practices

### Phase 3: Fallback Configuration (Low Priority)
8. Add `fallbackOnFailure` field to VPA CRD
9. Implement fallback logic in `TelemetryAwareSource.List()`
10. Add validation and documentation
11. Add E2E test for fallback behavior

## Code Changes Required

### 1. Add Condition Type (`types.go`)
```go
const TelemetryUnavailable VerticalPodAutoscalerConditionType = "TelemetryUnavailable"
```

### 2. Set Condition on Failure (`telemetry_source.go`)
```go
func (t *TelemetryAwareSource) List(ctx context.Context, namespace string, opts v1.ListOptions) (*v1beta1.PodMetricsList, error) {
    // ... existing code ...
    
    promMetrics, oomCounters, err := t.fetchPrometheusMetrics(ctx, address, vpas)
    if err != nil {
        klog.ErrorS(err, "Failed to fetch Prometheus metrics", "address", address)
        
        // NEW: Set condition for affected VPAs
        for _, vpa := range vpas {
            t.setTelemetryCondition(vpa, err)
        }
        
        // NEW: Increment error metric
        telemetryErrorsTotal.WithLabelValues(
            vpa.ID.Namespace,
            vpa.ID.VpaName,
            "prometheus",
            classifyError(err),
        ).Inc()
        
        continue
    }
    // ...
}
```

### 3. Expose Metrics (`metrics.go` - new file)
```go
var (
    telemetryErrorsTotal = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "vpa_telemetry_errors_total",
            Help: "Total number of telemetry fetch errors",
        },
        []string{"vpa_namespace", "vpa_name", "telemetry_source", "reason"},
    )
    
    telemetryStatus = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "vpa_telemetry_status",
            Help: "Current telemetry status (0=ok, 1=failing)",
        },
        []string{"vpa_namespace", "vpa_name", "telemetry_source"},
    )
)

func init() {
    prometheus.MustRegister(telemetryErrorsTotal)
    prometheus.MustRegister(telemetryStatus)
}
```

## Testing

### Unit Tests
- ✅ Already covered in `telemetry_integration_test.go`
  - Prometheus errors (400, 503, timeout)
  - Invalid data handling

### E2E Tests
- ✅ `should handle unreachable Prometheus without crashing`
  - Update to verify condition is set
  - Verify metrics are exposed

### New Tests Needed
- E2E test for `fallbackOnFailure: true` behavior
- Integration test for condition lifecycle (set → clear on recovery)

## Alternatives Considered

### Alternative 1: Auto-fallback without configuration
**Rejected:** Silent fallback masks infrastructure issues. Users should opt-in.

### Alternative 2: Retry with exponential backoff
**Complementary:** Can be added alongside condition setting. Good for transient failures.

### Alternative 3: Per-metric fallback
**Too complex:** Allows fallback for CPU but not memory. Hard to reason about.

## References

- TODO comment in `telemetry_source.go:122`: "Set VPA condition for telemetry failure"
- E2E test: `e2e/v1/telemetry_edge_cases.go` - "should handle unreachable Prometheus without crashing"
- Existing validation: `validateTelemetryConfig()` in `cluster_feeder.go`

## Decision

**Recommendation:** Implement Phase 1 (observability) immediately. Defer Phase 3 (fallback config) until user demand is confirmed.

**Rationale:**
1. Conditions + metrics solve the immediate observability gap
2. Fail-closed is the safest default
3. `fallbackOnFailure` can be added non-disruptively later

---

## Implementation Status

✅ **Phase 1: Observability - IMPLEMENTED**

**Completed:** 2025-10-05

**Changes Made:**
1. Added `TelemetryUnavailable` condition type to VPA CRD (`types.go`)
2. Created telemetry metrics module (`telemetry_metrics.go`):
   - **Strongly-typed `TelemetrySourceType`** enum (`prometheus`, `kubernetes`)
   - `vpa_telemetry_errors_total` (counter with labels: vpa_namespace, vpa_name, telemetry_source, reason)
   - `vpa_telemetry_status` (gauge: 0=ok, 1=failing)
   - `vpa_telemetry_query_duration_seconds` (histogram for CPU, memory, OOM queries)
   - Type-safe helper functions: `RecordTelemetryError()`, `RecordTelemetrySuccess()`, `RecordQueryDuration()`, `FormatTelemetryConditionMessage()`
3. Updated `TelemetryAwareSource` (`telemetry_source.go`):
   - Set/clear `TelemetryUnavailable` condition on fetch success/failure
   - Emit metrics on every fetch attempt (using strongly-typed constants)
   - Classify errors into categories: connection_failed, timeout, dns_failed, auth_failed, etc.
4. Updated E2E test (`telemetry_edge_cases.go`):
   - Verify `TelemetryUnavailable` condition is set when Prometheus is unreachable
   - Verify condition message contains actionable information

**Error Reason Classification:**
- `connection_failed` - Network connection errors
- `connection_timeout` - Connection timeout
- `timeout` - Query timeout (context.DeadlineExceeded)
- `dns_failed` - DNS resolution failure
- `auth_failed` - Authentication/authorization errors (401, 403)
- `query_error` - Invalid query or parse errors
- `server_error` - Prometheus server errors
- `invalid_response` - Malformed responses
- `canceled` - Context canceled
- `unknown` - Unclassified errors

**All tests passing:** ✅
- Unit tests: TestTelemetry* (all pass)
- Integration tests: TestTelemetryIntegration_* (all pass)
- E2E test: Updated to verify condition

---

✅ **Phase 3: Configurable Fallback - IMPLEMENTED**

**Completed:** 2025-10-05

**Changes Made:**
1. Added `FallbackOnFailure` field to `TelemetryConfig` in VPA CRD (`types.go`)
   - Type: `*bool` (optional, defaults to false for fail-closed behavior)
   - When `true`: Falls back to Kubernetes metrics-server on Prometheus failure
   - When `false` or unset: Fail-closed, no recommendations on failure
2. Updated codegen (`update-codegen.sh`) to generate DeepCopy methods
3. Implemented fallback logic in `TelemetryAwareSource.List()` (`telemetry_source.go`):
   - Added `shouldFallback()` helper method
   - On Prometheus failure: checks `fallbackOnFailure` flag
   - If enabled: adds VPA to Kubernetes metrics-server list for fallback
   - `TelemetryUnavailable` condition remains set to indicate primary source failure
4. Created comprehensive unit tests (`telemetry_fallback_test.go`):
   - TestShouldFallback_NilVPA
   - TestShouldFallback_NilTelemetry
   - TestShouldFallback_NilFallbackOnFailure (verifies default=false)
   - TestShouldFallback_ExplicitFalse
   - TestShouldFallback_ExplicitTrue
   - TestShouldFallback_BothVariants
5. Added E2E test for fallback behavior (`telemetry_edge_cases.go`):
   - Verifies VPA gets recommendations from Kubernetes metrics-server when Prometheus fails
   - Verifies `TelemetryUnavailable` condition is set even with successful fallback
6. Updated documentation (`telemetry-config.md`):
   - Added Error Handling section explaining observability and failure behavior
   - Added example configuration for `fallbackOnFailure: true`
   - Documented fail-closed vs graceful degradation tradeoffs

**Default Behavior:** Fail-closed (safe default)
- Prevents incorrect scaling decisions based on partial/missing data
- Explicit opt-in required for fallback behavior

**All tests passing:** ✅
- Unit tests: TestShouldFallback_* (all 6 pass)
- E2E test: "should fall back to Kubernetes metrics when Prometheus fails with fallbackOnFailure=true"

---

**Status:** Phase 1 ✅ | Phase 3 ✅ | Phase 2 (Alerting) Optional Future Enhancement
**Author:** VPA Telemetry Team
**Date:** 2025-10-05
