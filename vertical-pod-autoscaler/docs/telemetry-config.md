# VPA Telemetry Configuration

## Reference

### Telemetry Sources

The VPA recommender sources runtime metrics and OOM events from configurable backends. Each VPA specifies its telemetry source via the `spec.telemetry` field.

**Available sources:**
- `Kubernetes`: Uses Kubernetes metrics-server API (default behavior)
- `Prometheus`: Queries Prometheus for metrics and OOM counters
- `Auto`: Delegates to global defaults (equivalent to Kubernetes)

## Configuration Precedence

### Source Selection
1. **VPA-level config** (`spec.telemetry.source`): Takes highest precedence
2. **Global defaults**: When VPA telemetry is unset, uses Kubernetes metrics-server
3. **Auto mode**: `source: Auto` delegates to global defaults

### Prometheus Configuration
When `source: Prometheus`:
- **VPA-level Prometheus config** (`spec.telemetry.prometheus.*`): Takes precedence
- **Global flags** (e.g., `--prometheus-address`): Not used when VPA specifies telemetry
- Each VPA can point to a different Prometheus instance

### Backward Compatibility
- Existing VPAs without `spec.telemetry` continue using Kubernetes metrics-server
- Global flags (`--use-external-metrics`, `--prometheus-*`) remain functional for history loading
- OOMKilled pod status detection remains active as fallback

## Default Metric Queries

When using Prometheus with default queries (no custom query overrides):

### CPU Usage
```promql
rate(container_cpu_usage_seconds_total{container!="POD", container!="", namespace="<ns>", pod=~"<pod1|pod2>"}[5m])
```

### Memory Usage
```promql
container_memory_working_set_bytes{container!="POD", container!="", namespace="<ns>", pod=~"<pod1|pod2>"}
```

### OOM Events Counter
```promql
container_oom_events_total{container!="POD", container!="", namespace="<ns>", pod=~"<pod1|pod2>"}
```

**Label Requirements:**
- `namespace`: Pod namespace
- `pod` or `pod_name`: Pod name
- `container` or `name`: Container name

## Custom Query Overrides

Users can override default queries in `spec.telemetry.prometheus.query`:

```yaml
apiVersion: autoscaling.k8s.io/v1
kind: VerticalPodAutoscaler
metadata:
  name: custom-queries-vpa
spec:
  telemetry:
    source: Prometheus
    prometheus:
      address: "http://prometheus.monitoring.svc:9090"
      query:
        cpuUsageQuery: "my_custom_cpu_metric{app='myapp'}"
        memoryUsageQuery: "my_custom_memory_metric{app='myapp'}"
        oomCountQuery: "my_oom_counter{app='myapp'}"
```

**Behavior:** 
- Custom queries bypass automatic pod filtering. The query defines the complete scope.
- **Partial overrides:** When CPU or memory queries are provided without `oomCountQuery`, OOM counter fetching is disabled. The system falls back to Kubernetes `OOMKilled` status detection from pod events.
- **Disabling OOM counters:** Omit the `oomCountQuery` field to disable OOM counter queries while using custom CPU/memory queries.

## OOM Event Handling

### Two Types of OOMs

The VPA tracks **two distinct types of OOM events** that are mutually exclusive:

#### 1. Application-Level OOM (Prometheus-detected)
- **What:** Managed runtimes (e.g., .NET CLR, JVM) throw OOM exceptions internally
- **Container state:** Remains **RUNNING** - exception handled at application level
- **Detection:** Prometheus custom metrics (e.g., `dotnet_oom_exceptions_total`, `jvm_oom_count`)
- **Kubernetes OOMKilled:** Does **NOT** fire (kernel never intervened)
- **Example:** .NET `OutOfMemoryException` during request processing

#### 2. Kernel-Level OOM (Kubernetes OOMKilled)
- **What:** Container memory exceeds cgroup limits, killed by kernel
- **Container state:** **TERMINATED** - process forcibly killed
- **Detection:** Kubernetes pod status shows `OOMKilled`, restart count increments
- **Prometheus:** **CANNOT** see this (container is dead, can't report metrics)
- **Example:** Extremely constrained startup where container hits memory limit before runtime initializes

### Why Both Mechanisms Must Be Active

For managed memory languages (e.g., .NET, Java), both OOM types can occur:
- **Common:** Application OOM exceptions during normal operation → Detected by Prometheus
- **Rare:** Kernel OOMKilled during constrained startup → Detected by Kubernetes events

**Double-counting is impossible** because these events are mutually exclusive:
- If Prometheus sees an OOM counter increment → container is alive → Kubernetes OOMKilled did NOT fire
- If Kubernetes reports OOMKilled → container is dead → Prometheus cannot scrape it

### Prometheus OOM Counters
- Requires cumulative counter metric (e.g., `container_oom_events_total` or custom application metrics)
- System tracks previous values and detects deltas each cycle
- Multiple OOMs between samples are recorded individually
- Captures application-level OOM exceptions where container stays running

### Kubernetes OOMKilled Events
- Always active regardless of telemetry configuration
- Monitors pod status for `OOMKilled` termination reason
- Tracks container restart counts to detect new OOM events
- Captures kernel-level OOMs where container is terminated
- Essential fallback for OOMs that Prometheus cannot see

## How to Configure Telemetry

### Configure VPA to Use Kubernetes Metrics-Server
```yaml
apiVersion: autoscaling.k8s.io/v1
kind: VerticalPodAutoscaler
metadata:
  name: default-vpa
spec:
  targetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: my-app
  # Omit telemetry field to use Kubernetes metrics-server
```

### Configure VPA to Use Prometheus with Bearer Token
```yaml
apiVersion: autoscaling.k8s.io/v1
kind: VerticalPodAutoscaler
metadata:
  name: prometheus-vpa
spec:
  targetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: my-app
  telemetry:
    source: Prometheus
    prometheus:
      address: "https://prometheus.monitoring.svc:9090"
      insecure: false
      authentication:
        bearerToken: "my-secret-token"
```

### Configure VPA to Use Prometheus with Basic Authentication
```yaml
apiVersion: autoscaling.k8s.io/v1
kind: VerticalPodAutoscaler
metadata:
  name: prometheus-basic-auth-vpa
spec:
  targetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: my-app
  telemetry:
    source: Prometheus
    prometheus:
      address: "http://prometheus.monitoring.svc:9090"
      authentication:
        basicAuthUsername: "admin"
        basicAuthPassword: "password"
```

### Configure VPA with Automatic Fallback on Prometheus Failure
```yaml
apiVersion: autoscaling.k8s.io/v1
kind: VerticalPodAutoscaler
metadata:
  name: prometheus-with-fallback-vpa
spec:
  targetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: my-app
  telemetry:
    source: Prometheus
    prometheus:
      address: "http://prometheus.monitoring.svc:9090"
    fallbackOnFailure: true  # Fall back to Kubernetes metrics-server if Prometheus fails
```

When `fallbackOnFailure: true`:
- If Prometheus becomes unreachable, VPA automatically uses Kubernetes metrics-server
- VPA continues providing recommendations (graceful degradation)
- `TelemetryUnavailable` condition is set to alert you of the primary source failure
- When Prometheus recovers, VPA automatically switches back

When `fallbackOnFailure: false` or unset (default):
- VPA fails-closed: no recommendations when Prometheus is unreachable
- Safe default to prevent scaling decisions based on incomplete data
- Alerts via condition and metrics for monitoring

## Validation

The recommender validates telemetry configuration when loading VPAs:

### ConfigUnsupported Conditions
- `source: Prometheus` without `prometheus.address`
- Invalid authentication combinations

### Error Handling

When a telemetry source fails (e.g., Prometheus becomes unreachable), the VPA recommender provides observability and optional fallback:

#### Observability (Always Active)
- Sets `TelemetryUnavailable` condition on the VPA with a helpful error message
- Exposes `vpa_recommender_telemetry_status` metric (0=healthy, 1=failing)
- Increments `vpa_recommender_telemetry_errors_total` counter
- Logs errors: "Failed to fetch Prometheus metrics"

#### Failure Behavior
By default (`fallbackOnFailure: false` or unset):
- **Fail-closed**: VPA gets NO metrics when Prometheus is unreachable
- VPA does NOT provide recommendations (safe default to prevent incorrect scaling)
- Recommender continues running and monitoring other VPAs

With `fallbackOnFailure: true`:
- VPA automatically falls back to Kubernetes metrics-server
- Recommendations continue using metrics-server data
- `TelemetryUnavailable` condition remains set to indicate primary source failure
- Query failures are logged but don't block other VPAs

## Performance Considerations

### Prometheus Query Optimization
- Default queries are scoped per-VPA to specific pod names
- Namespace filtering applied automatically
- Queries are executed efficiently: `namespace="prod", pod=~"app-1|app-2"`

### Client Caching
- Prometheus clients are cached per address+auth combination
- Reused across metric fetch cycles
- Reduces connection overhead

## Future Work

- OpenTelemetry direct integration (currently can use Prometheus gateway as workaround)
- Query templating support for advanced use cases
- Per-VPA query timeouts and rate limiting
