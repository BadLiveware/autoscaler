# Using Prometheus for VPA Metrics

This document describes how to configure VPA to query Prometheus directly for container metrics and OOM events, bypassing the Kubernetes metrics-server and external-metrics API.

## Why Query Prometheus Directly?

1. **External-Metrics Conflict**: Only one external-metrics provider can be installed per cluster. If you're using KEDA or another tool that provides external-metrics, VPA cannot use that API.

2. **Managed Language OOM Support**: .NET, Java, and other managed languages throw internal OOM exceptions (e.g., `OutOfMemoryException`) that don't trigger Kubernetes OOMKilled events. These can be exported to Prometheus as counters.

3. **Custom Metrics**: Query any Prometheus metric for CPU, memory, or OOM events with custom PromQL.

## E2E Testing

The Prometheus integration includes comprehensive e2e tests that verify:
- Recommendations are generated from Prometheus metrics
- OOM events from Prometheus counters are detected
- Multiple OOM events are handled correctly
- Custom Prometheus queries work

### Running E2E Tests

To run the Prometheus integration e2e tests locally:

```bash
cd vertical-pod-autoscaler
./hack/run-e2e-locally.sh recommender-prometheus
```

This will:
1. Create a local KIND cluster
2. Deploy Prometheus and Pushgateway
3. Deploy VPA recommender with Prometheus source enabled
4. Run the test suite

### Test Structure

The tests use Prometheus Pushgateway to simulate OOM events:

```go
// Push an OOM counter to Prometheus via Pushgateway
err := pushgatewayClient.PushOOMCounter(
    pod.Namespace,
    pod.Name,
    containerName,
    2.0, // OOM count
)
```

The external OOM observer polls Prometheus (every 10s in tests) and detects counter increases, triggering memory recommendation updates.

## Architecture

```
┌──────────────┐
│ Prometheus   │ ← scrapes cAdvisor + app metrics
└──────┬───────┘
       │
       │ Direct PromQL queries
       │
┌──────▼───────┐
│ Prometheus   │
│ Metrics      │
│ Source       │
└──────┬───────┘
       │
       ├─→ CPU/Memory metrics → Recommender
       │
       └─→ OOM counters → External OOM Observer
```

## Configuration

### Command-Line Flags

The VPA recommender supports the following command-line flags for Prometheus integration:

#### Prometheus Metrics Source Flags

- `--use-prometheus-source`: Enable Prometheus as direct metrics source (default: false)
  - When enabled, bypasses the Kubernetes external-metrics API
  - Allows coexistence with tools like KEDA that also use external-metrics API
  
- `--prometheus-source-address`: Prometheus server address for direct queries
  - Falls back to `--prometheus-address` if not specified
  - Example: `http://prometheus.monitoring.svc:9090`
  
- `--prometheus-source-insecure`: Skip TLS verification (default: false)
  
- `--prometheus-source-timeout`: Query timeout duration (default: "30s")
  
- `--prometheus-source-bearer-token`: Bearer token for authentication
  
- `--prometheus-source-username`: Basic auth username
  
- `--prometheus-source-password`: Basic auth password
  
- `--prometheus-cpu-query`: Custom PromQL query for CPU usage
  - Use `%s` placeholders for namespace and pod regex
  - If not specified, uses default cAdvisor metrics
  
- `--prometheus-memory-query`: Custom PromQL query for memory usage
  - Use `%s` placeholders for namespace and pod regex
  - If not specified, uses default cAdvisor metrics
  
- `--prometheus-oom-query`: Custom PromQL query for OOM counter
  - Use `%s` placeholders for namespace and pod regex
  - Required for managed language OOM support
  - Example: `dotnet_gc_oom_count{namespace=~"%s", pod=~"%s"}`

#### External OOM Observer Flags

- `--use-external-oom-observer`: Enable external metrics for OOM detection (default: false)
  - When enabled, polls Prometheus for OOM counters instead of watching Kubernetes events
  - Enables managed language (.NET, Java, Go) OOM support
  
- `--oom-polling-interval`: How often to poll for OOM counter changes (default: 60s)
  - Adjust based on your application's OOM frequency and Prometheus scrape interval
  - Lower values provide faster OOM detection but increase Prometheus load

### Usage Examples

#### Basic Setup

Query Prometheus for standard cAdvisor metrics and .NET OOM events:

```bash
./recommender \
  --use-prometheus-source=true \
  --prometheus-source-address=http://prometheus.monitoring.svc:9090 \
  --use-external-oom-observer=true \
  --prometheus-oom-query='dotnet_gc_oom_count{namespace=~"%s", pod=~"%s"}' \
  --oom-polling-interval=60s
```

#### With Authentication

```bash
./recommender \
  --use-prometheus-source=true \
  --prometheus-source-address=https://prometheus.example.com \
  --prometheus-source-bearer-token="${PROMETHEUS_TOKEN}" \
  --prometheus-source-timeout=30s \
  --use-external-oom-observer=true \
  --prometheus-oom-query='dotnet_gc_oom_count{namespace=~"%s", pod=~"%s"}' \
  --oom-polling-interval=30s
```

#### Helm Deployment

```yaml
# values.yaml
recommender:
  extraArgs:
    use-prometheus-source: "true"
    prometheus-source-address: "http://prometheus.monitoring.svc:9090"
    use-external-oom-observer: "true"
    prometheus-oom-query: 'dotnet_gc_oom_count{namespace=~"%s", pod=~"%s"}'
    oom-polling-interval: "60s"
```

### Programmatic Configuration

#### 1. Basic Prometheus Metrics Source

Query Prometheus for standard cAdvisor metrics:

```go
import (
    "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/input/metrics"
)

// Create Prometheus source
prometheusSource, err := metrics.NewPrometheusMetricsSource(metrics.PrometheusSourceConfig{
    Address:      "http://prometheus.monitoring:9090",
    ClusterState: clusterState,
})

// Use as metrics client
metricsClient := metrics.NewMetricsClient(prometheusSource, "", "prometheus-client")
```

### 2. With Authentication

```go
prometheusSource, err := metrics.NewPrometheusMetricsSource(metrics.PrometheusSourceConfig{
    Address:      "https://prometheus.monitoring:9090",
    ClusterState: clusterState,
    Auth: &metrics.PrometheusAuth{
        BearerToken: "your-token-here",
    },
    InsecureSkipVerify: false,
    QueryTimeout: 30 * time.Second,
})
```

### 3. Custom Queries

Override default cAdvisor queries with custom PromQL:

```go
prometheusSource, err := metrics.NewPrometheusMetricsSource(metrics.PrometheusSourceConfig{
    Address:      "http://prometheus:9090",
    ClusterState: clusterState,
    // Custom CPU query (default uses rate(container_cpu_usage_seconds_total[5m]))
    CPUQuery: `rate(custom_cpu_metric{namespace="%s",pod=~"%s"}[5m])`,
    // Custom memory query (default uses container_memory_working_set_bytes)
    MemoryQuery: `custom_memory_metric{namespace="%s",pod=~"%s"}`,
    // Custom OOM counter query for managed languages
    OOMQuery: `dotnet_runtime_exceptions_total{type="OutOfMemoryException",namespace="%s",pod=~"%s"}`,
})
```

## Default Prometheus Queries

The source uses these default queries (querying cAdvisor metrics):

### CPU Usage
```promql
rate(container_cpu_usage_seconds_total{
    namespace="<namespace>",
    pod=~"<pod-regex>",
    container!="",
    container!="POD",
    image!=""
}[5m])
```

### Memory Usage
```promql
container_memory_working_set_bytes{
    namespace="<namespace>",
    pod=~"<pod-regex>",
    container!="",
    container!="POD",
    image!=""
}
```

### OOM Events
```promql
container_oom_events_total{
    namespace="<namespace>",
    pod=~"<pod-regex>",
    container!="",
    container!="POD"
}
```

## Managed Language OOM Support

For .NET, Java, Go applications that throw internal OOM exceptions:

### 1. Export OOM Metrics from Your App

**.NET Example:**
```csharp
using Prometheus;

private static readonly Counter OomCounter = Metrics.CreateCounter(
    "dotnet_runtime_exceptions_total",
    "Total exceptions thrown",
    new CounterConfiguration {
        LabelNames = new[] { "type", "namespace", "pod", "container" }
    });

try {
    // app code
} catch (OutOfMemoryException ex) {
    OomCounter.WithLabels(
        "OutOfMemoryException",
        Environment.GetEnvironmentVariable("POD_NAMESPACE"),
        Environment.GetEnvironmentVariable("POD_NAME"),
        Environment.GetEnvironmentVariable("CONTAINER_NAME")
    ).Inc();
    throw;
}
```

### 2. Configure Prometheus Scraping

```yaml
apiVersion: v1
kind: Service
metadata:
  name: my-dotnet-app
  annotations:
    prometheus.io/scrape: "true"
    prometheus.io/port: "8080"
    prometheus.io/path: "/metrics"
```

### 3. Use External OOM Observer

```go
import (
    "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/input/oom"
)

// Create Prometheus source with OOM query
prometheusSource, err := metrics.NewPrometheusMetricsSource(metrics.PrometheusSourceConfig{
    Address:      "http://prometheus:9090",
    ClusterState: clusterState,
    OOMQuery: `dotnet_runtime_exceptions_total{
        type="OutOfMemoryException",
        namespace="%s",
        pod=~"%s"
    }`,
})

// Create OOM observer that uses Prometheus counters
stopCh := make(chan struct{})
oomObserver := oom.NewExternalObserver(oom.ExternalObserverConfig{
    MetricsSource:   prometheusSource, // Prometheus source implements GetOOMCounters()
    ClusterState:    clusterState,
    PollingInterval: 60 * time.Second,
    StopChannel:     stopCh,
})

// Use in cluster state feeder
feeder := input.ClusterStateFeederFactory{
    MetricsClient: metrics.NewMetricsClient(prometheusSource, "", "prometheus-client"),
    OOMObserver:   oomObserver,
    // ... other fields
}.Make()
```

## Label Requirements

Prometheus metrics must include these labels:

- `namespace` - Kubernetes namespace
- `pod` - Pod name
- `container` - Container name

Example metric:
```
container_memory_working_set_bytes{
    namespace="default",
    pod="my-app-abc123",
    container="app",
    ...
} 524288000
```

## Troubleshooting

### No Metrics Returned

**Check Prometheus is accessible:**
```bash
curl http://prometheus:9090/api/v1/query?query=up
```

**Verify cAdvisor metrics exist:**
```bash
curl "http://prometheus:9090/api/v1/query?query=container_cpu_usage_seconds_total"
```

**Check label names match:**
```bash
# VPA expects: namespace, pod, container
# Some setups use: kubernetes_namespace, kubernetes_pod_name, etc.
```

### OOM Events Not Detected

**Verify OOM counter increases:**
```bash
# Initial value
curl "http://prometheus:9090/api/v1/query?query=dotnet_runtime_exceptions_total"

# After OOM, should be higher
curl "http://prometheus:9090/api/v1/query?query=dotnet_runtime_exceptions_total"
```

**Check observer logs:**
```bash
kubectl logs -n kube-system vpa-recommender | grep "External OOM"
```

## Comparison with Other Approaches

| Method | Pros | Cons |
|--------|------|------|
| **Kubernetes metrics-server** | Standard, simple setup | No OOM counters, no custom metrics |
| **External-metrics API** | Standard API | Conflicts with KEDA, complex setup |
| **Prometheus Direct** | No conflicts, supports OOM, flexible queries | Requires Prometheus, more configuration |

## Performance Considerations

- **Query Timeout**: Default 30s, adjust based on cluster size
- **Polling Interval**: OOM observer polls every 60s by default
- **Query Efficiency**: Queries are scoped per-VPA with pod regex filters

## Complete Example

```go
package main

import (
    "context"
    "time"
    
    "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/input"
    "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/input/metrics"
    "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/input/oom"
    "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
)

func setupVPAWithPrometheus(clusterState model.ClusterState) {
    // Create Prometheus metrics source
    prometheusSource, err := metrics.NewPrometheusMetricsSource(
        metrics.PrometheusSourceConfig{
            Address:      "http://prometheus.monitoring:9090",
            ClusterState: clusterState,
            QueryTimeout: 30 * time.Second,
            // Use default cAdvisor queries for CPU/memory
            // Custom query for .NET OOM events
            OOMQuery: `dotnet_runtime_exceptions_total{
                type="OutOfMemoryException",
                namespace="%s",
                pod=~"%s"
            }`,
        },
    )
    if err != nil {
        panic(err)
    }

    // Create OOM observer using Prometheus counters
    stopCh := make(chan struct{})
    oomObserver := oom.NewExternalObserver(oom.ExternalObserverConfig{
        MetricsSource:   prometheusSource,
        ClusterState:    clusterState,
        PollingInterval: 60 * time.Second,
        StopChannel:     stopCh,
    })

    // Create metrics client
    metricsClient := metrics.NewMetricsClient(
        prometheusSource,
        "",
        "prometheus-metrics-client",
    )

    // Create cluster state feeder
    feeder := input.ClusterStateFeederFactory{
        ClusterState:  clusterState,
        MetricsClient: metricsClient,
        OOMObserver:   oomObserver,
        // ... other fields (kubeClient, vpaLister, etc.)
    }.Make()

    // Use feeder in recommender
    _ = feeder
}
```
