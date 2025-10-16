# Using Prometheus for VPA Metrics

This document describes how to configure VPA to query Prometheus directly for container metrics and OOM events, bypassing the Kubernetes metrics-server and external-metrics API.

## Why Query Prometheus Directly?

1. **External-Metrics Conflict**: Only one external-metrics provider can be installed per cluster. If you're using KEDA or another tool that provides external-metrics, VPA cannot use that API.

2. **Managed Language OOM Support**: .NET, Java, and other managed languages throw internal OOM exceptions (e.g., `OutOfMemoryException`) that don't trigger Kubernetes OOMKilled events. These can be exported to Prometheus as counters.

3. **Custom Metrics**: Query any Prometheus metric for CPU, memory, or OOM events with custom PromQL.

## E2E Testing

The Prometheus integration includes comprehensive e2e tests that verify:
- **CPU and memory recommendations** are generated from Prometheus metrics (not metrics-server)
- **Dynamic recommendation updates** when resource consumption changes
- **OOM event detection** from Prometheus counters
- **Multiple OOM events** are handled correctly
- **Custom Prometheus queries** work as expected

### Running E2E Tests

To run the Prometheus integration e2e tests locally:

```bash
cd vertical-pod-autoscaler
./hack/run-e2e-locally.sh recommender-prometheus
```

This will:
1. Create a local KIND cluster
2. Deploy Prometheus and Pushgateway
3. Deploy VPA recommender with Prometheus source enabled (bypassing metrics-server)
4. Run the test suite

### Test Cases

#### 1. CPU and Memory Metrics from Prometheus

Tests that VPA generates recommendations based on actual CPU/memory consumption scraped by Prometheus:

```go
// Create resource consumer with known consumption
resourceConsumer = NewDynamicResourceConsumer(
    "test-metrics-consumer",
    namespace,
    KindDeployment,
    1,   // replicas
    100, // 100m CPU consumption
    150, // 150MB memory consumption
    ...
)

// Verify recommendations reflect Prometheus data
// CPU should be >= 25m (based on 100m consumption)
// Memory should be >= 250MB (VPA minimum)
```

This validates that the direct Prometheus integration works as a complete replacement for metrics-server.

#### 2. Dynamic Recommendation Updates

Tests that recommendations update when consumption patterns change:

```go
// Start with low consumption
resourceConsumer.ConsumeCPU(50)   // 50m
resourceConsumer.ConsumeMem(100)  // 100MB

// Wait for initial recommendation
vpa := WaitForRecommendationPresent()

// Increase consumption significantly
resourceConsumer.ConsumeCPU(300)  // 300m
resourceConsumer.ConsumeMem(300)  // 300MB

// Verify recommendations increase accordingly
```

This ensures Prometheus metrics are continuously processed and recommendations stay current with actual usage.

#### 3. OOM Event Detection

Tests that external OOM observer detects counter increases from Prometheus:

```go
// Push an OOM counter to Prometheus via Pushgateway
err := pushgatewayClient.PushOOMCounter(
    pod.Namespace,
    pod.Name,
    containerName,
    2.0, // OOM count increased from 1 to 2
)

// External OOM observer polls Prometheus (every 10s in tests)
// Detects counter increase and generates OomInfo event
// Memory recommendation should increase by at least 10%
```

This validates managed language OOM support (.NET, Java, Go) where OOMs don't result in `OOMKilled` pod status.

#### 4. Multiple OOM Events

Tests that multiple OOM events compound properly:

```go
// Push 3 OOM events sequentially
for i := 1; i <= 3; i++ {
    pushgatewayClient.PushOOMCounter(..., float64(i))
    time.Sleep(15 * time.Second) // Wait for Prometheus scrape
}

// Memory recommendation should increase by at least 20%
```

### Test Environment

- **Prometheus**: Scrapes metrics every 5 seconds
- **Pushgateway**: Accepts OOM counter pushes for testing
- **VPA Recommender**: 
  - Runs with `--recommender-interval=10s` for faster updates
  - Polls OOM counters every 10 seconds
  - Configured with `--use-prometheus-source=true`
  - Uses custom OOM query: `e2e_test_oom_events_total{namespace=~"%s", pod=~"%s"}`

### Resource Consumer

Tests use the Kubernetes `resource-consumer` utility which:
- Actively consumes specified CPU/memory via HTTP API
- Generates real metrics that Prometheus scrapes (not synthetic)
- Supports dynamic consumption changes via `ConsumeCPU()` and `ConsumeMem()` methods
- Properly cleans up after tests

This ensures test scenarios closely match real-world production workloads.

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
  - Use `{{namespace}}` and `{{pod}}` placeholders
  - If not specified, uses default cAdvisor metrics
  - Example: `rate(my_cpu_metric{namespace=~"{{namespace}}", pod=~"{{pod}}"}[5m])`

- `--prometheus-memory-query`: Custom PromQL query for memory usage
  - Use `{{namespace}}` and `{{pod}}` placeholders
  - If not specified, uses default cAdvisor metrics
  - Example: `my_memory_metric{namespace=~"{{namespace}}", pod=~"{{pod}}"}`

- `--prometheus-oom-query`: Custom PromQL query for OOM counter
  - Use `{{namespace}}` and `{{pod}}` placeholders
  - Required for managed language OOM support
  - Example: `dotnet_gc_oom_count{namespace=~"{{namespace}}", pod=~"{{pod}}"}`

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
    CPUQuery: `rate(custom_cpu_metric{namespace=~"{{namespace}}",pod=~"{{pod}}"}[5m])`,
    // Custom memory query (default uses container_memory_working_set_bytes)
    MemoryQuery: `custom_memory_metric{namespace=~"{{namespace}}",pod=~"{{pod}}"}`,
    // Custom OOM counter query for managed languages
    OOMQuery: `dotnet_runtime_exceptions_total{type="OutOfMemoryException",namespace=~"{{namespace}}",pod=~"{{pod}}"}`,
})
```

## Default Prometheus Queries

The source uses these default queries (querying cAdvisor metrics):

### CPU Usage
```promql
rate(container_cpu_usage_seconds_total{
    namespace=~"{{namespace}}",
    pod=~"{{pod}}",
    container!="",
    container!="POD",
    image!=""
}[5m])
```

### Memory Usage
```promql
container_memory_working_set_bytes{
    namespace=~"{{namespace}}",
    pod=~"{{pod}}",
    container!="",
    container!="POD",
    image!=""
}
```

### OOM Events
```promql
container_oom_events_total{
    namespace=~"{{namespace}}",
    pod=~"{{pod}}",
    container!="",
    container!="POD"
}
```

**Note**: Templates use `{{namespace}}` and `{{pod}}` placeholders for safe, order-independent substitution.

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
