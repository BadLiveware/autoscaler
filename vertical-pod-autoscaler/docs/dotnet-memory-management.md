# .NET Memory Management with Vertical Pod Autoscaler

This document describes how to configure Kubernetes Vertical Pod Autoscaler (VPA) to work effectively with .NET applications using external metrics for accurate memory management.

## Problem Statement

.NET applications present unique challenges for VPA's memory recommendations:

1. **CLR Memory Pre-allocation**: .NET runtime with server GC pre-allocates large memory pools, making container memory metrics (`container_memory_working_set_bytes`) inaccurate for scaling decisions.

2. **Invisible OOM Events**: .NET applications throw `System.OutOfMemoryException` before hitting container memory limits, so Kubernetes never sees `OOMKilled` events that VPA relies on for memory bump-up logic.

3. **GC vs Container Memory**: The memory that matters for .NET scaling is the GC-managed heap, not the total process memory visible to Kubernetes.

## Solution Overview

VPA now supports external metrics for both memory usage and OOM detection, enabling accurate .NET memory management:

- **Memory Metrics**: Use `process.runtime.dotnet.gc.objects.size` from OpenTelemetry .NET Runtime Instrumentation
- **OOM Detection**: Track `dotnet_exceptions_total{type="System.OutOfMemoryException"}` instead of Kubernetes `OOMKilled` events

## Prerequisites

1. **OpenTelemetry .NET Runtime Instrumentation** in your .NET applications
2. **Prometheus** server to scrape metrics
3. **Prometheus Adapter** to expose custom metrics API
4. **VPA** with external metrics support (v1.1.0+)

## Configuration

### 1. .NET Application Setup

Configure your .NET application with OpenTelemetry runtime instrumentation:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dotnet-app
spec:
  template:
    spec:
      containers:
      - name: app
        image: your-dotnet-app:latest
        env:
        # Enable OpenTelemetry .NET Runtime Instrumentation
        - name: OTEL_DOTNET_AUTO_PLUGINS
          value: "dotnet_runtime_instrumentation"
        - name: OTEL_METRICS_EXPORTER
          value: "prometheus"
        - name: OTEL_EXPORTER_PROMETHEUS_ENDPOINT
          value: "http://0.0.0.0:9090/metrics"
        # Optimize .NET GC for containerized environments
        - name: DOTNET_gcServer
          value: "1"
        - name: DOTNET_GCConserveMemory
          value: "1"
```

### 2. VPA Recommender Configuration

Configure VPA recommender with external metrics support:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: vpa-recommender
  namespace: kube-system
spec:
  template:
    spec:
      containers:
      - name: recommender
        image: registry.k8s.io/autoscaling/vpa-recommender:latest
        args:
        - --use-external-metrics=true
        - --external-metrics-memory-metric=process_runtime_dotnet_gc_objects_size
        - --external-metrics-oom-metric=dotnet_exceptions_total{type="System.OutOfMemoryException"}
        - --container-name-label=container
```

### 3. VPA Resource Configuration

Create VPA for your .NET application:

```yaml
apiVersion: autoscaling.k8s.io/v1
kind: VerticalPodAutoscaler
metadata:
  name: dotnet-app-vpa
spec:
  targetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: dotnet-app
  updatePolicy:
    updateMode: "Auto"
  resourcePolicy:
    containerPolicies:
    - containerName: app
      minAllowed:
        memory: 64Mi
      maxAllowed:
        memory: 2Gi
      controlledResources: ["memory"]
```

## Command Line Flags

The following new flag has been added to VPA recommender:

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--external-metrics-oom-metric` | string | "" | ALPHA. Metric to use with external metrics provider for OOM detection (e.g., `dotnet_exceptions_total{type="System.OutOfMemoryException"}`) |

## Metrics Requirements

### Memory Metric: `process_runtime_dotnet_gc_objects_size`

This metric measures the actual memory managed by the .NET Garbage Collector:

- **Excludes**: CLR overhead, pre-allocated pools, unmanaged memory
- **Includes**: Only objects tracked by the GC
- **Labels**: `pod`, `container`, `namespace`
- **Value**: Bytes of GC-managed memory

Example metric:
```
process_runtime_dotnet_gc_objects_size{pod="myapp-pod", container="app", namespace="default"} 104857600
```

### OOM Metric: `dotnet_exceptions_total`

This metric tracks .NET exceptions, filtered for OutOfMemoryException:

- **Query**: `dotnet_exceptions_total{type="System.OutOfMemoryException"}`
- **Labels**: `pod`, `container`, `namespace`, `type`
- **Value**: Cumulative count of OOM exceptions

Example metric:
```
dotnet_exceptions_total{pod="myapp-pod", container="app", namespace="default", type="System.OutOfMemoryException"} 3
```

## How It Works

1. **Memory Monitoring**: VPA queries `process_runtime_dotnet_gc_objects_size` for each container to get accurate GC memory usage
2. **OOM Detection**: VPA polls `dotnet_exceptions_total` and detects increases in OOM exception counts
3. **Recommendations**: VPA uses GC memory metrics for baseline recommendations and applies memory bump-up when OOM exceptions are detected
4. **Scaling**: VPA applies memory recommendations based on actual .NET memory usage patterns

## Benefits

- **Accurate Memory Scaling**: Based on actual GC heap usage, not total process memory
- **Proper OOM Handling**: Detects .NET OOM exceptions that Kubernetes misses
- **Better Resource Utilization**: Avoids over-provisioning due to CLR pre-allocation
- **Backward Compatibility**: Falls back to standard metrics when external metrics unavailable

## Troubleshooting

### Common Issues

1. **No external metrics available**
   - Verify Prometheus is scraping your application metrics
   - Check Prometheus Adapter configuration
   - Ensure custom metrics API is working: `kubectl get --raw "/apis/external.metrics.k8s.io/v1beta1"`

2. **OOM not detected**
   - Verify `dotnet_exceptions_total` metric is being exported
   - Check metric labels match container names
   - Confirm VPA is using external OOM observer: look for "Using external OOM observer" in logs

3. **Memory recommendations too low/high**
   - Monitor `process_runtime_dotnet_gc_objects_size` vs actual usage
   - Adjust VPA `minAllowed`/`maxAllowed` memory limits
   - Consider .NET GC settings (`DOTNET_GCConserveMemory`, heap limits)

### Debugging Commands

```bash
# Check VPA recommendations
kubectl describe vpa your-vpa-name

# View external metrics
kubectl get --raw "/apis/external.metrics.k8s.io/v1beta1/namespaces/default/process_runtime_dotnet_gc_objects_size"

# Check VPA recommender logs
kubectl logs -n kube-system deployment/vpa-recommender

# Monitor .NET metrics directly
kubectl port-forward pod/your-dotnet-pod 9090:9090
curl http://localhost:9090/metrics | grep -E "(process_runtime_dotnet_gc_objects_size|dotnet_exceptions_total)"
```

## Example

See [dotnet-example.yaml](../examples/dotnet-example.yaml) for a complete working example of VPA with .NET external metrics.

## Performance Considerations

- External metrics are queried every `--recommender-interval` (default: 1 minute)
- OOM checking adds minimal overhead to the recommender loop
- Prometheus queries are scoped to specific pods to minimize load
- Old container tracking data is cleaned up automatically

## Extensibility

This external metrics approach can be extended to other managed runtimes:

- **Java**: Use JVM heap metrics and OutOfMemoryError detection
- **Go**: Use Go runtime memory stats and panic detection  
- **Python**: Use Python GC metrics and MemoryError detection

The pattern established here provides a template for other languages with managed memory systems. 