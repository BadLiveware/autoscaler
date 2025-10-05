/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package metrics

import (
	"context"
	"crypto/tls"
	"fmt"
	"maps"
	"net/http"
	"sync"
	"time"

	promapi "github.com/prometheus/client_golang/api"
	prometheusv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	prommodel "github.com/prometheus/common/model"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"k8s.io/metrics/pkg/apis/metrics/v1beta1"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
)

// TelemetryAwareSource routes metrics queries based on per-VPA telemetry configuration.
type TelemetryAwareSource struct {
	clusterState     model.ClusterState
	kubeConfig       *rest.Config
	defaultSource    PodMetricsLister
	promClients      map[string]prometheusv1.API
	promClientsMutex sync.RWMutex
	lastOOMCounters  map[model.ContainerID]uint64 // Track OOM counters from last fetch
	oomMutex         sync.RWMutex
}

// NewTelemetryAwareSource creates a metrics source that respects VPA telemetry configs.
func NewTelemetryAwareSource(config *rest.Config, clusterState model.ClusterState, defaultSource PodMetricsLister) *TelemetryAwareSource {
	return &TelemetryAwareSource{
		clusterState:    clusterState,
		kubeConfig:      config,
		defaultSource:   defaultSource,
		promClients:     make(map[string]prometheusv1.API),
		lastOOMCounters: make(map[model.ContainerID]uint64),
	}
}

// GetOOMCounters returns the current OOM counters tracked by this source.
func (t *TelemetryAwareSource) GetOOMCounters() map[model.ContainerID]uint64 {
	t.oomMutex.RLock()
	defer t.oomMutex.RUnlock()

	result := make(map[model.ContainerID]uint64, len(t.lastOOMCounters))
	maps.Copy(result, t.lastOOMCounters)
	return result
}

// List fetches metrics for pods, routing to the appropriate backend based on VPA telemetry config.
func (t *TelemetryAwareSource) List(ctx context.Context, namespace string, opts v1.ListOptions) (*v1beta1.PodMetricsList, error) {
	result := &v1beta1.PodMetricsList{}

	// Group VPAs by their telemetry backend
	vpasUsingKubernetes := []*model.Vpa{}
	vpasUsingPrometheus := map[string][]*model.Vpa{} // keyed by prometheus address

	for _, vpa := range t.clusterState.VPAs() {
		if vpa.PodCount == 0 {
			continue
		}
		if namespace != "" && vpa.ID.Namespace != namespace {
			continue
		}

		source := t.getEffectiveTelemetrySource(vpa)
		switch source {
		case vpa_types.TelemetrySourceKubernetes, vpa_types.TelemetrySourceAuto:
			vpasUsingKubernetes = append(vpasUsingKubernetes, vpa)
		case vpa_types.TelemetrySourcePrometheus:
			address := t.getPrometheusAddress(vpa)
			if address == "" {
				klog.V(2).InfoS("VPA configured for Prometheus but missing address, falling back to default", "vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName))
				vpasUsingKubernetes = append(vpasUsingKubernetes, vpa)
			} else {
				vpasUsingPrometheus[address] = append(vpasUsingPrometheus[address], vpa)
			}
		}
	}

	// Fetch from Kubernetes metrics server for default VPAs
	if len(vpasUsingKubernetes) > 0 {
		klog.V(4).InfoS("Fetching metrics from Kubernetes source", "vpaCount", len(vpasUsingKubernetes), "namespace", namespace)
		metrics, err := t.defaultSource.List(ctx, namespace, opts)
		if err != nil {
			klog.ErrorS(err, "Failed to fetch metrics from default source")
		} else if metrics != nil {
			result.Items = append(result.Items, metrics.Items...)
		}
	}

	// Fetch from Prometheus for configured VPAs
	for address, vpas := range vpasUsingPrometheus {
		klog.V(3).InfoS("Fetching metrics from Prometheus", "address", address, "vpaCount", len(vpas))
		promMetrics, oomCounters, err := t.fetchPrometheusMetrics(ctx, address, vpas)
		if err != nil {
			klog.ErrorS(err, "Failed to fetch Prometheus metrics", "address", address, "vpaCount", len(vpas))

			// Set TelemetryUnavailable condition and emit metrics for affected VPAs
			for _, vpa := range vpas {
				t.setTelemetryCondition(vpa, address, err)
				RecordTelemetryError(vpa.ID.Namespace, vpa.ID.VpaName, TelemetrySourcePrometheus, err)

				// Check if fallback is enabled for this VPA
				if t.shouldFallback(vpa) {
					klog.InfoS("Falling back to Kubernetes metrics-server",
						"vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName),
						"reason", "Prometheus unavailable")

					// Mark as using fallback so we don't filter out Kubernetes metrics
					vpa.UsingFallback = true
					vpa.TelemetryFailed = false

					// Fetch metrics from Kubernetes immediately for this VPA
					fallbackMetrics, fallbackErr := t.defaultSource.List(ctx, namespace, opts)
					if fallbackErr != nil {
						klog.ErrorS(fallbackErr, "Failed to fetch fallback metrics from Kubernetes",
							"vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName))
					} else if fallbackMetrics != nil {
						result.Items = append(result.Items, fallbackMetrics.Items...)
						klog.V(3).InfoS("Successfully fell back to Kubernetes metrics",
							"vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName),
							"metricCount", len(fallbackMetrics.Items))
					}
				} else {
					// Fallback disabled - mark VPA as telemetry failed (fail-closed)
					vpa.TelemetryFailed = true
					vpa.UsingFallback = false
					klog.InfoS("Telemetry failed and fallback disabled, VPA will not receive metrics",
						"vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName))
				}
			}
			continue
		}

		// Clear TelemetryUnavailable condition and record success for affected VPAs
		for _, vpa := range vpas {
			t.clearTelemetryCondition(vpa)
			RecordTelemetrySuccess(vpa.ID.Namespace, vpa.ID.VpaName, TelemetrySourcePrometheus)
			// Clear telemetry status flags on success
			vpa.TelemetryFailed = false
			vpa.UsingFallback = false
			// UsingOOMOnly should remain set if it was set in hasOnlyCustomOOM path
		}

		if promMetrics != nil {
			result.Items = append(result.Items, promMetrics.Items...)
		}

		// Store OOM counters for delta tracking
		if len(oomCounters) > 0 {
			t.oomMutex.Lock()
			maps.Copy(t.lastOOMCounters, oomCounters)
			t.oomMutex.Unlock()
		}
	}

	return result, nil
}

func (t *TelemetryAwareSource) getEffectiveTelemetrySource(vpa *model.Vpa) vpa_types.TelemetrySource {
	if vpa.Telemetry == nil {
		return vpa_types.TelemetrySourceAuto
	}
	return vpa.Telemetry.Source
}

func (t *TelemetryAwareSource) getPrometheusAddress(vpa *model.Vpa) string {
	if vpa.Telemetry == nil || vpa.Telemetry.Prometheus == nil {
		return ""
	}
	return vpa.Telemetry.Prometheus.Address
}

func (t *TelemetryAwareSource) getOrCreatePrometheusClient(address string, auth *vpa_types.TelemetryAuth, insecure bool) (prometheusv1.API, error) {
	// Use address + auth signature as cache key
	cacheKey := address
	if auth != nil {
		cacheKey = fmt.Sprintf("%s:%s:%s", address, auth.BearerToken, auth.BasicAuthUsername)
	}

	t.promClientsMutex.RLock()
	client, exists := t.promClients[cacheKey]
	t.promClientsMutex.RUnlock()
	if exists {
		return client, nil
	}

	t.promClientsMutex.Lock()
	defer t.promClientsMutex.Unlock()

	// Double-check after acquiring write lock
	if client, exists := t.promClients[cacheKey]; exists {
		return client, nil
	}

	// Build Prometheus client with optional auth
	transport := promapi.DefaultRoundTripper
	if insecure {
		transport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	if auth != nil {
		if auth.BearerToken != "" {
			transport = &prometheusAuthTransport{
				token: auth.BearerToken,
				base:  transport,
			}
		} else if auth.BasicAuthUsername != "" && auth.BasicAuthPassword != "" {
			transport = &prometheusBasicAuthTransport{
				username: auth.BasicAuthUsername,
				password: auth.BasicAuthPassword,
				base:     transport,
			}
		}
	}

	promConfig := promapi.Config{
		Address:      address,
		RoundTripper: transport,
	}

	promClient, err := promapi.NewClient(promConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Prometheus client for %s: %w", address, err)
	}

	api := prometheusv1.NewAPI(promClient)
	t.promClients[cacheKey] = api
	return api, nil
}

func (t *TelemetryAwareSource) fetchPrometheusMetrics(ctx context.Context, address string, vpas []*model.Vpa) (*v1beta1.PodMetricsList, map[model.ContainerID]uint64, error) {
	// Get first VPA's config for auth and insecure flag (assume all VPAs sharing address use same config)
	var auth *vpa_types.TelemetryAuth
	var insecure bool
	var queryConfig *vpa_types.PrometheusTelemetryQuery
	if len(vpas) > 0 && vpas[0].Telemetry != nil && vpas[0].Telemetry.Prometheus != nil {
		auth = vpas[0].Telemetry.Prometheus.Authentication
		insecure = vpas[0].Telemetry.Prometheus.Insecure
		queryConfig = vpas[0].Telemetry.Prometheus.Query
	}

	client, err := t.getOrCreatePrometheusClient(address, auth, insecure)
	if err != nil {
		return nil, nil, err
	}

	result := &v1beta1.PodMetricsList{}
	allOOMCounters := make(map[model.ContainerID]uint64)
	now := time.Now()

	// Check if ALL queries are custom (for unfiltered mode)
	cpuQuery := t.getCPUQuery(queryConfig, vpas)
	memoryQuery := t.getMemoryQuery(queryConfig, vpas)
	oomQuery := t.getOOMCounterQuery(queryConfig, vpas)

	useFullCustomQueries := cpuQuery != "" && memoryQuery != ""

	// If only OOM is custom (partial custom queries), use Kubernetes for CPU/Memory
	// and only fetch OOM from Prometheus to avoid empty result issues
	hasOnlyCustomOOM := oomQuery != "" && cpuQuery == "" && memoryQuery == ""

	if hasOnlyCustomOOM {
		// Only OOM query is custom - fetch only OOM counters from Prometheus
		// CPU/Memory should come from Kubernetes metrics-server
		klog.V(3).InfoS("Fetching only OOM counters from Prometheus (partial custom queries)",
			"address", address, "vpaCount", len(vpas))

		oomCounters, err := t.queryOOMCounters(ctx, client, oomQuery, now)
		if err != nil {
			klog.ErrorS(err, "Failed to query OOM counters from Prometheus", "address", address)
			// OOM query failed - but don't fail the whole fetch since CPU/Memory come from Kubernetes
			// Just log the error and continue
		} else {
			maps.Copy(allOOMCounters, oomCounters)
			klog.V(4).InfoS("Successfully queried OOM counters", "query", oomQuery, "count", len(oomCounters))
		}

		// Don't fetch CPU/Memory from Prometheus - return empty podMetrics
		// Kubernetes metrics-server will provide those via normal flow
		// Mark VPAs as using OOM-only mode so Kubernetes metrics aren't skipped
		for _, vpa := range vpas {
			vpa.UsingOOMOnly = true
		}
		klog.V(4).InfoS("Fetched Prometheus metrics (OOM only)", "address", address, "vpaCount", len(vpas), "oomCounters", len(allOOMCounters))
		return result, allOOMCounters, nil
	} else if useFullCustomQueries {
		// All required queries are custom: use as-is without pod filtering
		cpuMetrics, err := t.queryPrometheus(ctx, client, cpuQuery, now)
		if err != nil {
			klog.ErrorS(err, "Failed to query CPU metrics from Prometheus", "address", address)
			// Return error for CPU query failure - this is a critical metric
			return nil, nil, fmt.Errorf("cpu query failed: %w", err)
		}

		memoryMetrics, err := t.queryPrometheus(ctx, client, memoryQuery, now)
		if err != nil {
			klog.ErrorS(err, "Failed to query memory metrics from Prometheus", "address", address)
			// Return error for memory query failure - this is a critical metric
			return nil, nil, fmt.Errorf("memory query failed: %w", err)
		}

		var oomCounters map[model.ContainerID]uint64
		if oomQuery != "" {
			oomCounters, err = t.queryOOMCounters(ctx, client, oomQuery, now)
			if err != nil {
				klog.ErrorS(err, "Failed to query OOM counters from Prometheus", "address", address)
				// OOM counter is optional - don't fail the whole fetch if it's unavailable
			} else {
				maps.Copy(allOOMCounters, oomCounters)
			}
		}

		podMetricsMap := t.aggregateMetricsByPod(cpuMetrics, memoryMetrics, oomCounters)
		for _, podMetrics := range podMetricsMap {
			result.Items = append(result.Items, podMetrics)
		}
	} else {
		// Use per-VPA queries (optimized with pod selectors, supports partial custom queries)
		var lastErr error
		successCount := 0
		for _, vpa := range vpas {
			vpaMetrics, vpaOOMCounters, err := t.fetchPrometheusMetricsForVPA(ctx, client, vpa, now, queryConfig)
			if err != nil {
				klog.ErrorS(err, "Failed to fetch Prometheus metrics for VPA", "vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName))
				lastErr = err
				continue
			}
			successCount++
			result.Items = append(result.Items, vpaMetrics.Items...)
			maps.Copy(allOOMCounters, vpaOOMCounters)
		}

		// If ALL VPAs failed, return the error to trigger fallback
		if successCount == 0 && lastErr != nil {
			return nil, nil, fmt.Errorf("all VPA queries failed: %w", lastErr)
		}
	}

	klog.V(4).InfoS("Fetched Prometheus metrics", "address", address, "vpaCount", len(vpas), "podCount", len(result.Items), "oomCounters", len(allOOMCounters))
	return result, allOOMCounters, nil
}

// Default metric queries aligned with cAdvisor/Prometheus conventions
const (
	defaultCPUQueryTemplate    = `rate(container_cpu_usage_seconds_total{container!="POD", container!="", namespace="%s", pod=~"%s"}[5m])`
	defaultMemoryQueryTemplate = `container_memory_working_set_bytes{container!="POD", container!="", namespace="%s", pod=~"%s"}`
	defaultOOMQueryTemplate    = `container_oom_events_total{container!="POD", container!="", namespace="%s", pod=~"%s"}`
)

func (t *TelemetryAwareSource) getCPUQuery(queryConfig *vpa_types.PrometheusTelemetryQuery, vpas []*model.Vpa) string {
	if queryConfig != nil && queryConfig.CPUUsageQuery != "" {
		return queryConfig.CPUUsageQuery
	}
	// Return empty for default case; per-VPA queries will be built separately
	return ""
}

func (t *TelemetryAwareSource) getMemoryQuery(queryConfig *vpa_types.PrometheusTelemetryQuery, vpas []*model.Vpa) string {
	if queryConfig != nil && queryConfig.MemoryUsageQuery != "" {
		return queryConfig.MemoryUsageQuery
	}
	// Return empty for default case; per-VPA queries will be built separately
	return ""
}

func (t *TelemetryAwareSource) getOOMCounterQuery(queryConfig *vpa_types.PrometheusTelemetryQuery, vpas []*model.Vpa) string {
	if queryConfig != nil && queryConfig.OOMCountQuery != "" {
		return queryConfig.OOMCountQuery
	}
	// Return empty for default case; per-VPA queries will be built separately
	return ""
}

func (t *TelemetryAwareSource) fetchPrometheusMetricsForVPA(ctx context.Context, client prometheusv1.API, vpa *model.Vpa, timestamp time.Time, queryConfig *vpa_types.PrometheusTelemetryQuery) (*v1beta1.PodMetricsList, map[model.ContainerID]uint64, error) {
	result := &v1beta1.PodMetricsList{}
	allOOMCounters := make(map[model.ContainerID]uint64)

	// Get pods matching this VPA
	matchingPods := t.clusterState.GetMatchingPods(vpa)
	if len(matchingPods) == 0 {
		return result, allOOMCounters, nil
	}

	// Build pod name regex for query
	podNames := ""
	for i, pod := range matchingPods {
		if i > 0 {
			podNames += "|"
		}
		podNames += pod.PodName
	}

	namespace := vpa.ID.Namespace

	// Build optimized queries for this VPA's pods (use custom queries if provided)
	var cpuQuery, memoryQuery, oomQuery string

	if queryConfig != nil && queryConfig.CPUUsageQuery != "" {
		cpuQuery = queryConfig.CPUUsageQuery
	} else {
		cpuQuery = fmt.Sprintf(defaultCPUQueryTemplate, namespace, podNames)
	}

	if queryConfig != nil && queryConfig.MemoryUsageQuery != "" {
		memoryQuery = queryConfig.MemoryUsageQuery
	} else {
		memoryQuery = fmt.Sprintf(defaultMemoryQueryTemplate, namespace, podNames)
	}

	if queryConfig != nil && queryConfig.OOMCountQuery != "" {
		oomQuery = queryConfig.OOMCountQuery
	} else {
		oomQuery = fmt.Sprintf(defaultOOMQueryTemplate, namespace, podNames)
	}

	cpuStart := time.Now()
	cpuMetrics, err := t.queryPrometheus(ctx, client, cpuQuery, timestamp)
	RecordQueryDuration(vpa.ID.Namespace, vpa.ID.VpaName, TelemetrySourcePrometheus, "cpu", time.Since(cpuStart))
	if err != nil {
		klog.V(4).InfoS("Failed to query CPU metrics", "vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName), "error", err)
		// Return error for CPU query failure - this is a critical metric
		return nil, nil, fmt.Errorf("cpu query failed: %w", err)
	}

	memoryStart := time.Now()
	memoryMetrics, err := t.queryPrometheus(ctx, client, memoryQuery, timestamp)
	RecordQueryDuration(vpa.ID.Namespace, vpa.ID.VpaName, TelemetrySourcePrometheus, "memory", time.Since(memoryStart))
	if err != nil {
		klog.V(4).InfoS("Failed to query memory metrics", "vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName), "error", err)
		// Return error for memory query failure - this is a critical metric
		return nil, nil, fmt.Errorf("memory query failed: %w", err)
	}

	oomStart := time.Now()
	oomCounters, err := t.queryOOMCounters(ctx, client, oomQuery, timestamp)
	RecordQueryDuration(vpa.ID.Namespace, vpa.ID.VpaName, TelemetrySourcePrometheus, "oom", time.Since(oomStart))
	if err != nil {
		klog.ErrorS(err, "Failed to query OOM counters", "vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName), "query", oomQuery)
		// OOM counter is optional - don't fail the whole fetch if it's unavailable
	} else {
		klog.V(4).InfoS("Successfully queried OOM counters", "vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName), "query", oomQuery, "count", len(oomCounters))
		maps.Copy(allOOMCounters, oomCounters)
	}

	podMetricsMap := t.aggregateMetricsByPod(cpuMetrics, memoryMetrics, oomCounters)
	for _, podMetrics := range podMetricsMap {
		result.Items = append(result.Items, podMetrics)
	}

	return result, allOOMCounters, nil
}

type prometheusMetricResult struct {
	containerID model.ContainerID
	value       float64
	timestamp   time.Time
}

func (t *TelemetryAwareSource) queryPrometheus(ctx context.Context, client prometheusv1.API, query string, timestamp time.Time) ([]prometheusMetricResult, error) {
	result, _, err := client.Query(ctx, query, timestamp)
	if err != nil {
		return nil, fmt.Errorf("prometheus query failed: %w", err)
	}

	vector, ok := result.(prommodel.Vector)
	if !ok {
		return nil, fmt.Errorf("expected vector result, got %T", result)
	}

	var metrics []prometheusMetricResult
	for _, sample := range vector {
		containerID, err := t.extractContainerID(sample.Metric)
		if err != nil {
			klog.V(4).InfoS("Skipping sample due to label extraction error", "error", err, "metric", sample.Metric)
			continue
		}

		metrics = append(metrics, prometheusMetricResult{
			containerID: containerID,
			value:       float64(sample.Value),
			timestamp:   sample.Timestamp.Time(),
		})
	}

	return metrics, nil
}

func (t *TelemetryAwareSource) queryOOMCounters(ctx context.Context, client prometheusv1.API, query string, timestamp time.Time) (map[model.ContainerID]uint64, error) {
	result, _, err := client.Query(ctx, query, timestamp)
	if err != nil {
		return nil, fmt.Errorf("oom counter query failed: %w", err)
	}

	vector, ok := result.(prommodel.Vector)
	if !ok {
		return nil, fmt.Errorf("expected vector result, got %T", result)
	}

	counters := make(map[model.ContainerID]uint64)
	for _, sample := range vector {
		containerID, err := t.extractContainerID(sample.Metric)
		if err != nil {
			klog.V(4).InfoS("Skipping OOM counter due to label extraction error", "error", err, "metric", sample.Metric)
			continue
		}

		counters[containerID] = uint64(sample.Value)
	}

	return counters, nil
}

func (t *TelemetryAwareSource) extractContainerID(metric prommodel.Metric) (model.ContainerID, error) {
	namespace, ok := metric["namespace"]
	if !ok {
		return model.ContainerID{}, fmt.Errorf("missing namespace label")
	}

	podName, ok := metric["pod"]
	if !ok {
		podName, ok = metric["pod_name"]
		if !ok {
			return model.ContainerID{}, fmt.Errorf("missing pod/pod_name label")
		}
	}

	containerName, ok := metric["container"]
	if !ok {
		containerName, ok = metric["name"]
		if !ok {
			return model.ContainerID{}, fmt.Errorf("missing container/name label")
		}
	}

	return model.ContainerID{
		PodID: model.PodID{
			Namespace: string(namespace),
			PodName:   string(podName),
		},
		ContainerName: string(containerName),
	}, nil
}

func (t *TelemetryAwareSource) aggregateMetricsByPod(cpuMetrics, memoryMetrics []prometheusMetricResult, oomCounters map[model.ContainerID]uint64) map[model.PodID]v1beta1.PodMetrics {
	podMetricsMap := make(map[model.PodID]*v1beta1.PodMetrics)

	// Helper to ensure pod entry exists
	ensurePod := func(podID model.PodID, timestamp time.Time) *v1beta1.PodMetrics {
		if pm, exists := podMetricsMap[podID]; exists {
			return pm
		}
		pm := &v1beta1.PodMetrics{
			ObjectMeta: v1.ObjectMeta{
				Name:      podID.PodName,
				Namespace: podID.Namespace,
			},
			Timestamp:  v1.Time{Time: timestamp},
			Window:     v1.Duration{Duration: 5 * time.Minute}, // Match CPU rate window
			Containers: []v1beta1.ContainerMetrics{},
		}
		podMetricsMap[podID] = pm
		return pm
	}

	// Build map of container resources and track which containers have metrics
	containerResources := make(map[model.ContainerID]apiv1.ResourceList)
	containersWithMetrics := make(map[model.ContainerID]bool)

	for _, metric := range cpuMetrics {
		containersWithMetrics[metric.containerID] = true
		if _, exists := containerResources[metric.containerID]; !exists {
			containerResources[metric.containerID] = make(apiv1.ResourceList)
		}
		// Convert cores to millicores
		cpuMillis := int64(metric.value * 1000)
		containerResources[metric.containerID][apiv1.ResourceCPU] = *resource.NewMilliQuantity(cpuMillis, resource.DecimalSI)
	}

	for _, metric := range memoryMetrics {
		containersWithMetrics[metric.containerID] = true
		if _, exists := containerResources[metric.containerID]; !exists {
			containerResources[metric.containerID] = make(apiv1.ResourceList)
		}
		memoryBytes := int64(metric.value)
		containerResources[metric.containerID][apiv1.ResourceMemory] = *resource.NewQuantity(memoryBytes, resource.BinarySI)
	}

	// Build pod metrics from container resources
	for containerID, resources := range containerResources {
		pm := ensurePod(containerID.PodID, time.Now())
		pm.Containers = append(pm.Containers, v1beta1.ContainerMetrics{
			Name:  containerID.ContainerName,
			Usage: resources,
		})
	}

	// Note: OOM counters are currently tracked but not yet integrated into the metrics flow.
	// They will be processed separately when workstream 4 is implemented.
	if len(oomCounters) > 0 {
		klog.V(4).InfoS("Received OOM counters from Prometheus", "containerCount", len(oomCounters))
	}

	// Convert map to return type
	finalResult := make(map[model.PodID]v1beta1.PodMetrics)
	for podID, pm := range podMetricsMap {
		finalResult[podID] = *pm
	}

	return finalResult
}

// prometheusAuthTransport injects bearer token into HTTP requests.
type prometheusAuthTransport struct {
	token string
	base  http.RoundTripper
}

func (t *prometheusAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rt := t.base
	if rt == nil {
		rt = http.DefaultTransport
	}
	cloned := req.Clone(req.Context())
	cloned.Header.Set("Authorization", fmt.Sprintf("Bearer %s", t.token))
	return rt.RoundTrip(cloned)
}

// prometheusBasicAuthTransport injects basic auth into HTTP requests.
type prometheusBasicAuthTransport struct {
	username string
	password string
	base     http.RoundTripper
}

func (t *prometheusBasicAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rt := t.base
	if rt == nil {
		rt = http.DefaultTransport
	}
	cloned := req.Clone(req.Context())
	cloned.SetBasicAuth(t.username, t.password)
	return rt.RoundTrip(cloned)
}

// setTelemetryCondition sets the TelemetryUnavailable condition on a VPA.
func (t *TelemetryAwareSource) setTelemetryCondition(vpa *model.Vpa, address string, err error) {
	if vpa == nil {
		return
	}

	message := FormatTelemetryConditionMessage(TelemetrySourcePrometheus, address, err)
	reason := ClassifyTelemetryError(err)

	vpa.Conditions.Set(vpa_types.TelemetryUnavailable, true, reason, message)
	klog.V(2).InfoS("Set TelemetryUnavailable condition",
		"vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName),
		"reason", reason,
		"message", message)
}

// clearTelemetryCondition removes the TelemetryUnavailable condition from a VPA.
func (t *TelemetryAwareSource) clearTelemetryCondition(vpa *model.Vpa) {
	if vpa == nil {
		return
	}

	// Only clear if the condition was set
	if _, exists := vpa.Conditions[vpa_types.TelemetryUnavailable]; exists {
		delete(vpa.Conditions, vpa_types.TelemetryUnavailable)
		klog.V(3).InfoS("Cleared TelemetryUnavailable condition",
			"vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName))
	}
}

// shouldFallback checks if a VPA should fall back to Kubernetes metrics-server on failure.
func (t *TelemetryAwareSource) shouldFallback(vpa *model.Vpa) bool {
	if vpa == nil || vpa.Telemetry == nil {
		return false
	}

	// Default is false (fail-closed for safety)
	if vpa.Telemetry.FallbackOnFailure == nil {
		return false
	}

	return *vpa.Telemetry.FallbackOnFailure
}
