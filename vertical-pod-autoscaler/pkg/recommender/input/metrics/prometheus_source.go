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
	"net/http"
	"strings"
	"sync"
	"time"

	promapi "github.com/prometheus/client_golang/api"
	prometheusv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	prommodel "github.com/prometheus/common/model"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"k8s.io/metrics/pkg/apis/metrics/v1beta1"

	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
)

const (
	// Default Prometheus queries using cAdvisor metrics with named template placeholders
	defaultPromCPUQuery    = `rate(container_cpu_usage_seconds_total{namespace=~"{{namespace}}",pod=~"{{pod}}",container!="",container!="POD",image!=""}[5m])`
	defaultPromMemoryQuery = `container_memory_working_set_bytes{namespace=~"{{namespace}}",pod=~"{{pod}}",container!="",container!="POD",image!=""}`
	defaultPromOOMQuery    = `container_oom_events_total{namespace=~"{{namespace}}",pod=~"{{pod}}",container!="",container!="POD"}`
	defaultNamespaceLabel  = "namespace"
	defaultPodLabel        = "pod"
	defaultContainerLabel  = "container"
)

// PrometheusSourceConfig configures the Prometheus metrics source.
type PrometheusSourceConfig struct {
	// Address of the Prometheus server (e.g., "http://prometheus.monitoring:9090")
	Address string
	// Authentication for Prometheus (optional)
	Auth *PrometheusAuth
	// InsecureSkipVerify skips TLS certificate verification
	InsecureSkipVerify bool
	// QueryTimeout for Prometheus queries
	QueryTimeout time.Duration
	// CPUQuery custom PromQL for CPU metrics (optional, uses default if empty)
	CPUQuery string
	// MemoryQuery custom PromQL for memory metrics (optional, uses default if empty)
	MemoryQuery string
	// OOMQuery custom PromQL for OOM counter metrics (optional, uses default if empty)
	OOMQuery string
	// ClusterState for looking up VPAs and matching pods
	ClusterState model.ClusterState
	// NamespaceLabel specifies which label contains the namespace in Prometheus results
	NamespaceLabel string
	// PodLabel specifies which label contains the pod name in Prometheus results
	PodLabel string
	// ContainerLabel specifies which label contains the container name in Prometheus results
	ContainerLabel string
}

// PrometheusAuth contains authentication credentials for Prometheus.
type PrometheusAuth struct {
	// BearerToken for token-based authentication
	BearerToken string
	// BasicAuth credentials
	BasicAuthUsername string
	BasicAuthPassword string
}

// prometheusMetricsSource queries Prometheus directly for container metrics.
type prometheusMetricsSource struct {
	client       prometheusv1.API
	clusterState model.ClusterState
	config       PrometheusSourceConfig

	// Track OOM counters for external observer integration
	lastOOMCounters map[model.ContainerID]uint64
	oomMutex        sync.RWMutex
}

// NewPrometheusMetricsSource creates a metrics source that queries Prometheus directly.
// This bypasses the external-metrics API, allowing VPA to coexist with KEDA and other
// external-metrics consumers.
func NewPrometheusMetricsSource(config PrometheusSourceConfig) (PodMetricsLister, error) {
	if config.Address == "" {
		return nil, fmt.Errorf("prometheus address is required")
	}

	if config.QueryTimeout == 0 {
		config.QueryTimeout = 30 * time.Second
	}

	// Set default queries if not provided
	if config.CPUQuery == "" {
		config.CPUQuery = defaultPromCPUQuery
	}
	if config.MemoryQuery == "" {
		config.MemoryQuery = defaultPromMemoryQuery
	}
	if config.OOMQuery == "" {
		config.OOMQuery = defaultPromOOMQuery
	}
	if config.NamespaceLabel == "" {
		config.NamespaceLabel = defaultNamespaceLabel
	}
	if config.PodLabel == "" {
		config.PodLabel = defaultPodLabel
	}
	if config.ContainerLabel == "" {
		config.ContainerLabel = defaultContainerLabel
	}

	// Create Prometheus client
	transport := promapi.DefaultRoundTripper
	if config.InsecureSkipVerify {
		if httpTransport, ok := transport.(*http.Transport); ok {
			httpTransport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
		}
	}

	// Add authentication if provided
	if config.Auth != nil {
		if config.Auth.BearerToken != "" {
			transport = &prometheusAuthTransport{
				token: config.Auth.BearerToken,
				base:  transport,
			}
		} else if config.Auth.BasicAuthUsername != "" && config.Auth.BasicAuthPassword != "" {
			transport = &prometheusBasicAuthTransport{
				username: config.Auth.BasicAuthUsername,
				password: config.Auth.BasicAuthPassword,
				base:     transport,
			}
		}
	}

	promConfig := promapi.Config{
		Address:      config.Address,
		RoundTripper: transport,
	}

	promClient, err := promapi.NewClient(promConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Prometheus client: %w", err)
	}

	api := prometheusv1.NewAPI(promClient)

	return &prometheusMetricsSource{
		client:          api,
		clusterState:    config.ClusterState,
		config:          config,
		lastOOMCounters: make(map[model.ContainerID]uint64),
	}, nil
}

// List fetches pod metrics from Prometheus for the specified namespace.
func (p *prometheusMetricsSource) List(ctx context.Context, namespace string, opts v1.ListOptions) (*v1beta1.PodMetricsList, error) {
	result := &v1beta1.PodMetricsList{}
	now := time.Now()

	// Get VPAs from cluster state to know which pods to query
	vpas := p.getRelevantVPAs(namespace)
	if len(vpas) == 0 {
		klog.V(4).InfoS("No VPAs found for namespace", "namespace", namespace)
		return result, nil
	}

	// Query metrics for each VPA's pods
	for _, vpa := range vpas {
		podMetrics, oomCounters, err := p.queryVPAMetrics(ctx, vpa, now)
		if err != nil {
			klog.ErrorS(err, "Failed to query Prometheus metrics for VPA",
				"vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName))
			continue
		}

		result.Items = append(result.Items, podMetrics...)

		// Update OOM counters
		if len(oomCounters) > 0 {
			p.oomMutex.Lock()
			for containerID, count := range oomCounters {
				p.lastOOMCounters[containerID] = count
			}
			p.oomMutex.Unlock()
		}
	}

	klog.V(3).InfoS("Fetched Prometheus metrics",
		"namespace", namespace,
		"vpaCount", len(vpas),
		"podMetrics", len(result.Items))

	return result, nil
}

// GetOOMCounters returns the latest OOM counters from Prometheus.
// This method actively queries Prometheus to get fresh data for the external OOM observer.
func (p *prometheusMetricsSource) GetOOMCounters() map[model.ContainerID]uint64 {
	ctx, cancel := context.WithTimeout(context.Background(), p.config.QueryTimeout)
	defer cancel()

	result := make(map[model.ContainerID]uint64)

	// Query OOM counters for all VPAs
	vpas := p.getRelevantVPAs("")
	for _, vpa := range vpas {
		matchingPods := p.clusterState.GetMatchingPods(vpa)
		if len(matchingPods) == 0 {
			continue
		}

		podRegex := buildPodRegex(matchingPods)
		oomQuery := substituteQueryTemplate(p.config.OOMQuery, vpa.ID.Namespace, podRegex)

		// Query OOM counters
		oomMetrics, err := p.queryPrometheus(ctx, oomQuery, time.Now())
		if err != nil {
			klog.V(4).ErrorS(err, "Failed to query OOM counters",
				"vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName))
			continue
		}

		// Parse OOM counters
		for _, metric := range oomMetrics {
			count := uint64(metric.value)
			result[metric.containerID] = count
		}
	}

	// Update cache
	p.oomMutex.Lock()
	p.lastOOMCounters = result
	p.oomMutex.Unlock()

	return result
}

func (p *prometheusMetricsSource) getRelevantVPAs(namespace string) []*model.Vpa {
	var vpas []*model.Vpa
	for _, vpa := range p.clusterState.VPAs() {
		if vpa.PodCount == 0 {
			continue
		}
		if namespace != "" && vpa.ID.Namespace != namespace {
			continue
		}
		vpas = append(vpas, vpa)
	}
	return vpas
}

func (p *prometheusMetricsSource) queryVPAMetrics(ctx context.Context, vpa *model.Vpa, timestamp time.Time) ([]v1beta1.PodMetrics, map[model.ContainerID]uint64, error) {
	// Get pods matching this VPA
	matchingPods := p.clusterState.GetMatchingPods(vpa)
	if len(matchingPods) == 0 {
		return nil, nil, nil
	}

	// Build pod name regex for Prometheus query
	podRegex := buildPodRegex(matchingPods)
	namespace := vpa.ID.Namespace

	// Build queries using named template substitution
	cpuQuery := substituteQueryTemplate(p.config.CPUQuery, namespace, podRegex)
	memoryQuery := substituteQueryTemplate(p.config.MemoryQuery, namespace, podRegex)
	oomQuery := substituteQueryTemplate(p.config.OOMQuery, namespace, podRegex)

	// Query CPU metrics
	cpuMetrics, err := p.queryPrometheus(ctx, cpuQuery, timestamp)
	if err != nil {
		return nil, nil, fmt.Errorf("cpu query failed: %w", err)
	}

	// Query memory metrics
	memoryMetrics, err := p.queryPrometheus(ctx, memoryQuery, timestamp)
	if err != nil {
		return nil, nil, fmt.Errorf("memory query failed: %w", err)
	}

	// Query OOM counters (optional, don't fail if unavailable)
	oomCounters := make(map[model.ContainerID]uint64)
	if p.config.OOMQuery != "" {
		counters, err := p.queryOOMCounters(ctx, oomQuery, timestamp)
		if err != nil {
			klog.V(4).InfoS("Failed to query OOM counters (non-fatal)",
				"vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName),
				"error", err)
		} else {
			oomCounters = counters
		}
	}

	// Aggregate metrics by pod
	podMetrics := p.aggregateMetricsByPod(cpuMetrics, memoryMetrics, timestamp)

	return podMetrics, oomCounters, nil
}

func (p *prometheusMetricsSource) queryPrometheus(ctx context.Context, query string, timestamp time.Time) ([]prometheusMetric, error) {
	queryCtx, cancel := context.WithTimeout(ctx, p.config.QueryTimeout)
	defer cancel()

	klog.V(4).InfoS("Executing Prometheus query", "query", query, "timestamp", timestamp)
	result, warnings, err := p.client.Query(queryCtx, query, timestamp)
	if err != nil {
		return nil, fmt.Errorf("prometheus query failed: %w", err)
	}

	if len(warnings) > 0 {
		klog.V(4).InfoS("Prometheus query warnings", "warnings", warnings)
	}

	vector, ok := result.(prommodel.Vector)
	if !ok {
		return nil, fmt.Errorf("expected vector result, got %T", result)
	}

	var metrics []prometheusMetric
	for _, sample := range vector {
		containerID, err := extractContainerIDFromLabels(sample.Metric, p.config.NamespaceLabel, p.config.PodLabel, p.config.ContainerLabel)
		if err != nil {
			klog.V(4).InfoS("Skipping sample due to label extraction error",
				"error", err,
				"labels", sample.Metric)
			continue
		}

		metrics = append(metrics, prometheusMetric{
			containerID: containerID,
			value:       float64(sample.Value),
			timestamp:   sample.Timestamp.Time(),
		})
	}

	return metrics, nil
}

func (p *prometheusMetricsSource) queryOOMCounters(ctx context.Context, query string, timestamp time.Time) (map[model.ContainerID]uint64, error) {
	queryCtx, cancel := context.WithTimeout(ctx, p.config.QueryTimeout)
	defer cancel()

	result, _, err := p.client.Query(queryCtx, query, timestamp)
	if err != nil {
		return nil, fmt.Errorf("oom counter query failed: %w", err)
	}

	vector, ok := result.(prommodel.Vector)
	if !ok {
		return nil, fmt.Errorf("expected vector result, got %T", result)
	}

	counters := make(map[model.ContainerID]uint64)
	for _, sample := range vector {
		containerID, err := extractContainerIDFromLabels(sample.Metric, p.config.NamespaceLabel, p.config.PodLabel, p.config.ContainerLabel)
		if err != nil {
			klog.V(4).InfoS("Skipping OOM counter due to label extraction error",
				"error", err,
				"labels", sample.Metric)
			continue
		}

		counters[containerID] = uint64(sample.Value)
	}

	return counters, nil
}

func (p *prometheusMetricsSource) aggregateMetricsByPod(cpuMetrics, memoryMetrics []prometheusMetric, timestamp time.Time) []v1beta1.PodMetrics {
	podMetricsMap := make(map[model.PodID]*v1beta1.PodMetrics)

	// Helper to ensure pod entry exists
	ensurePod := func(podID model.PodID) *v1beta1.PodMetrics {
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

	// Build map of container resources
	containerResources := make(map[model.ContainerID]apiv1.ResourceList)

	// Add CPU metrics
	for _, metric := range cpuMetrics {
		if _, exists := containerResources[metric.containerID]; !exists {
			containerResources[metric.containerID] = make(apiv1.ResourceList)
		}
		// Convert cores to millicores
		cpuMillis := int64(metric.value * 1000)
		containerResources[metric.containerID][apiv1.ResourceCPU] = *resource.NewMilliQuantity(cpuMillis, resource.DecimalSI)
	}

	// Add memory metrics
	for _, metric := range memoryMetrics {
		if _, exists := containerResources[metric.containerID]; !exists {
			containerResources[metric.containerID] = make(apiv1.ResourceList)
		}
		memoryBytes := int64(metric.value)
		containerResources[metric.containerID][apiv1.ResourceMemory] = *resource.NewQuantity(memoryBytes, resource.BinarySI)
	}

	// Build pod metrics from container resources
	for containerID, resources := range containerResources {
		pm := ensurePod(containerID.PodID)
		pm.Containers = append(pm.Containers, v1beta1.ContainerMetrics{
			Name:  containerID.ContainerName,
			Usage: resources,
		})
	}

	// Convert map to slice
	result := make([]v1beta1.PodMetrics, 0, len(podMetricsMap))
	for _, pm := range podMetricsMap {
		result = append(result, *pm)
	}

	return result
}

// prometheusMetric represents a single metric value from Prometheus.
type prometheusMetric struct {
	containerID model.ContainerID
	value       float64
	timestamp   time.Time
}

// buildPodRegex creates a regex pattern for matching pod names in Prometheus queries.
func buildPodRegex(pods []model.PodID) string {
	if len(pods) == 0 {
		return ".*"
	}

	regex := ""
	for i, pod := range pods {
		if i > 0 {
			regex += "|"
		}
		regex += pod.PodName
	}
	return regex
}

// substituteQueryTemplate replaces named placeholders in Prometheus query templates.
// Uses {{namespace}} and {{pod}} placeholders for safe, order-independent template substitution.
func substituteQueryTemplate(query, namespace, podRegex string) string {
	result := strings.ReplaceAll(query, "{{namespace}}", namespace)
	result = strings.ReplaceAll(result, "{{pod}}", podRegex)
	return result
}

// extractContainerIDFromLabels extracts namespace, pod, and container from Prometheus labels.
func extractContainerIDFromLabels(labels prommodel.Metric, namespaceLabel, podLabel, containerLabel string) (model.ContainerID, error) {
	namespace, ok := labels[prommodel.LabelName(namespaceLabel)]
	if !ok {
		return model.ContainerID{}, fmt.Errorf("missing '%s' label", namespaceLabel)
	}

	pod, ok := labels[prommodel.LabelName(podLabel)]
	if !ok {
		return model.ContainerID{}, fmt.Errorf("missing '%s' label", podLabel)
	}

	container, ok := labels[prommodel.LabelName(containerLabel)]
	if !ok {
		return model.ContainerID{}, fmt.Errorf("missing '%s' label", containerLabel)
	}

	return model.ContainerID{
		PodID: model.PodID{
			Namespace: string(namespace),
			PodName:   string(pod),
		},
		ContainerName: string(container),
	}, nil
}

// prometheusAuthTransport adds bearer token authentication to HTTP requests.
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

// prometheusBasicAuthTransport adds basic authentication to HTTP requests.
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
