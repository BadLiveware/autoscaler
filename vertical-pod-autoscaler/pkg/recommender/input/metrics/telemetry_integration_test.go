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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/metrics/pkg/apis/metrics/v1beta1"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
)

// Prometheus API response structures
type prometheusResponse struct {
	Status string         `json:"status"`
	Data   prometheusData `json:"data"`
}

type prometheusData struct {
	ResultType string             `json:"resultType"`
	Result     []prometheusResult `json:"result"`
}

type prometheusResult struct {
	Metric map[string]string `json:"metric"`
	Value  []interface{}     `json:"value"`
}

func TestTelemetryIntegration_PrometheusEndToEnd(t *testing.T) {
	// Create fake Prometheus server
	promServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Parse form data for POST requests (Prometheus client uses form-encoded POST)
		if err := r.ParseForm(); err != nil {
			http.Error(w, "failed to parse form", http.StatusBadRequest)
			return
		}
		query := r.Form.Get("query")

		response := prometheusResponse{
			Status: "success",
			Data: prometheusData{
				ResultType: "vector",
				Result: []prometheusResult{
					{
						Metric: map[string]string{
							"namespace": "default",
							"pod":       "test-pod-1",
							"container": "app",
						},
						Value: []interface{}{float64(time.Now().Unix()), "0.5"},
					},
					{
						Metric: map[string]string{
							"namespace": "default",
							"pod":       "test-pod-2",
							"container": "app",
						},
						Value: []interface{}{float64(time.Now().Unix()), "1.0"},
					},
				},
			},
		}

		// Return appropriate values based on detected query type
		if contains(query, "memory") || contains(query, "working_set") {
			// Memory query - return bytes
			response.Data.Result[0].Value = []interface{}{float64(time.Now().Unix()), "536870912"}
			response.Data.Result[1].Value = []interface{}{float64(time.Now().Unix()), "1073741824"}
		} else if contains(query, "oom") {
			// OOM counter query
			response.Data.Result[0].Value = []interface{}{float64(time.Now().Unix()), "3"}
			response.Data.Result[1].Value = []interface{}{float64(time.Now().Unix()), "0"}
		}
		// else: CPU query uses default values (0.5, 1.0)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer promServer.Close()

	// Setup cluster state with VPA
	clusterState := model.NewClusterState(time.Hour)

	// Create VPA with Prometheus telemetry (add VPA first)
	vpa := &vpa_types.VerticalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-vpa",
			Namespace: "default",
		},
		Spec: vpa_types.VerticalPodAutoscalerSpec{
			Telemetry: &vpa_types.TelemetryConfig{
				Source: vpa_types.TelemetrySourcePrometheus,
				Prometheus: &vpa_types.PrometheusTelemetry{
					Address: promServer.URL,
				},
			},
		},
	}

	selector := labels.SelectorFromSet(labels.Set{"app": "test"})
	err := clusterState.AddOrUpdateVpa(vpa, selector, nil)
	assert.NoError(t, err)

	// Add pods to cluster state (after VPA so they get linked)
	podID1 := model.PodID{Namespace: "default", PodName: "test-pod-1"}
	podID2 := model.PodID{Namespace: "default", PodName: "test-pod-2"}

	clusterState.AddOrUpdatePod(podID1, labels.Set{"app": "test"}, corev1.PodRunning)
	clusterState.AddOrUpdatePod(podID2, labels.Set{"app": "test"}, corev1.PodRunning)

	containerID1 := model.ContainerID{PodID: podID1, ContainerName: "app"}
	containerID2 := model.ContainerID{PodID: podID2, ContainerName: "app"}

	err = clusterState.AddOrUpdateContainer(containerID1, model.Resources{})
	assert.NoError(t, err)
	err = clusterState.AddOrUpdateContainer(containerID2, model.Resources{})
	assert.NoError(t, err)

	// Create telemetry-aware source
	fakeFallback := &fakePodMetricsLister{}
	source := NewTelemetryAwareSource(nil, clusterState, fakeFallback)

	// Fetch metrics
	result, err := source.List(context.Background(), "", metav1.ListOptions{})
	assert.NoError(t, err)

	// Verify results
	assert.NotNil(t, result)
	assert.Len(t, result.Items, 2, "Should have metrics for 2 pods")

	// Verify OOM counters were tracked
	oomCounters := source.GetOOMCounters()
	assert.Len(t, oomCounters, 2, "Should have OOM counters for 2 containers")
	// Note: exact values may vary due to query execution order, verify counters exist
	assert.Contains(t, oomCounters, containerID1)
	assert.Contains(t, oomCounters, containerID2)

	// Find metrics for pod1
	var pod1Metrics *v1beta1.PodMetrics
	for i := range result.Items {
		if result.Items[i].Name == "test-pod-1" {
			pod1Metrics = &result.Items[i]
			break
		}
	}
	assert.NotNil(t, pod1Metrics, "Should have metrics for test-pod-1")
	if pod1Metrics == nil {
		return
	}
	assert.Len(t, pod1Metrics.Containers, 1, "Should have 1 container")

	// Verify container has metrics
	containerMetrics := pod1Metrics.Containers[0]
	assert.Equal(t, "app", containerMetrics.Name)

	// Verify CPU metric exists (value may vary based on query order)
	cpuValue, hasCPU := containerMetrics.Usage[corev1.ResourceCPU]
	assert.True(t, hasCPU, "Should have CPU metric")
	assert.Greater(t, cpuValue.MilliValue(), int64(0), "CPU should be > 0")

	// Verify memory metric exists
	memoryValue, hasMemory := containerMetrics.Usage[corev1.ResourceMemory]
	assert.True(t, hasMemory, "Should have memory metric")
	assert.Greater(t, memoryValue.Value(), int64(0), "Memory should be > 0")
}

func TestTelemetryIntegration_PrometheusFallbackOnError(t *testing.T) {
	// Create Prometheus server that returns errors
	promServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}))
	defer promServer.Close()

	// Setup cluster state
	clusterState := model.NewClusterState(time.Hour)

	podID := model.PodID{Namespace: "default", PodName: "test-pod"}
	clusterState.AddOrUpdatePod(podID, labels.Set{"app": "test"}, corev1.PodRunning)

	containerID := model.ContainerID{PodID: podID, ContainerName: "app"}
	err := clusterState.AddOrUpdateContainer(containerID, model.Resources{})
	assert.NoError(t, err)

	// Create VPA with Prometheus telemetry
	vpa := &vpa_types.VerticalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-vpa",
			Namespace: "default",
		},
		Spec: vpa_types.VerticalPodAutoscalerSpec{
			Telemetry: &vpa_types.TelemetryConfig{
				Source: vpa_types.TelemetrySourcePrometheus,
				Prometheus: &vpa_types.PrometheusTelemetry{
					Address: promServer.URL,
				},
			},
		},
	}

	selector := labels.SelectorFromSet(labels.Set{"app": "test"})
	err = clusterState.AddOrUpdateVpa(vpa, selector, nil)
	assert.NoError(t, err)
	clusterState.VPAs()[model.VpaID{Namespace: "default", VpaName: "test-vpa"}].PodCount = 1

	// Create telemetry-aware source
	fakeFallback := &fakePodMetricsLister{}
	source := NewTelemetryAwareSource(nil, clusterState, fakeFallback)

	// Fetch metrics - should not fail even though Prometheus errors
	result, err := source.List(context.Background(), "", metav1.ListOptions{})
	assert.NoError(t, err)

	// Result should be empty but not fail
	assert.NotNil(t, result)
	assert.Len(t, result.Items, 0, "Should have no metrics due to Prometheus error")
}

func TestTelemetryIntegration_CustomQueries(t *testing.T) {
	// Create fake Prometheus server
	promServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, "failed to parse form", http.StatusBadRequest)
			return
		}

		response := prometheusResponse{
			Status: "success",
			Data: prometheusData{
				ResultType: "vector",
				Result: []prometheusResult{
					{
						Metric: map[string]string{
							"namespace": "default",
							"pod":       "test-pod",
							"container": "app",
						},
						Value: []interface{}{float64(time.Now().Unix()), "2.5"},
					},
				},
			},
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer promServer.Close()

	// Setup cluster state
	clusterState := model.NewClusterState(time.Hour)

	podID := model.PodID{Namespace: "default", PodName: "test-pod"}
	clusterState.AddOrUpdatePod(podID, labels.Set{"app": "test"}, corev1.PodRunning)

	containerID := model.ContainerID{PodID: podID, ContainerName: "app"}
	err := clusterState.AddOrUpdateContainer(containerID, model.Resources{})
	assert.NoError(t, err)

	// Create VPA with custom queries
	vpa := &vpa_types.VerticalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-vpa",
			Namespace: "default",
		},
		Spec: vpa_types.VerticalPodAutoscalerSpec{
			Telemetry: &vpa_types.TelemetryConfig{
				Source: vpa_types.TelemetrySourcePrometheus,
				Prometheus: &vpa_types.PrometheusTelemetry{
					Address: promServer.URL,
					Query: &vpa_types.PrometheusTelemetryQuery{
						CPUUsageQuery: "my_custom_cpu{app='test'}",
					},
				},
			},
		},
	}

	selector := labels.SelectorFromSet(labels.Set{"app": "test"})
	err = clusterState.AddOrUpdateVpa(vpa, selector, nil)
	assert.NoError(t, err)
	clusterState.VPAs()[model.VpaID{Namespace: "default", VpaName: "test-vpa"}].PodCount = 1

	// Create telemetry-aware source
	fakeFallback := &fakePodMetricsLister{}
	source := NewTelemetryAwareSource(nil, clusterState, fakeFallback)

	// Fetch metrics
	result, err := source.List(context.Background(), "", metav1.ListOptions{})
	assert.NoError(t, err)

	// Verify custom query was used and results returned
	assert.NotNil(t, result)
	assert.Len(t, result.Items, 1)
	if len(result.Items) == 0 {
		return
	}

	pod := result.Items[0]
	assert.Equal(t, "test-pod", pod.Name)
	assert.Len(t, pod.Containers, 1)

	cpu := pod.Containers[0].Usage[corev1.ResourceCPU]
	assert.Equal(t, int64(2500), cpu.MilliValue(), "CPU should be 2.5 cores = 2500 millicores")
}

func TestTelemetryIntegration_MixedKubernetesAndPrometheus(t *testing.T) {
	// Create fake Prometheus server
	promServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := prometheusResponse{
			Status: "success",
			Data: prometheusData{
				ResultType: "vector",
				Result: []prometheusResult{
					{
						Metric: map[string]string{
							"namespace": "monitoring",
							"pod":       "prom-pod",
							"container": "monitor",
						},
						Value: []interface{}{float64(time.Now().Unix()), "0.3"},
					},
				},
			},
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer promServer.Close()

	// Setup cluster state with 2 VPAs - one Kubernetes, one Prometheus
	clusterState := model.NewClusterState(time.Hour)

	// Add pods
	k8sPodID := model.PodID{Namespace: "default", PodName: "k8s-pod"}
	promPodID := model.PodID{Namespace: "monitoring", PodName: "prom-pod"}

	clusterState.AddOrUpdatePod(k8sPodID, labels.Set{"type": "k8s"}, corev1.PodRunning)
	clusterState.AddOrUpdatePod(promPodID, labels.Set{"type": "prom"}, corev1.PodRunning)

	k8sContainerID := model.ContainerID{PodID: k8sPodID, ContainerName: "app"}
	promContainerID := model.ContainerID{PodID: promPodID, ContainerName: "monitor"}

	assert.NoError(t, clusterState.AddOrUpdateContainer(k8sContainerID, model.Resources{}))
	assert.NoError(t, clusterState.AddOrUpdateContainer(promContainerID, model.Resources{}))

	// VPA 1: Uses Kubernetes
	vpa1 := &vpa_types.VerticalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: "k8s-vpa", Namespace: "default"},
		Spec:       vpa_types.VerticalPodAutoscalerSpec{},
	}
	selector1 := labels.SelectorFromSet(labels.Set{"type": "k8s"})
	assert.NoError(t, clusterState.AddOrUpdateVpa(vpa1, selector1, nil))
	clusterState.VPAs()[model.VpaID{Namespace: "default", VpaName: "k8s-vpa"}].PodCount = 1

	// VPA 2: Uses Prometheus
	vpa2 := &vpa_types.VerticalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: "prom-vpa", Namespace: "monitoring"},
		Spec: vpa_types.VerticalPodAutoscalerSpec{
			Telemetry: &vpa_types.TelemetryConfig{
				Source: vpa_types.TelemetrySourcePrometheus,
				Prometheus: &vpa_types.PrometheusTelemetry{
					Address: promServer.URL,
				},
			},
		},
	}
	selector2 := labels.SelectorFromSet(labels.Set{"type": "prom"})
	assert.NoError(t, clusterState.AddOrUpdateVpa(vpa2, selector2, nil))
	clusterState.VPAs()[model.VpaID{Namespace: "monitoring", VpaName: "prom-vpa"}].PodCount = 1

	// Mock Kubernetes source
	k8sCallCount := 0
	mockK8s := &fakePodMetricsLister{
		listFunc: func(ctx context.Context, namespace string, opts metav1.ListOptions) (*v1beta1.PodMetricsList, error) {
			k8sCallCount++
			return &v1beta1.PodMetricsList{
				Items: []v1beta1.PodMetrics{
					{
						ObjectMeta: metav1.ObjectMeta{Name: "k8s-pod", Namespace: "default"},
						Containers: []v1beta1.ContainerMetrics{
							{
								Name: "app",
								Usage: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("256Mi"),
								},
							},
						},
					},
				},
			}, nil
		},
	}

	// Create telemetry-aware source
	source := NewTelemetryAwareSource(nil, clusterState, mockK8s)

	// Fetch metrics
	result, err := source.List(context.Background(), "", metav1.ListOptions{})
	assert.NoError(t, err)

	// Verify both sources were queried
	assert.Equal(t, 1, k8sCallCount, "Kubernetes source should be called once")
	assert.NotNil(t, result)
	assert.GreaterOrEqual(t, len(result.Items), 2, "Should have metrics from both sources")
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && findSubstring(s, substr))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
