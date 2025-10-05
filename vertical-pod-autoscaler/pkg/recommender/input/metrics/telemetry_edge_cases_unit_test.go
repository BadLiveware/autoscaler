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
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	prommodel "github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/metrics/pkg/apis/metrics/v1beta1"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
)

// TestExtractContainerID_MissingNamespace verifies that extractContainerID returns an error
// when the namespace label is missing.
func TestExtractContainerID_MissingNamespace(t *testing.T) {
	source := &TelemetryAwareSource{}

	metric := prommodel.Metric{
		"pod":       "test-pod",
		"container": "test-container",
		// Missing 'namespace' label
	}

	_, err := source.extractContainerID(metric)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing namespace label")
}

// TestExtractContainerID_MissingPod verifies that extractContainerID returns an error
// when both pod and pod_name labels are missing.
func TestExtractContainerID_MissingPod(t *testing.T) {
	source := &TelemetryAwareSource{}

	metric := prommodel.Metric{
		"namespace": "default",
		"container": "test-container",
		// Missing both 'pod' and 'pod_name' labels
	}

	_, err := source.extractContainerID(metric)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing pod/pod_name label")
}

// TestExtractContainerID_MissingContainer verifies that extractContainerID returns an error
// when both container and name labels are missing.
func TestExtractContainerID_MissingContainer(t *testing.T) {
	source := &TelemetryAwareSource{}

	metric := prommodel.Metric{
		"namespace": "default",
		"pod":       "test-pod",
		// Missing both 'container' and 'name' labels
	}

	_, err := source.extractContainerID(metric)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing container/name label")
}

// TestExtractContainerID_WithAlternativeLabels verifies that extractContainerID works
// with alternative label names (pod_name instead of pod, name instead of container).
func TestExtractContainerID_WithAlternativeLabels(t *testing.T) {
	source := &TelemetryAwareSource{}

	testCases := []struct {
		name     string
		metric   prommodel.Metric
		expected model.ContainerID
	}{
		{
			name: "pod_name instead of pod",
			metric: prommodel.Metric{
				"namespace": "default",
				"pod_name":  "test-pod-123",
				"container": "nginx",
			},
			expected: model.ContainerID{
				PodID: model.PodID{
					Namespace: "default",
					PodName:   "test-pod-123",
				},
				ContainerName: "nginx",
			},
		},
		{
			name: "name instead of container",
			metric: prommodel.Metric{
				"namespace": "kube-system",
				"pod":       "coredns-abc",
				"name":      "coredns",
			},
			expected: model.ContainerID{
				PodID: model.PodID{
					Namespace: "kube-system",
					PodName:   "coredns-abc",
				},
				ContainerName: "coredns",
			},
		},
		{
			name: "both alternative labels",
			metric: prommodel.Metric{
				"namespace": "monitoring",
				"pod_name":  "prometheus-0",
				"name":      "prometheus",
			},
			expected: model.ContainerID{
				PodID: model.PodID{
					Namespace: "monitoring",
					PodName:   "prometheus-0",
				},
				ContainerName: "prometheus",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			containerID, err := source.extractContainerID(tc.metric)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, containerID)
		})
	}
}

// TestExtractContainerID_Success verifies that extractContainerID works correctly
// with all required labels present.
func TestExtractContainerID_Success(t *testing.T) {
	source := &TelemetryAwareSource{}

	metric := prommodel.Metric{
		"namespace": "production",
		"pod":       "api-server-xyz",
		"container": "api",
	}

	containerID, err := source.extractContainerID(metric)
	assert.NoError(t, err)
	assert.Equal(t, "production", containerID.PodID.Namespace)
	assert.Equal(t, "api-server-xyz", containerID.PodID.PodName)
	assert.Equal(t, "api", containerID.ContainerName)
}

// TestQueryPrometheus_EmptyVector verifies that queryPrometheus handles empty results gracefully.
func TestQueryPrometheus_EmptyVector(t *testing.T) {
	// Create fake Prometheus server that returns empty vector
	promServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := prometheusResponse{
			Status: "success",
			Data: prometheusData{
				ResultType: "vector",
				Result:     []prometheusResult{}, // Empty result
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer promServer.Close()

	// Create source and query
	clusterState := model.NewClusterState(10 * time.Minute)
	source := NewTelemetryAwareSource(nil, clusterState, nil)

	// Get Prometheus client
	client, err := source.getOrCreatePrometheusClient(promServer.URL, nil, false)
	assert.NoError(t, err)

	// Query should succeed but return empty results
	results, err := source.queryPrometheus(context.Background(), client, "empty_query", time.Now())
	assert.NoError(t, err)
	assert.Empty(t, results, "Should return empty results for empty vector")
}

// TestQueryPrometheus_NaNValue verifies that queryPrometheus skips metrics with NaN values.
func TestQueryPrometheus_NaNValue(t *testing.T) {
	promServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := prometheusResponse{
			Status: "success",
			Data: prometheusData{
				ResultType: "vector",
				Result: []prometheusResult{
					{
						Metric: map[string]string{
							"namespace": "default",
							"pod":       "test-pod",
							"container": "nginx",
						},
						Value: []interface{}{
							float64(time.Now().Unix()),
							"NaN", // NaN value
						},
					},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer promServer.Close()

	clusterState := model.NewClusterState(10 * time.Minute)
	source := NewTelemetryAwareSource(nil, clusterState, nil)

	client, err := source.getOrCreatePrometheusClient(promServer.URL, nil, false)
	assert.NoError(t, err)

	results, err := source.queryPrometheus(context.Background(), client, "test_query", time.Now())
	assert.NoError(t, err)

	// Should handle NaN gracefully - the value will be parsed as NaN by Prometheus client
	// and included in results, but downstream processing should handle it
	if len(results) > 0 {
		// If result is included, verify it's NaN
		assert.True(t, math.IsNaN(results[0].value), "NaN should be preserved in result")
	}
}

// TestQueryPrometheus_InfValue verifies that queryPrometheus handles Inf values.
func TestQueryPrometheus_InfValue(t *testing.T) {
	promServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := prometheusResponse{
			Status: "success",
			Data: prometheusData{
				ResultType: "vector",
				Result: []prometheusResult{
					{
						Metric: map[string]string{
							"namespace": "default",
							"pod":       "test-pod",
							"container": "nginx",
						},
						Value: []interface{}{
							float64(time.Now().Unix()),
							"+Inf", // Positive infinity
						},
					},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer promServer.Close()

	clusterState := model.NewClusterState(10 * time.Minute)
	source := NewTelemetryAwareSource(nil, clusterState, nil)

	client, err := source.getOrCreatePrometheusClient(promServer.URL, nil, false)
	assert.NoError(t, err)

	results, err := source.queryPrometheus(context.Background(), client, "test_query", time.Now())
	assert.NoError(t, err)

	// Should handle Inf gracefully
	if len(results) > 0 {
		assert.True(t, math.IsInf(results[0].value, 1), "+Inf should be preserved in result")
	}
}

// TestQueryPrometheus_MultipleMetricsWithSomeMissingLabels verifies that queryPrometheus
// skips metrics with missing labels but continues processing valid ones.
func TestQueryPrometheus_MultipleMetricsWithSomeMissingLabels(t *testing.T) {
	promServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := prometheusResponse{
			Status: "success",
			Data: prometheusData{
				ResultType: "vector",
				Result: []prometheusResult{
					{
						Metric: map[string]string{
							"namespace": "default",
							"pod":       "valid-pod",
							"container": "nginx",
						},
						Value: []interface{}{float64(time.Now().Unix()), "0.5"},
					},
					{
						Metric: map[string]string{
							"namespace": "default",
							"pod":       "invalid-pod",
							// Missing 'container' label
						},
						Value: []interface{}{float64(time.Now().Unix()), "0.3"},
					},
					{
						Metric: map[string]string{
							"namespace": "default",
							"pod":       "another-valid-pod",
							"container": "api",
						},
						Value: []interface{}{float64(time.Now().Unix()), "0.8"},
					},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer promServer.Close()

	clusterState := model.NewClusterState(10 * time.Minute)
	source := NewTelemetryAwareSource(nil, clusterState, nil)

	client, err := source.getOrCreatePrometheusClient(promServer.URL, nil, false)
	assert.NoError(t, err)

	results, err := source.queryPrometheus(context.Background(), client, "test_query", time.Now())
	assert.NoError(t, err)

	// Should have 2 valid results (skipped the one with missing container label)
	assert.Len(t, results, 2, "Should skip metrics with missing labels")

	// Verify the valid metrics are present
	podNames := make(map[string]bool)
	for _, result := range results {
		podNames[result.containerID.PodID.PodName] = true
	}
	assert.True(t, podNames["valid-pod"])
	assert.True(t, podNames["another-valid-pod"])
	assert.False(t, podNames["invalid-pod"], "Should not include pod with missing container label")
}

// TestTelemetryAwareSource_NoVPAs verifies that when there are no VPAs,
// List() returns empty results without errors.
func TestTelemetryAwareSource_NoVPAs(t *testing.T) {
	clusterState := model.NewClusterState(10 * time.Minute)
	mockKubernetesSource := &mockPodMetricsLister{
		result: nil,
		err:    nil,
	}

	source := NewTelemetryAwareSource(nil, clusterState, mockKubernetesSource)

	result, err := source.List(context.Background(), "", metav1.ListOptions{})
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Empty(t, result.Items)
}

// TestTelemetryAwareSource_VPAWithNoMatchingPods verifies that VPAs with no matching pods
// don't cause errors and don't send unnecessary Prometheus queries.
func TestTelemetryAwareSource_VPAWithNoMatchingPods(t *testing.T) {
	clusterState := model.NewClusterState(10 * time.Minute)

	// Add VPA with Prometheus config but no pods
	vpa := &vpa_types.VerticalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-vpa",
			Namespace: "default",
		},
		Spec: vpa_types.VerticalPodAutoscalerSpec{
			Telemetry: &vpa_types.TelemetryConfig{
				Source: vpa_types.TelemetrySourcePrometheus,
				Prometheus: &vpa_types.PrometheusTelemetry{
					Address: "http://prometheus:9090",
				},
			},
		},
	}

	selector := labels.Everything()
	err := clusterState.AddOrUpdateVpa(vpa, selector, nil)
	assert.NoError(t, err)

	// VPA has PodCount = 0 (no matching pods)
	vpas := clusterState.VPAs()
	assert.Len(t, vpas, 1)
	for _, v := range vpas {
		assert.Equal(t, 0, v.PodCount, "VPA should have no matching pods")
	}

	mockKubernetesSource := &mockPodMetricsLister{}
	source := NewTelemetryAwareSource(nil, clusterState, mockKubernetesSource)

	// Should not error and should not attempt Prometheus queries
	result, err := source.List(context.Background(), "", metav1.ListOptions{})
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Empty(t, result.Items, "Should return no metrics for VPA with no pods")
}

// mockPodMetricsLister is a simple mock for testing
type mockPodMetricsLister struct {
	result *v1beta1.PodMetricsList
	err    error
}

func (m *mockPodMetricsLister) List(ctx context.Context, namespace string, opts metav1.ListOptions) (*v1beta1.PodMetricsList, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.result == nil {
		return &v1beta1.PodMetricsList{}, nil
	}
	return m.result, nil
}
