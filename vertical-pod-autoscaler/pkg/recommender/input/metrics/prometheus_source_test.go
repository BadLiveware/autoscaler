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
	"testing"
	"time"

	prommodel "github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"

	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
)

func TestExtractContainerIDFromLabels(t *testing.T) {
	tests := []struct {
		name        string
		labels      prommodel.Metric
		expected    model.ContainerID
		expectError bool
	}{
		{
			name: "valid labels",
			labels: prommodel.Metric{
				"namespace": "default",
				"pod":       "my-pod-abc123",
				"container": "app",
			},
			expected: model.ContainerID{
				PodID: model.PodID{
					Namespace: "default",
					PodName:   "my-pod-abc123",
				},
				ContainerName: "app",
			},
			expectError: false,
		},
		{
			name: "missing namespace",
			labels: prommodel.Metric{
				"pod":       "my-pod",
				"container": "app",
			},
			expectError: true,
		},
		{
			name: "missing pod",
			labels: prommodel.Metric{
				"namespace": "default",
				"container": "app",
			},
			expectError: true,
		},
		{
			name: "missing container",
			labels: prommodel.Metric{
				"namespace": "default",
				"pod":       "my-pod",
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := extractContainerIDFromLabels(tt.labels)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestBuildPodRegex(t *testing.T) {
	tests := []struct {
		name     string
		pods     []model.PodID
		expected string
	}{
		{
			name:     "empty list",
			pods:     []model.PodID{},
			expected: ".*",
		},
		{
			name: "single pod",
			pods: []model.PodID{
				{Namespace: "default", PodName: "my-pod-abc123"},
			},
			expected: "my-pod-abc123",
		},
		{
			name: "multiple pods",
			pods: []model.PodID{
				{Namespace: "default", PodName: "pod-1"},
				{Namespace: "default", PodName: "pod-2"},
				{Namespace: "default", PodName: "pod-3"},
			},
			expected: "pod-1|pod-2|pod-3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildPodRegex(tt.pods)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPrometheusMetricsSource_AggregateMetricsByPod(t *testing.T) {
	clusterState := model.NewClusterState(time.Hour)
	source := &prometheusMetricsSource{
		clusterState: clusterState,
	}

	timestamp := time.Now()

	cpuMetrics := []prometheusMetric{
		{
			containerID: model.ContainerID{
				PodID:         model.PodID{Namespace: "default", PodName: "pod-1"},
				ContainerName: "app",
			},
			value:     0.5, // 500 millicores
			timestamp: timestamp,
		},
		{
			containerID: model.ContainerID{
				PodID:         model.PodID{Namespace: "default", PodName: "pod-1"},
				ContainerName: "sidecar",
			},
			value:     0.1, // 100 millicores
			timestamp: timestamp,
		},
	}

	memoryMetrics := []prometheusMetric{
		{
			containerID: model.ContainerID{
				PodID:         model.PodID{Namespace: "default", PodName: "pod-1"},
				ContainerName: "app",
			},
			value:     512 * 1024 * 1024, // 512 MB
			timestamp: timestamp,
		},
		{
			containerID: model.ContainerID{
				PodID:         model.PodID{Namespace: "default", PodName: "pod-1"},
				ContainerName: "sidecar",
			},
			value:     256 * 1024 * 1024, // 256 MB
			timestamp: timestamp,
		},
	}

	podMetrics := source.aggregateMetricsByPod(cpuMetrics, memoryMetrics, timestamp)

	assert.Len(t, podMetrics, 1, "Should have 1 pod")
	assert.Equal(t, "pod-1", podMetrics[0].Name)
	assert.Equal(t, "default", podMetrics[0].Namespace)
	assert.Len(t, podMetrics[0].Containers, 2, "Should have 2 containers")

	// Check that both containers have both CPU and memory
	containerMap := make(map[string]int)
	for i, container := range podMetrics[0].Containers {
		containerMap[container.Name] = i
		assert.NotNil(t, container.Usage["cpu"])
		assert.NotNil(t, container.Usage["memory"])
	}

	assert.Contains(t, containerMap, "app")
	assert.Contains(t, containerMap, "sidecar")

	// Verify app container metrics
	appContainer := podMetrics[0].Containers[containerMap["app"]]
	cpuQuantity := appContainer.Usage["cpu"]
	memoryQuantity := appContainer.Usage["memory"]
	assert.Equal(t, int64(500), cpuQuantity.MilliValue())
	assert.Equal(t, int64(512*1024*1024), memoryQuantity.Value())

	// Verify sidecar container metrics
	sidecarContainer := podMetrics[0].Containers[containerMap["sidecar"]]
	cpuQuantity = sidecarContainer.Usage["cpu"]
	memoryQuantity = sidecarContainer.Usage["memory"]
	assert.Equal(t, int64(100), cpuQuantity.MilliValue())
	assert.Equal(t, int64(256*1024*1024), memoryQuantity.Value())
}

func TestPrometheusSourceConfig_Validation(t *testing.T) {
	clusterState := model.NewClusterState(time.Hour)

	tests := []struct {
		name        string
		config      PrometheusSourceConfig
		expectError bool
	}{
		{
			name: "valid config",
			config: PrometheusSourceConfig{
				Address:      "http://prometheus:9090",
				ClusterState: clusterState,
			},
			expectError: false,
		},
		{
			name: "missing address",
			config: PrometheusSourceConfig{
				ClusterState: clusterState,
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewPrometheusMetricsSource(tt.config)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestPrometheusAuthTransport(t *testing.T) {
	transport := &prometheusAuthTransport{
		token: "test-token-123",
	}

	// This is a basic test - in reality you'd mock the HTTP transport
	assert.Equal(t, "test-token-123", transport.token)
}

func TestPrometheusBasicAuthTransport(t *testing.T) {
	transport := &prometheusBasicAuthTransport{
		username: "admin",
		password: "secret",
	}

	assert.Equal(t, "admin", transport.username)
	assert.Equal(t, "secret", transport.password)
}
