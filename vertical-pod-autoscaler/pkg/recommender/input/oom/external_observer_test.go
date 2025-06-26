/*
Copyright 2024 The Kubernetes Authors.

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

package oom

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/metrics/pkg/apis/external_metrics/v1beta1"
	"k8s.io/metrics/pkg/client/external_metrics"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
	controllerfetcher "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/target/controller_fetcher"
)

// MockClusterState implements a mock cluster state for testing
type MockClusterState struct {
	vpas       map[model.VpaID]*model.Vpa
	pods       map[model.PodID]*model.PodState
	containers map[model.ContainerID]*model.ContainerState
}

func NewMockClusterState() *MockClusterState {
	return &MockClusterState{
		vpas:       make(map[model.VpaID]*model.Vpa),
		pods:       make(map[model.PodID]*model.PodState),
		containers: make(map[model.ContainerID]*model.ContainerState),
	}
}

func (m *MockClusterState) VPAs() map[model.VpaID]*model.Vpa {
	return m.vpas
}

func (m *MockClusterState) Pods() map[model.PodID]*model.PodState {
	return m.pods
}

func (m *MockClusterState) GetContainer(containerID model.ContainerID) *model.ContainerState {
	return m.containers[containerID]
}

func (m *MockClusterState) GetMatchingPods(vpa *model.Vpa) []model.PodID {
	var matchingPods []model.PodID
	for podID := range m.pods {
		if podID.Namespace == vpa.ID.Namespace {
			matchingPods = append(matchingPods, podID)
		}
	}
	return matchingPods
}

// Implement required interface methods as no-ops for testing
func (m *MockClusterState) StateMapSize() int                                             { return 0 }
func (m *MockClusterState) AddOrUpdatePod(model.PodID, labels.Set, corev1.PodPhase)       {}
func (m *MockClusterState) DeletePod(model.PodID)                                         {}
func (m *MockClusterState) AddOrUpdateContainer(model.ContainerID, model.Resources) error { return nil }
func (m *MockClusterState) AddSample(*model.ContainerUsageSampleWithKey) error            { return nil }
func (m *MockClusterState) RecordOOM(model.ContainerID, time.Time, model.ResourceAmount) error {
	return nil
}
func (m *MockClusterState) AddOrUpdateVpa(*vpa_types.VerticalPodAutoscaler, labels.Selector) error {
	return nil
}
func (m *MockClusterState) DeleteVpa(model.VpaID) error { return nil }
func (m *MockClusterState) MakeAggregateStateKey(*model.PodState, string) model.AggregateStateKey {
	return nil
}
func (m *MockClusterState) RateLimitedGarbageCollectAggregateCollectionStates(context.Context, time.Time, controllerfetcher.ControllerFetcher) {
}
func (m *MockClusterState) RecordRecommendation(*model.Vpa, time.Time) error { return nil }
func (m *MockClusterState) GetControllerForPodUnderVPA(context.Context, *model.PodState, controllerfetcher.ControllerFetcher) *controllerfetcher.ControllerKeyWithAPIVersion {
	return nil
}
func (m *MockClusterState) GetControllingVPA(*model.PodState) *model.Vpa       { return nil }
func (m *MockClusterState) SetObservedVPAs([]*vpa_types.VerticalPodAutoscaler) {}
func (m *MockClusterState) ObservedVPAs() []*vpa_types.VerticalPodAutoscaler   { return nil }

// MockExternalMetricsClient implements a mock external metrics client for testing
type MockExternalMetricsClient struct {
	metricsData map[string]map[string]*v1beta1.ExternalMetricValueList // namespace -> metricName -> data
}

func NewMockExternalMetricsClient() *MockExternalMetricsClient {
	return &MockExternalMetricsClient{
		metricsData: make(map[string]map[string]*v1beta1.ExternalMetricValueList),
	}
}

func (m *MockExternalMetricsClient) NamespacedMetrics(namespace string) external_metrics.MetricsInterface {
	return &MockMetricsInterface{
		client:    m,
		namespace: namespace,
	}
}

func (m *MockExternalMetricsClient) SetMetricsData(namespace, metricName string, data *v1beta1.ExternalMetricValueList) {
	if m.metricsData[namespace] == nil {
		m.metricsData[namespace] = make(map[string]*v1beta1.ExternalMetricValueList)
	}
	m.metricsData[namespace][metricName] = data
}

// MockMetricsInterface implements the MetricsInterface for testing
type MockMetricsInterface struct {
	client    *MockExternalMetricsClient
	namespace string
}

func (m *MockMetricsInterface) List(metricName string, metricSelector labels.Selector) (*v1beta1.ExternalMetricValueList, error) {
	if nsData, exists := m.client.metricsData[m.namespace]; exists {
		if metricData, exists := nsData[metricName]; exists {
			// Filter metrics by selector
			filteredItems := []v1beta1.ExternalMetricValue{}

			for _, item := range metricData.Items {
				// Convert metric labels to label set and check if selector matches
				labelSet := labels.Set(item.MetricLabels)
				if metricSelector.Matches(labelSet) {
					filteredItems = append(filteredItems, item)
				}
			}

			return &v1beta1.ExternalMetricValueList{
				Items: filteredItems,
			}, nil
		}
	}
	// Return empty list if no data found
	return &v1beta1.ExternalMetricValueList{}, nil
}

// Helper function to set up test cluster state
func setupTestClusterState() *MockClusterState {
	clusterState := NewMockClusterState()

	// Create VPA
	vpaID := model.VpaID{Namespace: "test-ns", VpaName: "test-vpa"}
	vpa := &model.Vpa{
		ID:          vpaID,
		PodSelector: labels.NewSelector(),
		PodCount:    2,
	}
	clusterState.vpas[vpaID] = vpa

	// Create pods
	pod1ID := model.PodID{Namespace: "test-ns", PodName: "test-pod-1"}
	pod2ID := model.PodID{Namespace: "test-ns", PodName: "test-pod-2"}

	clusterState.pods[pod1ID] = &model.PodState{
		ID: pod1ID,
		Containers: map[string]*model.ContainerState{
			"app":     {},
			"sidecar": {},
		},
	}
	clusterState.pods[pod2ID] = &model.PodState{
		ID: pod2ID,
		Containers: map[string]*model.ContainerState{
			"app": {},
		},
	}

	// Create container states with memory requests
	clusterState.containers[model.ContainerID{PodID: pod1ID, ContainerName: "app"}] = &model.ContainerState{
		Request: model.Resources{
			model.ResourceMemory: model.ResourceAmount(512 * 1024 * 1024), // 512Mi
		},
	}
	clusterState.containers[model.ContainerID{PodID: pod1ID, ContainerName: "sidecar"}] = &model.ContainerState{
		Request: model.Resources{
			model.ResourceMemory: model.ResourceAmount(256 * 1024 * 1024), // 256Mi
		},
	}
	clusterState.containers[model.ContainerID{PodID: pod2ID, ContainerName: "app"}] = &model.ContainerState{
		Request: model.Resources{
			model.ResourceMemory: model.ResourceAmount(1024 * 1024 * 1024), // 1Gi
		},
	}

	return clusterState
}

// Helper function to collect OOM events from the channel
func collectOOMEvents(ch chan OomInfo, expectedCount int, timeout time.Duration) []OomInfo {
	var events []OomInfo
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for i := 0; i < expectedCount; i++ {
		select {
		case event := <-ch:
			events = append(events, event)
		case <-timer.C:
			return events // Return what we have so far
		}
	}
	return events
}

func TestExternalOomObserver_CheckForOOMs_NoMetricConfigured(t *testing.T) {
	clusterState := setupTestClusterState()
	observer := &ExternalOomObserver{
		observedOomsChannel: make(chan OomInfo, 100),
		oomMetricName:       "", // No metric configured
		containerNameLabel:  "container",
		clusterState:        clusterState,
		lastOomCounts:       make(map[model.ContainerID]int64),
	}

	ctx := context.Background()
	err := observer.CheckForOOMs(ctx)

	assert.NoError(t, err)
	assert.Empty(t, observer.observedOomsChannel)
}

func TestExternalOomObserver_CheckForOOMs_NewOOMs(t *testing.T) {
	clusterState := setupTestClusterState()

	// Setup mock external metrics client with OOM data
	mockClient := NewMockExternalMetricsClient()
	metricData := &v1beta1.ExternalMetricValueList{
		Items: []v1beta1.ExternalMetricValue{
			{
				MetricName: "dotnet_exceptions_total",
				Value:      *resource.NewQuantity(3, resource.DecimalSI),
				Timestamp:  metav1.NewTime(time.Now()),
				MetricLabels: map[string]string{
					"pod":       "test-pod-1",
					"container": "app",
					"type":      "System.OutOfMemoryException",
				},
			},
			{
				MetricName: "dotnet_exceptions_total",
				Value:      *resource.NewQuantity(1, resource.DecimalSI),
				Timestamp:  metav1.NewTime(time.Now()),
				MetricLabels: map[string]string{
					"pod":       "test-pod-1",
					"container": "sidecar",
					"type":      "System.OutOfMemoryException",
				},
			},
			{
				MetricName: "dotnet_exceptions_total",
				Value:      *resource.NewQuantity(2, resource.DecimalSI),
				Timestamp:  metav1.NewTime(time.Now()),
				MetricLabels: map[string]string{
					"pod":       "test-pod-2",
					"container": "app",
					"type":      "System.OutOfMemoryException",
				},
			},
		},
	}
	mockClient.SetMetricsData("test-ns", "dotnet_exceptions_total", metricData)

	observer := &ExternalOomObserver{
		observedOomsChannel: make(chan OomInfo, 100),
		externalClient:      mockClient,
		oomMetricName:       "dotnet_exceptions_total",
		containerNameLabel:  "container",
		clusterState:        clusterState,
		lastOomCounts:       make(map[model.ContainerID]int64),
	}

	ctx := context.Background()
	err := observer.CheckForOOMs(ctx)
	require.NoError(t, err)

	// Should receive 6 OOMs total (3 + 1 + 2)
	oomEvents := collectOOMEvents(observer.observedOomsChannel, 6, 200*time.Millisecond)
	assert.Len(t, oomEvents, 6)

	// Verify OOM counts are tracked
	expectedCounts := map[model.ContainerID]int64{
		{PodID: model.PodID{Namespace: "test-ns", PodName: "test-pod-1"}, ContainerName: "app"}:     3,
		{PodID: model.PodID{Namespace: "test-ns", PodName: "test-pod-1"}, ContainerName: "sidecar"}: 1,
		{PodID: model.PodID{Namespace: "test-ns", PodName: "test-pod-2"}, ContainerName: "app"}:     2,
	}

	for containerID, expectedCount := range expectedCounts {
		assert.Equal(t, expectedCount, observer.lastOomCounts[containerID],
			"OOM count mismatch for container %v", containerID)
	}
}

func TestExternalOomObserver_CheckForOOMs_IncrementalOOMs(t *testing.T) {
	clusterState := setupTestClusterState()
	mockClient := NewMockExternalMetricsClient()

	observer := &ExternalOomObserver{
		observedOomsChannel: make(chan OomInfo, 100),
		externalClient:      mockClient,
		oomMetricName:       "dotnet_exceptions_total",
		containerNameLabel:  "container",
		clusterState:        clusterState,
		lastOomCounts:       make(map[model.ContainerID]int64),
	}

	containerID := model.ContainerID{
		PodID:         model.PodID{Namespace: "test-ns", PodName: "test-pod-1"},
		ContainerName: "app",
	}

	// First run: 2 OOMs
	metricData := &v1beta1.ExternalMetricValueList{
		Items: []v1beta1.ExternalMetricValue{
			{
				MetricName: "dotnet_exceptions_total",
				Value:      *resource.NewQuantity(2, resource.DecimalSI),
				Timestamp:  metav1.NewTime(time.Now()),
				MetricLabels: map[string]string{
					"pod":       "test-pod-1",
					"container": "app",
					"type":      "System.OutOfMemoryException",
				},
			},
		},
	}
	mockClient.SetMetricsData("test-ns", "dotnet_exceptions_total", metricData)

	ctx := context.Background()
	err := observer.CheckForOOMs(ctx)
	require.NoError(t, err)

	// Should receive 2 OOMs
	oomEvents := collectOOMEvents(observer.observedOomsChannel, 2, 100*time.Millisecond)
	assert.Len(t, oomEvents, 2)
	assert.Equal(t, int64(2), observer.lastOomCounts[containerID])

	// Second run: 5 OOMs total (3 new ones)
	metricData.Items[0].Value = *resource.NewQuantity(5, resource.DecimalSI)
	mockClient.SetMetricsData("test-ns", "dotnet_exceptions_total", metricData)

	err = observer.CheckForOOMs(ctx)
	require.NoError(t, err)

	// Should receive 3 new OOMs
	oomEvents = collectOOMEvents(observer.observedOomsChannel, 3, 100*time.Millisecond)
	assert.Len(t, oomEvents, 3)
	assert.Equal(t, int64(5), observer.lastOomCounts[containerID])
}

func TestExternalOomObserver_CheckForOOMs_NoNewOOMs(t *testing.T) {
	clusterState := setupTestClusterState()
	mockClient := NewMockExternalMetricsClient()

	observer := &ExternalOomObserver{
		observedOomsChannel: make(chan OomInfo, 100),
		externalClient:      mockClient,
		oomMetricName:       "dotnet_exceptions_total",
		containerNameLabel:  "container",
		clusterState:        clusterState,
		lastOomCounts:       make(map[model.ContainerID]int64),
	}

	containerID := model.ContainerID{
		PodID:         model.PodID{Namespace: "test-ns", PodName: "test-pod-1"},
		ContainerName: "app",
	}

	// Pre-populate with existing count
	observer.lastOomCounts[containerID] = 3

	// Metrics show same count
	metricData := &v1beta1.ExternalMetricValueList{
		Items: []v1beta1.ExternalMetricValue{
			{
				MetricName: "dotnet_exceptions_total",
				Value:      *resource.NewQuantity(3, resource.DecimalSI),
				Timestamp:  metav1.NewTime(time.Now()),
				MetricLabels: map[string]string{
					"pod":       "test-pod-1",
					"container": "app",
					"type":      "System.OutOfMemoryException",
				},
			},
		},
	}
	mockClient.SetMetricsData("test-ns", "dotnet_exceptions_total", metricData)

	ctx := context.Background()
	err := observer.CheckForOOMs(ctx)
	require.NoError(t, err)

	// Should not receive any new OOMs
	oomEvents := collectOOMEvents(observer.observedOomsChannel, 0, 50*time.Millisecond)
	assert.Empty(t, oomEvents)
	assert.Equal(t, int64(3), observer.lastOomCounts[containerID])
}

func TestExternalOomObserver_CheckForOOMs_MissingContainerLabel(t *testing.T) {
	clusterState := setupTestClusterState()
	mockClient := NewMockExternalMetricsClient()

	// Metric without container label
	metricData := &v1beta1.ExternalMetricValueList{
		Items: []v1beta1.ExternalMetricValue{
			{
				MetricName: "dotnet_exceptions_total",
				Value:      *resource.NewQuantity(2, resource.DecimalSI),
				Timestamp:  metav1.NewTime(time.Now()),
				MetricLabels: map[string]string{
					"pod":  "test-pod-1",
					"type": "System.OutOfMemoryException",
					// Missing container label
				},
			},
		},
	}
	mockClient.SetMetricsData("test-ns", "dotnet_exceptions_total", metricData)

	observer := &ExternalOomObserver{
		observedOomsChannel: make(chan OomInfo, 100),
		externalClient:      mockClient,
		oomMetricName:       "dotnet_exceptions_total",
		containerNameLabel:  "container",
		clusterState:        clusterState,
		lastOomCounts:       make(map[model.ContainerID]int64),
	}

	ctx := context.Background()
	err := observer.CheckForOOMs(ctx)
	require.NoError(t, err)

	// Should not receive any OOMs due to missing container label
	oomEvents := collectOOMEvents(observer.observedOomsChannel, 0, 50*time.Millisecond)
	assert.Empty(t, oomEvents)
	assert.Empty(t, observer.lastOomCounts)
}

func TestExternalOomObserver_CheckForOOMs_NoMetricsAvailable(t *testing.T) {
	clusterState := setupTestClusterState()
	mockClient := NewMockExternalMetricsClient()

	// No metrics available - don't set any data

	observer := &ExternalOomObserver{
		observedOomsChannel: make(chan OomInfo, 100),
		externalClient:      mockClient,
		oomMetricName:       "dotnet_exceptions_total",
		containerNameLabel:  "container",
		clusterState:        clusterState,
		lastOomCounts:       make(map[model.ContainerID]int64),
	}

	ctx := context.Background()
	err := observer.CheckForOOMs(ctx)
	require.NoError(t, err)

	// Should not receive any OOMs
	oomEvents := collectOOMEvents(observer.observedOomsChannel, 0, 50*time.Millisecond)
	assert.Empty(t, oomEvents)
	assert.Empty(t, observer.lastOomCounts)
}

func TestExternalOomObserver_CheckForOOMs_ContainerWithoutMemoryRequest(t *testing.T) {
	clusterState := setupTestClusterState()

	// Add a container without memory request
	podID := model.PodID{Namespace: "test-ns", PodName: "test-pod-3"}
	podState := &model.PodState{
		ID:         podID,
		Containers: map[string]*model.ContainerState{"no-request": {}},
	}
	clusterState.pods[podID] = podState
	// Don't add to containers map - simulates missing container state

	mockClient := NewMockExternalMetricsClient()
	metricData := &v1beta1.ExternalMetricValueList{
		Items: []v1beta1.ExternalMetricValue{
			{
				MetricName: "dotnet_exceptions_total",
				Value:      *resource.NewQuantity(1, resource.DecimalSI),
				Timestamp:  metav1.NewTime(time.Now()),
				MetricLabels: map[string]string{
					"pod":       "test-pod-3",
					"container": "no-request",
					"type":      "System.OutOfMemoryException",
				},
			},
		},
	}
	mockClient.SetMetricsData("test-ns", "dotnet_exceptions_total", metricData)

	observer := &ExternalOomObserver{
		observedOomsChannel: make(chan OomInfo, 100),
		externalClient:      mockClient,
		oomMetricName:       "dotnet_exceptions_total",
		containerNameLabel:  "container",
		clusterState:        clusterState,
		lastOomCounts:       make(map[model.ContainerID]int64),
	}

	ctx := context.Background()
	err := observer.CheckForOOMs(ctx)
	require.NoError(t, err)

	// Should still receive OOM with default memory
	oomEvents := collectOOMEvents(observer.observedOomsChannel, 1, 100*time.Millisecond)
	require.Len(t, oomEvents, 1)

	// Should use default fallback memory (100Mi)
	assert.Equal(t, model.ResourceAmount(100*1024*1024), oomEvents[0].Memory)
}

func TestExternalOomObserver_CleanupOldContainers(t *testing.T) {
	clusterState := setupTestClusterState()
	observer := &ExternalOomObserver{
		observedOomsChannel: make(chan OomInfo, 100),
		oomMetricName:       "dotnet_exceptions_total",
		containerNameLabel:  "container",
		clusterState:        clusterState,
		lastOomCounts:       make(map[model.ContainerID]int64),
	}

	// Add some OOM counts for containers that exist
	existingContainer := model.ContainerID{
		PodID:         model.PodID{Namespace: "test-ns", PodName: "test-pod-1"},
		ContainerName: "app",
	}
	observer.lastOomCounts[existingContainer] = 5

	// Add OOM count for a container that doesn't exist
	removedContainer := model.ContainerID{
		PodID:         model.PodID{Namespace: "test-ns", PodName: "removed-pod"},
		ContainerName: "app",
	}
	observer.lastOomCounts[removedContainer] = 3

	// Call cleanup
	observer.CleanupOldContainers()

	// Existing container should still be tracked
	assert.Contains(t, observer.lastOomCounts, existingContainer)
	assert.Equal(t, int64(5), observer.lastOomCounts[existingContainer])

	// Removed container should be cleaned up
	assert.NotContains(t, observer.lastOomCounts, removedContainer)
}

func TestExternalOomObserver_Interface(t *testing.T) {
	observer := &ExternalOomObserver{
		observedOomsChannel: make(chan OomInfo, 100),
		oomMetricName:       "test_metric",
		containerNameLabel:  "container",
		lastOomCounts:       make(map[model.ContainerID]int64),
	}

	// Verify that ExternalOomObserver implements Observer interface
	var _ Observer = observer

	// Verify that ExternalOomObserver implements ExternalOomChecker interface
	var _ ExternalOomChecker = observer

	// Test interface methods
	assert.NotNil(t, observer.GetObservedOomsChannel())

	// Test no-op methods - should not panic
	observer.OnEvent(nil)
	observer.OnAdd(nil, false)
	observer.OnUpdate(nil, nil)
	observer.OnDelete(nil)
}

func TestExternalOomObserver_ChannelOverflow(t *testing.T) {
	clusterState := setupTestClusterState()

	// Create observer with small channel buffer
	observer := &ExternalOomObserver{
		observedOomsChannel: make(chan OomInfo, 2), // Small buffer
		externalClient:      NewMockExternalMetricsClient(),
		oomMetricName:       "dotnet_exceptions_total",
		containerNameLabel:  "container",
		clusterState:        clusterState,
		lastOomCounts:       make(map[model.ContainerID]int64),
	}

	// Fill the channel
	observer.observedOomsChannel <- OomInfo{}
	observer.observedOomsChannel <- OomInfo{}

	// Set up metrics with many OOMs
	mockClient := observer.externalClient.(*MockExternalMetricsClient)
	metricData := &v1beta1.ExternalMetricValueList{
		Items: []v1beta1.ExternalMetricValue{
			{
				MetricName: "dotnet_exceptions_total",
				Value:      *resource.NewQuantity(10, resource.DecimalSI),
				Timestamp:  metav1.NewTime(time.Now()),
				MetricLabels: map[string]string{
					"pod":       "test-pod-1",
					"container": "app",
					"type":      "System.OutOfMemoryException",
				},
			},
		},
	}
	mockClient.SetMetricsData("test-ns", "dotnet_exceptions_total", metricData)

	ctx := context.Background()
	err := observer.CheckForOOMs(ctx)

	// Should not error even when channel is full
	assert.NoError(t, err)

	// OOM count should still be updated even if channel is full
	containerID := model.ContainerID{
		PodID:         model.PodID{Namespace: "test-ns", PodName: "test-pod-1"},
		ContainerName: "app",
	}
	assert.Equal(t, int64(10), observer.lastOomCounts[containerID])
}
