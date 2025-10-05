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

package oom

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"

	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
)

// mockMetricsSource is a test implementation of ExternalMetricsSource
type mockMetricsSource struct {
	counters map[model.ContainerID]uint64
	mutex    sync.RWMutex
}

func newMockMetricsSource() *mockMetricsSource {
	return &mockMetricsSource{
		counters: make(map[model.ContainerID]uint64),
	}
}

func (m *mockMetricsSource) GetOOMCounters() map[model.ContainerID]uint64 {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	result := make(map[model.ContainerID]uint64)
	for k, v := range m.counters {
		result[k] = v
	}
	return result
}

func (m *mockMetricsSource) setCounter(containerID model.ContainerID, count uint64) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.counters[containerID] = count
}

func (m *mockMetricsSource) incrementCounter(containerID model.ContainerID) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.counters[containerID]++
}

func TestExternalObserver_NewOOMDetection(t *testing.T) {
	clusterState := model.NewClusterState(time.Hour)
	metricsSource := newMockMetricsSource()
	stopCh := make(chan struct{})
	defer close(stopCh)

	// Create test container in cluster state
	podID := model.PodID{Namespace: "default", PodName: "test-pod"}
	containerID := model.ContainerID{PodID: podID, ContainerName: "app"}

	clusterState.AddOrUpdatePod(podID, labels.Set{}, apiv1.PodRunning)
	err := clusterState.AddOrUpdateContainer(containerID, model.Resources{
		model.ResourceMemory: model.MemoryAmountFromBytes(512 * 1024 * 1024), // 512Mi
	})
	assert.NoError(t, err)

	// Create observer with very short polling interval for testing
	observer := NewExternalObserver(ExternalObserverConfig{
		MetricsSource:   metricsSource,
		ClusterState:    clusterState,
		PollingInterval: 50 * time.Millisecond,
		StopChannel:     stopCh,
	})

	// Set initial counter
	metricsSource.setCounter(containerID, 0)
	time.Sleep(100 * time.Millisecond) // Wait for first poll

	// Increment counter to simulate OOM
	metricsSource.incrementCounter(containerID)

	// Wait for observer to detect
	select {
	case oomInfo := <-observer.GetObservedOomsChannel():
		assert.Equal(t, containerID, oomInfo.ContainerID)
		assert.Equal(t, model.MemoryAmountFromBytes(512*1024*1024), oomInfo.Memory)
		assert.WithinDuration(t, time.Now(), oomInfo.Timestamp, 2*time.Second)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Expected OOM event but none received")
	}
}

func TestExternalObserver_MultipleOOMs(t *testing.T) {
	clusterState := model.NewClusterState(time.Hour)
	metricsSource := newMockMetricsSource()
	stopCh := make(chan struct{})
	defer close(stopCh)

	// Create test container
	podID := model.PodID{Namespace: "default", PodName: "test-pod"}
	containerID := model.ContainerID{PodID: podID, ContainerName: "app"}

	clusterState.AddOrUpdatePod(podID, labels.Set{}, apiv1.PodRunning)
	err := clusterState.AddOrUpdateContainer(containerID, model.Resources{
		model.ResourceMemory: model.MemoryAmountFromBytes(256 * 1024 * 1024), // 256Mi
	})
	assert.NoError(t, err)

	observer := NewExternalObserver(ExternalObserverConfig{
		MetricsSource:   metricsSource,
		ClusterState:    clusterState,
		PollingInterval: 50 * time.Millisecond,
		StopChannel:     stopCh,
	})

	// Set initial counter to 0
	metricsSource.setCounter(containerID, 0)
	time.Sleep(100 * time.Millisecond)

	// Simulate counter jumping by 3 (3 OOMs happened)
	metricsSource.setCounter(containerID, 3)

	// Should receive 3 OOM events
	receivedOOMs := 0
	timeout := time.After(500 * time.Millisecond)

	for receivedOOMs < 3 {
		select {
		case oomInfo := <-observer.GetObservedOomsChannel():
			assert.Equal(t, containerID, oomInfo.ContainerID)
			receivedOOMs++
		case <-timeout:
			t.Fatalf("Expected 3 OOM events but only received %d", receivedOOMs)
		}
	}

	assert.Equal(t, 3, receivedOOMs)
}

func TestExternalObserver_MultipleContainers(t *testing.T) {
	clusterState := model.NewClusterState(time.Hour)
	metricsSource := newMockMetricsSource()
	stopCh := make(chan struct{})
	defer close(stopCh)

	// Create two containers
	podID := model.PodID{Namespace: "default", PodName: "test-pod"}
	container1 := model.ContainerID{PodID: podID, ContainerName: "app"}
	container2 := model.ContainerID{PodID: podID, ContainerName: "sidecar"}

	clusterState.AddOrUpdatePod(podID, labels.Set{}, apiv1.PodRunning)
	err := clusterState.AddOrUpdateContainer(container1, model.Resources{
		model.ResourceMemory: model.MemoryAmountFromBytes(512 * 1024 * 1024),
	})
	assert.NoError(t, err)
	err = clusterState.AddOrUpdateContainer(container2, model.Resources{
		model.ResourceMemory: model.MemoryAmountFromBytes(256 * 1024 * 1024),
	})
	assert.NoError(t, err)

	observer := NewExternalObserver(ExternalObserverConfig{
		MetricsSource:   metricsSource,
		ClusterState:    clusterState,
		PollingInterval: 50 * time.Millisecond,
		StopChannel:     stopCh,
	})

	// Initialize counters
	metricsSource.setCounter(container1, 0)
	metricsSource.setCounter(container2, 0)
	time.Sleep(100 * time.Millisecond)

	// Increment both counters
	metricsSource.incrementCounter(container1)
	metricsSource.incrementCounter(container2)

	// Should receive OOMs from both containers
	receivedContainers := make(map[model.ContainerID]bool)
	timeout := time.After(500 * time.Millisecond)

	for len(receivedContainers) < 2 {
		select {
		case oomInfo := <-observer.GetObservedOomsChannel():
			receivedContainers[oomInfo.ContainerID] = true
		case <-timeout:
			t.Fatalf("Expected OOMs from 2 containers but only received from %d", len(receivedContainers))
		}
	}

	assert.True(t, receivedContainers[container1])
	assert.True(t, receivedContainers[container2])
}

func TestExternalObserver_NoCounterIncrease(t *testing.T) {
	clusterState := model.NewClusterState(time.Hour)
	metricsSource := newMockMetricsSource()
	stopCh := make(chan struct{})
	defer close(stopCh)

	podID := model.PodID{Namespace: "default", PodName: "test-pod"}
	containerID := model.ContainerID{PodID: podID, ContainerName: "app"}

	clusterState.AddOrUpdatePod(podID, labels.Set{}, apiv1.PodRunning)
	err := clusterState.AddOrUpdateContainer(containerID, model.Resources{
		model.ResourceMemory: model.MemoryAmountFromBytes(512 * 1024 * 1024),
	})
	assert.NoError(t, err)

	observer := NewExternalObserver(ExternalObserverConfig{
		MetricsSource:   metricsSource,
		ClusterState:    clusterState,
		PollingInterval: 50 * time.Millisecond,
		StopChannel:     stopCh,
	})

	// Set counter but don't change it
	metricsSource.setCounter(containerID, 5)
	time.Sleep(200 * time.Millisecond) // Wait for multiple polls

	// Should not receive any OOM events
	select {
	case oomInfo := <-observer.GetObservedOomsChannel():
		t.Fatalf("Did not expect OOM event but received one for %v", oomInfo.ContainerID)
	case <-time.After(100 * time.Millisecond):
		// Expected: no OOM event
	}
}

func TestExternalObserver_MissingMemoryRequest(t *testing.T) {
	clusterState := model.NewClusterState(time.Hour)
	metricsSource := newMockMetricsSource()
	stopCh := make(chan struct{})
	defer close(stopCh)

	// Create container without memory request
	podID := model.PodID{Namespace: "default", PodName: "test-pod"}
	containerID := model.ContainerID{PodID: podID, ContainerName: "app"}

	clusterState.AddOrUpdatePod(podID, labels.Set{}, apiv1.PodRunning)
	err := clusterState.AddOrUpdateContainer(containerID, nil) // No resource request
	assert.NoError(t, err)

	observer := NewExternalObserver(ExternalObserverConfig{
		MetricsSource:   metricsSource,
		ClusterState:    clusterState,
		PollingInterval: 50 * time.Millisecond,
		StopChannel:     stopCh,
	})

	metricsSource.setCounter(containerID, 0)
	time.Sleep(100 * time.Millisecond)

	metricsSource.incrementCounter(containerID)

	// Should still receive OOM event, just with 0 memory
	select {
	case oomInfo := <-observer.GetObservedOomsChannel():
		assert.Equal(t, containerID, oomInfo.ContainerID)
		assert.Equal(t, model.ResourceAmount(0), oomInfo.Memory)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Expected OOM event but none received")
	}
}

func TestTelemetryMetricsSourceAdapter(t *testing.T) {
	mockProvider := newMockMetricsSource()

	containerID := model.ContainerID{
		PodID:         model.PodID{Namespace: "default", PodName: "test-pod"},
		ContainerName: "app",
	}

	mockProvider.setCounter(containerID, 42)

	adapter := NewTelemetryMetricsSourceAdapter(mockProvider)
	counters := adapter.GetOOMCounters()

	assert.NotNil(t, counters)
	assert.Equal(t, uint64(42), counters[containerID])
}
