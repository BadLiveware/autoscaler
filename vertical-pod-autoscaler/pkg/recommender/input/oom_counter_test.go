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

package input

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"

	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/input/metrics"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/input/oom"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
)

// Mock metrics client that returns controllable OOM counters
type mockMetricsClientWithOOM struct {
	oomCounters map[model.ContainerID]uint64
}

func (m *mockMetricsClientWithOOM) GetContainersMetrics(ctx context.Context) ([]*metrics.ContainerMetricsSnapshot, error) {
	var snapshots []*metrics.ContainerMetricsSnapshot

	for containerID, count := range m.oomCounters {
		countCopy := count
		snapshot := &metrics.ContainerMetricsSnapshot{
			ID:           containerID,
			SnapshotTime: time.Now(),
			Usage: model.Resources{
				model.ResourceCPU:    model.ResourceAmount(1000),               // 1 core
				model.ResourceMemory: model.ResourceAmount(1024 * 1024 * 1024), // 1GB
			},
			OOMCount: &countCopy,
		}
		snapshots = append(snapshots, snapshot)
	}

	return snapshots, nil
}

func TestOOMCounterDeltaDetection(t *testing.T) {
	clusterState := model.NewClusterState(time.Hour)

	// Add test pod and container
	podID := model.PodID{Namespace: "default", PodName: "test-pod"}
	containerID := model.ContainerID{PodID: podID, ContainerName: "test-container"}

	clusterState.AddOrUpdatePod(podID, labels.Set{}, apiv1.PodRunning)
	err := clusterState.AddOrUpdateContainer(containerID, model.Resources{
		model.ResourceMemory: model.ResourceAmount(512 * 1024 * 1024),
	})
	assert.NoError(t, err)

	// Create feeder with mock metrics client
	mockClient := &mockMetricsClientWithOOM{
		oomCounters: map[model.ContainerID]uint64{
			containerID: 5, // Initial OOM count
		},
	}

	feeder := &clusterStateFeeder{
		metricsClient: mockClient,
		clusterState:  clusterState,
		oomCounters:   make(map[model.ContainerID]uint64),
		oomChan:       make(<-chan oom.OomInfo),
	}

	// First load - should record initial counter but not trigger OOM
	feeder.LoadRealTimeMetrics(context.Background())
	assert.Equal(t, uint64(5), feeder.oomCounters[containerID])

	// Second load with same counter - no delta
	feeder.LoadRealTimeMetrics(context.Background())
	assert.Equal(t, uint64(5), feeder.oomCounters[containerID])

	// Third load with increased counter - should detect delta
	mockClient.oomCounters[containerID] = 7 // Increased by 2
	feeder.LoadRealTimeMetrics(context.Background())
	assert.Equal(t, uint64(7), feeder.oomCounters[containerID])

	// Verify OOM was recorded (we can't easily assert this without exposing internals,
	// but we can verify the counter was updated)
}

func TestOOMCounterWithNoInitialValue(t *testing.T) {
	clusterState := model.NewClusterState(time.Hour)

	podID := model.PodID{Namespace: "default", PodName: "test-pod"}
	containerID := model.ContainerID{PodID: podID, ContainerName: "test-container"}

	clusterState.AddOrUpdatePod(podID, labels.Set{}, apiv1.PodRunning)
	err := clusterState.AddOrUpdateContainer(containerID, model.Resources{
		model.ResourceMemory: model.ResourceAmount(512 * 1024 * 1024),
	})
	assert.NoError(t, err)

	mockClient := &mockMetricsClientWithOOM{
		oomCounters: map[model.ContainerID]uint64{
			containerID: 3,
		},
	}

	feeder := &clusterStateFeeder{
		metricsClient: mockClient,
		clusterState:  clusterState,
		oomCounters:   make(map[model.ContainerID]uint64),
		oomChan:       make(<-chan oom.OomInfo),
	}

	// First load establishes baseline - no OOM recorded
	feeder.LoadRealTimeMetrics(context.Background())
	assert.Equal(t, uint64(3), feeder.oomCounters[containerID])
}

func TestOOMCounterFallbackToKubernetesEvents(t *testing.T) {
	clusterState := model.NewClusterState(time.Hour)

	podID := model.PodID{Namespace: "default", PodName: "test-pod"}
	containerID := model.ContainerID{PodID: podID, ContainerName: "test-container"}

	clusterState.AddOrUpdatePod(podID, labels.Set{}, apiv1.PodRunning)
	err := clusterState.AddOrUpdateContainer(containerID, model.Resources{
		model.ResourceMemory: model.ResourceAmount(512 * 1024 * 1024),
	})
	assert.NoError(t, err)

	// Mock client that doesn't provide OOM counters
	mockClient := &mockMetricsClientWithOOM{
		oomCounters: map[model.ContainerID]uint64{}, // Empty - no OOM counters
	}

	// Create OOM channel for Kubernetes events
	oomChan := make(chan oom.OomInfo, 10)

	feeder := &clusterStateFeeder{
		metricsClient: mockClient,
		clusterState:  clusterState,
		oomCounters:   make(map[model.ContainerID]uint64),
		oomChan:       oomChan,
	}

	// Send OOM event via Kubernetes channel
	oomChan <- oom.OomInfo{
		ContainerID: containerID,
		Timestamp:   time.Now(),
		Memory:      model.ResourceAmount(512 * 1024 * 1024),
	}

	// Load metrics - should process OOM from channel
	feeder.LoadRealTimeMetrics(context.Background())

	// OOM counter map should still be empty (no Prometheus counters)
	assert.Empty(t, feeder.oomCounters)

	// But OOM event was processed from channel (verified by no panic/error)
}
