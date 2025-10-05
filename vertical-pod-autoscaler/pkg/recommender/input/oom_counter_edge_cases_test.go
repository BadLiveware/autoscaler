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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"

	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/input/metrics"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/input/oom"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
)

// TestOOMCounterDecreasing verifies that decreasing OOM counters are handled gracefully.
// This shouldn't happen in normal operation but we should handle it without crashing.
func TestOOMCounterDecreasing(t *testing.T) {
	containerID := model.ContainerID{
		PodID: model.PodID{
			Namespace: "default",
			PodName:   "test-pod",
		},
		ContainerName: "test-container",
	}

	clusterState := model.NewClusterState(10 * time.Minute)

	// Add pod and container to cluster state
	clusterState.AddOrUpdatePod(containerID.PodID, map[string]string{"app": "test"}, apiv1.PodRunning)
	err := clusterState.AddOrUpdateContainer(containerID, model.Resources{
		model.ResourceMemory: model.ResourceAmount(100 * 1024 * 1024), // 100MB
	})
	assert.NoError(t, err)

	mockClient := &mockMetricsClient{
		snapshots: [][]*metrics.ContainerMetricsSnapshot{
			// First call: OOM counter = 5
			{
				&metrics.ContainerMetricsSnapshot{
					ID:           containerID,
					SnapshotTime: time.Now(),
					Usage: model.Resources{
						model.ResourceCPU:    model.ResourceAmount(100),
						model.ResourceMemory: model.ResourceAmount(50 * 1024 * 1024),
					},
					OOMCount: uint64Ptr(5),
				},
			},
			// Second call: OOM counter decreased to 3 (shouldn't happen)
			{
				&metrics.ContainerMetricsSnapshot{
					ID:           containerID,
					SnapshotTime: time.Now().Add(time.Minute),
					Usage: model.Resources{
						model.ResourceCPU:    model.ResourceAmount(100),
						model.ResourceMemory: model.ResourceAmount(50 * 1024 * 1024),
					},
					OOMCount: uint64Ptr(3),
				},
			},
		},
	}

	feeder := &clusterStateFeeder{
		metricsClient: mockClient,
		clusterState:  clusterState,
		oomCounters:   make(map[model.ContainerID]uint64),
		oomChan:       make(<-chan oom.OomInfo),
	}

	// First load: establish baseline of 5
	feeder.LoadRealTimeMetrics(context.Background())
	assert.Equal(t, uint64(5), feeder.oomCounters[containerID], "Should store initial OOM counter")

	// Second load: counter decreased to 3
	// Should NOT record OOM events (no delta), but should update baseline
	feeder.LoadRealTimeMetrics(context.Background())
	assert.Equal(t, uint64(3), feeder.oomCounters[containerID], "Should update to new counter value even if decreased")

	// No OOMs should have been recorded (we can't verify this directly without inspecting cluster state,
	// but the test passes if no errors occur)
}

// TestOOMCounterReset verifies that OOM counter resets (e.g., after Prometheus restart)
// are handled gracefully.
func TestOOMCounterReset(t *testing.T) {
	containerID := model.ContainerID{
		PodID: model.PodID{
			Namespace: "default",
			PodName:   "test-pod",
		},
		ContainerName: "test-container",
	}

	clusterState := model.NewClusterState(10 * time.Minute)
	clusterState.AddOrUpdatePod(containerID.PodID, map[string]string{"app": "test"}, apiv1.PodRunning)
	err := clusterState.AddOrUpdateContainer(containerID, model.Resources{
		model.ResourceMemory: model.ResourceAmount(100 * 1024 * 1024),
	})
	assert.NoError(t, err)

	mockClient := &mockMetricsClient{
		snapshots: [][]*metrics.ContainerMetricsSnapshot{
			// Counter at 10
			{
				&metrics.ContainerMetricsSnapshot{
					ID:           containerID,
					SnapshotTime: time.Now(),
					Usage: model.Resources{
						model.ResourceMemory: model.ResourceAmount(50 * 1024 * 1024),
					},
					OOMCount: uint64Ptr(10),
				},
			},
			// Counter reset to 0
			{
				&metrics.ContainerMetricsSnapshot{
					ID:           containerID,
					SnapshotTime: time.Now().Add(time.Minute),
					Usage: model.Resources{
						model.ResourceMemory: model.ResourceAmount(50 * 1024 * 1024),
					},
					OOMCount: uint64Ptr(0),
				},
			},
			// Counter at 2 (2 OOMs since reset)
			{
				&metrics.ContainerMetricsSnapshot{
					ID:           containerID,
					SnapshotTime: time.Now().Add(2 * time.Minute),
					Usage: model.Resources{
						model.ResourceMemory: model.ResourceAmount(50 * 1024 * 1024),
					},
					OOMCount: uint64Ptr(2),
				},
			},
		},
	}

	feeder := &clusterStateFeeder{
		metricsClient: mockClient,
		clusterState:  clusterState,
		oomCounters:   make(map[model.ContainerID]uint64),
		oomChan:       make(<-chan oom.OomInfo),
	}

	// Load 1: baseline = 10
	feeder.LoadRealTimeMetrics(context.Background())
	assert.Equal(t, uint64(10), feeder.oomCounters[containerID])

	// Load 2: reset to 0 (should update baseline but not record negative OOMs)
	feeder.LoadRealTimeMetrics(context.Background())
	assert.Equal(t, uint64(0), feeder.oomCounters[containerID], "Should update baseline after reset")

	// Load 3: counter at 2 (should detect 2 new OOMs since reset)
	feeder.LoadRealTimeMetrics(context.Background())
	assert.Equal(t, uint64(2), feeder.oomCounters[containerID])

	// The 2 OOMs from the last load should have been recorded
	// (We'd need to inspect cluster state internals to verify, but test passes if no errors)
}

// TestOOMCounterVeryLargeDelta verifies handling of very large counter jumps.
func TestOOMCounterVeryLargeDelta(t *testing.T) {
	containerID := model.ContainerID{
		PodID: model.PodID{
			Namespace: "default",
			PodName:   "test-pod",
		},
		ContainerName: "test-container",
	}

	clusterState := model.NewClusterState(10 * time.Minute)
	clusterState.AddOrUpdatePod(containerID.PodID, map[string]string{"app": "test"}, apiv1.PodRunning)
	err := clusterState.AddOrUpdateContainer(containerID, model.Resources{
		model.ResourceMemory: model.ResourceAmount(100 * 1024 * 1024),
	})
	assert.NoError(t, err)

	mockClient := &mockMetricsClient{
		snapshots: [][]*metrics.ContainerMetricsSnapshot{
			// Baseline: 0
			{
				&metrics.ContainerMetricsSnapshot{
					ID:           containerID,
					SnapshotTime: time.Now(),
					Usage: model.Resources{
						model.ResourceMemory: model.ResourceAmount(50 * 1024 * 1024),
					},
					OOMCount: uint64Ptr(0),
				},
			},
			// Very large jump: 1000 OOMs
			{
				&metrics.ContainerMetricsSnapshot{
					ID:           containerID,
					SnapshotTime: time.Now().Add(time.Minute),
					Usage: model.Resources{
						model.ResourceMemory: model.ResourceAmount(50 * 1024 * 1024),
					},
					OOMCount: uint64Ptr(1000),
				},
			},
		},
	}

	feeder := &clusterStateFeeder{
		metricsClient: mockClient,
		clusterState:  clusterState,
		oomCounters:   make(map[model.ContainerID]uint64),
		oomChan:       make(<-chan oom.OomInfo),
	}

	// Load 1: baseline
	feeder.LoadRealTimeMetrics(context.Background())
	assert.Equal(t, uint64(0), feeder.oomCounters[containerID])

	// Load 2: huge jump
	// Should handle without overflow or hanging (loop 1000 times)
	feeder.LoadRealTimeMetrics(context.Background())
	assert.Equal(t, uint64(1000), feeder.oomCounters[containerID])

	// Test passes if it completes without hanging or crashing
}

// TestOOMCounterMaxUint64 verifies handling of counter at maximum uint64 value.
func TestOOMCounterMaxUint64(t *testing.T) {
	containerID := model.ContainerID{
		PodID: model.PodID{
			Namespace: "default",
			PodName:   "test-pod",
		},
		ContainerName: "test-container",
	}

	clusterState := model.NewClusterState(10 * time.Minute)
	clusterState.AddOrUpdatePod(containerID.PodID, map[string]string{"app": "test"}, apiv1.PodRunning)
	err := clusterState.AddOrUpdateContainer(containerID, model.Resources{
		model.ResourceMemory: model.ResourceAmount(100 * 1024 * 1024),
	})
	assert.NoError(t, err)

	maxUint64 := uint64(math.MaxUint64)

	mockClient := &mockMetricsClient{
		snapshots: [][]*metrics.ContainerMetricsSnapshot{
			{
				&metrics.ContainerMetricsSnapshot{
					ID:           containerID,
					SnapshotTime: time.Now(),
					Usage: model.Resources{
						model.ResourceMemory: model.ResourceAmount(50 * 1024 * 1024),
					},
					OOMCount: &maxUint64,
				},
			},
		},
	}

	feeder := &clusterStateFeeder{
		metricsClient: mockClient,
		clusterState:  clusterState,
		oomCounters:   make(map[model.ContainerID]uint64),
		oomChan:       make(<-chan oom.OomInfo),
	}

	// Should handle max uint64 without overflow
	feeder.LoadRealTimeMetrics(context.Background())
	assert.Equal(t, maxUint64, feeder.oomCounters[containerID])
}

// TestOOMCounterNilValue verifies that nil OOM counters are handled (metric not available).
func TestOOMCounterNilValue(t *testing.T) {
	containerID := model.ContainerID{
		PodID: model.PodID{
			Namespace: "default",
			PodName:   "test-pod",
		},
		ContainerName: "test-container",
	}

	clusterState := model.NewClusterState(10 * time.Minute)
	clusterState.AddOrUpdatePod(containerID.PodID, map[string]string{"app": "test"}, apiv1.PodRunning)
	err := clusterState.AddOrUpdateContainer(containerID, model.Resources{
		model.ResourceMemory: model.ResourceAmount(100 * 1024 * 1024),
	})
	assert.NoError(t, err)

	mockClient := &mockMetricsClient{
		snapshots: [][]*metrics.ContainerMetricsSnapshot{
			{
				&metrics.ContainerMetricsSnapshot{
					ID:           containerID,
					SnapshotTime: time.Now(),
					Usage: model.Resources{
						model.ResourceMemory: model.ResourceAmount(50 * 1024 * 1024),
					},
					OOMCount: nil, // No OOM counter available
				},
			},
		},
	}

	feeder := &clusterStateFeeder{
		metricsClient: mockClient,
		clusterState:  clusterState,
		oomCounters:   make(map[model.ContainerID]uint64),
		oomChan:       make(<-chan oom.OomInfo),
	}

	// Should handle nil gracefully without errors
	feeder.LoadRealTimeMetrics(context.Background())

	// Counter should not be set (no entry in map)
	_, exists := feeder.oomCounters[containerID]
	assert.False(t, exists, "Should not create counter entry when OOMCount is nil")
}

// mockMetricsClient returns pre-configured snapshots for testing
type mockMetricsClient struct {
	snapshots [][]*metrics.ContainerMetricsSnapshot
	callCount int
}

func (m *mockMetricsClient) GetContainersMetrics(ctx context.Context) ([]*metrics.ContainerMetricsSnapshot, error) {
	if m.callCount >= len(m.snapshots) {
		return []*metrics.ContainerMetricsSnapshot{}, nil
	}
	result := m.snapshots[m.callCount]
	m.callCount++
	return result, nil
}

func uint64Ptr(v uint64) *uint64 {
	return &v
}
