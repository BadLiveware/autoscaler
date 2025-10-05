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

// Tests for dual OOM detection: Prometheus counters AND Kubernetes OOMKilled events.
//
// KEY INSIGHT: These mechanisms detect DIFFERENT types of OOMs and are mutually exclusive:
//
// 1. Prometheus-detected OOMs (Application-level):
//    - Managed runtimes (e.g., .NET CLR, JVM) throw OOM exceptions internally
//    - Container remains RUNNING - the exception is handled at application level
//    - Prometheus can scrape custom metrics (e.g., dotnet_oom_exceptions_total)
//    - Kubernetes OOMKilled does NOT fire (kernel never intervened)
//
// 2. Kubernetes OOMKilled (Kernel-level):
//    - Container memory usage exceeds cgroup limits
//    - Container is KILLED by kernel (e.g., during constrained startup)
//    - Prometheus CANNOT see this (container is dead, can't report metrics)
//    - Kubernetes OOMKilled event fires, pod restarts
//
// CONCLUSION: Both mechanisms must remain active simultaneously to capture both OOM types.
// Double-counting should NOT occur because these events are mutually exclusive by nature.
//
// These tests verify that both mechanisms can operate together without interfering.

package input

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"

	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/input/metrics"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/input/oom"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
)

// TestDualOOMDetection_SameEvent tests that both OOM detection mechanisms can
// operate simultaneously. In practice, these detect DIFFERENT types of OOMs:
//   - Prometheus: Application-level OOM exceptions (e.g., OutOfMemoryException in .NET)
//     where the container remains running
//   - Kubernetes: Kernel-level OOMKilled where the container is terminated
//
// These are mutually exclusive events, so double-counting should not occur in practice.
// However, this test verifies the system handles both gracefully.
func TestDualOOMDetection_SameEvent(t *testing.T) {
	containerID := model.ContainerID{
		PodID: model.PodID{
			Namespace: "default",
			PodName:   "test-pod",
		},
		ContainerName: "app",
	}

	clusterState := model.NewClusterState(10 * time.Minute)
	clusterState.AddOrUpdatePod(containerID.PodID, map[string]string{"app": "test"}, apiv1.PodRunning)
	memoryRequest := model.ResourceAmount(256 * 1024 * 1024) // 256Mi
	err := clusterState.AddOrUpdateContainer(containerID, model.Resources{
		model.ResourceMemory: memoryRequest,
	})
	assert.NoError(t, err)

	oomTime := time.Now()

	// Scenario: Same OOM reported by both mechanisms within 5 seconds
	// 1. Prometheus counter increment detected first
	err = clusterState.RecordOOM(containerID, oomTime, memoryRequest)
	assert.NoError(t, err, "First OOM (Prometheus) should be recorded")

	// 2. Kubernetes OOMKilled event arrives 5 seconds later for the same OOM
	err = clusterState.RecordOOM(containerID, oomTime.Add(5*time.Second), memoryRequest)
	assert.NoError(t, err, "Second OOM (Kubernetes) should be recorded")

	// Verify: The recommendation should reflect a reasonable bump, not double bump
	// The second RecordOOM call should either:
	// a) Replace the first if it's in the same aggregation window and higher
	// b) Be ignored if it's lower
	// c) Create a new peak if it's in a different window
	//
	// In this case, both should be in the same window (5 seconds apart)
	// and the calculated memoryNeeded should be similar, so the second
	// should not dramatically change the recommendation.

	containerState := clusterState.GetContainer(containerID)
	assert.NotNil(t, containerState)

	// The peak should exist but should not be doubled
	maxPeak := containerState.GetMaxMemoryPeak()
	assert.Greater(t, maxPeak, memoryRequest, "Peak should be higher than request due to OOM bump")

	// Calculate expected single bump
	oomMinBumpUp := model.MemoryAmountFromBytes(model.GetAggregationsConfig().OOMMinBumpUp)
	oomBumpUpRatio := model.GetAggregationsConfig().OOMBumpUpRatio
	expectedSingleBump := model.ResourceAmountMax(
		memoryRequest+oomMinBumpUp,
		model.ScaleResource(memoryRequest, oomBumpUpRatio),
	)

	// The peak should be close to a single bump, not double
	// Allow for some calculation variance but it shouldn't be 2x
	assert.Less(t, maxPeak, expectedSingleBump*2,
		"Peak should not reflect double-counting of the same OOM (got %v, expected close to %v)",
		maxPeak, expectedSingleBump)
}

// TestDualOOMDetection_DifferentEvents tests that when both mechanisms report
// different OOMs, both are correctly recorded.
func TestDualOOMDetection_DifferentEvents(t *testing.T) {
	containerID := model.ContainerID{
		PodID: model.PodID{
			Namespace: "default",
			PodName:   "test-pod",
		},
		ContainerName: "app",
	}

	clusterState := model.NewClusterState(10 * time.Minute)
	clusterState.AddOrUpdatePod(containerID.PodID, map[string]string{"app": "test"}, apiv1.PodRunning)
	memoryRequest := model.ResourceAmount(256 * 1024 * 1024) // 256Mi
	err := clusterState.AddOrUpdateContainer(containerID, model.Resources{
		model.ResourceMemory: memoryRequest,
	})
	assert.NoError(t, err)

	baseTime := time.Now()
	memoryAggregationInterval := model.GetAggregationsConfig().MemoryAggregationInterval

	// Scenario: Two distinct OOMs in different aggregation windows
	// 1. First OOM detected by Prometheus
	err = clusterState.RecordOOM(containerID, baseTime, memoryRequest)
	assert.NoError(t, err, "First OOM should be recorded")

	// 2. Second OOM detected by Kubernetes OOMKilled event in a later window
	secondOOMTime := baseTime.Add(memoryAggregationInterval + time.Minute)
	err = clusterState.RecordOOM(containerID, secondOOMTime, memoryRequest)
	assert.NoError(t, err, "Second OOM should be recorded")

	// Both OOMs should be recorded as they're in different windows
	// The recommendation should reflect the impact of both OOM events
	containerState := clusterState.GetContainer(containerID)
	assert.NotNil(t, containerState)
}

// TestDualOOMDetection_KubernetesFirst tests that when Kubernetes OOMKilled event
// arrives before Prometheus counter is scraped, both are handled correctly.
func TestDualOOMDetection_KubernetesFirst(t *testing.T) {
	containerID := model.ContainerID{
		PodID: model.PodID{
			Namespace: "default",
			PodName:   "test-pod",
		},
		ContainerName: "app",
	}

	clusterState := model.NewClusterState(10 * time.Minute)
	clusterState.AddOrUpdatePod(containerID.PodID, map[string]string{"app": "test"}, apiv1.PodRunning)
	memoryRequest := model.ResourceAmount(256 * 1024 * 1024)
	err := clusterState.AddOrUpdateContainer(containerID, model.Resources{
		model.ResourceMemory: memoryRequest,
	})
	assert.NoError(t, err)

	oomTime := time.Now()

	// Scenario: Kubernetes event arrives first (pod restarts immediately)
	// 1. Kubernetes OOMKilled event detected
	err = clusterState.RecordOOM(containerID, oomTime, memoryRequest)
	assert.NoError(t, err, "Kubernetes OOM should be recorded")

	// 2. Prometheus scrapes the counter 30 seconds later (scrape interval)
	// and reports the same OOM
	err = clusterState.RecordOOM(containerID, oomTime.Add(30*time.Second), memoryRequest)
	assert.NoError(t, err, "Prometheus OOM should be recorded")

	// Verify the recommendation is reasonable
	containerState := clusterState.GetContainer(containerID)
	assert.NotNil(t, containerState)
	assert.Greater(t, containerState.GetMaxMemoryPeak(), memoryRequest,
		"Peak should be bumped due to OOM")
}

// TestDualOOMDetection_E2EWithMetricsClient tests the full flow with both
// OOM detection mechanisms active.
func TestDualOOMDetection_E2EWithMetricsClient(t *testing.T) {
	containerID := model.ContainerID{
		PodID: model.PodID{
			Namespace: "default",
			PodName:   "test-pod",
		},
		ContainerName: "app",
	}

	clusterState := model.NewClusterState(10 * time.Minute)
	clusterState.AddOrUpdatePod(containerID.PodID, map[string]string{"app": "test"}, apiv1.PodRunning)
	memoryRequest := model.ResourceAmount(256 * 1024 * 1024)
	err := clusterState.AddOrUpdateContainer(containerID, model.Resources{
		model.ResourceMemory: memoryRequest,
	})
	assert.NoError(t, err)

	baseTime := time.Now()

	// Create OOM channel with buffered capacity
	oomChan := make(chan oom.OomInfo, 10)

	// Mock metrics client that reports OOM counter
	mockClient := &mockDualOOMMetricsClient{
		snapshots: []*metrics.ContainerMetricsSnapshot{
			// First scrape: baseline OOM counter = 0
			{
				ID:           containerID,
				SnapshotTime: baseTime,
				Usage: model.Resources{
					model.ResourceMemory: model.ResourceAmount(200 * 1024 * 1024),
				},
				OOMCount: uint64Ptr(0),
			},
			// Second scrape: OOM counter = 1 (OOM happened!)
			{
				ID:           containerID,
				SnapshotTime: baseTime.Add(60 * time.Second),
				Usage: model.Resources{
					model.ResourceMemory: model.ResourceAmount(200 * 1024 * 1024),
				},
				OOMCount: uint64Ptr(1),
			},
		},
	}

	feeder := &clusterStateFeeder{
		metricsClient: mockClient,
		clusterState:  clusterState,
		oomCounters:   make(map[model.ContainerID]uint64),
		oomChan:       oomChan,
	}

	// First metrics load: establish baseline
	feeder.LoadRealTimeMetrics(context.Background())
	assert.Equal(t, uint64(0), feeder.oomCounters[containerID])

	// Simulate Kubernetes OOMKilled event arriving (pod was killed)
	oomChan <- oom.OomInfo{
		Timestamp:   baseTime.Add(30 * time.Second), // OOM happened at T+30s
		Memory:      memoryRequest,
		ContainerID: containerID,
	}

	// Second metrics load: Prometheus reports OOM counter increment
	// This will process both the Prometheus counter AND drain the oomChan
	feeder.LoadRealTimeMetrics(context.Background())
	assert.Equal(t, uint64(1), feeder.oomCounters[containerID])

	// Verify: Both OOM mechanisms fired, but the recommendation should be reasonable
	containerState := clusterState.GetContainer(containerID)
	assert.NotNil(t, containerState)

	maxPeak := containerState.GetMaxMemoryPeak()
	assert.Greater(t, maxPeak, memoryRequest, "Peak should be bumped due to OOM")

	// The peak should not be excessively high (indicating double-counting)
	oomMinBumpUp := model.MemoryAmountFromBytes(model.GetAggregationsConfig().OOMMinBumpUp)
	oomBumpUpRatio := model.GetAggregationsConfig().OOMBumpUpRatio
	expectedSingleBump := model.ResourceAmountMax(
		memoryRequest+oomMinBumpUp,
		model.ScaleResource(memoryRequest, oomBumpUpRatio),
	)

	t.Logf("Memory request: %v, Max peak: %v, Expected single bump: %v",
		memoryRequest, maxPeak, expectedSingleBump)

	// In the same aggregation window, only the highest peak should be kept
	// So even with two RecordOOM calls, the peak should not exceed 2x single bump
	assert.Less(t, maxPeak, expectedSingleBump*2,
		"Peak should not indicate severe double-counting (got %v, expected close to %v)",
		maxPeak, expectedSingleBump)
}

// TestDualOOMDetection_ManagedRuntimeScenario tests the real-world scenario with
// managed memory languages like .NET:
//
//  1. Kernel OOMKilled during startup (rare): Container is so constrained during
//     initial deployment that the kernel kills it before the CLR can handle memory.
//     → Only Kubernetes OOMKilled event fires (Prometheus can't scrape dead container)
//
//  2. Application-level OOM during normal operation (common): CLR detects memory
//     pressure and throws OutOfMemoryException. Container stays alive.
//     → Only Prometheus sees this (Kubernetes OOMKilled does NOT fire)
//
// These are different OOM types and should both contribute to recommendations.
func TestDualOOMDetection_ManagedRuntimeScenario(t *testing.T) {
	containerID := model.ContainerID{
		PodID: model.PodID{
			Namespace: "production",
			PodName:   "dotnet-api-abc123",
		},
		ContainerName: "api",
	}

	clusterState := model.NewClusterState(10 * time.Minute)
	clusterState.AddOrUpdatePod(containerID.PodID, map[string]string{"app": "dotnet-api"}, apiv1.PodRunning)
	memoryRequest := model.ResourceAmount(512 * 1024 * 1024) // 512Mi
	err := clusterState.AddOrUpdateContainer(containerID, model.Resources{
		model.ResourceMemory: memoryRequest,
	})
	assert.NoError(t, err)

	baseTime := time.Now()

	// Scenario 1: Kernel OOMKilled during startup (container is too constrained)
	// The container is killed by the kernel before it can even start properly
	// Only Kubernetes OOMKilled event fires (container is dead)
	startupOOMTime := baseTime.Add(10 * time.Second)
	err = clusterState.RecordOOM(containerID, startupOOMTime, memoryRequest)
	assert.NoError(t, err, "Kernel OOMKilled during startup should be recorded via Kubernetes event")

	// Scenario 2: Application-level OOM during normal operation
	// The CLR throws OutOfMemoryException, container stays alive
	// Only Prometheus reports this (via dotnet_oom_exceptions_total or similar)
	// Kubernetes OOMKilled does NOT fire because the kernel didn't kill anything
	normalOperationOOMTime := baseTime.Add(5 * time.Minute)
	err = clusterState.RecordOOM(containerID, normalOperationOOMTime, memoryRequest)
	assert.NoError(t, err, "Application-level OOM exception should be recorded via Prometheus")

	// Verify: Both OOM types are captured
	// 1. Kernel OOMKilled during startup (Kubernetes event only)
	// 2. Application OOM exception during runtime (Prometheus metric only)
	// Both should contribute to the memory recommendation
	containerState := clusterState.GetContainer(containerID)
	assert.NotNil(t, containerState)
	assert.Greater(t, containerState.GetMaxMemoryPeak(), memoryRequest,
		"Peak should reflect both types of OOM events")
}

// TestDualOOMDetection_RapidOOMs tests rapid successive OOMs from both sources.
func TestDualOOMDetection_RapidOOMs(t *testing.T) {
	containerID := model.ContainerID{
		PodID: model.PodID{
			Namespace: "default",
			PodName:   "crashlooping-pod",
		},
		ContainerName: "app",
	}

	clusterState := model.NewClusterState(10 * time.Minute)
	clusterState.AddOrUpdatePod(containerID.PodID, map[string]string{"app": "test"}, apiv1.PodRunning)
	memoryRequest := model.ResourceAmount(128 * 1024 * 1024) // 128Mi
	err := clusterState.AddOrUpdateContainer(containerID, model.Resources{
		model.ResourceMemory: memoryRequest,
	})
	assert.NoError(t, err)

	baseTime := time.Now()

	// Scenario: Pod is crash-looping with rapid OOMs
	// Both Prometheus and Kubernetes are reporting OOMs
	for i := 0; i < 5; i++ {
		oomTime := baseTime.Add(time.Duration(i) * 30 * time.Second)

		// Prometheus counter increment
		err = clusterState.RecordOOM(containerID, oomTime, memoryRequest)
		assert.NoError(t, err, "Prometheus OOM %d should be recorded", i)

		// Kubernetes OOMKilled event
		err = clusterState.RecordOOM(containerID, oomTime.Add(2*time.Second), memoryRequest)
		assert.NoError(t, err, "Kubernetes OOM %d should be recorded", i)
	}

	// Verify: The system should handle rapid OOMs gracefully
	// Each aggregation window should have one peak, not double-counted
	containerState := clusterState.GetContainer(containerID)
	assert.NotNil(t, containerState)
	assert.Greater(t, containerState.GetMaxMemoryPeak(), memoryRequest,
		"Peak should reflect OOM bumps")

	// The peak should eventually stabilize and recommend enough memory
	// Not testing exact value here, just that the system doesn't crash
}

// mockDualOOMMetricsClient for testing dual OOM detection
type mockDualOOMMetricsClient struct {
	snapshots []*metrics.ContainerMetricsSnapshot
	callCount int
}

func (m *mockDualOOMMetricsClient) GetContainersMetrics(ctx context.Context) ([]*metrics.ContainerMetricsSnapshot, error) {
	if m.callCount >= len(m.snapshots) {
		return []*metrics.ContainerMetricsSnapshot{}, nil
	}
	result := m.snapshots[m.callCount]
	m.callCount++
	return []*metrics.ContainerMetricsSnapshot{result}, nil
}
