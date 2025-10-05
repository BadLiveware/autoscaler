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
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
)

// ExternalMetricsSource provides OOM counter data from external metrics systems (e.g., Prometheus).
type ExternalMetricsSource interface {
	// GetOOMCounters returns current OOM counter values for all containers.
	// The returned map is keyed by ContainerID with counter values.
	GetOOMCounters() map[model.ContainerID]uint64
}

// externalObserver queries external metrics for OOM events instead of watching Kubernetes events.
// This is designed for managed languages (like .NET, Java, Go) that throw internal OOM exceptions
// rather than being OOMKilled by the Linux kernel.
type externalObserver struct {
	observedOomsChannel chan OomInfo
	metricsSource       ExternalMetricsSource
	clusterState        model.ClusterState
	lastCounters        map[model.ContainerID]uint64
	countersMutex       sync.RWMutex
	stopCh              <-chan struct{}
	pollingInterval     time.Duration
}

// ExternalObserverConfig configures the external OOM observer.
type ExternalObserverConfig struct {
	// MetricsSource provides OOM counter data from external metrics.
	MetricsSource ExternalMetricsSource
	// ClusterState is used to look up container memory requests.
	ClusterState model.ClusterState
	// PollingInterval determines how often to query for new OOM events.
	// Default is 60 seconds if not specified.
	PollingInterval time.Duration
	// StopChannel signals when to stop polling.
	StopChannel <-chan struct{}
}

// NewExternalObserver creates an OOM observer that queries external metrics.
func NewExternalObserver(config ExternalObserverConfig) Observer {
	if config.PollingInterval == 0 {
		config.PollingInterval = 60 * time.Second
	}

	o := &externalObserver{
		observedOomsChannel: make(chan OomInfo, 5000),
		metricsSource:       config.MetricsSource,
		clusterState:        config.ClusterState,
		lastCounters:        make(map[model.ContainerID]uint64),
		stopCh:              config.StopChannel,
		pollingInterval:     config.PollingInterval,
	}

	// Start polling in background
	go o.pollOOMCounters()

	return o
}

// GetObservedOomsChannel returns the channel where OOM events are published.
func (o *externalObserver) GetObservedOomsChannel() chan OomInfo {
	return o.observedOomsChannel
}

// pollOOMCounters periodically queries external metrics and detects OOM counter increases.
func (o *externalObserver) pollOOMCounters() {
	ticker := time.NewTicker(o.pollingInterval)
	defer ticker.Stop()

	klog.V(2).InfoS("External OOM observer started polling", "interval", o.pollingInterval)

	for {
		select {
		case <-o.stopCh:
			klog.V(2).InfoS("External OOM observer stopped")
			close(o.observedOomsChannel)
			return
		case <-ticker.C:
			o.checkForNewOOMs()
		}
	}
}

// checkForNewOOMs queries current counters and detects increases since last check.
func (o *externalObserver) checkForNewOOMs() {
	currentCounters := o.metricsSource.GetOOMCounters()
	if currentCounters == nil {
		klog.V(4).InfoS("No OOM counters available from metrics source")
		return
	}

	o.countersMutex.Lock()
	defer o.countersMutex.Unlock()

	now := time.Now()
	detectedCount := 0

	for containerID, currentCount := range currentCounters {
		lastCount, exists := o.lastCounters[containerID]

		// If this is a new container, just record the initial value without generating events
		if !exists {
			o.lastCounters[containerID] = currentCount
			klog.V(4).InfoS("Recording initial OOM counter", "container", containerID, "count", currentCount)
			continue
		}

		// If counter increased, we have new OOM(s)
		if currentCount > lastCount {
			numNewOOMs := currentCount - lastCount

			// Get memory request from cluster state
			memoryRequest := o.getContainerMemoryRequest(containerID)

			// Create OOM events for each counter increase
			// In practice, we usually see one OOM at a time, but counters could increase by more
			for i := uint64(0); i < numNewOOMs; i++ {
				oomInfo := OomInfo{
					Timestamp:   now,
					Memory:      memoryRequest,
					ContainerID: containerID,
				}

				select {
				case o.observedOomsChannel <- oomInfo:
					detectedCount++
					klog.V(3).InfoS("External OOM detected",
						"container", containerID,
						"namespace", containerID.Namespace,
						"pod", containerID.PodName,
						"previousCount", lastCount,
						"currentCount", currentCount,
						"memoryRequest", memoryRequest)
				default:
					klog.ErrorS(nil, "OOM channel full, dropping OOM event", "container", containerID)
				}
			}

			// Update last known counter
			o.lastCounters[containerID] = currentCount
		}
	}

	// Clean up counters for containers that no longer exist
	o.cleanupStaleCounters(currentCounters)

	if detectedCount > 0 {
		klog.V(2).InfoS("External OOM detection completed", "newOOMs", detectedCount, "totalContainers", len(currentCounters))
	} else {
		klog.V(4).InfoS("External OOM detection completed", "newOOMs", 0, "totalContainers", len(currentCounters))
	}
}

// getContainerMemoryRequest retrieves the memory request for a container from cluster state.
func (o *externalObserver) getContainerMemoryRequest(containerID model.ContainerID) model.ResourceAmount {
	pod, exists := o.clusterState.Pods()[containerID.PodID]
	if !exists {
		klog.V(4).InfoS("Pod not found in cluster state for OOM event", "pod", containerID.PodID)
		return 0
	}

	container, exists := pod.Containers[containerID.ContainerName]
	if !exists {
		klog.V(4).InfoS("Container not found in pod state for OOM event",
			"pod", containerID.PodID,
			"container", containerID.ContainerName)
		return 0
	}

	if container.Request == nil {
		klog.V(4).InfoS("No memory request found for container",
			"pod", containerID.PodID,
			"container", containerID.ContainerName)
		return 0
	}

	memoryRequest, exists := container.Request[model.ResourceMemory]
	if !exists {
		klog.V(4).InfoS("No memory resource request found for container",
			"pod", containerID.PodID,
			"container", containerID.ContainerName)
		return 0
	}

	return memoryRequest
}

// cleanupStaleCounters removes counters for containers that no longer exist.
func (o *externalObserver) cleanupStaleCounters(currentCounters map[model.ContainerID]uint64) {
	for containerID := range o.lastCounters {
		if _, exists := currentCounters[containerID]; !exists {
			delete(o.lastCounters, containerID)
			klog.V(4).InfoS("Cleaned up stale OOM counter", "container", containerID)
		}
	}
}

// OnEvent is a no-op for external observer as it doesn't watch Kubernetes events.
func (o *externalObserver) OnEvent(event *apiv1.Event) {
	// No-op: external observer doesn't process Kubernetes events
}

// OnAdd is a no-op for external observer as it doesn't watch pod additions.
func (o *externalObserver) OnAdd(obj interface{}, isInInitialList bool) {
	// No-op: external observer doesn't watch pod lifecycle
}

// OnUpdate is a no-op for external observer as it doesn't watch pod updates.
func (o *externalObserver) OnUpdate(oldObj, newObj interface{}) {
	// No-op: external observer doesn't watch pod lifecycle
}

// OnDelete is a no-op for external observer as it doesn't watch pod deletions.
func (o *externalObserver) OnDelete(obj interface{}) {
	// No-op: external observer doesn't watch pod lifecycle
}
