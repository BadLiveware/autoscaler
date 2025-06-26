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

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"k8s.io/metrics/pkg/client/external_metrics"

	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
)

// ExternalOomChecker defines the interface for checking external OOM metrics
type ExternalOomChecker interface {
	CheckForOOMs(ctx context.Context) error
	CleanupOldContainers()
}

// ExternalOomObserver can observe OOM events from external metrics instead of Kubernetes events.
type ExternalOomObserver struct {
	observedOomsChannel chan OomInfo
	externalClient      external_metrics.ExternalMetricsClient
	oomMetricName       string
	containerNameLabel  string
	clusterState        model.ClusterState
	lastOomCounts       map[model.ContainerID]int64
}

// NewExternalObserver returns new instance of the external OOM observer.
func NewExternalObserver(config *rest.Config, oomMetricName, containerNameLabel string, clusterState model.ClusterState) (*ExternalOomObserver, error) {
	extClient, err := external_metrics.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	return &ExternalOomObserver{
		observedOomsChannel: make(chan OomInfo, 5000),
		externalClient:      extClient,
		oomMetricName:       oomMetricName,
		containerNameLabel:  containerNameLabel,
		clusterState:        clusterState,
		lastOomCounts:       make(map[model.ContainerID]int64),
	}, nil
}

func (o *ExternalOomObserver) GetObservedOomsChannel() chan OomInfo {
	return o.observedOomsChannel
}

// OnEvent is a no-op for external observer since we don't watch Kubernetes events
func (o *ExternalOomObserver) OnEvent(*apiv1.Event) {}

// OnAdd is Noop
func (o *ExternalOomObserver) OnAdd(obj interface{}, isInInitialList bool) {}

// OnUpdate is Noop for external observer
func (o *ExternalOomObserver) OnUpdate(oldObj, newObj interface{}) {}

// OnDelete is Noop
func (o *ExternalOomObserver) OnDelete(obj interface{}) {}

// CheckForOOMs queries external metrics for OOM counts and detects new OOMs
func (o *ExternalOomObserver) CheckForOOMs(ctx context.Context) error {
	if o.oomMetricName == "" {
		return nil // No external OOM metric configured
	}

	klog.V(4).InfoS("Checking for external OOMs", "metric", o.oomMetricName)

	// Query all VPAs and their matching pods to check for OOM metrics
	for _, vpa := range o.clusterState.VPAs() {
		if vpa.PodCount == 0 {
			continue
		}

		nsClient := o.externalClient.NamespacedMetrics(vpa.ID.Namespace)
		pods := o.clusterState.GetMatchingPods(vpa)

		for _, podID := range pods {
			podNameReq, err := labels.NewRequirement("pod", selection.Equals, []string{podID.PodName})
			if err != nil {
				klog.ErrorS(err, "Failed to create pod selector requirement", "pod", podID.PodName)
				continue
			}
			selector := vpa.PodSelector.Add(*podNameReq)

			// Query the external metrics API for OOM counts
			metrics, err := nsClient.List(o.oomMetricName, selector)
			if err != nil {
				klog.V(4).InfoS("Failed to query external OOM metrics", "error", err, "metric", o.oomMetricName, "namespace", vpa.ID.Namespace, "pod", podID.PodName)
				continue
			}

			if metrics == nil || len(metrics.Items) == 0 {
				klog.V(5).InfoS("No external OOM metrics found", "metric", o.oomMetricName, "namespace", vpa.ID.Namespace, "pod", podID.PodName)
				continue
			}

			// Process each metric item (one per container)
			for _, item := range metrics.Items {
				containerName, hasContainer := item.MetricLabels[o.containerNameLabel]
				if !hasContainer {
					klog.V(4).InfoS("OOM metric missing container name label", "label", o.containerNameLabel, "metric", o.oomMetricName)
					continue
				}

				containerID := model.ContainerID{
					PodID: model.PodID{
						Namespace: vpa.ID.Namespace,
						PodName:   podID.PodName,
					},
					ContainerName: containerName,
				}

				// Get current OOM count from the metric
				currentCount := item.Value.Value()
				lastCount, exists := o.lastOomCounts[containerID]

				// If this is a new container or count has increased, we have new OOMs
				if !exists {
					// For new containers, treat the current count as new OOMs if > 0
					if currentCount > 0 {
						klog.V(3).InfoS("Detected initial external OOMs for new container", "container", containerID, "newOOMs", currentCount)

						// For each initial OOM, create an OOM event
						for i := int64(0); i < currentCount; i++ {
							// Get the container's current memory request as an estimate
							var memoryUsed model.ResourceAmount
							if containerState := o.clusterState.GetContainer(containerID); containerState != nil {
								memoryUsed = containerState.Request[model.ResourceMemory]
							} else {
								// Default fallback - use a reasonable estimate
								memoryUsed = model.ResourceAmount(100 * 1024 * 1024) // 100Mi
							}

							oomInfo := OomInfo{
								Timestamp:   item.Timestamp.Time.UTC(),
								Memory:      memoryUsed,
								ContainerID: containerID,
							}

							select {
							case o.observedOomsChannel <- oomInfo:
								klog.V(4).InfoS("Sent external OOM to channel", "oomInfo", oomInfo)
							default:
								klog.V(2).InfoS("OOM channel full, dropping OOM event", "oomInfo", oomInfo)
							}
						}
					}
					o.lastOomCounts[containerID] = currentCount
					klog.V(4).InfoS("Initialized OOM count for container", "container", containerID, "count", currentCount)
					continue
				}

				if currentCount > lastCount {
					newOOMs := currentCount - lastCount
					klog.V(3).InfoS("Detected new external OOMs", "container", containerID, "newOOMs", newOOMs, "totalCount", currentCount)

					// For each new OOM, create an OOM event
					for i := int64(0); i < newOOMs; i++ {
						// Get the container's current memory request as an estimate
						var memoryUsed model.ResourceAmount
						if containerState := o.clusterState.GetContainer(containerID); containerState != nil {
							memoryUsed = containerState.Request[model.ResourceMemory]
						} else {
							// Default fallback - use a reasonable estimate
							memoryUsed = model.ResourceAmount(100 * 1024 * 1024) // 100Mi
						}

						oomInfo := OomInfo{
							Timestamp:   item.Timestamp.Time.UTC(),
							Memory:      memoryUsed,
							ContainerID: containerID,
						}

						select {
						case o.observedOomsChannel <- oomInfo:
							klog.V(4).InfoS("Sent external OOM to channel", "oomInfo", oomInfo)
						default:
							klog.V(2).InfoS("OOM channel full, dropping OOM event", "oomInfo", oomInfo)
						}
					}

					o.lastOomCounts[containerID] = currentCount
				}
			}
		}
	}

	return nil
}

// CleanupOldContainers removes tracking for containers that no longer exist
func (o *ExternalOomObserver) CleanupOldContainers() {
	// Get all currently tracked containers
	currentContainers := make(map[model.ContainerID]bool)
	for _, vpa := range o.clusterState.VPAs() {
		pods := o.clusterState.GetMatchingPods(vpa)
		for _, podID := range pods {
			if podState, exists := o.clusterState.Pods()[podID]; exists {
				for containerName := range podState.Containers {
					containerID := model.ContainerID{
						PodID:         podID,
						ContainerName: containerName,
					}
					currentContainers[containerID] = true
				}
			}
		}
	}

	// Remove tracking for containers that no longer exist
	for containerID := range o.lastOomCounts {
		if !currentContainers[containerID] {
			delete(o.lastOomCounts, containerID)
			klog.V(4).InfoS("Cleaned up OOM tracking for removed container", "container", containerID)
		}
	}
}
