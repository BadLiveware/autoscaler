/*
Copyright 2026 The Kubernetes Authors.

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
	"fmt"
	"math"
	"sync"
	"time"

	prometheusv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	prommodel "github.com/prometheus/common/model"
	"k8s.io/klog/v2"

	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/annotations"
)

// promQueryAPI is the slice of the Prometheus v1 API that this observer
// needs. Defined locally so tests can stub it without implementing the full
// prometheusv1.API surface.
type promQueryAPI interface {
	Query(ctx context.Context, query string, ts time.Time, opts ...prometheusv1.Option) (prommodel.Value, prometheusv1.Warnings, error)
}

// clusterStateView is the slice of model.ClusterState the observer reads.
// Same rationale as promQueryAPI: keeps tests light and decouples from
// future ClusterState additions.
type clusterStateView interface {
	VPAs() map[model.VpaID]*model.Vpa
	Pods() map[model.PodID]*model.PodState
}

// PrometheusObserver polls a per-VPA Prometheus counter and emits OomInfo
// records into a shared channel for each observed counter increase.
//
// It exists for runtimes that catch out-of-memory conditions internally and
// don't trigger a container OOMKill — most notably .NET, where an
// OutOfMemoryException is caught and the pod keeps running, leaving the
// recommender blind to memory pressure. By exposing a counter that the
// runtime increments on each OOM event, those events can drive VPA's
// existing OOM bump-up logic.
//
// The observer does NOT implement the Observer interface intentionally:
// Observer is a Kubernetes event/informer pattern, while this is a
// timer-driven poller. They coexist by writing to the same OomInfo channel.
type PrometheusObserver struct {
	promAPI        promQueryAPI
	clusterState   clusterStateView
	oomChan        chan<- OomInfo
	pollInterval   time.Duration
	queryTimeout   time.Duration
	podLabel       string
	containerLabel string

	// VPAs that have completed at least one successful poll. The first
	// successful poll's increase() result is discarded so a recommender
	// restart does not replay historical counter increments as a synthetic
	// OOM burst.
	mu   sync.Mutex
	seen map[model.VpaID]struct{}
}

// PrometheusObserverConfig is the configuration for PrometheusObserver.
type PrometheusObserverConfig struct {
	// API is the Prometheus client. Reuse history.NewPrometheusAPI to share
	// the same instrumentation as the history provider.
	API prometheusv1.API
	// ClusterState is read on every poll to discover VPAs with the OOM
	// counter annotation and to look up matching pods and container memory
	// requests.
	ClusterState model.ClusterState
	// OomChan receives synthetic OomInfo records, typically the same channel
	// the existing event-driven Observer writes to.
	OomChan chan<- OomInfo
	// PollInterval is both how often we query Prometheus and the [range]
	// window passed to increase(). Match it to the recommender's
	// MetricsFetcherInterval so each event is observed exactly once under
	// nominal conditions. Brief Prometheus outages may cause missed events.
	PollInterval time.Duration
	// QueryTimeout bounds each poll's PromQL execution.
	QueryTimeout time.Duration
	// PodLabel and ContainerLabel are the Prometheus label names used to
	// identify the pod and container. Match the labels emitted by the
	// counter exporter (commonly "pod" and "container").
	PodLabel       string
	ContainerLabel string
}

// NewPrometheusObserver constructs a PrometheusObserver from cfg.
func NewPrometheusObserver(cfg PrometheusObserverConfig) *PrometheusObserver {
	return &PrometheusObserver{
		promAPI:        cfg.API,
		clusterState:   cfg.ClusterState,
		oomChan:        cfg.OomChan,
		pollInterval:   cfg.PollInterval,
		queryTimeout:   cfg.QueryTimeout,
		podLabel:       cfg.PodLabel,
		containerLabel: cfg.ContainerLabel,
		seen:           make(map[model.VpaID]struct{}),
	}
}

// Run polls Prometheus on the configured interval until ctx is done. Intended
// to be launched in its own goroutine.
func (o *PrometheusObserver) Run(ctx context.Context) {
	ticker := time.NewTicker(o.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			o.pollOnce(ctx)
		}
	}
}

func (o *PrometheusObserver) pollOnce(ctx context.Context) {
	vpas := o.clusterState.VPAs()
	for _, vpa := range vpas {
		selector := annotations.OOMCounterMetric(vpa.Annotations)
		if selector == "" {
			continue
		}
		o.pollVPA(ctx, vpa, selector)
	}
	o.pruneSeen(vpas)
}

// pruneSeen drops first-poll-baseline entries for VPAs that no longer exist.
// Without this the seen map grows unboundedly on VPA churn.
func (o *PrometheusObserver) pruneSeen(current map[model.VpaID]*model.Vpa) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for id := range o.seen {
		if _, ok := current[id]; !ok {
			delete(o.seen, id)
		}
	}
}

// pollVPA queries Prometheus for the counter selector annotated on this VPA.
// The selector is the user's instant vector selector (e.g.
// `dotnet_oome{deployment="api"}`); we only wrap it with sum-by + increase().
// We do NOT additionally filter by VPA pod selector — the user's matchers
// own scoping.
func (o *PrometheusObserver) pollVPA(ctx context.Context, vpa *model.Vpa, selector string) {
	queryCtx, cancel := context.WithTimeout(ctx, o.queryTimeout)
	defer cancel()

	// sum-by guards against multiple series for the same (pod, container)
	// when the counter exposes orthogonal labels (e.g. exception type).
	query := fmt.Sprintf(
		"sum by (%s, %s) (increase(%s[%s]))",
		o.podLabel, o.containerLabel, selector, prommodel.Duration(o.pollInterval).String(),
	)

	val, warns, err := o.promAPI.Query(queryCtx, query, time.Now())
	if err != nil {
		klog.V(2).InfoS("Prometheus OOM query failed", "vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName), "err", err)
		return
	}
	for _, w := range warns {
		klog.V(4).InfoS("Prometheus OOM query warning", "vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName), "warning", w)
	}

	o.mu.Lock()
	_, alreadySeen := o.seen[vpa.ID]
	o.seen[vpa.ID] = struct{}{}
	o.mu.Unlock()

	if !alreadySeen {
		klog.V(4).InfoS("Prometheus OOM observer: discarding first-poll delta", "vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName))
		return
	}

	samples, ok := val.(prommodel.Vector)
	if !ok {
		klog.V(2).InfoS("Prometheus OOM query returned unexpected type", "vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName), "type", fmt.Sprintf("%T", val))
		return
	}

	pods := o.clusterState.Pods()

	for _, sample := range samples {
		podName := string(sample.Metric[prommodel.LabelName(o.podLabel)])
		ctrName := string(sample.Metric[prommodel.LabelName(o.containerLabel)])
		if podName == "" || ctrName == "" {
			continue
		}
		// floor: increase() can return small fractional values across rate
		// boundaries, but each emitted OomInfo represents a discrete event.
		count := int64(math.Floor(float64(sample.Value)))
		if count <= 0 {
			continue
		}

		podID := model.PodID{Namespace: vpa.ID.Namespace, PodName: podName}
		podState, ok := pods[podID]
		if !ok {
			continue
		}
		container, ok := podState.Containers[ctrName]
		if !ok {
			continue
		}
		memory := container.Request[model.ResourceMemory]
		if memory == 0 {
			klog.V(4).InfoS("Prometheus OOM observer: skipping container with no memory request", "vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName), "pod", podName, "container", ctrName)
			continue
		}

		ts := sample.Timestamp.Time().UTC()
		for range count {
			o.oomChan <- OomInfo{
				Timestamp: ts,
				Memory:    memory,
				ContainerID: model.ContainerID{
					PodID:         podID,
					ContainerName: ctrName,
				},
			}
		}
		klog.V(2).InfoS("Prometheus OOM observer: emitted events", "vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName), "pod", podName, "container", ctrName, "count", count)
	}
}
