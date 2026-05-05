/*
Copyright 2023 The Kubernetes Authors.

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
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"k8s.io/metrics/pkg/apis/metrics/v1beta1"
	resourceclient "k8s.io/metrics/pkg/client/clientset/versioned/typed/metrics/v1beta1"
	"k8s.io/metrics/pkg/client/external_metrics"

	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/annotations"
)

// PodMetricsLister wraps both metrics-client and External Metrics
type PodMetricsLister interface {
	List(ctx context.Context, namespace string, opts metav1.ListOptions) (*v1beta1.PodMetricsList, error)
}

// podMetricsSource is the metrics-client source of metrics.
type podMetricsSource struct {
	metricsGetter resourceclient.PodMetricsesGetter
}

// NewPodMetricsesSource Returns a Source-wrapper around PodMetricsesGetter.
func NewPodMetricsesSource(source resourceclient.PodMetricsesGetter) PodMetricsLister {
	return podMetricsSource{metricsGetter: source}
}

func (s podMetricsSource) List(ctx context.Context, namespace string, opts metav1.ListOptions) (*v1beta1.PodMetricsList, error) {
	podMetricsInterface := s.metricsGetter.PodMetricses(namespace)
	return podMetricsInterface.List(ctx, opts)
}

// externalMetricsClient is the External Metrics source of metrics.
type externalMetricsClient struct {
	externalClient external_metrics.ExternalMetricsClient
	options        ExternalClientOptions
	clusterState   model.ClusterState
}

// ExternalClientOptions specifies parameters for using an External Metrics Client.
type ExternalClientOptions struct {
	// ResourceMetrics is the cluster-wide default metric selector per resource,
	// applied when a VPA does not set the per-resource annotation. The value
	// is parsed as a Prometheus instant vector selector
	// (e.g. `metric_name{matcher,...}`); a bare metric name is equivalent to
	// `metric_name{}`.
	ResourceMetrics map[corev1.ResourceName]string
	// PodNameLabel and ContainerNameLabel are the metric label names used to
	// attribute returned samples back to a (pod, container).
	PodNameLabel       string
	ContainerNameLabel string
	// AnnotatedVPAsOnly, when true, restricts the client to only iterate VPAs
	// that opt into external metrics via annotations. Used when the client is
	// composed inside a multiSource alongside metrics-server, so non-annotated
	// VPAs are served by metrics-server instead of being silently skipped or
	// double-counted.
	AnnotatedVPAsOnly bool
}

// NewExternalClient returns a Source for an External Metrics Client.
func NewExternalClient(c *rest.Config, clusterState model.ClusterState, options ExternalClientOptions) PodMetricsLister {
	extClient, err := external_metrics.NewForConfig(c)
	if err != nil {
		klog.ErrorS(err, "Failed initializing external metrics client")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}
	return &externalMetricsClient{
		externalClient: extClient,
		options:        options,
		clusterState:   clusterState,
	}
}

func (s *externalMetricsClient) List(ctx context.Context, namespace string, opts metav1.ListOptions) (*v1beta1.PodMetricsList, error) {
	result := v1beta1.PodMetricsList{}

	for _, vpa := range s.clusterState.VPAs() {
		if vpa.PodCount == 0 {
			continue
		}

		if namespace != "" && vpa.ID.Namespace != namespace {
			continue
		}

		if s.options.AnnotatedVPAsOnly && !annotations.HasExternalMetricOverride(vpa.Annotations) {
			continue
		}

		s.appendVPASamples(vpa, &result)
	}
	return &result, nil
}

// appendVPASamples issues one external-metrics query per opted-in resource
// (no per-pod fan-out and no VPA pod-selector filtering — the caller's
// PromQL-style annotation, or the global flag default, owns scoping). Result
// items are bucketed into PodMetrics keyed by the configured pod label.
func (s *externalMetricsClient) appendVPASamples(vpa *model.Vpa, out *v1beta1.PodMetricsList) {
	type ctrKey struct{ pod, container string }
	usage := make(map[ctrKey]corev1.ResourceList)
	var window metav1.Duration
	var timestamp metav1.Time

	nsClient := s.externalClient.NamespacedMetrics(vpa.ID.Namespace)

	for _, resource := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		raw := s.selectorStringFor(vpa, resource)
		if raw == "" {
			continue
		}
		metricName, sel, err := annotations.ParseInstantVectorSelector(raw)
		if err != nil {
			klog.V(2).InfoS("External Metrics: invalid selector", "vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName), "resource", resource, "value", raw, "err", err)
			continue
		}

		m, err := nsClient.List(metricName, sel)
		if err != nil {
			klog.V(2).InfoS("External Metrics: query failed", "vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName), "resource", resource, "metric", metricName, "err", err)
			continue
		}
		if m == nil || len(m.Items) == 0 {
			klog.V(4).InfoS("External Metrics: no items", "vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName), "resource", resource, "metric", metricName)
			continue
		}
		klog.V(4).InfoS("External Metrics: query succeeded", "vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName), "resource", resource, "metric", metricName, "items", len(m.Items))

		timestamp = m.Items[0].Timestamp
		if m.Items[0].WindowSeconds != nil {
			window = metav1.Duration{Duration: time.Duration(*m.Items[0].WindowSeconds) * time.Second}
		}

		for _, val := range m.Items {
			podName := val.MetricLabels[s.options.PodNameLabel]
			ctrName := val.MetricLabels[s.options.ContainerNameLabel]
			if podName == "" || ctrName == "" {
				continue
			}
			key := ctrKey{pod: podName, container: ctrName}
			if usage[key] == nil {
				usage[key] = make(corev1.ResourceList)
			}
			usage[key][resource] = val.Value
		}
	}

	if len(usage) == 0 {
		return
	}
	perPod := make(map[string]*v1beta1.PodMetrics)
	for key, res := range usage {
		pm, ok := perPod[key.pod]
		if !ok {
			pm = &v1beta1.PodMetrics{
				ObjectMeta: metav1.ObjectMeta{Namespace: vpa.ID.Namespace, Name: key.pod},
				Timestamp:  timestamp,
				Window:     window,
			}
			perPod[key.pod] = pm
		}
		pm.Containers = append(pm.Containers, v1beta1.ContainerMetrics{Name: key.container, Usage: res})
	}
	for _, pm := range perPod {
		out.Items = append(out.Items, *pm)
	}
}

// selectorStringFor returns the raw instant-vector-selector string for the
// given resource, falling back from VPA annotation to the global flag.
func (s *externalMetricsClient) selectorStringFor(vpa *model.Vpa, resource corev1.ResourceName) string {
	if v := annotations.ExternalMetricForResource(vpa.Annotations, resource); v != "" {
		return v
	}
	return s.options.ResourceMetrics[resource]
}
