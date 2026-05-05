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

package annotations

import (
	corev1 "k8s.io/api/core/v1"
)

const (
	// ExternalMetricsAnnotationPrefix is the namespace for per-VPA external
	// metric source overrides. Pick a domain at fork time.
	ExternalMetricsAnnotationPrefix = "external.vpa.k8s.io/"

	// ExternalCPUMetricAnnotation, when set on a VPA, names the
	// external.metrics.k8s.io metric used as CPU usage for that VPA's pods.
	ExternalCPUMetricAnnotation = ExternalMetricsAnnotationPrefix + "cpu-metric"

	// ExternalMemoryMetricAnnotation, when set on a VPA, names the
	// external.metrics.k8s.io metric used as memory usage for that VPA's pods.
	ExternalMemoryMetricAnnotation = ExternalMetricsAnnotationPrefix + "memory-metric"

	// OOMCounterMetricAnnotation, when set on a VPA, names a Prometheus
	// counter metric whose increases are treated as OOM events for the VPA's
	// pods. Use case: .NET OutOfMemoryException, where the runtime catches
	// the failure and the container does not OOMKill.
	OOMCounterMetricAnnotation = ExternalMetricsAnnotationPrefix + "oom-counter-metric"

	// HistoryQueryCPUAnnotation, when set on a VPA, is a PromQL query used
	// for one-shot CPU history backfill on the VPA's first observation.
	HistoryQueryCPUAnnotation = ExternalMetricsAnnotationPrefix + "history-query-cpu"

	// HistoryQueryMemoryAnnotation, when set on a VPA, is a PromQL query used
	// for one-shot memory history backfill on the VPA's first observation.
	HistoryQueryMemoryAnnotation = ExternalMetricsAnnotationPrefix + "history-query-memory"
)

// ExternalMetricForResource returns the per-VPA external-metrics metric name
// for the given resource, or "" if no annotation is set. Only ResourceCPU and
// ResourceMemory are recognized.
func ExternalMetricForResource(annotations map[string]string, resource corev1.ResourceName) string {
	if annotations == nil {
		return ""
	}
	switch resource {
	case corev1.ResourceCPU:
		return annotations[ExternalCPUMetricAnnotation]
	case corev1.ResourceMemory:
		return annotations[ExternalMemoryMetricAnnotation]
	}
	return ""
}

// HasExternalMetricOverride reports whether a VPA opts into per-VPA external
// metrics for at least one resource.
func HasExternalMetricOverride(annotations map[string]string) bool {
	if annotations == nil {
		return false
	}
	return annotations[ExternalCPUMetricAnnotation] != "" ||
		annotations[ExternalMemoryMetricAnnotation] != ""
}

// OOMCounterMetric returns the per-VPA Prometheus OOM counter metric name, or
// "" if not set.
func OOMCounterMetric(annotations map[string]string) string {
	if annotations == nil {
		return ""
	}
	return annotations[OOMCounterMetricAnnotation]
}

// HistoryQueryForResource returns the per-VPA PromQL backfill query for the
// given resource, or "" if not set. Only ResourceCPU and ResourceMemory are
// recognized.
func HistoryQueryForResource(annotations map[string]string, resource corev1.ResourceName) string {
	if annotations == nil {
		return ""
	}
	switch resource {
	case corev1.ResourceCPU:
		return annotations[HistoryQueryCPUAnnotation]
	case corev1.ResourceMemory:
		return annotations[HistoryQueryMemoryAnnotation]
	}
	return ""
}

// HasHistoryQuery reports whether a VPA opts into per-VPA history backfill
// for at least one resource.
func HasHistoryQuery(annotations map[string]string) bool {
	if annotations == nil {
		return false
	}
	return annotations[HistoryQueryCPUAnnotation] != "" ||
		annotations[HistoryQueryMemoryAnnotation] != ""
}
