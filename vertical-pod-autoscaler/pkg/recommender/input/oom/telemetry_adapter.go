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
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
)

// TelemetryOOMCounterProvider is the interface for getting OOM counters from telemetry sources.
// This matches the GetOOMCounters method on TelemetryAwareSource.
type TelemetryOOMCounterProvider interface {
	GetOOMCounters() map[model.ContainerID]uint64
}

// telemetryMetricsSourceAdapter adapts a telemetry source to the ExternalMetricsSource interface.
type telemetryMetricsSourceAdapter struct {
	provider TelemetryOOMCounterProvider
}

// NewTelemetryMetricsSourceAdapter creates an adapter that allows the external observer
// to use OOM counters from a telemetry-aware metrics source.
func NewTelemetryMetricsSourceAdapter(provider TelemetryOOMCounterProvider) ExternalMetricsSource {
	return &telemetryMetricsSourceAdapter{
		provider: provider,
	}
}

// GetOOMCounters retrieves OOM counters from the underlying telemetry provider.
func (a *telemetryMetricsSourceAdapter) GetOOMCounters() map[model.ContainerID]uint64 {
	return a.provider.GetOOMCounters()
}
