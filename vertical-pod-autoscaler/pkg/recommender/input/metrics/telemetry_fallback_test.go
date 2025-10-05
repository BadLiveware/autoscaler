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

package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
)

func TestShouldFallback_NilVPA(t *testing.T) {
	source := &TelemetryAwareSource{}
	assert.False(t, source.shouldFallback(nil), "nil VPA should not fallback")
}

func TestShouldFallback_NilTelemetry(t *testing.T) {
	source := &TelemetryAwareSource{}
	vpa := &model.Vpa{
		Telemetry: nil,
	}
	assert.False(t, source.shouldFallback(vpa), "VPA with nil telemetry should not fallback")
}

func TestShouldFallback_NilFallbackOnFailure(t *testing.T) {
	source := &TelemetryAwareSource{}
	vpa := &model.Vpa{
		Telemetry: &vpa_types.TelemetryConfig{
			Source: vpa_types.TelemetrySourcePrometheus,
			Prometheus: &vpa_types.PrometheusTelemetry{
				Address: "http://prometheus:9090",
			},
			FallbackOnFailure: nil, // Default should be false (fail-closed)
		},
	}
	assert.False(t, source.shouldFallback(vpa), "VPA with nil FallbackOnFailure should not fallback (default=false)")
}

func TestShouldFallback_ExplicitFalse(t *testing.T) {
	source := &TelemetryAwareSource{}
	fallbackFalse := false
	vpa := &model.Vpa{
		Telemetry: &vpa_types.TelemetryConfig{
			Source: vpa_types.TelemetrySourcePrometheus,
			Prometheus: &vpa_types.PrometheusTelemetry{
				Address: "http://prometheus:9090",
			},
			FallbackOnFailure: &fallbackFalse,
		},
	}
	assert.False(t, source.shouldFallback(vpa), "VPA with FallbackOnFailure=false should not fallback")
}

func TestShouldFallback_ExplicitTrue(t *testing.T) {
	source := &TelemetryAwareSource{}
	fallbackTrue := true
	vpa := &model.Vpa{
		Telemetry: &vpa_types.TelemetryConfig{
			Source: vpa_types.TelemetrySourcePrometheus,
			Prometheus: &vpa_types.PrometheusTelemetry{
				Address: "http://prometheus:9090",
			},
			FallbackOnFailure: &fallbackTrue,
		},
	}
	assert.True(t, source.shouldFallback(vpa), "VPA with FallbackOnFailure=true should fallback")
}

func TestShouldFallback_BothVariants(t *testing.T) {
	// Verify that shouldFallback correctly handles both enabled and disabled states
	source := &TelemetryAwareSource{}

	fallbackTrue := true
	fallbackFalse := false

	vpaWithFallback := &model.Vpa{
		ID: model.VpaID{Namespace: "test-namespace", VpaName: "vpa-with-fallback"},
		Telemetry: &vpa_types.TelemetryConfig{
			Source: vpa_types.TelemetrySourcePrometheus,
			Prometheus: &vpa_types.PrometheusTelemetry{
				Address: "http://non-existent-prometheus:9090",
			},
			FallbackOnFailure: &fallbackTrue,
		},
	}

	vpaWithoutFallback := &model.Vpa{
		ID: model.VpaID{Namespace: "test-namespace", VpaName: "vpa-without-fallback"},
		Telemetry: &vpa_types.TelemetryConfig{
			Source: vpa_types.TelemetrySourcePrometheus,
			Prometheus: &vpa_types.PrometheusTelemetry{
				Address: "http://non-existent-prometheus:9090",
			},
			FallbackOnFailure: &fallbackFalse,
		},
	}

	assert.True(t, source.shouldFallback(vpaWithFallback),
		"VPA with fallback enabled should return true")
	assert.False(t, source.shouldFallback(vpaWithoutFallback),
		"VPA with fallback disabled should return false")
}
