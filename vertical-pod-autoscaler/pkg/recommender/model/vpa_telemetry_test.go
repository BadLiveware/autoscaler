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

package model

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/labels"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
)

func TestSetTelemetryConfig_NilConfigAndDefaults(t *testing.T) {
	vpa := NewVpa(VpaID{Namespace: "test", VpaName: "test-vpa"}, labels.Nothing(), time.Now())
	vpa.SetTelemetryConfig(nil, nil)
	assert.Nil(t, vpa.Telemetry)
}

func TestSetTelemetryConfig_OnlyDefaults(t *testing.T) {
	defaults := &vpa_types.TelemetryConfig{
		Source: vpa_types.TelemetrySourceKubernetes,
	}
	vpa := NewVpa(VpaID{Namespace: "test", VpaName: "test-vpa"}, labels.Nothing(), time.Now())
	vpa.SetTelemetryConfig(nil, defaults)

	assert.NotNil(t, vpa.Telemetry)
	assert.Equal(t, vpa_types.TelemetrySourceKubernetes, vpa.Telemetry.Source)
}

func TestSetTelemetryConfig_OverrideDefaults(t *testing.T) {
	defaults := &vpa_types.TelemetryConfig{
		Source: vpa_types.TelemetrySourceKubernetes,
	}
	userConfig := &vpa_types.TelemetryConfig{
		Source: vpa_types.TelemetrySourcePrometheus,
		Prometheus: &vpa_types.PrometheusTelemetry{
			Address: "http://prometheus.svc:9090",
		},
	}

	vpa := NewVpa(VpaID{Namespace: "test", VpaName: "test-vpa"}, labels.Nothing(), time.Now())
	vpa.SetTelemetryConfig(userConfig, defaults)

	assert.NotNil(t, vpa.Telemetry)
	assert.Equal(t, vpa_types.TelemetrySourcePrometheus, vpa.Telemetry.Source)
	assert.NotNil(t, vpa.Telemetry.Prometheus)
	assert.Equal(t, "http://prometheus.svc:9090", vpa.Telemetry.Prometheus.Address)
}

func TestSetTelemetryConfig_EmptySourceDefaultsToAuto(t *testing.T) {
	userConfig := &vpa_types.TelemetryConfig{
		Prometheus: &vpa_types.PrometheusTelemetry{
			Address: "http://prometheus.svc:9090",
		},
	}

	vpa := NewVpa(VpaID{Namespace: "test", VpaName: "test-vpa"}, labels.Nothing(), time.Now())
	vpa.SetTelemetryConfig(userConfig, nil)

	assert.NotNil(t, vpa.Telemetry)
	assert.Equal(t, vpa_types.TelemetrySourceAuto, vpa.Telemetry.Source)
}

func TestSetTelemetryConfig_PrometheusWithAuth(t *testing.T) {
	userConfig := &vpa_types.TelemetryConfig{
		Source: vpa_types.TelemetrySourcePrometheus,
		Prometheus: &vpa_types.PrometheusTelemetry{
			Address: "https://prometheus.svc:9090",
			Authentication: &vpa_types.TelemetryAuth{
				BearerToken: "my-secret-token",
			},
			Insecure: true,
		},
	}

	vpa := NewVpa(VpaID{Namespace: "test", VpaName: "test-vpa"}, labels.Nothing(), time.Now())
	vpa.SetTelemetryConfig(userConfig, nil)

	assert.NotNil(t, vpa.Telemetry)
	assert.NotNil(t, vpa.Telemetry.Prometheus)
	assert.NotNil(t, vpa.Telemetry.Prometheus.Authentication)
	assert.Equal(t, "my-secret-token", vpa.Telemetry.Prometheus.Authentication.BearerToken)
	assert.True(t, vpa.Telemetry.Prometheus.Insecure)
}

func TestSetTelemetryConfig_PrometheusPartialOverlay(t *testing.T) {
	defaults := &vpa_types.TelemetryConfig{
		Source: vpa_types.TelemetrySourcePrometheus,
		Prometheus: &vpa_types.PrometheusTelemetry{
			Address: "http://default-prom:9090",
		},
	}

	// User only overrides the prometheus address, keeping default source
	userConfig := &vpa_types.TelemetryConfig{
		Prometheus: &vpa_types.PrometheusTelemetry{
			Address: "http://user-prom:9090",
		},
	}

	vpa := NewVpa(VpaID{Namespace: "test", VpaName: "test-vpa"}, labels.Nothing(), time.Now())
	vpa.SetTelemetryConfig(userConfig, defaults)

	assert.NotNil(t, vpa.Telemetry)
	assert.Equal(t, vpa_types.TelemetrySourcePrometheus, vpa.Telemetry.Source)
	assert.NotNil(t, vpa.Telemetry.Prometheus)
	assert.Equal(t, "http://user-prom:9090", vpa.Telemetry.Prometheus.Address)
}

func TestSetTelemetryConfig_DeepCopyIsolation(t *testing.T) {
	sharedDefaults := &vpa_types.TelemetryConfig{
		Source: vpa_types.TelemetrySourceKubernetes,
	}

	vpa1 := NewVpa(VpaID{Namespace: "test", VpaName: "vpa1"}, labels.Nothing(), time.Now())
	vpa2 := NewVpa(VpaID{Namespace: "test", VpaName: "vpa2"}, labels.Nothing(), time.Now())

	vpa1.SetTelemetryConfig(nil, sharedDefaults)
	vpa2.SetTelemetryConfig(nil, sharedDefaults)

	// Mutating vpa1's telemetry should not affect vpa2
	vpa1.Telemetry.Source = vpa_types.TelemetrySourcePrometheus

	assert.Equal(t, vpa_types.TelemetrySourcePrometheus, vpa1.Telemetry.Source)
	assert.Equal(t, vpa_types.TelemetrySourceKubernetes, vpa2.Telemetry.Source)
	assert.Equal(t, vpa_types.TelemetrySourceKubernetes, sharedDefaults.Source)
}
