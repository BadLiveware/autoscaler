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
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
)

func TestValidateTelemetryConfig_NilTelemetry(t *testing.T) {
	feeder := &clusterStateFeeder{}
	vpa := &vpa_types.VerticalPodAutoscaler{
		ObjectMeta: v1.ObjectMeta{
			Name:      "test-vpa",
			Namespace: "default",
		},
		Spec: vpa_types.VerticalPodAutoscalerSpec{
			Telemetry: nil,
		},
	}

	condition := feeder.validateTelemetryConfig(vpa)
	assert.Nil(t, condition)
}

func TestValidateTelemetryConfig_KubernetesSource(t *testing.T) {
	feeder := &clusterStateFeeder{}
	vpa := &vpa_types.VerticalPodAutoscaler{
		ObjectMeta: v1.ObjectMeta{
			Name:      "test-vpa",
			Namespace: "default",
		},
		Spec: vpa_types.VerticalPodAutoscalerSpec{
			Telemetry: &vpa_types.TelemetryConfig{
				Source: vpa_types.TelemetrySourceKubernetes,
			},
		},
	}

	condition := feeder.validateTelemetryConfig(vpa)
	assert.Nil(t, condition, "Kubernetes source should not require additional config")
}

func TestValidateTelemetryConfig_PrometheusWithAddress(t *testing.T) {
	feeder := &clusterStateFeeder{}
	vpa := &vpa_types.VerticalPodAutoscaler{
		ObjectMeta: v1.ObjectMeta{
			Name:      "test-vpa",
			Namespace: "default",
		},
		Spec: vpa_types.VerticalPodAutoscalerSpec{
			Telemetry: &vpa_types.TelemetryConfig{
				Source: vpa_types.TelemetrySourcePrometheus,
				Prometheus: &vpa_types.PrometheusTelemetry{
					Address: "http://prometheus.svc:9090",
				},
			},
		},
	}

	condition := feeder.validateTelemetryConfig(vpa)
	assert.Nil(t, condition, "Valid Prometheus config should pass validation")
}

func TestValidateTelemetryConfig_PrometheusWithoutAddress(t *testing.T) {
	feeder := &clusterStateFeeder{}
	vpa := &vpa_types.VerticalPodAutoscaler{
		ObjectMeta: v1.ObjectMeta{
			Name:      "test-vpa",
			Namespace: "default",
		},
		Spec: vpa_types.VerticalPodAutoscalerSpec{
			Telemetry: &vpa_types.TelemetryConfig{
				Source:     vpa_types.TelemetrySourcePrometheus,
				Prometheus: nil,
			},
		},
	}

	condition := feeder.validateTelemetryConfig(vpa)
	assert.NotNil(t, condition, "Prometheus source without config should fail validation")
	assert.Equal(t, vpa_types.ConfigUnsupported, condition.conditionType)
	assert.Contains(t, condition.message, "prometheus.address is missing")
}

func TestValidateTelemetryConfig_PrometheusWithEmptyAddress(t *testing.T) {
	feeder := &clusterStateFeeder{}
	vpa := &vpa_types.VerticalPodAutoscaler{
		ObjectMeta: v1.ObjectMeta{
			Name:      "test-vpa",
			Namespace: "default",
		},
		Spec: vpa_types.VerticalPodAutoscalerSpec{
			Telemetry: &vpa_types.TelemetryConfig{
				Source: vpa_types.TelemetrySourcePrometheus,
				Prometheus: &vpa_types.PrometheusTelemetry{
					Address: "",
				},
			},
		},
	}

	condition := feeder.validateTelemetryConfig(vpa)
	assert.NotNil(t, condition, "Empty Prometheus address should fail validation")
	assert.Equal(t, vpa_types.ConfigUnsupported, condition.conditionType)
}

func TestValidateTelemetryConfig_AutoSource(t *testing.T) {
	feeder := &clusterStateFeeder{}
	vpa := &vpa_types.VerticalPodAutoscaler{
		ObjectMeta: v1.ObjectMeta{
			Name:      "test-vpa",
			Namespace: "default",
		},
		Spec: vpa_types.VerticalPodAutoscalerSpec{
			Telemetry: &vpa_types.TelemetryConfig{
				Source: vpa_types.TelemetrySourceAuto,
			},
		},
	}

	condition := feeder.validateTelemetryConfig(vpa)
	assert.Nil(t, condition, "Auto source should not require validation")
}
