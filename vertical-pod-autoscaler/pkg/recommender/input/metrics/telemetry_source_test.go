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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/metrics/pkg/apis/metrics/v1beta1"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
)

type fakePodMetricsLister struct {
	listFunc func(ctx context.Context, namespace string, opts v1.ListOptions) (*v1beta1.PodMetricsList, error)
}

func (f *fakePodMetricsLister) List(ctx context.Context, namespace string, opts v1.ListOptions) (*v1beta1.PodMetricsList, error) {
	if f.listFunc != nil {
		return f.listFunc(ctx, namespace, opts)
	}
	return &v1beta1.PodMetricsList{}, nil
}

func TestTelemetryAwareSource_AllKubernetesSource(t *testing.T) {
	clusterState := model.NewClusterState(time.Hour)

	// Add VPA with default/Kubernetes telemetry
	vpa := &vpa_types.VerticalPodAutoscaler{
		ObjectMeta: v1.ObjectMeta{
			Name:      "test-vpa",
			Namespace: "default",
		},
		Spec: vpa_types.VerticalPodAutoscalerSpec{},
	}
	selector := labels.Everything()
	err := clusterState.AddOrUpdateVpa(vpa, selector, nil)
	assert.NoError(t, err)

	// Set PodCount so VPA is considered active
	clusterState.VPAs()[model.VpaID{Namespace: "default", VpaName: "test-vpa"}].PodCount = 1

	// Mock default source to return test data
	callCount := 0
	mockDefault := &fakePodMetricsLister{
		listFunc: func(ctx context.Context, namespace string, opts v1.ListOptions) (*v1beta1.PodMetricsList, error) {
			callCount++
			return &v1beta1.PodMetricsList{
				Items: []v1beta1.PodMetrics{
					{ObjectMeta: v1.ObjectMeta{Name: "test-pod", Namespace: "default"}},
				},
			}, nil
		},
	}

	source := NewTelemetryAwareSource(nil, clusterState, mockDefault)
	result, err := source.List(context.Background(), "", v1.ListOptions{})

	assert.NoError(t, err)
	assert.Equal(t, 1, callCount, "Default source should be called once")
	assert.Len(t, result.Items, 1)
}

func TestTelemetryAwareSource_PrometheusSource(t *testing.T) {
	clusterState := model.NewClusterState(time.Hour)

	// Add VPA with Prometheus telemetry
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
	selector := labels.Everything()
	err := clusterState.AddOrUpdateVpa(vpa, selector, nil)
	assert.NoError(t, err)

	// Set PodCount so VPA is considered active
	clusterState.VPAs()[model.VpaID{Namespace: "default", VpaName: "test-vpa"}].PodCount = 1

	callCount := 0
	mockDefault := &fakePodMetricsLister{
		listFunc: func(ctx context.Context, namespace string, opts v1.ListOptions) (*v1beta1.PodMetricsList, error) {
			callCount++
			return &v1beta1.PodMetricsList{}, nil
		},
	}

	source := NewTelemetryAwareSource(nil, clusterState, mockDefault)
	result, err := source.List(context.Background(), "", v1.ListOptions{})

	assert.NoError(t, err)
	assert.Equal(t, 0, callCount, "Default source should not be called for Prometheus-backed VPAs")
	// Prometheus implementation returns empty for now (TODO marker in code)
	assert.NotNil(t, result)
}

func TestTelemetryAwareSource_MixedSources(t *testing.T) {
	clusterState := model.NewClusterState(time.Hour)

	// Add VPA with Kubernetes telemetry
	vpa1 := &vpa_types.VerticalPodAutoscaler{
		ObjectMeta: v1.ObjectMeta{
			Name:      "k8s-vpa",
			Namespace: "default",
		},
		Spec: vpa_types.VerticalPodAutoscalerSpec{
			Telemetry: &vpa_types.TelemetryConfig{
				Source: vpa_types.TelemetrySourceKubernetes,
			},
		},
	}

	// Add VPA with Prometheus telemetry
	vpa2 := &vpa_types.VerticalPodAutoscaler{
		ObjectMeta: v1.ObjectMeta{
			Name:      "prom-vpa",
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

	selector := labels.Everything()
	err := clusterState.AddOrUpdateVpa(vpa1, selector, nil)
	assert.NoError(t, err)
	err = clusterState.AddOrUpdateVpa(vpa2, selector, nil)
	assert.NoError(t, err)

	// Set PodCounts so VPAs are considered active
	clusterState.VPAs()[model.VpaID{Namespace: "default", VpaName: "k8s-vpa"}].PodCount = 1
	clusterState.VPAs()[model.VpaID{Namespace: "default", VpaName: "prom-vpa"}].PodCount = 1

	callCount := 0
	mockDefault := &fakePodMetricsLister{
		listFunc: func(ctx context.Context, namespace string, opts v1.ListOptions) (*v1beta1.PodMetricsList, error) {
			callCount++
			return &v1beta1.PodMetricsList{
				Items: []v1beta1.PodMetrics{
					{ObjectMeta: v1.ObjectMeta{Name: "k8s-pod", Namespace: "default"}},
				},
			}, nil
		},
	}

	source := NewTelemetryAwareSource(nil, clusterState, mockDefault)
	result, err := source.List(context.Background(), "", v1.ListOptions{})

	assert.NoError(t, err)
	assert.Equal(t, 1, callCount, "Default source should be called for Kubernetes-backed VPA")
	assert.NotNil(t, result)
}

func TestTelemetryAwareSource_PrometheusWithoutAddress_FallbackToKubernetes(t *testing.T) {
	clusterState := model.NewClusterState(time.Hour)

	// Add VPA with Prometheus source but missing address
	vpa := &vpa_types.VerticalPodAutoscaler{
		ObjectMeta: v1.ObjectMeta{
			Name:      "test-vpa",
			Namespace: "default",
		},
		Spec: vpa_types.VerticalPodAutoscalerSpec{
			Telemetry: &vpa_types.TelemetryConfig{
				Source: vpa_types.TelemetrySourcePrometheus,
				// Prometheus config is nil - should fallback
			},
		},
	}
	selector := labels.Everything()
	err := clusterState.AddOrUpdateVpa(vpa, selector, nil)
	assert.NoError(t, err)

	// Set PodCount so VPA is considered active
	clusterState.VPAs()[model.VpaID{Namespace: "default", VpaName: "test-vpa"}].PodCount = 1

	callCount := 0
	mockDefault := &fakePodMetricsLister{
		listFunc: func(ctx context.Context, namespace string, opts v1.ListOptions) (*v1beta1.PodMetricsList, error) {
			callCount++
			return &v1beta1.PodMetricsList{}, nil
		},
	}

	source := NewTelemetryAwareSource(nil, clusterState, mockDefault)
	_, err = source.List(context.Background(), "", v1.ListOptions{})

	assert.NoError(t, err)
	assert.Equal(t, 1, callCount, "Should fallback to default source when Prometheus address missing")
}

func TestTelemetryAwareSource_GetEffectiveTelemetrySource(t *testing.T) {
	source := &TelemetryAwareSource{}

	tests := []struct {
		name     string
		vpa      *model.Vpa
		expected vpa_types.TelemetrySource
	}{
		{
			name:     "nil telemetry config",
			vpa:      &model.Vpa{Telemetry: nil},
			expected: vpa_types.TelemetrySourceAuto,
		},
		{
			name: "explicit Kubernetes",
			vpa: &model.Vpa{
				Telemetry: &vpa_types.TelemetryConfig{
					Source: vpa_types.TelemetrySourceKubernetes,
				},
			},
			expected: vpa_types.TelemetrySourceKubernetes,
		},
		{
			name: "explicit Prometheus",
			vpa: &model.Vpa{
				Telemetry: &vpa_types.TelemetryConfig{
					Source: vpa_types.TelemetrySourcePrometheus,
				},
			},
			expected: vpa_types.TelemetrySourcePrometheus,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := source.getEffectiveTelemetrySource(tc.vpa)
			assert.Equal(t, tc.expected, result)
		})
	}
}
