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
)

func TestSubstituteQueryTemplate(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		namespace string
		podRegex  string
		expected  string
	}{
		{
			name:      "basic substitution",
			query:     `rate(cpu{namespace=~"{{namespace}}", pod=~"{{pod}}"}[5m])`,
			namespace: "default",
			podRegex:  "(pod1|pod2)",
			expected:  `rate(cpu{namespace=~"default", pod=~"(pod1|pod2)"}[5m])`,
		},
		{
			name:      "multiple occurrences",
			query:     `sum(cpu{namespace=~"{{namespace}}"}) by (namespace) / count(pods{namespace=~"{{namespace}}", pod=~"{{pod}}"})`,
			namespace: "kube-system",
			podRegex:  ".*",
			expected:  `sum(cpu{namespace=~"kube-system"}) by (namespace) / count(pods{namespace=~"kube-system", pod=~".*"})`,
		},
		{
			name:      "only namespace placeholder",
			query:     `metric{namespace=~"{{namespace}}"}`,
			namespace: "monitoring",
			podRegex:  "(pod.*)",
			expected:  `metric{namespace=~"monitoring"}`,
		},
		{
			name:      "only pod placeholder",
			query:     `metric{pod=~"{{pod}}"}`,
			namespace: "default",
			podRegex:  "app-.*",
			expected:  `metric{pod=~"app-.*"}`,
		},
		{
			name:      "no placeholders",
			query:     `metric{app="my-app"}`,
			namespace: "default",
			podRegex:  ".*",
			expected:  `metric{app="my-app"}`,
		},
		{
			name:      "default CPU query",
			query:     defaultPromCPUQuery,
			namespace: "production",
			podRegex:  "(web-.*|api-.*)",
			expected:  `rate(container_cpu_usage_seconds_total{namespace=~"production",pod=~"(web-.*|api-.*)",container!="",container!="POD",image!=""}[5m])`,
		},
		{
			name:      "default memory query",
			query:     defaultPromMemoryQuery,
			namespace: "staging",
			podRegex:  "backend-.*",
			expected:  `container_memory_working_set_bytes{namespace=~"staging",pod=~"backend-.*",container!="",container!="POD",image!=""}`,
		},
		{
			name:      "default OOM query",
			query:     defaultPromOOMQuery,
			namespace: "default",
			podRegex:  "app",
			expected:  `container_oom_events_total{namespace=~"default",pod=~"app",container!="",container!="POD"}`,
		},
		{
			name:      "custom .NET OOM query",
			query:     `dotnet_gc_oom_count{namespace=~"{{namespace}}", pod=~"{{pod}}"}`,
			namespace: "dotnet-apps",
			podRegex:  "(service-a|service-b)",
			expected:  `dotnet_gc_oom_count{namespace=~"dotnet-apps", pod=~"(service-a|service-b)"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := substituteQueryTemplate(tt.query, tt.namespace, tt.podRegex)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSubstituteQueryTemplate_OrderIndependent(t *testing.T) {
	// Test that order doesn't matter with named templates
	query1 := `metric{namespace=~"{{namespace}}", pod=~"{{pod}}"}`
	query2 := `metric{pod=~"{{pod}}", namespace=~"{{namespace}}"}`

	namespace := "test-ns"
	podRegex := "pod-.*"

	result1 := substituteQueryTemplate(query1, namespace, podRegex)
	result2 := substituteQueryTemplate(query2, namespace, podRegex)

	// Both should substitute correctly regardless of order in the template
	assert.Equal(t, `metric{namespace=~"test-ns", pod=~"pod-.*"}`, result1)
	assert.Equal(t, `metric{pod=~"pod-.*", namespace=~"test-ns"}`, result2)
}

func TestSubstituteQueryTemplate_SpecialCharacters(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		podRegex  string
	}{
		{
			name:      "namespace with dashes",
			namespace: "my-app-namespace",
			podRegex:  "pod",
		},
		{
			name:      "complex regex",
			namespace: "default",
			podRegex:  "(pod-[a-z0-9]+-[a-z0-9]+)",
		},
		{
			name:      "regex with special chars",
			namespace: "test",
			podRegex:  `pod-\d+`,
		},
	}

	query := `metric{namespace=~"{{namespace}}", pod=~"{{pod}}"}`

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Should not panic or corrupt the query
			result := substituteQueryTemplate(query, tt.namespace, tt.podRegex)
			assert.Contains(t, result, tt.namespace)
			assert.Contains(t, result, tt.podRegex)
			assert.NotContains(t, result, "{{namespace}}")
			assert.NotContains(t, result, "{{pod}}")
		})
	}
}
