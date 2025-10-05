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

package autoscaling

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/framework"
)

// TelemetryMetric represents a parsed Prometheus metric
type TelemetryMetric struct {
	Name   string
	Labels map[string]string
	Value  float64
}

// GetRecommenderMetrics fetches metrics from the VPA recommender pod
func GetRecommenderMetrics(f *framework.Framework) (string, error) {
	// Find the recommender pod
	pods, err := f.ClientSet.CoreV1().Pods("kube-system").List(context.TODO(), metav1.ListOptions{
		LabelSelector: "app=vpa-recommender",
	})
	if err != nil {
		return "", fmt.Errorf("failed to list recommender pods: %w", err)
	}
	if len(pods.Items) == 0 {
		return "", fmt.Errorf("no recommender pods found")
	}

	podName := pods.Items[0].Name
	namespace := "kube-system"

	// Use the Kubernetes API proxy to access the metrics endpoint
	// The recommender exposes metrics on port 8942 at /metrics
	proxyRequest := f.ClientSet.CoreV1().RESTClient().
		Get().
		Namespace(namespace).
		Resource("pods").
		Name(fmt.Sprintf("%s:8942", podName)).
		SubResource("proxy").
		Suffix("metrics")

	result := proxyRequest.Do(context.TODO())
	if result.Error() != nil {
		return "", fmt.Errorf("failed to proxy request to recommender metrics: %w", result.Error())
	}

	body, err := result.Raw()
	if err != nil {
		return "", fmt.Errorf("failed to read metrics response: %w", err)
	}

	return string(body), nil
}

// ParsePrometheusMetrics parses Prometheus text format metrics
func ParsePrometheusMetrics(metricsText string) ([]TelemetryMetric, error) {
	var metrics []TelemetryMetric

	// Regex to parse metrics: metric_name{label1="value1",label2="value2"} value
	// Also handles metrics without labels: metric_name value
	metricRegex := regexp.MustCompile(`^([a-zA-Z_:][a-zA-Z0-9_:]*)\{([^}]*)\}\s+([0-9.e+-]+)`)
	metricNoLabelsRegex := regexp.MustCompile(`^([a-zA-Z_:][a-zA-Z0-9_:]*)\s+([0-9.e+-]+)`)

	lines := strings.Split(metricsText, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)

		// Skip comments and empty lines
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Try to match metric with labels
		if matches := metricRegex.FindStringSubmatch(line); matches != nil {
			metricName := matches[1]
			labelsStr := matches[2]
			valueStr := matches[3]

			value, err := strconv.ParseFloat(valueStr, 64)
			if err != nil {
				continue // Skip invalid values
			}

			labels := parseLabels(labelsStr)
			metrics = append(metrics, TelemetryMetric{
				Name:   metricName,
				Labels: labels,
				Value:  value,
			})
		} else if matches := metricNoLabelsRegex.FindStringSubmatch(line); matches != nil {
			// Try to match metric without labels
			metricName := matches[1]
			valueStr := matches[2]

			value, err := strconv.ParseFloat(valueStr, 64)
			if err != nil {
				continue // Skip invalid values
			}

			metrics = append(metrics, TelemetryMetric{
				Name:   metricName,
				Labels: make(map[string]string),
				Value:  value,
			})
		}
	}

	return metrics, nil
}

// parseLabels parses label string like: label1="value1",label2="value2"
func parseLabels(labelsStr string) map[string]string {
	labels := make(map[string]string)
	if labelsStr == "" {
		return labels
	}

	// Split by comma, but be careful with escaped commas
	labelRegex := regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)="([^"]*)"`)
	matches := labelRegex.FindAllStringSubmatch(labelsStr, -1)

	for _, match := range matches {
		if len(match) == 3 {
			labels[match[1]] = match[2]
		}
	}

	return labels
}

// FindTelemetryMetric finds a specific telemetry metric by name and label filters
func FindTelemetryMetric(metrics []TelemetryMetric, name string, labelFilters map[string]string) *TelemetryMetric {
	for _, metric := range metrics {
		if metric.Name != name {
			continue
		}

		// Check if all label filters match
		allMatch := true
		for filterKey, filterValue := range labelFilters {
			if metric.Labels[filterKey] != filterValue {
				allMatch = false
				break
			}
		}

		if allMatch {
			return &metric
		}
	}

	return nil
}

// GetTelemetryErrorCount returns the total error count for a VPA from vpa_telemetry_errors_total
func GetTelemetryErrorCount(f *framework.Framework, vpaNamespace, vpaName string) (float64, error) {
	metricsText, err := GetRecommenderMetrics(f)
	if err != nil {
		return 0, err
	}

	metrics, err := ParsePrometheusMetrics(metricsText)
	if err != nil {
		return 0, err
	}

	metric := FindTelemetryMetric(metrics, "vpa_recommender_telemetry_errors_total", map[string]string{
		"vpa_namespace": vpaNamespace,
		"vpa_name":      vpaName,
	})

	if metric == nil {
		return 0, nil // No errors recorded yet
	}

	return metric.Value, nil
}

// GetTelemetryStatus returns the telemetry status for a VPA (0=ok, 1=failing)
func GetTelemetryStatus(f *framework.Framework, vpaNamespace, vpaName string) (float64, error) {
	metricsText, err := GetRecommenderMetrics(f)
	if err != nil {
		return -1, err
	}

	metrics, err := ParsePrometheusMetrics(metricsText)
	if err != nil {
		return -1, err
	}

	metric := FindTelemetryMetric(metrics, "vpa_recommender_telemetry_status", map[string]string{
		"vpa_namespace": vpaNamespace,
		"vpa_name":      vpaName,
	})

	if metric == nil {
		return -1, fmt.Errorf("telemetry_status metric not found for VPA %s/%s", vpaNamespace, vpaName)
	}

	return metric.Value, nil
}
