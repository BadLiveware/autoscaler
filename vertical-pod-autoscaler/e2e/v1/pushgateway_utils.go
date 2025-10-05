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
	"strings"
	"time"

	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2eservice "k8s.io/kubernetes/test/e2e/framework/service"
)

const (
	pushgatewayNamespace = "monitoring"
	pushgatewayService   = "pushgateway"
	pushTimeout          = 10 * time.Second
)

// PushgatewayClient provides methods to push metrics to Prometheus Pushgateway
type PushgatewayClient struct {
	clientSet clientset.Interface
}

// NewPushgatewayClient creates a new Pushgateway client
func NewPushgatewayClient(clientSet clientset.Interface) *PushgatewayClient {
	return &PushgatewayClient{
		clientSet: clientSet,
	}
}

// PushMetric pushes a single metric to Pushgateway
// The metric will be exposed with the provided labels
func (p *PushgatewayClient) PushMetric(metricName string, value float64, labels map[string]string) error {
	job := labels["job"]
	if job == "" {
		return fmt.Errorf("job label is required")
	}

	// Build the metric in Prometheus exposition format
	var metricLines []string

	// Add HELP and TYPE lines
	metricLines = append(metricLines, fmt.Sprintf("# HELP %s Fake metric for E2E testing", metricName))
	metricLines = append(metricLines, fmt.Sprintf("# TYPE %s counter", metricName))

	// Build label string
	var labelPairs []string
	for k, v := range labels {
		labelPairs = append(labelPairs, fmt.Sprintf("%s=\"%s\"", k, v))
	}
	labelString := strings.Join(labelPairs, ",")

	// Add the metric line
	metricLines = append(metricLines, fmt.Sprintf("%s{%s} %v", metricName, labelString, value))

	metricData := strings.Join(metricLines, "\n") + "\n"

	// Build Pushgateway URL path
	// Format: /metrics/job/<JOB_NAME>{/<LABEL_NAME>/<LABEL_VALUE>}
	path := fmt.Sprintf("metrics/job/%s", job)

	// Add instance label if present (special case in Pushgateway)
	if instance, ok := labels["instance"]; ok && instance != "" {
		path = fmt.Sprintf("%s/instance/%s", path, instance)
	}

	framework.Logf("Pushing metric to Pushgateway service: %s (path: %s)", pushgatewayService, path)
	framework.Logf("Metric data:\n%s", metricData)

	// Use Kubernetes API proxy to access the Pushgateway service
	ctx, cancel := context.WithTimeout(context.Background(), pushTimeout)
	defer cancel()

	proxyRequest, err := e2eservice.GetServicesProxyRequest(p.clientSet, p.clientSet.CoreV1().RESTClient().Post())
	if err != nil {
		return fmt.Errorf("failed to create proxy request: %w", err)
	}

	req := proxyRequest.Namespace(pushgatewayNamespace).
		Name(pushgatewayService + ":9091").
		Suffix(path).
		Body([]byte(metricData))

	req.SetHeader("Content-Type", "text/plain")

	result := req.Do(ctx)
	if err := result.Error(); err != nil {
		return fmt.Errorf("failed to push metric: %w", err)
	}

	body, err := result.Raw()
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	framework.Logf("Successfully pushed metric to Pushgateway: %s", string(body))
	return nil
}

// PushOOMCounter pushes a fake e2e_test_oom_events_total metric
// This simulates OOM events for testing VPA OOM detection
// We use a unique metric name to avoid conflicts with real cadvisor metrics
func (p *PushgatewayClient) PushOOMCounter(namespace, pod, container string, oomCount float64) error {
	labels := map[string]string{
		"job":       "e2e-test-oom", // Unique job name for test metrics
		"namespace": namespace,
		"pod":       pod,
		"container": container,
		"instance":  pod, // Use pod name as instance to avoid overwrites in Pushgateway
	}

	return p.PushMetric("e2e_test_oom_events_total", oomCount, labels)
}

// DeleteMetrics deletes all metrics for a job from Pushgateway
func (p *PushgatewayClient) DeleteMetrics(job string) error {
	path := fmt.Sprintf("metrics/job/%s", job)

	ctx, cancel := context.WithTimeout(context.Background(), pushTimeout)
	defer cancel()

	proxyRequest, err := e2eservice.GetServicesProxyRequest(p.clientSet, p.clientSet.CoreV1().RESTClient().Delete())
	if err != nil {
		return fmt.Errorf("failed to create proxy request: %w", err)
	}

	req := proxyRequest.Namespace(pushgatewayNamespace).
		Name(pushgatewayService + ":9091").
		Suffix(path)

	result := req.Do(ctx)
	if err := result.Error(); err != nil {
		// Ignore 404 errors - metrics might not exist
		if strings.Contains(err.Error(), "404") {
			framework.Logf("Metrics for job %s not found (already deleted or never existed)", job)
			return nil
		}
		return fmt.Errorf("failed to delete metrics: %w", err)
	}

	framework.Logf("Successfully deleted metrics for job %s", job)
	return nil
}

// WaitForPushgatewayReady waits for Pushgateway to be ready
func WaitForPushgatewayReady(clientSet clientset.Interface, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		proxyRequest, err := e2eservice.GetServicesProxyRequest(clientSet, clientSet.CoreV1().RESTClient().Get())
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}

		req := proxyRequest.Namespace(pushgatewayNamespace).
			Name(pushgatewayService + ":9091").
			Suffix("-/ready")

		result := req.Do(ctx)
		if err := result.Error(); err == nil {
			body, _ := result.Raw()
			framework.Logf("Pushgateway is ready: %s", string(body))
			return nil
		}

		time.Sleep(2 * time.Second)
	}

	return fmt.Errorf("pushgateway not ready after %v", timeout)
}
