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
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// TelemetrySourceType represents the type of telemetry backend
type TelemetrySourceType string

const (
	// TelemetrySourcePrometheus indicates Prometheus as the telemetry backend
	TelemetrySourcePrometheus TelemetrySourceType = "prometheus"
	// TelemetrySourceKubernetes indicates Kubernetes metrics-server as the telemetry backend
	TelemetrySourceKubernetes TelemetrySourceType = "kubernetes"
)

// String returns the string representation of the telemetry source
func (t TelemetrySourceType) String() string {
	return string(t)
}

var (
	// TelemetryErrorsTotal counts telemetry fetch errors by VPA, source, and reason
	TelemetryErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "vpa",
			Subsystem: "recommender",
			Name:      "telemetry_errors_total",
			Help:      "Total number of telemetry fetch errors",
		},
		[]string{"vpa_namespace", "vpa_name", "telemetry_source", "reason"},
	)

	// TelemetryStatus indicates current telemetry health (0=ok, 1=failing)
	TelemetryStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "vpa",
			Subsystem: "recommender",
			Name:      "telemetry_status",
			Help:      "Current telemetry status (0=ok, 1=failing)",
		},
		[]string{"vpa_namespace", "vpa_name", "telemetry_source"},
	)

	// TelemetryQueryDuration tracks query latency by VPA, source, and query type
	TelemetryQueryDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "vpa",
			Subsystem: "recommender",
			Name:      "telemetry_query_duration_seconds",
			Help:      "Duration of telemetry queries in seconds",
			Buckets:   prometheus.DefBuckets, // 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10
		},
		[]string{"vpa_namespace", "vpa_name", "telemetry_source", "query_type"},
	)
)

func init() {
	prometheus.MustRegister(TelemetryErrorsTotal)
	prometheus.MustRegister(TelemetryStatus)
	prometheus.MustRegister(TelemetryQueryDuration)
}

// ClassifyTelemetryError categorizes errors for metrics labeling
func ClassifyTelemetryError(err error) string {
	if err == nil {
		return "unknown"
	}

	// Check for context errors (timeout/cancellation)
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}

	// Check for network errors
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return "connection_timeout"
		}
		return "connection_failed"
	}

	// Check for DNS errors
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return "dns_failed"
	}

	// Check for URL errors
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		if urlErr.Timeout() {
			return "connection_timeout"
		}
		return "connection_failed"
	}

	// Prometheus API errors are wrapped, so check the error message for common patterns
	// The promapi.Error type and its constants are not exported in a usable way

	// Check error message for common patterns
	errMsg := strings.ToLower(err.Error())
	if strings.Contains(errMsg, "authentication") || strings.Contains(errMsg, "unauthorized") || strings.Contains(errMsg, "401") {
		return "auth_failed"
	}
	if strings.Contains(errMsg, "403") || strings.Contains(errMsg, "forbidden") {
		return "auth_failed"
	}
	if strings.Contains(errMsg, "timeout") {
		return "timeout"
	}
	if strings.Contains(errMsg, "connection") || strings.Contains(errMsg, "dial") {
		return "connection_failed"
	}
	if strings.Contains(errMsg, "no such host") || strings.Contains(errMsg, "dns") {
		return "dns_failed"
	}
	if strings.Contains(errMsg, "parse error") || strings.Contains(errMsg, "invalid") {
		return "query_error"
	}

	return "unknown"
}

// RecordTelemetryError increments error counter and sets status to failing
func RecordTelemetryError(vpaNamespace, vpaName string, source TelemetrySourceType, err error) {
	reason := ClassifyTelemetryError(err)
	TelemetryErrorsTotal.WithLabelValues(vpaNamespace, vpaName, source.String(), reason).Inc()
	TelemetryStatus.WithLabelValues(vpaNamespace, vpaName, source.String()).Set(1) // 1 = failing
}

// RecordTelemetrySuccess sets status to ok
func RecordTelemetrySuccess(vpaNamespace, vpaName string, source TelemetrySourceType) {
	TelemetryStatus.WithLabelValues(vpaNamespace, vpaName, source.String()).Set(0) // 0 = ok
}

// RecordQueryDuration records the duration of a telemetry query
func RecordQueryDuration(vpaNamespace, vpaName string, source TelemetrySourceType, queryType string, duration time.Duration) {
	TelemetryQueryDuration.WithLabelValues(vpaNamespace, vpaName, source.String(), queryType).Observe(duration.Seconds())
}

// FormatTelemetryConditionMessage creates a user-friendly error message for the VPA condition
func FormatTelemetryConditionMessage(source TelemetrySourceType, address string, err error) string {
	reason := ClassifyTelemetryError(err)

	switch reason {
	case "timeout", "connection_timeout":
		return fmt.Sprintf("Telemetry source '%s' at %s is unreachable: connection timeout", source, address)
	case "connection_failed":
		return fmt.Sprintf("Telemetry source '%s' at %s is unreachable: connection failed", source, address)
	case "dns_failed":
		return fmt.Sprintf("Telemetry source '%s' at %s is unreachable: DNS resolution failed", source, address)
	case "auth_failed":
		return fmt.Sprintf("Telemetry source '%s' at %s authentication failed", source, address)
	case "query_error":
		return fmt.Sprintf("Telemetry source '%s' at %s query failed: %v", source, address, err)
	case "server_error":
		return fmt.Sprintf("Telemetry source '%s' at %s returned server error: %v", source, address, err)
	case "invalid_response":
		return fmt.Sprintf("Telemetry source '%s' at %s returned invalid response: %v", source, address, err)
	default:
		return fmt.Sprintf("Telemetry source '%s' at %s failed: %v", source, address, err)
	}
}
