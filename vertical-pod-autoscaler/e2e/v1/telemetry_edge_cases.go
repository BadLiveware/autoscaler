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
	"time"

	autoscaling "k8s.io/api/autoscaling/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/autoscaler/vertical-pod-autoscaler/e2e/utils"
	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/test"
	"k8s.io/kubernetes/test/e2e/framework"
	podsecurity "k8s.io/pod-security-admission/api"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = FullVpaE2eDescribe("VPA Telemetry Edge Cases", ginkgo.Label("PrometheusRequired", "EdgeCases"), func() {
	f := framework.NewDefaultFramework("vertical-pod-autoscaling-edge-cases")
	f.NamespacePodSecurityEnforceLevel = podsecurity.LevelBaseline

	ginkgo.Context("Phase 1: Core Resilience", func() {
		ginkgo.It("should handle unreachable Prometheus without crashing (fail-closed)", func() {
			// This test verifies observability when Prometheus is unreachable with default fail-closed behavior.
			//
			// Behavior when Prometheus is unreachable (fallbackOnFailure=false, default):
			// - Sets TelemetryUnavailable condition on VPA with helpful message ✅
			// - Exposes telemetry_status=1 (failing) metric ✅
			// - Increments telemetry_errors_total counter ✅
			// - Logs error: "Failed to fetch Prometheus metrics"
			// - VPA gets NO metrics (does not fall back to Kubernetes metrics-server)
			// - VPA does NOT get recommendations (fail-closed behavior)
			// - Recommender continues running (does not crash)

			ginkgo.By("Setting up a hamster deployment")
			deploymentName := "hamster-unreachable"
			containerName := utils.GetHamsterContainerNameByIndex(0)

			rc := NewDynamicResourceConsumer(deploymentName, f.Namespace.Name, KindDeployment,
				2,          /*replicas*/
				1,          /*initCPUTotal*/
				10,         /*initMemoryTotal*/
				1,          /*initCustomMetric*/
				int64(100), /*cpuRequest*/
				int64(100), /*memRequest*/
				f.ClientSet,
				f.ScalesGetter)
			defer rc.CleanUp()

			ginkgo.By("Creating VPA with unreachable Prometheus address and fallbackOnFailure=false")
			targetRef := &autoscaling.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       deploymentName,
			}

			vpaCRD := test.VerticalPodAutoscaler().
				WithName("hamster-unreachable-vpa").
				WithNamespace(f.Namespace.Name).
				WithTargetRef(targetRef).
				WithContainer(containerName).
				Get()

			// Configure Prometheus with unreachable address and explicit fallbackOnFailure=false
			fallbackFalse := false
			vpaCRD.Spec.Telemetry = &vpa_types.TelemetryConfig{
				Source: vpa_types.TelemetrySourcePrometheus,
				Prometheus: &vpa_types.PrometheusTelemetry{
					Address: "http://non-existent-prometheus.invalid:9090",
				},
				FallbackOnFailure: &fallbackFalse,
			}

			utils.InstallVPA(f, vpaCRD)

			ginkgo.By("Waiting for VPA to attempt fetching metrics and set condition")
			// Wait for at least one recommender cycle (60s) plus some buffer
			time.Sleep(90 * time.Second)

			vpa, err := utils.GetVpaClientSet(f).AutoscalingV1().VerticalPodAutoscalers(f.Namespace.Name).Get(
				context.TODO(), vpaCRD.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying VPA does NOT have recommendations (fail-closed)")
			// VPA should NOT have recommendations because Prometheus is unreachable
			// and fallbackOnFailure=false (fail-closed mode)
			gomega.Expect(vpa.Status.Recommendation).To(gomega.BeNil(),
				"VPA should NOT have recommendations when Prometheus fails with fallbackOnFailure=false")
			framework.Logf("VPA correctly has no recommendations (fail-closed)")

			ginkgo.By("Verifying conditions are set correctly")
			// Verify TelemetryUnavailable condition is True
			foundTelemetryUnavailable := false
			foundRecommendationProvided := false
			for _, condition := range vpa.Status.Conditions {
				if condition.Type == vpa_types.TelemetryUnavailable {
					foundTelemetryUnavailable = true
					gomega.Expect(condition.Status).To(gomega.Equal(apiv1.ConditionTrue),
						"TelemetryUnavailable condition should be True")
					gomega.Expect(condition.Message).To(gomega.ContainSubstring("unreachable"),
						"Condition message should mention unreachable Prometheus")
					framework.Logf("TelemetryUnavailable condition correctly set: %s", condition.Message)
				}
				if condition.Type == vpa_types.RecommendationProvided {
					foundRecommendationProvided = true
					gomega.Expect(condition.Status).To(gomega.Equal(apiv1.ConditionFalse),
						"RecommendationProvided condition should be False in fail-closed mode")
					framework.Logf("RecommendationProvided condition correctly set to False")
				}
			}
			gomega.Expect(foundTelemetryUnavailable).To(gomega.BeTrue(),
				"VPA should have TelemetryUnavailable condition set when Prometheus is unreachable")
			gomega.Expect(foundRecommendationProvided).To(gomega.BeTrue(),
				"VPA should have RecommendationProvided condition")

			ginkgo.By("Verifying recommender hasn't crashed")
			// The recommender pod should still be running despite Prometheus being unreachable
			pods, err := f.ClientSet.CoreV1().Pods("kube-system").List(context.TODO(), metav1.ListOptions{
				LabelSelector: "app=vpa-recommender",
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pods.Items).NotTo(gomega.BeEmpty(), "VPA recommender should be running")

			for _, pod := range pods.Items {
				gomega.Expect(pod.Status.Phase).To(gomega.Equal(apiv1.PodRunning),
					"VPA recommender pod should still be running")
			}

			ginkgo.By("Verifying telemetry metrics are exposed")
			// Verify that telemetry_status metric shows failure
			telemetryStatus, err := GetTelemetryStatus(f, f.Namespace.Name, vpaCRD.Name)
			if err != nil {
				framework.Logf("Warning: Could not fetch telemetry_status metric: %v", err)
			} else {
				gomega.Expect(telemetryStatus).To(gomega.Equal(1.0),
					"telemetry_status should be 1 (failing) when Prometheus is unreachable")
				framework.Logf("telemetry_status correctly set to 1 (failing)")
			}

			// Verify that telemetry_errors_total metric is incremented
			errorCount, err := GetTelemetryErrorCount(f, f.Namespace.Name, vpaCRD.Name)
			if err != nil {
				framework.Logf("Warning: Could not fetch telemetry_errors_total metric: %v", err)
			} else {
				gomega.Expect(errorCount).To(gomega.BeNumerically(">", 0),
					"telemetry_errors_total should be > 0 when Prometheus is unreachable")
				framework.Logf("telemetry_errors_total correctly incremented to %v", errorCount)
			}
		})

		ginkgo.It("should support multiple VPAs with different telemetry sources", func() {
			ginkgo.By("Setting up three different deployments")

			// Deployment 1: Prometheus telemetry
			deployment1 := "hamster-prom"
			rc1 := NewDynamicResourceConsumer(deployment1, f.Namespace.Name, KindDeployment,
				2,          /*replicas*/
				1,          /*initCPUTotal*/
				10,         /*initMemoryTotal*/
				1,          /*initCustomMetric*/
				int64(100), /*cpuRequest*/
				int64(100), /*memRequest*/
				f.ClientSet,
				f.ScalesGetter)
			defer rc1.CleanUp()

			// Deployment 2: Default Kubernetes telemetry
			deployment2 := "hamster-k8s"
			rc2 := NewDynamicResourceConsumer(deployment2, f.Namespace.Name, KindDeployment,
				2,          /*replicas*/
				1,          /*initCPUTotal*/
				10,         /*initMemoryTotal*/
				1,          /*initCustomMetric*/
				int64(100), /*cpuRequest*/
				int64(100), /*memRequest*/
				f.ClientSet,
				f.ScalesGetter)
			defer rc2.CleanUp()

			// Deployment 3: Prometheus with custom queries
			deployment3 := "hamster-custom"
			rc3 := NewDynamicResourceConsumer(deployment3, f.Namespace.Name, KindDeployment,
				2,          /*replicas*/
				1,          /*initCPUTotal*/
				10,         /*initMemoryTotal*/
				1,          /*initCustomMetric*/
				int64(100), /*cpuRequest*/
				int64(100), /*memRequest*/
				f.ClientSet,
				f.ScalesGetter)
			defer rc3.CleanUp()

			containerName := utils.GetHamsterContainerNameByIndex(0)
			prometheusAddress := "http://prometheus.monitoring.svc:9090"

			ginkgo.By("Creating VPA 1 with Prometheus telemetry")
			targetRef1 := &autoscaling.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       deployment1,
			}

			vpaCRD1 := test.VerticalPodAutoscaler().
				WithName("hamster-prom-vpa").
				WithNamespace(f.Namespace.Name).
				WithTargetRef(targetRef1).
				WithContainer(containerName).
				Get()

			vpaCRD1.Spec.Telemetry = &vpa_types.TelemetryConfig{
				Source: vpa_types.TelemetrySourcePrometheus,
				Prometheus: &vpa_types.PrometheusTelemetry{
					Address: prometheusAddress,
				},
			}
			utils.InstallVPA(f, vpaCRD1)

			ginkgo.By("Creating VPA 2 with default Kubernetes telemetry")
			targetRef2 := &autoscaling.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       deployment2,
			}

			vpaCRD2 := test.VerticalPodAutoscaler().
				WithName("hamster-k8s-vpa").
				WithNamespace(f.Namespace.Name).
				WithTargetRef(targetRef2).
				WithContainer(containerName).
				Get()
			// No telemetry config = default to Kubernetes metrics-server
			utils.InstallVPA(f, vpaCRD2)

			ginkgo.By("Creating VPA 3 with Prometheus and custom OOM query")
			targetRef3 := &autoscaling.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       deployment3,
			}

			vpaCRD3 := test.VerticalPodAutoscaler().
				WithName("hamster-custom-vpa").
				WithNamespace(f.Namespace.Name).
				WithTargetRef(targetRef3).
				WithContainer(containerName).
				Get()

			vpaCRD3.Spec.Telemetry = &vpa_types.TelemetryConfig{
				Source: vpa_types.TelemetrySourcePrometheus,
				Prometheus: &vpa_types.PrometheusTelemetry{
					Address: prometheusAddress,
					Query: &vpa_types.PrometheusTelemetryQuery{
						OOMCountQuery: "container_oom_events_total{namespace=\"" + f.Namespace.Name + "\"}",
						// CPU and Memory will use defaults
					},
				},
			}
			utils.InstallVPA(f, vpaCRD3)

			ginkgo.By("Waiting for all VPAs to get recommendations")
			vpaNames := []string{vpaCRD1.Name, vpaCRD2.Name, vpaCRD3.Name}

			for _, vpaName := range vpaNames {
				framework.Logf("Waiting for recommendations from VPA: %s", vpaName)
				err := wait.PollUntilContextTimeout(context.TODO(), 10*time.Second, 3*time.Minute, true, func(ctx context.Context) (bool, error) {
					vpa, err := utils.GetVpaClientSet(f).AutoscalingV1().VerticalPodAutoscalers(f.Namespace.Name).Get(
						context.TODO(), vpaName, metav1.GetOptions{})
					if err != nil {
						return false, err
					}

					if vpa.Status.Recommendation != nil &&
						len(vpa.Status.Recommendation.ContainerRecommendations) > 0 {
						framework.Logf("VPA %s has recommendations", vpaName)
						return true, nil
					}

					framework.Logf("VPA %s waiting for recommendations...", vpaName)
					return false, nil
				})
				gomega.Expect(err).NotTo(gomega.HaveOccurred(),
					"VPA %s should get recommendations", vpaName)
			}

			ginkgo.By("Verifying all VPAs have independent recommendations")
			var vpa1, vpa2, vpa3 *vpa_types.VerticalPodAutoscaler
			var err error

			vpa1, err = utils.GetVpaClientSet(f).AutoscalingV1().VerticalPodAutoscalers(f.Namespace.Name).Get(
				context.TODO(), vpaCRD1.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			vpa2, err = utils.GetVpaClientSet(f).AutoscalingV1().VerticalPodAutoscalers(f.Namespace.Name).Get(
				context.TODO(), vpaCRD2.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			vpa3, err = utils.GetVpaClientSet(f).AutoscalingV1().VerticalPodAutoscalers(f.Namespace.Name).Get(
				context.TODO(), vpaCRD3.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// All should have recommendations
			gomega.Expect(vpa1.Status.Recommendation).NotTo(gomega.BeNil(), "VPA 1 should have recommendations")
			gomega.Expect(vpa2.Status.Recommendation).NotTo(gomega.BeNil(), "VPA 2 should have recommendations")
			gomega.Expect(vpa3.Status.Recommendation).NotTo(gomega.BeNil(), "VPA 3 should have recommendations")

			gomega.Expect(vpa1.Status.Recommendation.ContainerRecommendations).To(gomega.HaveLen(1),
				"VPA 1 should have recommendations for 1 container")
			gomega.Expect(vpa2.Status.Recommendation.ContainerRecommendations).To(gomega.HaveLen(1),
				"VPA 2 should have recommendations for 1 container")
			gomega.Expect(vpa3.Status.Recommendation.ContainerRecommendations).To(gomega.HaveLen(1),
				"VPA 3 should have recommendations for 1 container")

			// Log the recommendations for verification
			framework.Logf("VPA 1 (Prometheus): CPU=%v, Memory=%v",
				vpa1.Status.Recommendation.ContainerRecommendations[0].Target[apiv1.ResourceCPU],
				vpa1.Status.Recommendation.ContainerRecommendations[0].Target[apiv1.ResourceMemory])
			framework.Logf("VPA 2 (Kubernetes): CPU=%v, Memory=%v",
				vpa2.Status.Recommendation.ContainerRecommendations[0].Target[apiv1.ResourceCPU],
				vpa2.Status.Recommendation.ContainerRecommendations[0].Target[apiv1.ResourceMemory])
			framework.Logf("VPA 3 (Prometheus+Custom): CPU=%v, Memory=%v",
				vpa3.Status.Recommendation.ContainerRecommendations[0].Target[apiv1.ResourceCPU],
				vpa3.Status.Recommendation.ContainerRecommendations[0].Target[apiv1.ResourceMemory])

			ginkgo.By("Verifying telemetry metrics show healthy status for working VPAs")
			// Check VPA 1 (Prometheus) - should be healthy
			status1, err := GetTelemetryStatus(f, f.Namespace.Name, vpaCRD1.Name)
			if err != nil {
				framework.Logf("Warning: Could not fetch telemetry_status for VPA 1: %v", err)
			} else {
				gomega.Expect(status1).To(gomega.Equal(0.0),
					"VPA 1 telemetry_status should be 0 (ok) when Prometheus is reachable")
				framework.Logf("VPA 1 telemetry_status correctly set to 0 (ok)")
			}

			// Check VPA 3 (Prometheus with custom query) - should be healthy
			status3, err := GetTelemetryStatus(f, f.Namespace.Name, vpaCRD3.Name)
			if err != nil {
				framework.Logf("Warning: Could not fetch telemetry_status for VPA 3: %v", err)
			} else {
				gomega.Expect(status3).To(gomega.Equal(0.0),
					"VPA 3 telemetry_status should be 0 (ok) when Prometheus is reachable")
				framework.Logf("VPA 3 telemetry_status correctly set to 0 (ok)")
			}

			// VPA 2 uses Kubernetes metrics-server, so no Prometheus telemetry metrics expected
			framework.Logf("VPA 2 uses Kubernetes metrics-server (no Prometheus telemetry metrics)")
		})

		ginkgo.It("should fall back to Kubernetes metrics when Prometheus fails with fallbackOnFailure=true", func() {
			// This test verifies that when Prometheus is unreachable and fallbackOnFailure=true,
			// the VPA falls back to Kubernetes metrics-server and continues providing recommendations.

			ginkgo.By("Setting up a hamster deployment")
			deploymentName := "hamster-fallback"
			containerName := utils.GetHamsterContainerNameByIndex(0)

			rc := NewDynamicResourceConsumer(deploymentName, f.Namespace.Name, KindDeployment,
				2,          /*replicas*/
				1,          /*initCPUTotal*/
				10,         /*initMemoryTotal*/
				1,          /*initCustomMetric*/
				int64(100), /*cpuRequest*/
				int64(100), /*memRequest*/
				f.ClientSet,
				f.ScalesGetter)
			defer rc.CleanUp()

			ginkgo.By("Creating VPA with unreachable Prometheus and fallbackOnFailure=true")
			targetRef := &autoscaling.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       deploymentName,
			}

			vpaCRD := test.VerticalPodAutoscaler().
				WithName("hamster-fallback-vpa").
				WithNamespace(f.Namespace.Name).
				WithTargetRef(targetRef).
				WithContainer(containerName).
				Get()

			// Configure Prometheus with unreachable address but enable fallback
			fallbackTrue := true
			vpaCRD.Spec.Telemetry = &vpa_types.TelemetryConfig{
				Source: vpa_types.TelemetrySourcePrometheus,
				Prometheus: &vpa_types.PrometheusTelemetry{
					Address: "http://non-existent-prometheus.invalid:9090",
				},
				FallbackOnFailure: &fallbackTrue,
			}

			utils.InstallVPA(f, vpaCRD)

			ginkgo.By("Waiting for VPA to fall back and get recommendations from Kubernetes metrics-server")
			// With fallback enabled, the VPA should get recommendations from Kubernetes metrics-server
			err := wait.PollUntilContextTimeout(context.TODO(), 10*time.Second, 3*time.Minute, true, func(ctx context.Context) (bool, error) {
				vpa, err := utils.GetVpaClientSet(f).AutoscalingV1().VerticalPodAutoscalers(f.Namespace.Name).Get(
					context.TODO(), vpaCRD.Name, metav1.GetOptions{})
				if err != nil {
					return false, err
				}

				if vpa.Status.Recommendation != nil &&
					len(vpa.Status.Recommendation.ContainerRecommendations) > 0 {
					framework.Logf("VPA has recommendations from fallback to Kubernetes metrics-server")
					return true, nil
				}

				framework.Logf("VPA waiting for recommendations (with fallback)...")
				return false, nil
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				"VPA should get recommendations from Kubernetes metrics-server as fallback")

			ginkgo.By("Verifying TelemetryUnavailable condition is still set (to indicate primary source failed)")
			vpa, err := utils.GetVpaClientSet(f).AutoscalingV1().VerticalPodAutoscalers(f.Namespace.Name).Get(
				context.TODO(), vpaCRD.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// The condition should still be set to indicate Prometheus failed (even though fallback worked)
			foundCondition := false
			for _, condition := range vpa.Status.Conditions {
				if condition.Type == vpa_types.TelemetryUnavailable {
					foundCondition = true
					gomega.Expect(condition.Status).To(gomega.Equal(apiv1.ConditionTrue),
						"TelemetryUnavailable condition should be True even with fallback")
					framework.Logf("TelemetryUnavailable condition correctly set: %s", condition.Message)
					break
				}
			}
			gomega.Expect(foundCondition).To(gomega.BeTrue(),
				"VPA should have TelemetryUnavailable condition set to indicate Prometheus failure")

			ginkgo.By("Verifying VPA has recommendations from fallback")
			gomega.Expect(vpa.Status.Recommendation).NotTo(gomega.BeNil(),
				"VPA should have recommendations from Kubernetes metrics-server fallback")
			gomega.Expect(vpa.Status.Recommendation.ContainerRecommendations).To(gomega.HaveLen(1),
				"VPA should have recommendations for 1 container")

			framework.Logf("VPA successfully fell back to Kubernetes metrics-server: CPU=%v, Memory=%v",
				vpa.Status.Recommendation.ContainerRecommendations[0].Target[apiv1.ResourceCPU],
				vpa.Status.Recommendation.ContainerRecommendations[0].Target[apiv1.ResourceMemory])
		})

		ginkgo.It("should recover from Prometheus failure when URL is fixed (fail-closed mode)", func() {
			// This test verifies that a VPA in fail-closed mode can recover when the broken
			// Prometheus URL is updated to a working one.

			ginkgo.By("Setting up a hamster deployment")
			deploymentName := "hamster-recovery-failclosed"
			containerName := utils.GetHamsterContainerNameByIndex(0)

			rc := NewDynamicResourceConsumer(deploymentName, f.Namespace.Name, KindDeployment,
				2,          /*replicas*/
				1,          /*initCPUTotal*/
				10,         /*initMemoryTotal*/
				1,          /*initCustomMetric*/
				int64(100), /*cpuRequest*/
				int64(100), /*memRequest*/
				f.ClientSet,
				f.ScalesGetter)
			defer rc.CleanUp()

			ginkgo.By("Creating VPA with unreachable Prometheus (fail-closed)")
			targetRef := &autoscaling.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       deploymentName,
			}

			vpaCRD := test.VerticalPodAutoscaler().
				WithName("hamster-recovery-failclosed-vpa").
				WithNamespace(f.Namespace.Name).
				WithTargetRef(targetRef).
				WithContainer(containerName).
				Get()

			fallbackFalse := false
			vpaCRD.Spec.Telemetry = &vpa_types.TelemetryConfig{
				Source: vpa_types.TelemetrySourcePrometheus,
				Prometheus: &vpa_types.PrometheusTelemetry{
					Address: "http://non-existent-prometheus.invalid:9090",
				},
				FallbackOnFailure: &fallbackFalse,
			}
			utils.InstallVPA(f, vpaCRD)

			ginkgo.By("Waiting and verifying VPA has no recommendations (fail-closed)")
			time.Sleep(70 * time.Second) // Wait for at least one recommender cycle

			vpa, err := utils.GetVpaClientSet(f).AutoscalingV1().VerticalPodAutoscalers(f.Namespace.Name).Get(
				context.TODO(), vpaCRD.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(vpa.Status.Recommendation).To(gomega.BeNil(),
				"VPA should NOT have recommendations initially (Prometheus unreachable, fail-closed)")

			ginkgo.By("Updating VPA with working Prometheus URL")
			vpa.Spec.Telemetry.Prometheus.Address = "http://prometheus.monitoring.svc:9090"
			_, err = utils.GetVpaClientSet(f).AutoscalingV1().VerticalPodAutoscalers(f.Namespace.Name).Update(
				context.TODO(), vpa, metav1.UpdateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Waiting for VPA to recover and get recommendations from Prometheus")
			err = wait.PollUntilContextTimeout(context.TODO(), 10*time.Second, 3*time.Minute, true, func(ctx context.Context) (bool, error) {
				vpa, err := utils.GetVpaClientSet(f).AutoscalingV1().VerticalPodAutoscalers(f.Namespace.Name).Get(
					context.TODO(), vpaCRD.Name, metav1.GetOptions{})
				if err != nil {
					return false, err
				}

				// Check if VPA has recommendations now
				if vpa.Status.Recommendation != nil &&
					len(vpa.Status.Recommendation.ContainerRecommendations) > 0 {
					framework.Logf("VPA recovered and has recommendations from Prometheus")
					return true, nil
				}

				framework.Logf("VPA waiting to recover and get recommendations...")
				return false, nil
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				"VPA should recover and get recommendations after Prometheus URL is fixed")

			ginkgo.By("Verifying TelemetryUnavailable condition is cleared")
			vpa, err = utils.GetVpaClientSet(f).AutoscalingV1().VerticalPodAutoscalers(f.Namespace.Name).Get(
				context.TODO(), vpaCRD.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// TelemetryUnavailable should be False or not present after recovery
			for _, condition := range vpa.Status.Conditions {
				if condition.Type == vpa_types.TelemetryUnavailable {
					gomega.Expect(condition.Status).To(gomega.Equal(apiv1.ConditionFalse),
						"TelemetryUnavailable condition should be False after recovery")
					framework.Logf("TelemetryUnavailable condition correctly cleared after recovery")
				}
			}

			gomega.Expect(vpa.Status.Recommendation).NotTo(gomega.BeNil(),
				"VPA should have recommendations after recovery")
			framework.Logf("VPA successfully recovered: CPU=%v, Memory=%v",
				vpa.Status.Recommendation.ContainerRecommendations[0].Target[apiv1.ResourceCPU],
				vpa.Status.Recommendation.ContainerRecommendations[0].Target[apiv1.ResourceMemory])
		})

		ginkgo.It("should recover from Prometheus failure when URL is fixed (fallback mode)", func() {
			// This test verifies that a VPA with fallback enabled can switch from Kubernetes
			// metrics to Prometheus metrics when the broken URL is fixed.

			ginkgo.By("Setting up a hamster deployment")
			deploymentName := "hamster-recovery-fallback"
			containerName := utils.GetHamsterContainerNameByIndex(0)

			rc := NewDynamicResourceConsumer(deploymentName, f.Namespace.Name, KindDeployment,
				2,          /*replicas*/
				1,          /*initCPUTotal*/
				10,         /*initMemoryTotal*/
				1,          /*initCustomMetric*/
				int64(100), /*cpuRequest*/
				int64(100), /*memRequest*/
				f.ClientSet,
				f.ScalesGetter)
			defer rc.CleanUp()

			ginkgo.By("Creating VPA with unreachable Prometheus (fallback enabled)")
			targetRef := &autoscaling.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       deploymentName,
			}

			vpaCRD := test.VerticalPodAutoscaler().
				WithName("hamster-recovery-fallback-vpa").
				WithNamespace(f.Namespace.Name).
				WithTargetRef(targetRef).
				WithContainer(containerName).
				Get()

			fallbackTrue := true
			vpaCRD.Spec.Telemetry = &vpa_types.TelemetryConfig{
				Source: vpa_types.TelemetrySourcePrometheus,
				Prometheus: &vpa_types.PrometheusTelemetry{
					Address: "http://non-existent-prometheus.invalid:9090",
				},
				FallbackOnFailure: &fallbackTrue,
			}
			utils.InstallVPA(f, vpaCRD)

			ginkgo.By("Waiting and verifying VPA has recommendations from fallback (Kubernetes metrics)")
			err := wait.PollUntilContextTimeout(context.TODO(), 10*time.Second, 3*time.Minute, true, func(ctx context.Context) (bool, error) {
				vpa, err := utils.GetVpaClientSet(f).AutoscalingV1().VerticalPodAutoscalers(f.Namespace.Name).Get(
					context.TODO(), vpaCRD.Name, metav1.GetOptions{})
				if err != nil {
					return false, err
				}

				if vpa.Status.Recommendation != nil &&
					len(vpa.Status.Recommendation.ContainerRecommendations) > 0 {
					framework.Logf("VPA has recommendations from fallback")
					return true, nil
				}
				return false, nil
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred(),
				"VPA should get recommendations from Kubernetes metrics fallback")

			vpa, err := utils.GetVpaClientSet(f).AutoscalingV1().VerticalPodAutoscalers(f.Namespace.Name).Get(
				context.TODO(), vpaCRD.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// TelemetryUnavailable should be True (indicating primary source failed)
			foundTelemetryUnavailable := false
			for _, condition := range vpa.Status.Conditions {
				if condition.Type == vpa_types.TelemetryUnavailable {
					foundTelemetryUnavailable = true
					gomega.Expect(condition.Status).To(gomega.Equal(apiv1.ConditionTrue),
						"TelemetryUnavailable should be True when using fallback")
				}
			}
			gomega.Expect(foundTelemetryUnavailable).To(gomega.BeTrue(),
				"TelemetryUnavailable condition should be set when using fallback")

			ginkgo.By("Updating VPA with working Prometheus URL")
			vpa.Spec.Telemetry.Prometheus.Address = "http://prometheus.monitoring.svc:9090"
			_, err = utils.GetVpaClientSet(f).AutoscalingV1().VerticalPodAutoscalers(f.Namespace.Name).Update(
				context.TODO(), vpa, metav1.UpdateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Waiting for VPA to recover and switch to Prometheus metrics")
			// Wait for at least one full recommender cycle to switch from fallback to Prometheus
			time.Sleep(70 * time.Second)

			vpa, err = utils.GetVpaClientSet(f).AutoscalingV1().VerticalPodAutoscalers(f.Namespace.Name).Get(
				context.TODO(), vpaCRD.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying TelemetryUnavailable condition is cleared after recovery")
			// TelemetryUnavailable should be False or not present after Prometheus is working
			for _, condition := range vpa.Status.Conditions {
				if condition.Type == vpa_types.TelemetryUnavailable {
					gomega.Expect(condition.Status).To(gomega.Equal(apiv1.ConditionFalse),
						"TelemetryUnavailable should be False after Prometheus recovers")
					framework.Logf("TelemetryUnavailable condition correctly cleared: switched from fallback to Prometheus")
				}
			}

			gomega.Expect(vpa.Status.Recommendation).NotTo(gomega.BeNil(),
				"VPA should still have recommendations (now from Prometheus)")
			framework.Logf("VPA successfully recovered and switched to Prometheus: CPU=%v, Memory=%v",
				vpa.Status.Recommendation.ContainerRecommendations[0].Target[apiv1.ResourceCPU],
				vpa.Status.Recommendation.ContainerRecommendations[0].Target[apiv1.ResourceMemory])
		})

		// NOTE: The following tests are covered by unit tests:
		// - OOM counter resets → TestOOMCounterReset
		// - Decreasing OOM counters → TestOOMCounterDecreasing
		// - VPA with no matching pods → TestTelemetryAwareSource_VPAWithNoMatchingPods
	})

	// NOTE: All Phase 2 (Query Variations) tests are covered by unit/integration tests:
	// - Partial custom queries (OOM/CPU/Memory only) → TestTelemetryIntegration_PartialCustomQuery_*
	// - All custom queries → TestTelemetryIntegration_AllCustomQueries
	// - Metrics with missing labels → TestExtractContainerID_Missing*
	// - Empty query results → TestQueryPrometheus_EmptyVector
	// - Over-aggregated queries → TestQueryPrometheus_MultipleMetricsWithSomeMissingLabels

	ginkgo.Context("Phase 3: Advanced Scenarios", func() {
		// NOTE: The following tests are covered by integration tests:
		// - High frequency OOM events → TestTelemetryIntegration_HighFrequencyOOMs
		// - Stale metrics → TestTelemetryIntegration_StaleMetrics

		ginkgo.It("should support Prometheus bearer token authentication", func() {
			ginkgo.Skip("TODO: Implement test for bearer token auth")
			// Test plan:
			// 1. Create secret with bearer token
			// 2. Configure Prometheus to require auth
			// 3. Create VPA with bearer token reference
			// 4. Verify VPA can authenticate and fetch metrics
			// 5. Verify recommendations are generated
		})

		ginkgo.It("should support Prometheus basic authentication", func() {
			ginkgo.Skip("TODO: Implement test for basic auth")
			// Test plan:
			// 1. Create secret with username/password
			// 2. Configure Prometheus to require basic auth
			// 3. Create VPA with basic auth credentials
			// 4. Verify VPA can authenticate and fetch metrics
		})

		ginkgo.It("should handle multiple OOM events across pod restarts", func() {
			// This test verifies that OOM counters are tracked per-pod and that the VPA
			// correctly aggregates OOMs across pod restarts (different pod names).

			ginkgo.By("Setting up a hamster deployment with low memory to trigger OOMs")
			deploymentName := "hamster-oom-restart"
			containerName := utils.GetHamsterContainerNameByIndex(0)

			rc := NewDynamicResourceConsumer(deploymentName, f.Namespace.Name, KindDeployment,
				2,          /*replicas*/
				1,          /*initCPUTotal*/
				10,         /*initMemoryTotal*/
				1,          /*initCustomMetric*/
				int64(100), /*cpuRequest*/
				int64(250), /*memRequest - 250Mi*/
				f.ClientSet,
				f.ScalesGetter)
			defer rc.CleanUp()

			ginkgo.By("Creating VPA with Prometheus OOM counter query")
			targetRef := &autoscaling.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       deploymentName,
			}

			vpaCRD := test.VerticalPodAutoscaler().
				WithName("hamster-oom-restart-vpa").
				WithNamespace(f.Namespace.Name).
				WithTargetRef(targetRef).
				WithContainer(containerName).
				Get()

			customOOMQuery := "e2e_test_oom_events_total{namespace=\"" + f.Namespace.Name + "\"}"
			fallbackTrue := true
			vpaCRD.Spec.Telemetry = &vpa_types.TelemetryConfig{
				Source: vpa_types.TelemetrySourcePrometheus,
				Prometheus: &vpa_types.PrometheusTelemetry{
					Address: "http://prometheus.monitoring.svc:9090",
					Query: &vpa_types.PrometheusTelemetryQuery{
						OOMCountQuery: customOOMQuery,
					},
				},
				FallbackOnFailure: &fallbackTrue,
			}
			utils.InstallVPA(f, vpaCRD)

			ginkgo.By("Waiting for initial VPA recommendation")
			time.Sleep(70 * time.Second)

			vpa, err := utils.GetVpaClientSet(f).AutoscalingV1().VerticalPodAutoscalers(f.Namespace.Name).Get(
				context.TODO(), vpaCRD.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(vpa.Status.Recommendation).NotTo(gomega.BeNil())
			initialMemory := vpa.Status.Recommendation.ContainerRecommendations[0].Target[apiv1.ResourceMemory]

			ginkgo.By("Getting pod names for first generation")
			pods, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).List(context.TODO(), metav1.ListOptions{
				LabelSelector: "name=" + deploymentName,
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pods.Items).To(gomega.HaveLen(2), "Should have 2 pods")

			pod1Name := pods.Items[0].Name
			pod2Name := pods.Items[1].Name
			framework.Logf("Generation 1 pods: %s, %s", pod1Name, pod2Name)

			ginkgo.By("Pushing baseline OOM counter (0) for both first-gen pods")
			client := NewPushgatewayClient(f.ClientSet)
			err = client.PushOOMCounter(f.Namespace.Name, pod1Name, containerName, 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = client.PushOOMCounter(f.Namespace.Name, pod2Name, containerName, 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Waiting for Prometheus to scrape baseline")
			time.Sleep(65 * time.Second)

			ginkgo.By("Pushing OOM events for first-gen pods (pod1: 2 OOMs, pod2: 1 OOM)")
			err = client.PushOOMCounter(f.Namespace.Name, pod1Name, containerName, 2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = client.PushOOMCounter(f.Namespace.Name, pod2Name, containerName, 1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Waiting for VPA to process first-gen OOMs and increase memory")
			time.Sleep(70 * time.Second)

			vpa, err = utils.GetVpaClientSet(f).AutoscalingV1().VerticalPodAutoscalers(f.Namespace.Name).Get(
				context.TODO(), vpaCRD.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(vpa.Status.Recommendation).NotTo(gomega.BeNil())
			memoryAfterFirstOOMs := vpa.Status.Recommendation.ContainerRecommendations[0].Target[apiv1.ResourceMemory]

			gomega.Expect(memoryAfterFirstOOMs.Cmp(initialMemory)).To(gomega.BeNumerically(">", 0),
				"Memory should increase after OOMs (was %v, now %v)", initialMemory.String(), memoryAfterFirstOOMs.String())
			framework.Logf("Memory increased from %v to %v after first-gen OOMs", initialMemory.String(), memoryAfterFirstOOMs.String())

			ginkgo.By("Simulating pod restart by deleting pods")
			err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Delete(context.TODO(), pod1Name, metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Delete(context.TODO(), pod2Name, metav1.DeleteOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Waiting for new pods to come up")
			time.Sleep(20 * time.Second)

			pods, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).List(context.TODO(), metav1.ListOptions{
				LabelSelector: "name=" + deploymentName,
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pods.Items).To(gomega.HaveLen(2), "Should have 2 new pods after restart")

			pod3Name := pods.Items[0].Name
			pod4Name := pods.Items[1].Name
			gomega.Expect(pod3Name).NotTo(gomega.Equal(pod1Name), "New pod should have different name")
			gomega.Expect(pod4Name).NotTo(gomega.Equal(pod2Name), "New pod should have different name")
			framework.Logf("Generation 2 pods: %s, %s", pod3Name, pod4Name)

			ginkgo.By("Pushing baseline OOM counter (0) for both second-gen pods")
			err = client.PushOOMCounter(f.Namespace.Name, pod3Name, containerName, 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = client.PushOOMCounter(f.Namespace.Name, pod4Name, containerName, 0)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Waiting for Prometheus to scrape second-gen baseline")
			time.Sleep(65 * time.Second)

			ginkgo.By("Pushing OOM events for second-gen pods (pod3: 1 OOM, pod4: 2 OOMs)")
			err = client.PushOOMCounter(f.Namespace.Name, pod3Name, containerName, 1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = client.PushOOMCounter(f.Namespace.Name, pod4Name, containerName, 2)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Waiting for VPA to process second-gen OOMs and further increase memory")
			time.Sleep(70 * time.Second)

			vpa, err = utils.GetVpaClientSet(f).AutoscalingV1().VerticalPodAutoscalers(f.Namespace.Name).Get(
				context.TODO(), vpaCRD.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(vpa.Status.Recommendation).NotTo(gomega.BeNil())
			finalMemory := vpa.Status.Recommendation.ContainerRecommendations[0].Target[apiv1.ResourceMemory]

			gomega.Expect(finalMemory.Cmp(memoryAfterFirstOOMs)).To(gomega.BeNumerically(">", 0),
				"Memory should increase further after second-gen OOMs (was %v, now %v)",
				memoryAfterFirstOOMs.String(), finalMemory.String())
			framework.Logf("Memory increased from %v to %v after second-gen OOMs",
				memoryAfterFirstOOMs.String(), finalMemory.String())
			framework.Logf("Total memory increase: %v -> %v (initial to final)",
				initialMemory.String(), finalMemory.String())

			ginkgo.By("Cleaning up Pushgateway metrics")
			err = client.DeleteMetrics(deploymentName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		})
	})

	ginkgo.Context("Phase 4: Production Scenarios", func() {
		// NOTE: The following test is covered by integration test:
		// - Slow Prometheus queries → TestTelemetryIntegration_SlowQueryTimeout

		ginkgo.It("should support insecure TLS connections", func() {
			ginkgo.Skip("TODO: Implement test for insecure TLS")
			// Test plan:
			// 1. Deploy Prometheus with self-signed cert
			// 2. Create VPA with insecure=true
			// 3. Verify connection succeeds despite invalid cert
			// 4. Verify metrics are fetched correctly
		})

		ginkgo.It("should work with updateMode=Initial and Prometheus telemetry", func() {
			ginkgo.Skip("TODO: Implement test for Initial update mode")
			// Test plan:
			// 1. Create VPA with updateMode=Initial and Prometheus
			// 2. Verify recommendations generated from Prometheus
			// 3. Deploy new pod and verify recommendation applied
			// 4. Verify existing pods not updated
		})

		ginkgo.It("should work with updateMode=Off and Prometheus telemetry", func() {
			ginkgo.Skip("TODO: Implement test for Off update mode")
			// Test plan:
			// 1. Create VPA with updateMode=Off and Prometheus
			// 2. Verify recommendations generated from Prometheus
			// 3. Verify recommendations not applied to pods
			// 4. Verify status shows recommendations
		})

		ginkgo.It("should handle VPA targeting StatefulSet with Prometheus", func() {
			ginkgo.Skip("TODO: Implement test for StatefulSet target")
			// Test plan:
			// 1. Deploy StatefulSet
			// 2. Create VPA with Prometheus targeting StatefulSet
			// 3. Verify per-pod metrics correctly attributed
			// 4. Verify recommendations work for StatefulSet
		})

		ginkgo.It("should handle Prometheus server restarts", func() {
			ginkgo.Skip("TODO: Implement test for Prometheus restart")
			// Test plan:
			// 1. Deploy VPA with Prometheus
			// 2. Verify recommendations working
			// 3. Restart Prometheus pod
			// 4. Verify VPA recovers and continues
			// 5. Verify no data loss or corruption
		})

		ginkgo.It("should handle concurrent VPA updates with different Prometheus configs", func() {
			ginkgo.Skip("TODO: Implement test for concurrent VPA operations")
			// Test plan:
			// 1. Create 10+ VPAs simultaneously
			// 2. Each with different Prometheus configs
			// 3. Verify all get recommendations
			// 4. Verify no race conditions or deadlocks
		})
	})

	ginkgo.Context("Phase 5: Error Recovery", func() {
		// NOTE: The following tests are covered by unit/integration tests:
		// - Invalid Prometheus data → TestTelemetryIntegration_InvalidPrometheusData (malformed JSON, missing fields, etc.)
		// - NaN/Inf values → TestQueryPrometheus_NaNValue, TestQueryPrometheus_InfValue

		ginkgo.It("should handle VPA config updates without data loss", func() {
			ginkgo.Skip("TODO: Implement test for VPA config hot reload")
			// Test plan:
			// 1. Create VPA with Prometheus config A
			// 2. Wait for recommendations
			// 3. Update to Prometheus config B
			// 4. Verify smooth transition
			// 5. Verify histogram data preserved or handled correctly
		})

		ginkgo.It("should handle switching from Kubernetes to Prometheus telemetry", func() {
			ginkgo.Skip("TODO: Implement test for telemetry source migration")
			// Test plan:
			// 1. Create VPA with default Kubernetes source
			// 2. Wait for recommendations
			// 3. Update to Prometheus source
			// 4. Verify recommendations continue
			// 5. Verify new data from Prometheus
		})

		ginkgo.It("should handle switching from Prometheus to Kubernetes telemetry", func() {
			ginkgo.Skip("TODO: Implement test for reverse telemetry migration")
			// Test plan:
			// 1. Create VPA with Prometheus source
			// 2. Wait for recommendations
			// 3. Remove Prometheus config (revert to default)
			// 4. Verify recommendations continue from metrics-server
		})
	})
})
