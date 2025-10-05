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
	"k8s.io/apimachinery/pkg/api/resource"
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

var _ = FullVpaE2eDescribe("VPA Telemetry", func() {
	f := framework.NewDefaultFramework("vertical-pod-autoscaling-telemetry")
	f.NamespacePodSecurityEnforceLevel = podsecurity.LevelBaseline

	ginkgo.Describe("with Prometheus telemetry source", ginkgo.Label("PrometheusRequired"), func() {
		var rc *ResourceConsumer

		ginkgo.BeforeEach(func() {
			ns := f.Namespace.Name
			ginkgo.By("Setting up a hamster deployment")
			rc = NewDynamicResourceConsumer("hamster", ns, KindDeployment,
				2,          /*replicas*/
				1,          /*initCPUTotal*/
				10,         /*initMemoryTotal*/
				1,          /*initCustomMetric*/
				int64(100), /*cpuRequest*/
				int64(100), /*memRequest*/
				f.ClientSet,
				f.ScalesGetter)
		})

		ginkgo.AfterEach(func() {
			rc.CleanUp()
		})

		ginkgo.It("should provide recommendations using Prometheus metrics", func() {
			ginkgo.By("Setting up a VPA with Prometheus telemetry")
			targetRef := &autoscaling.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       "hamster",
			}

			containerName := utils.GetHamsterContainerNameByIndex(0)
			vpaCRD := test.VerticalPodAutoscaler().
				WithName("hamster-prometheus-vpa").
				WithNamespace(f.Namespace.Name).
				WithTargetRef(targetRef).
				WithContainer(containerName).
				Get()

			// Add Prometheus telemetry configuration
			// Note: This test requires a Prometheus instance to be available
			// Using the test Prometheus deployed in monitoring namespace
			prometheusAddress := "http://prometheus.monitoring.svc:9090"

			vpaCRD.Spec.Telemetry = &vpa_types.TelemetryConfig{
				Source: vpa_types.TelemetrySourcePrometheus,
				Prometheus: &vpa_types.PrometheusTelemetry{
					Address: prometheusAddress,
				},
			}

			utils.InstallVPA(f, vpaCRD)

			ginkgo.By("Waiting for recommendation to be filled")
			vpaClientSet := utils.GetVpaClientSet(f)
			_, err := utils.WaitForRecommendationPresent(vpaClientSet, vpaCRD)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verifying VPA has no ConfigUnsupported condition")
			vpa, err := vpaClientSet.AutoscalingV1().VerticalPodAutoscalers(f.Namespace.Name).Get(
				context.TODO(), vpaCRD.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			for _, condition := range vpa.Status.Conditions {
				if condition.Type == vpa_types.ConfigUnsupported {
					gomega.Expect(condition.Status).To(gomega.Equal(apiv1.ConditionFalse),
						"VPA should not have ConfigUnsupported condition with valid Prometheus config")
				}
			}

			ginkgo.By("Verifying recommendation has expected fields")
			gomega.Expect(vpa.Status.Recommendation).NotTo(gomega.BeNil())
			gomega.Expect(vpa.Status.Recommendation.ContainerRecommendations).NotTo(gomega.BeEmpty())
			recommendation := vpa.Status.Recommendation.ContainerRecommendations[0]
			gomega.Expect(recommendation.Target).NotTo(gomega.BeNil())
			gomega.Expect(recommendation.LowerBound).NotTo(gomega.BeNil())
			gomega.Expect(recommendation.UpperBound).NotTo(gomega.BeNil())
		})

		ginkgo.It("should detect OOM events from Prometheus counters", func() {
			ginkgo.By("Setting up a VPA with Prometheus telemetry")
			targetRef := &autoscaling.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       "hamster",
			}

			containerName := utils.GetHamsterContainerNameByIndex(0)

			vpaCRD := test.VerticalPodAutoscaler().
				WithName("hamster-oom-vpa").
				WithNamespace(f.Namespace.Name).
				WithTargetRef(targetRef).
				WithContainer(containerName).
				Get()

			prometheusAddress := "http://prometheus.monitoring.svc:9090"

			vpaCRD.Spec.Telemetry = &vpa_types.TelemetryConfig{
				Source: vpa_types.TelemetrySourcePrometheus,
				Prometheus: &vpa_types.PrometheusTelemetry{
					Address: prometheusAddress,
					Query: &vpa_types.PrometheusTelemetryQuery{
						// Use our unique test metric name to avoid conflicts with real cadvisor metrics
						OOMCountQuery: "e2e_test_oom_events_total",
					},
				},
			}

			utils.InstallVPA(f, vpaCRD)

			ginkgo.By("Waiting for initial recommendation to establish baseline")
			vpaClientSet := utils.GetVpaClientSet(f)
			_, err := utils.WaitForRecommendationPresent(vpaClientSet, vpaCRD)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "VPA should provide initial recommendation")

			// Get the initial recommendation
			vpa, err := utils.GetVpaClientSet(f).AutoscalingV1().VerticalPodAutoscalers(f.Namespace.Name).Get(
				context.TODO(), vpaCRD.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			initialMemory := vpa.Status.Recommendation.ContainerRecommendations[0].Target[apiv1.ResourceMemory]
			framework.Logf("Initial memory recommendation: %v", initialMemory.String())

			ginkgo.By("Waiting for Pushgateway to be ready")
			err = WaitForPushgatewayReady(f.ClientSet, 30*time.Second)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Pushgateway should be ready")

			ginkgo.By("Pushing initial OOM counter (baseline = 0)")
			pgClient := NewPushgatewayClient(f.ClientSet)
			defer pgClient.DeleteMetrics("e2e-test-oom")

			// Get all pod names from deployment
			podList, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).List(context.TODO(), metav1.ListOptions{
				LabelSelector: "name=hamster",
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(podList.Items)).To(gomega.BeNumerically(">", 0), "Should have at least one hamster pod")

			// Push baseline OOM counter = 0 for ALL pods
			for _, pod := range podList.Items {
				framework.Logf("Pushing baseline OOM counter for pod: %s", pod.Name)
				err = pgClient.PushOOMCounter(f.Namespace.Name, pod.Name, containerName, 0)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Should push initial OOM counter for pod %s", pod.Name)
			}

			ginkgo.By("Waiting for Prometheus to scrape baseline metric and VPA to process it")
			time.Sleep(65 * time.Second) // Wait for Prometheus scrape (5s) + VPA recommender cycle (60s)

			// Check what Prometheus sees after baseline
			framework.Logf("=== After baseline push, checking Prometheus ===")
			time.Sleep(2 * time.Second)
			// Query what's actually in Prometheus
			framework.Logf("Expected: OOM counter = 0 for %d pods", len(podList.Items))

			ginkgo.By("Simulating 3 OOM events by incrementing counter for all pods")
			for _, pod := range podList.Items {
				framework.Logf("Pushing OOM counter=3 for pod: %s", pod.Name)
				err = pgClient.PushOOMCounter(f.Namespace.Name, pod.Name, containerName, 3)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Should push OOM counter with 3 events for pod %s", pod.Name)
			}

			framework.Logf("=== After OOM increment, checking state ===")
			framework.Logf("Expected: OOM counter = 3 for %d pods (delta of 3 per pod = %d total OOM events)",
				len(podList.Items), len(podList.Items)*3)

			ginkgo.By("Waiting for VPA to detect OOM delta and adjust recommendation")
			// Check VPA state periodically during the wait
			for i := 0; i < 7; i++ {
				time.Sleep(10 * time.Second)
				vpaCheck, err := utils.GetVpaClientSet(f).AutoscalingV1().VerticalPodAutoscalers(f.Namespace.Name).Get(
					context.TODO(), vpaCRD.Name, metav1.GetOptions{})
				if err == nil {
					currentMem := vpaCheck.Status.Recommendation.ContainerRecommendations[0].Target[apiv1.ResourceMemory]
					framework.Logf("Check %d/7: Current memory recommendation: %v (initial was %v)",
						i+1, currentMem.String(), initialMemory.String())
				}
			}

			// Verify VPA detected the OOMs and increased memory recommendation
			vpa, err = utils.GetVpaClientSet(f).AutoscalingV1().VerticalPodAutoscalers(f.Namespace.Name).Get(
				context.TODO(), vpaCRD.Name, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			newMemory := vpa.Status.Recommendation.ContainerRecommendations[0].Target[apiv1.ResourceMemory]
			framework.Logf("New memory recommendation after OOM: %v", newMemory.String())

			// Memory recommendation should increase after detecting OOMs
			gomega.Expect(newMemory.Cmp(initialMemory)).To(gomega.BeNumerically(">", 0),
				"Memory recommendation should increase after OOM events (was %v, now %v)",
				initialMemory.String(), newMemory.String())
		})
	})

	ginkgo.Describe("with invalid Prometheus configuration", func() {
		ginkgo.It("should set ConfigUnsupported condition for missing address", func() {
			ginkgo.By("Creating VPA with Prometheus source but no address")
			targetRef := &autoscaling.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       "test-deployment",
			}

			vpaCRD := test.VerticalPodAutoscaler().
				WithName("invalid-prometheus-vpa").
				WithNamespace(f.Namespace.Name).
				WithTargetRef(targetRef).
				WithContainer("test-container").
				Get()

			vpaCRD.Spec.Telemetry = &vpa_types.TelemetryConfig{
				Source: vpa_types.TelemetrySourcePrometheus,
				// Prometheus config intentionally omitted
			}

			utils.InstallVPA(f, vpaCRD)

			ginkgo.By("Verifying ConfigUnsupported condition is set")
			vpaClientSet := utils.GetVpaClientSet(f)
			var vpa *vpa_types.VerticalPodAutoscaler
			err := wait.PollUntilContextTimeout(context.TODO(), 5*time.Second, 30*time.Second, true, func(ctx context.Context) (bool, error) {
				var err error
				vpa, err = vpaClientSet.AutoscalingV1().VerticalPodAutoscalers(f.Namespace.Name).Get(
					ctx, vpaCRD.Name, metav1.GetOptions{})
				if err != nil {
					return false, err
				}

				for _, condition := range vpa.Status.Conditions {
					if condition.Type == vpa_types.ConfigUnsupported && condition.Status == apiv1.ConditionTrue {
						framework.Logf("Found ConfigUnsupported condition: %s", condition.Message)
						return true, nil
					}
				}
				return false, nil
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "VPA should have ConfigUnsupported condition")

			// Verify the message mentions missing address
			for _, condition := range vpa.Status.Conditions {
				if condition.Type == vpa_types.ConfigUnsupported {
					gomega.Expect(condition.Message).To(gomega.ContainSubstring("address"),
						"ConfigUnsupported message should mention missing address")
				}
			}
		})
	})
})

// waitForRecommendationAboveMemory waits until VPA recommendation exceeds the specified memory threshold
func waitForRecommendationAboveMemory(f *framework.Framework, vpa *vpa_types.VerticalPodAutoscaler, timeout time.Duration, threshold resource.Quantity) error {
	vpaClientSet := utils.GetVpaClientSet(f)
	return wait.PollUntilContextTimeout(context.TODO(), 10*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		currentVPA, err := vpaClientSet.AutoscalingV1().VerticalPodAutoscalers(vpa.Namespace).Get(
			ctx, vpa.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		if currentVPA.Status.Recommendation == nil || len(currentVPA.Status.Recommendation.ContainerRecommendations) == 0 {
			framework.Logf("Recommendation not yet available")
			return false, nil
		}

		memoryTarget := currentVPA.Status.Recommendation.ContainerRecommendations[0].Target[apiv1.ResourceMemory]
		framework.Logf("Current memory recommendation: %v (threshold: %v)", memoryTarget.String(), threshold.String())

		return memoryTarget.Cmp(threshold) > 0, nil
	})
}
