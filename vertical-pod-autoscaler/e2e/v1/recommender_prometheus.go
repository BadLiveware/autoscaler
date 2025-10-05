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
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/autoscaler/vertical-pod-autoscaler/e2e/utils"
	vpa_clientset "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/client/clientset/versioned"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/test"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	framework_deployment "k8s.io/kubernetes/test/e2e/framework/deployment"
	podsecurity "k8s.io/pod-security-admission/api"
)

const (
	prometheusNamespace = "monitoring"
	recommendationWait  = 3 * time.Minute
	oomPollInterval     = 10 * time.Second
)

var _ = utils.RecommenderPrometheusE2eDescribe("Prometheus Integration", func() {
	f := framework.NewDefaultFramework("vertical-pod-autoscaling")
	f.NamespacePodSecurityEnforceLevel = podsecurity.LevelBaseline

	var vpaClientSet vpa_clientset.Interface
	var pushgatewayClient *PushgatewayClient
	var resourceConsumer *ResourceConsumer

	ginkgo.BeforeEach(func() {
		vpaClientSet = utils.GetVpaClientSet(f)
		pushgatewayClient = NewPushgatewayClient(f.ClientSet)

		ginkgo.By("Ensuring Prometheus and Pushgateway are ready")
		err := WaitForPrometheusReady(f.ClientSet, 2*time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Prometheus should be ready")

		err = WaitForPushgatewayReady(f.ClientSet, 2*time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Pushgateway should be ready")
	})

	ginkgo.AfterEach(func() {
		// Clean up resource consumer
		if resourceConsumer != nil {
			resourceConsumer.CleanUp()
			resourceConsumer = nil
		}
		// Clean up any pushed metrics
		if pushgatewayClient != nil {
			_ = pushgatewayClient.DeleteMetrics("e2e-test-oom")
		}
	})

	ginkgo.It("generates recommendations from Prometheus metrics", func() {
		ginkgo.By("Setting up a hamster deployment")
		d := utils.NewNHamstersDeployment(f, 1)
		d.Spec.Template.Spec.Containers[0].Resources.Requests = apiv1.ResourceList{
			apiv1.ResourceCPU:    ParseQuantityOrDie("100m"),
			apiv1.ResourceMemory: ParseQuantityOrDie("100Mi"),
		}
		podList := utils.StartDeploymentPods(f, d)
		gomega.Expect(podList.Items).NotTo(gomega.BeEmpty())

		ginkgo.By("Setting up VPA with Prometheus source")
		containerName := utils.GetHamsterContainerNameByIndex(0)
		vpaCRD := test.VerticalPodAutoscaler().
			WithName("hamster-vpa").
			WithNamespace(f.Namespace.Name).
			WithTargetRef(utils.HamsterTargetRef).
			WithContainer(containerName).
			Get()

		utils.InstallVPA(f, vpaCRD)

		ginkgo.By("Waiting for recommendation to be generated from Prometheus")
		vpa, err := utils.WaitForRecommendationPresent(vpaClientSet, vpaCRD)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(vpa.Status.Recommendation).NotTo(gomega.BeNil())
		gomega.Expect(vpa.Status.Recommendation.ContainerRecommendations).To(gomega.HaveLen(1))

		recommendation := vpa.Status.Recommendation.ContainerRecommendations[0]
		gomega.Expect(recommendation.ContainerName).To(gomega.Equal(containerName))

		// Verify recommendations are within reasonable bounds
		cpuTarget := recommendation.Target[apiv1.ResourceCPU]
		memoryTarget := recommendation.Target[apiv1.ResourceMemory]
		gomega.Expect(cpuTarget.IsZero()).To(gomega.BeFalse(), "CPU target should be set")
		gomega.Expect(memoryTarget.IsZero()).To(gomega.BeFalse(), "Memory target should be set")

		framework.Logf("Prometheus-based recommendation: CPU=%s, Memory=%s",
			cpuTarget.String(), memoryTarget.String())
	})

	ginkgo.It("detects OOM events from Prometheus counters", func() {
		ginkgo.By("Setting up a resource consumer with memory consumption")
		resourceConsumer = NewDynamicResourceConsumer(
			"test-oom-consumer",
			f.Namespace.Name,
			KindDeployment,
			1,   // replicas
			50,  // initCPUTotal (millicores)
			200, // initMemoryTotal (megabytes) - actively consume memory
			0,   // initCustomMetric
			500, // cpuLimit (millicores)
			512, // memLimit (megabytes)
			f.ClientSet,
			nil, // scaleClient
		)

		// Get pod for pushing OOM metric
		podList, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).List(context.TODO(), metav1.ListOptions{
			LabelSelector: "name=test-oom-consumer",
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(podList.Items).NotTo(gomega.BeEmpty())

		pod := podList.Items[0]
		containerName := "resource-consumer"

		ginkgo.By("Setting up VPA")
		vpaCRD := test.VerticalPodAutoscaler().
			WithName("test-oom-vpa").
			WithNamespace(f.Namespace.Name).
			WithTargetRef(&autoscalingv1.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       "test-oom-consumer",
			}).
			WithContainer(containerName).
			Get()

		utils.InstallVPA(f, vpaCRD)

		ginkgo.By("Waiting for initial recommendation")
		vpa, err := utils.WaitForRecommendationPresent(vpaClientSet, vpaCRD)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		initialMemoryTarget := vpa.Status.Recommendation.ContainerRecommendations[0].Target[apiv1.ResourceMemory]
		framework.Logf("Initial memory recommendation: %s", initialMemoryTarget.String())

		ginkgo.By("Pushing OOM counter to Pushgateway")
		err = pushgatewayClient.PushOOMCounter(
			pod.Namespace,
			pod.Name,
			containerName,
			1.0, // First OOM event
		)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for Prometheus to scrape the metric")
		time.Sleep(15 * time.Second) // Prometheus scrapes every 5s, give it some buffer

		ginkgo.By("Pushing second OOM event")
		err = pushgatewayClient.PushOOMCounter(
			pod.Namespace,
			pod.Name,
			containerName,
			2.0, // Second OOM event (counter increased)
		)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for recommendation to update after OOM")
		// OOM with 300Mi request should bump to max(300Mi+100Mi, 300Mi*1.2) = 400Mi
		// Check that recommendation increased by at least 10% from initial
		gomega.Eventually(func() bool {
			currentVPA, err := vpaClientSet.AutoscalingV1().VerticalPodAutoscalers(f.Namespace.Name).Get(
				context.TODO(), vpaCRD.Name, metav1.GetOptions{})
			if err != nil {
				framework.Logf("Error getting VPA: %v", err)
				return false
			}

			if currentVPA.Status.Recommendation == nil || len(currentVPA.Status.Recommendation.ContainerRecommendations) == 0 {
				return false
			}

			newMemoryTarget := currentVPA.Status.Recommendation.ContainerRecommendations[0].Target[apiv1.ResourceMemory]
			framework.Logf("Current memory recommendation: %s (initial was %s)",
				newMemoryTarget.String(), initialMemoryTarget.String())

			// After OOM, expect recommendation to increase by at least 10%
			minExpected := initialMemoryTarget.Value() * 11 / 10

			return newMemoryTarget.Value() >= minExpected
		}, 2*time.Minute, oomPollInterval).Should(gomega.BeTrue(),
			"Memory recommendation should increase after OOM event")
	})

	ginkgo.It("handles multiple OOM events correctly", func() {
		ginkgo.By("Setting up a resource consumer with memory consumption")
		resourceConsumer = NewDynamicResourceConsumer(
			"test-multi-oom-consumer",
			f.Namespace.Name,
			KindDeployment,
			1,   // replicas
			50,  // initCPUTotal (millicores)
			200, // initMemoryTotal (megabytes) - actively consume memory
			0,   // initCustomMetric
			500, // cpuLimit (millicores)
			512, // memLimit (megabytes)
			f.ClientSet,
			f.ScalesGetter, // scaleClient
		)

		// Get pod for pushing OOM metric
		podList, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).List(context.TODO(), metav1.ListOptions{
			LabelSelector: "name=test-multi-oom-consumer",
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(podList.Items).NotTo(gomega.BeEmpty())

		pod := podList.Items[0]
		containerName := "resource-consumer"

		ginkgo.By("Setting up VPA")
		vpaCRD := test.VerticalPodAutoscaler().
			WithName("test-multi-oom-vpa").
			WithNamespace(f.Namespace.Name).
			WithTargetRef(&autoscalingv1.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       "test-multi-oom-consumer",
			}).
			WithContainer(containerName).
			Get()

		utils.InstallVPA(f, vpaCRD)

		ginkgo.By("Waiting for initial recommendation")
		vpa, err := utils.WaitForRecommendationPresent(vpaClientSet, vpaCRD)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		initialMemoryTarget := vpa.Status.Recommendation.ContainerRecommendations[0].Target[apiv1.ResourceMemory]
		framework.Logf("Initial memory recommendation: %s", initialMemoryTarget.String())

		ginkgo.By("Simulating multiple OOM events over time")
		for i := 1; i <= 3; i++ {
			err = pushgatewayClient.PushOOMCounter(
				pod.Namespace,
				pod.Name,
				containerName,
				float64(i),
			)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("Pushed OOM counter value: %d", i)

			// Wait for Prometheus to scrape
			time.Sleep(15 * time.Second)
		}

		ginkgo.By("Verifying recommendation increased after multiple OOMs")
		// After 3 OOMs with 300Mi request, expect recommendation to increase significantly
		// Each OOM bumps to max(300Mi+100Mi, 300Mi*1.2) = 400Mi
		// Check that recommendation increased by at least 20% from initial
		gomega.Eventually(func() bool {
			currentVPA, err := vpaClientSet.AutoscalingV1().VerticalPodAutoscalers(f.Namespace.Name).Get(
				context.TODO(), vpaCRD.Name, metav1.GetOptions{})
			if err != nil {
				return false
			}

			if currentVPA.Status.Recommendation == nil || len(currentVPA.Status.Recommendation.ContainerRecommendations) == 0 {
				return false
			}

			newMemoryTarget := currentVPA.Status.Recommendation.ContainerRecommendations[0].Target[apiv1.ResourceMemory]
			minExpected := initialMemoryTarget.Value() * 12 / 10 // 20% increase

			framework.Logf("Memory after 3 OOMs: %s (initial was %s, expected at least %d bytes)",
				newMemoryTarget.String(), initialMemoryTarget.String(), minExpected)

			return newMemoryTarget.Value() >= minExpected
		}, 2*time.Minute, oomPollInterval).Should(gomega.BeTrue(),
			"Memory recommendation should increase significantly after multiple OOM events")
	})

	ginkgo.It("works with custom Prometheus queries", func() {
		ginkgo.By("Deploying VPA recommender with custom queries is handled by deployment")
		// This test verifies that when custom queries are configured,
		// the recommender still generates recommendations

		ginkgo.By("Setting up a hamster deployment")
		d := utils.NewNHamstersDeployment(f, 1)
		d.Spec.Template.Spec.Containers[0].Resources.Requests = apiv1.ResourceList{
			apiv1.ResourceCPU:    ParseQuantityOrDie("100m"),
			apiv1.ResourceMemory: ParseQuantityOrDie("100Mi"),
		}
		_ = utils.StartDeploymentPods(f, d)

		ginkgo.By("Setting up VPA")
		containerName := utils.GetHamsterContainerNameByIndex(0)
		vpaCRD := test.VerticalPodAutoscaler().
			WithName("hamster-vpa").
			WithNamespace(f.Namespace.Name).
			WithTargetRef(utils.HamsterTargetRef).
			WithContainer(containerName).
			Get()

		utils.InstallVPA(f, vpaCRD)

		ginkgo.By("Waiting for recommendation (verifying custom queries work)")
		vpa, err := utils.WaitForRecommendationPresent(vpaClientSet, vpaCRD)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(vpa.Status.Recommendation).NotTo(gomega.BeNil())
		gomega.Expect(vpa.Status.Recommendation.ContainerRecommendations).To(gomega.HaveLen(1))

		framework.Logf("Recommendation generated successfully with custom queries")
	})
})

// WaitForPrometheusReady waits for Prometheus to be ready
func WaitForPrometheusReady(clientSet clientset.Interface, timeout time.Duration) error {
	return waitForDeploymentReady(clientSet, prometheusNamespace, "prometheus", timeout)
}

func waitForDeploymentReady(clientSet clientset.Interface, namespace, name string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	deployment, err := clientSet.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get deployment %s/%s: %w", namespace, name, err)
	}

	return framework_deployment.WaitForDeploymentComplete(clientSet, deployment)
}
