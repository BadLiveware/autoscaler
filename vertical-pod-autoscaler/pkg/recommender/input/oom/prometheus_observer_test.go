/*
Copyright The Kubernetes Authors.

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

package oom

import (
	"context"
	"errors"
	"testing"
	"time"

	prometheusv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	prommodel "github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"

	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/annotations"
)

// fakeAPI is a stub for promQueryAPI that returns a queue of canned results.
type fakeAPI struct {
	calls   int
	results []prommodel.Value
	err     error
}

func (f *fakeAPI) Query(_ context.Context, _ string, _ time.Time, _ ...prometheusv1.Option) (prommodel.Value, prometheusv1.Warnings, error) {
	f.calls++
	if f.err != nil {
		return nil, nil, f.err
	}
	if len(f.results) == 0 {
		return prommodel.Vector{}, nil, nil
	}
	v := f.results[0]
	if len(f.results) > 1 {
		f.results = f.results[1:]
	}
	return v, nil, nil
}

// fakeClusterState implements just the slice of ClusterState the observer
// reads.
type fakeClusterState struct {
	vpas map[model.VpaID]*model.Vpa
	pods map[model.PodID]*model.PodState
}

func (f *fakeClusterState) VPAs() map[model.VpaID]*model.Vpa      { return f.vpas }
func (f *fakeClusterState) Pods() map[model.PodID]*model.PodState { return f.pods }

func vpaWithOOMAnnotation(ns, name, metric string) *model.Vpa {
	return &model.Vpa{
		ID:          model.VpaID{Namespace: ns, VpaName: name},
		Annotations: map[string]string{annotations.OOMCounterMetricAnnotation: metric},
	}
}

func podWithMemRequest(ns, name, ctr string, memBytes int64) *model.PodState {
	return &model.PodState{
		ID: model.PodID{Namespace: ns, PodName: name},
		Containers: map[string]*model.ContainerState{
			ctr: {
				Request: model.Resources{
					model.ResourceMemory: model.ResourceAmount(memBytes),
				},
			},
		},
	}
}

func sample(podName, ctrName string, value float64, ts time.Time) *prommodel.Sample {
	return &prommodel.Sample{
		Metric: prommodel.Metric{
			"pod":       prommodel.LabelValue(podName),
			"container": prommodel.LabelValue(ctrName),
		},
		Value:     prommodel.SampleValue(value),
		Timestamp: prommodel.TimeFromUnixNano(ts.UnixNano()),
	}
}

func newTestObserver(api promQueryAPI, cs clusterStateView, ch chan<- OomInfo) *PrometheusObserver {
	return &PrometheusObserver{
		promAPI:        api,
		clusterState:   cs,
		oomChan:        ch,
		pollInterval:   time.Minute,
		queryTimeout:   5 * time.Second,
		podLabel:       "pod",
		containerLabel: "container",
		seen:           make(map[model.VpaID]struct{}),
	}
}

func TestPrometheusObserver_FirstPollDiscarded(t *testing.T) {
	vpa := vpaWithOOMAnnotation("ns", "vpa1", "dotnet_oome")
	pod := podWithMemRequest("ns", "pod-a", "ctr", 256<<20)

	cs := &fakeClusterState{
		vpas: map[model.VpaID]*model.Vpa{vpa.ID: vpa},
		pods: map[model.PodID]*model.PodState{pod.ID: pod},
	}
	api := &fakeAPI{
		// Every query returns the same vector with one event. The first
		// call should be discarded; the second should produce an OomInfo.
		results: []prommodel.Value{
			prommodel.Vector{sample("pod-a", "ctr", 1, time.Now())},
		},
	}
	ch := make(chan OomInfo, 4)
	o := newTestObserver(api, cs, ch)

	o.pollOnce(context.Background())
	assert.Equal(t, 1, api.calls)
	assert.Empty(t, ch, "first poll's events must be discarded")

	o.pollOnce(context.Background())
	assert.Equal(t, 2, api.calls)
	assert.Len(t, ch, 1, "second poll should emit one OomInfo")

	got := <-ch
	assert.Equal(t, "ns", got.ContainerID.Namespace)
	assert.Equal(t, "pod-a", got.ContainerID.PodName)
	assert.Equal(t, "ctr", got.ContainerID.ContainerName)
	assert.Equal(t, model.ResourceAmount(256<<20), got.Memory)
}

func TestPrometheusObserver_FractionalIncreaseFloored(t *testing.T) {
	vpa := vpaWithOOMAnnotation("ns", "vpa1", "m")
	pod := podWithMemRequest("ns", "pod-a", "ctr", 1<<20)
	cs := &fakeClusterState{
		vpas: map[model.VpaID]*model.Vpa{vpa.ID: vpa},
		pods: map[model.PodID]*model.PodState{pod.ID: pod},
	}
	api := &fakeAPI{results: []prommodel.Value{prommodel.Vector{
		sample("pod-a", "ctr", 2.7, time.Now()),
	}}}
	ch := make(chan OomInfo, 8)
	o := newTestObserver(api, cs, ch)

	o.pollOnce(context.Background()) // first poll discarded
	o.pollOnce(context.Background())
	assert.Len(t, ch, 2, "increase 2.7 should floor to 2 events")
}

func TestPrometheusObserver_SkipsZeroOrNegativeIncrease(t *testing.T) {
	vpa := vpaWithOOMAnnotation("ns", "vpa1", "m")
	pod := podWithMemRequest("ns", "pod-a", "ctr", 1<<20)
	cs := &fakeClusterState{
		vpas: map[model.VpaID]*model.Vpa{vpa.ID: vpa},
		pods: map[model.PodID]*model.PodState{pod.ID: pod},
	}
	api := &fakeAPI{results: []prommodel.Value{prommodel.Vector{
		sample("pod-a", "ctr", 0, time.Now()),
	}}}
	ch := make(chan OomInfo, 8)
	o := newTestObserver(api, cs, ch)

	o.pollOnce(context.Background())
	o.pollOnce(context.Background())
	assert.Empty(t, ch, "zero-increase samples should not produce events")
}

func TestPrometheusObserver_SkipsUnannotatedVPAs(t *testing.T) {
	vpaPlain := &model.Vpa{ID: model.VpaID{Namespace: "ns", VpaName: "plain"}}
	cs := &fakeClusterState{
		vpas: map[model.VpaID]*model.Vpa{vpaPlain.ID: vpaPlain},
	}
	api := &fakeAPI{}
	o := newTestObserver(api, cs, make(chan OomInfo, 1))
	o.pollOnce(context.Background())
	assert.Equal(t, 0, api.calls, "no query should be issued for VPA without annotation")
}

func TestPrometheusObserver_QueryErrorDoesNotMarkSeen(t *testing.T) {
	vpa := vpaWithOOMAnnotation("ns", "vpa1", "m")
	pod := podWithMemRequest("ns", "pod-a", "ctr", 1<<20)
	cs := &fakeClusterState{
		vpas: map[model.VpaID]*model.Vpa{vpa.ID: vpa},
		pods: map[model.PodID]*model.PodState{pod.ID: pod},
	}
	failing := &fakeAPI{err: errors.New("prometheus down")}
	ch := make(chan OomInfo, 4)
	o := newTestObserver(failing, cs, ch)

	o.pollOnce(context.Background())
	o.pollOnce(context.Background())
	assert.Empty(t, ch)

	// Recover: the next successful poll must still discard (we never
	// successfully observed a baseline).
	failing.err = nil
	failing.results = []prommodel.Value{prommodel.Vector{sample("pod-a", "ctr", 1, time.Now())}}
	o.pollOnce(context.Background())
	assert.Empty(t, ch, "first successful poll after errors is the baseline; events must still be discarded")

	// Subsequent successful poll emits.
	failing.results = []prommodel.Value{prommodel.Vector{sample("pod-a", "ctr", 1, time.Now())}}
	o.pollOnce(context.Background())
	assert.Len(t, ch, 1)
}

func TestPrometheusObserver_DoesNotFilterByVPASelector(t *testing.T) {
	// The user's annotation selector is the source of scoping; the observer
	// MUST NOT additionally filter results by VPA pod selector. If the user's
	// matchers leak across workloads, that's their responsibility.
	vpa := vpaWithOOMAnnotation("ns", "vpa1", "m")
	known := podWithMemRequest("ns", "pod-a", "ctr", 1<<20)
	alsoKnown := podWithMemRequest("ns", "pod-other", "ctr", 1<<20)
	cs := &fakeClusterState{
		vpas: map[model.VpaID]*model.Vpa{vpa.ID: vpa},
		pods: map[model.PodID]*model.PodState{
			known.ID:     known,
			alsoKnown.ID: alsoKnown,
		},
	}
	api := &fakeAPI{results: []prommodel.Value{prommodel.Vector{
		sample("pod-a", "ctr", 1, time.Now()),
		sample("pod-other", "ctr", 5, time.Now()),
	}}}
	ch := make(chan OomInfo, 8)
	o := newTestObserver(api, cs, ch)

	o.pollOnce(context.Background())
	o.pollOnce(context.Background())
	assert.Len(t, ch, 6, "events from every pod returned by the user's selector should be emitted")
}

func TestPrometheusObserver_SkipsUnknownPods(t *testing.T) {
	// Pods present in Prometheus results but not in clusterState (e.g.
	// already deleted, or in a different namespace the observer doesn't
	// track) get dropped because we can't resolve their memory request.
	vpa := vpaWithOOMAnnotation("ns", "vpa1", "m")
	known := podWithMemRequest("ns", "pod-a", "ctr", 1<<20)
	cs := &fakeClusterState{
		vpas: map[model.VpaID]*model.Vpa{vpa.ID: vpa},
		pods: map[model.PodID]*model.PodState{known.ID: known},
	}
	api := &fakeAPI{results: []prommodel.Value{prommodel.Vector{
		sample("pod-a", "ctr", 1, time.Now()),
		sample("pod-ghost", "ctr", 5, time.Now()),
	}}}
	ch := make(chan OomInfo, 8)
	o := newTestObserver(api, cs, ch)

	o.pollOnce(context.Background())
	o.pollOnce(context.Background())
	assert.Len(t, ch, 1, "only the known pod should yield events")
	got := <-ch
	assert.Equal(t, "pod-a", got.ContainerID.PodName)
}

func TestPrometheusObserver_SkipsContainerWithNoMemRequest(t *testing.T) {
	vpa := vpaWithOOMAnnotation("ns", "vpa1", "m")
	pod := &model.PodState{
		ID: model.PodID{Namespace: "ns", PodName: "pod-a"},
		Containers: map[string]*model.ContainerState{
			"ctr": {Request: model.Resources{}},
		},
	}
	cs := &fakeClusterState{
		vpas: map[model.VpaID]*model.Vpa{vpa.ID: vpa},
		pods: map[model.PodID]*model.PodState{pod.ID: pod},
	}
	api := &fakeAPI{results: []prommodel.Value{prommodel.Vector{
		sample("pod-a", "ctr", 1, time.Now()),
	}}}
	ch := make(chan OomInfo, 4)
	o := newTestObserver(api, cs, ch)

	o.pollOnce(context.Background())
	o.pollOnce(context.Background())
	assert.Empty(t, ch, "containers without a memory request must be skipped")
}
