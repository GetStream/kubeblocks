/*
Copyright (C) 2022-2026 ApeCloud Co., Ltd

This file is part of KubeBlocks project

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package cluster

import (
	"context"
	"sort"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	appsv1 "github.com/apecloud/kubeblocks/apis/apps/v1"
	"github.com/apecloud/kubeblocks/pkg/controller/graph"
	"github.com/apecloud/kubeblocks/pkg/controller/model"
	ictrlutil "github.com/apecloud/kubeblocks/pkg/controllerutil"
)

const (
	serialTestNS       = "default"
	serialTestCluster  = "c1"
	serialTestSharding = "shard"
	serialTestSDName   = "sd"
)

func TestComponentIsReady(t *testing.T) {
	ready := &appsv1.Component{}
	ready.Generation = 2
	ready.Status.ObservedGeneration = 2
	ready.Status.Phase = appsv1.RunningComponentPhase
	if !componentIsReady(ready) {
		t.Fatalf("running + observed generation should be ready")
	}

	rolling := ready.DeepCopy()
	rolling.Status.ObservedGeneration = 1
	if componentIsReady(rolling) {
		t.Fatalf("generation not yet observed should not be ready")
	}

	notRunning := ready.DeepCopy()
	notRunning.Status.Phase = ""
	if componentIsReady(notRunning) {
		t.Fatalf("non-running phase should not be ready")
	}

	if componentIsReady(nil) {
		t.Fatalf("nil component should not be ready")
	}

	stopped := &appsv1.Component{}
	stopped.Generation = 1
	stopped.Status.ObservedGeneration = 1
	stopped.Spec.Stop = ptr.To(true)
	stopped.Status.Phase = appsv1.StoppedComponentPhase
	if !componentIsReady(stopped) {
		t.Fatalf("stopped component in stopped phase should be ready")
	}
}

func newSerialTestGraph(cluster *appsv1.Cluster) (model.GraphClient, *graph.DAG) {
	scheme := runtime.NewScheme()
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	graphCli := model.NewGraphClient(fake.NewClientBuilder().WithScheme(scheme).Build())
	dag := graph.NewDAG()
	graphCli.Root(dag, cluster, cluster, model.ActionStatusPtr())
	return graphCli, dag
}

func newSerialTestCtx(cluster *appsv1.Cluster, strategy *appsv1.UpdateStrategy) *clusterTransformContext {
	return &clusterTransformContext{
		Context:   context.Background(),
		Cluster:   cluster,
		shardings: []*appsv1.ClusterSharding{{Name: serialTestSharding, ShardingDef: serialTestSDName}},
		shardingDefs: map[string]*appsv1.ShardingDefinition{
			serialTestSDName: {Spec: appsv1.ShardingDefinitionSpec{UpdateStrategy: strategy}},
		},
	}
}

func serialTestComp(name string, ann map[string]string, gen int64, observed int64, phase appsv1.ComponentPhase) *appsv1.Component {
	comp := &appsv1.Component{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   serialTestNS,
			Annotations: ann,
			Generation:  gen,
		},
	}
	comp.Status.ObservedGeneration = observed
	comp.Status.Phase = phase
	return comp
}

func updatedShardNames(graphCli model.GraphClient, dag *graph.DAG) []string {
	var names []string
	for _, obj := range graphCli.FindAll(dag, &appsv1.Component{}) {
		if graphCli.IsAction(dag, obj, model.ActionUpdatePtr()) {
			names = append(names, obj.GetName())
		}
	}
	sort.Strings(names)
	return names
}

// pending shard: proto carries an annotation the running comp lacks -> a diff.
func pendingShard(name string) (running, proto *appsv1.Component) {
	running = serialTestComp(name, nil, 1, 1, appsv1.RunningComponentPhase)
	proto = serialTestComp(name, map[string]string{"kubeblocks.io/restart": "now"}, 0, 0, "")
	return
}

// appliedReady shard: running matches proto and is fully reconciled.
func appliedReadyShard(name string) (running, proto *appsv1.Component) {
	ann := map[string]string{"kubeblocks.io/restart": "now"}
	running = serialTestComp(name, ann, 2, 2, appsv1.RunningComponentPhase)
	proto = serialTestComp(name, ann, 0, 0, "")
	return
}

// appliedRolling shard: spec applied (no diff) but still rolling (generation not observed).
func appliedRollingShard(name string) (running, proto *appsv1.Component) {
	ann := map[string]string{"kubeblocks.io/restart": "now"}
	running = serialTestComp(name, ann, 2, 1, appsv1.UpdatingComponentPhase)
	proto = serialTestComp(name, ann, 0, 0, "")
	return
}

func TestShardingUpdateSerial_RollsOneShardThenRequeues(t *testing.T) {
	cluster := &appsv1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: serialTestCluster, Namespace: serialTestNS, Generation: 5}}
	transCtx := newSerialTestCtx(cluster, ptr.To(appsv1.SerialStrategy))
	graphCli, dag := newSerialTestGraph(cluster)
	transCtx.Client = graphCli

	runA, protoA := pendingShard("c1-shard-a")
	runB, protoB := pendingShard("c1-shard-b")
	running := map[string]*appsv1.Component{runA.Name: runA, runB.Name: runB}
	proto := map[string]*appsv1.Component{protoA.Name: protoA, protoB.Name: protoB}

	h := &clusterShardingHandler{}
	err := h.updateComps(transCtx, dag, serialTestSharding, running, proto, sets.New(runA.Name, runB.Name))

	if !ictrlutil.IsDelayedRequeueError(err) {
		t.Fatalf("serial update with multiple pending shards must requeue, got: %v", err)
	}
	got := updatedShardNames(graphCli, dag)
	if len(got) != 1 || got[0] != "c1-shard-a" {
		t.Fatalf("serial update must roll exactly the first shard, got: %v", got)
	}
}

func TestShardingUpdateSerial_AdvancesAfterReady(t *testing.T) {
	cluster := &appsv1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: serialTestCluster, Namespace: serialTestNS, Generation: 5}}
	transCtx := newSerialTestCtx(cluster, ptr.To(appsv1.SerialStrategy))
	graphCli, dag := newSerialTestGraph(cluster)
	transCtx.Client = graphCli

	runA, protoA := appliedReadyShard("c1-shard-a") // done
	runB, protoB := pendingShard("c1-shard-b")      // next
	running := map[string]*appsv1.Component{runA.Name: runA, runB.Name: runB}
	proto := map[string]*appsv1.Component{protoA.Name: protoA, protoB.Name: protoB}

	h := &clusterShardingHandler{}
	err := h.updateComps(transCtx, dag, serialTestSharding, running, proto, sets.New(runA.Name, runB.Name))

	if !ictrlutil.IsDelayedRequeueError(err) {
		t.Fatalf("serial update with a remaining shard must requeue, got: %v", err)
	}
	got := updatedShardNames(graphCli, dag)
	if len(got) != 1 || got[0] != "c1-shard-b" {
		t.Fatalf("serial update must advance to the next shard once the prior is ready, got: %v", got)
	}
}

func TestShardingUpdateSerial_WaitsForRollingShard(t *testing.T) {
	cluster := &appsv1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: serialTestCluster, Namespace: serialTestNS, Generation: 5}}
	transCtx := newSerialTestCtx(cluster, ptr.To(appsv1.SerialStrategy))
	graphCli, dag := newSerialTestGraph(cluster)
	transCtx.Client = graphCli

	runA, protoA := appliedRollingShard("c1-shard-a") // applied but still rolling
	runB, protoB := pendingShard("c1-shard-b")        // must NOT start yet
	running := map[string]*appsv1.Component{runA.Name: runA, runB.Name: runB}
	proto := map[string]*appsv1.Component{protoA.Name: protoA, protoB.Name: protoB}

	h := &clusterShardingHandler{}
	err := h.updateComps(transCtx, dag, serialTestSharding, running, proto, sets.New(runA.Name, runB.Name))

	if !ictrlutil.IsDelayedRequeueError(err) {
		t.Fatalf("serial update must requeue while a shard is still rolling, got: %v", err)
	}
	got := updatedShardNames(graphCli, dag)
	if len(got) != 0 {
		t.Fatalf("serial update must not start the next shard while the prior is rolling, got: %v", got)
	}
}

func TestShardingUpdateParallel_PreservesBehavior(t *testing.T) {
	for _, tc := range []struct {
		name     string
		strategy *appsv1.UpdateStrategy
	}{
		{"explicit-parallel", ptr.To(appsv1.ParallelStrategy)},
		{"unset-defaults-to-parallel-behavior", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cluster := &appsv1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: serialTestCluster, Namespace: serialTestNS, Generation: 5}}
			transCtx := newSerialTestCtx(cluster, tc.strategy)
			graphCli, dag := newSerialTestGraph(cluster)
			transCtx.Client = graphCli

			runA, protoA := pendingShard("c1-shard-a")
			runB, protoB := pendingShard("c1-shard-b")
			running := map[string]*appsv1.Component{runA.Name: runA, runB.Name: runB}
			proto := map[string]*appsv1.Component{protoA.Name: protoA, protoB.Name: protoB}

			h := &clusterShardingHandler{}
			err := h.updateComps(transCtx, dag, serialTestSharding, running, proto, sets.New(runA.Name, runB.Name))

			if err != nil {
				t.Fatalf("parallel update must not requeue, got: %v", err)
			}
			got := updatedShardNames(graphCli, dag)
			if len(got) != 2 {
				t.Fatalf("parallel update must roll all changed shards at once, got: %v", got)
			}
		})
	}
}
