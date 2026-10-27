// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package functionaltest

import (
	"context"
	"maps"
	"testing"

	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func createActorWithLabels(t *testing.T, tc *testContext, name string, labels map[string]string) *ateapipb.Actor {
	t.Helper()
	actor, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		Labels:        labels,
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	return actor
}

// assertActorHeld checks err is the ACTOR_HELD refusal naming hold.
func assertActorHeld(t *testing.T, err error, hold string) {
	t.Helper()
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		t.Fatalf("got %v, want FailedPrecondition", err)
	}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			if info.GetReason() != "ACTOR_HELD" || info.GetDomain() != "ate.dev" || info.GetMetadata()["hold"] != hold {
				t.Fatalf("ErrorInfo = %v, want reason ACTOR_HELD naming %s", info, hold)
			}
			return
		}
	}
	t.Fatalf("status %v carries no ErrorInfo", st)
}

func TestSetActorLabels(t *testing.T) {
	ns := namespaceForTest("ns-set-labels")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	createTemplate(t, tc, ns)
	ctx := context.Background()
	ref := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id1"}

	created := createActorWithLabels(t, tc, "id1", map[string]string{"team": "a"})
	if got := created.GetLabels(); !maps.Equal(got, map[string]string{"team": "a"}) {
		t.Fatalf("CreateActor labels = %v", got)
	}

	set, err := tc.client.SetActorLabels(ctx, &ateapipb.SetActorLabelsRequest{
		Actor:      ref,
		Labels:     map[string]string{"agent-cost/budget-group": "test-atespace.root"},
		DeleteKeys: []string{"team", "absent"},
	})
	if err != nil {
		t.Fatalf("SetActorLabels failed: %v", err)
	}
	want := map[string]string{"agent-cost/budget-group": "test-atespace.root"}
	if !maps.Equal(set.GetLabels(), want) {
		t.Errorf("SetActorLabels labels = %v, want %v", set.GetLabels(), want)
	}

	// Idempotent: the same request writes nothing.
	again, err := tc.client.SetActorLabels(ctx, &ateapipb.SetActorLabelsRequest{Actor: ref, Labels: want})
	if err != nil {
		t.Fatalf("SetActorLabels (repeat) failed: %v", err)
	}
	if again.GetMetadata().GetVersion() != set.GetMetadata().GetVersion() {
		t.Errorf("repeat SetActorLabels bumped the version %d -> %d", set.GetMetadata().GetVersion(), again.GetMetadata().GetVersion())
	}

	got, err := tc.client.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if !maps.Equal(got.GetLabels(), want) {
		t.Errorf("GetActor labels = %v, want %v", got.GetLabels(), want)
	}
	list, err := tc.client.ListActors(ctx, &ateapipb.ListActorsRequest{Atespace: testAtespace})
	if err != nil {
		t.Fatalf("ListActors failed: %v", err)
	}
	if len(list.GetActors()) != 1 || !maps.Equal(list.GetActors()[0].GetLabels(), want) {
		t.Errorf("ListActors = %v, want one actor with labels %v", list.GetActors(), want)
	}

	// UpdateActor is a whole-object replace, but keeps the stored labels.
	update := proto.CloneOf(got)
	update.Labels = nil
	updated, err := tc.client.UpdateActor(ctx, &ateapipb.UpdateActorRequest{Actor: update})
	if err != nil {
		t.Fatalf("UpdateActor failed: %v", err)
	}
	if !maps.Equal(updated.GetLabels(), want) {
		t.Errorf("UpdateActor labels = %v, want %v", updated.GetLabels(), want)
	}

	_, err = tc.client.SetActorLabels(ctx, &ateapipb.SetActorLabelsRequest{Actor: ref, Labels: map[string]string{"bad key!": "v"}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("SetActorLabels with an invalid key: got %v, want InvalidArgument", err)
	}
}

func TestAddActorHold_SuspendsRunningActorAndBlocksResume(t *testing.T) {
	ns := namespaceForTest("ns-hold-running")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	createTemplate(t, tc, ns)
	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
	ctx := context.Background()
	ref := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id1"}

	createActorWithLabels(t, tc, "id1", nil)
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}

	hold := &ateapipb.ActorHold{Name: "agent-cost/budget", Reason: "budget exhausted"}
	held, err := tc.client.AddActorHold(ctx, &ateapipb.AddActorHoldRequest{Actor: ref, Hold: hold})
	if err != nil {
		t.Fatalf("AddActorHold failed: %v", err)
	}
	if state := held.GetStatus().GetState(); state != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Fatalf("AddActorHold left the actor %s, want SUSPENDED", state)
	}
	holds := held.GetStatus().GetHolds()
	if len(holds) != 1 || holds[0].GetName() != hold.GetName() || holds[0].GetReason() != hold.GetReason() || holds[0].GetCreatedAt() == nil {
		t.Fatalf("holds = %v, want one stamped %s", holds, hold.GetName())
	}

	// Idempotent: a repeat keeps the first created_at.
	again, err := tc.client.AddActorHold(ctx, &ateapipb.AddActorHoldRequest{Actor: ref, Hold: hold})
	if err != nil {
		t.Fatalf("AddActorHold (repeat) failed: %v", err)
	}
	if got := again.GetStatus().GetHolds(); len(got) != 1 || !proto.Equal(got[0].GetCreatedAt(), holds[0].GetCreatedAt()) {
		t.Errorf("repeat AddActorHold holds = %v, want %v", got, holds)
	}

	_, err = tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref})
	assertActorHeld(t, err, "agent-cost/budget")

	// The hold is persisted, not cached.
	got, err := tc.persistence.GetActor(ctx, resources.ActorRefFromObjectRef(ref))
	if err != nil {
		t.Fatalf("store GetActor failed: %v", err)
	}
	if len(got.GetStatus().GetHolds()) != 1 {
		t.Errorf("stored holds = %v, want one", got.GetStatus().GetHolds())
	}

	removed, err := tc.client.RemoveActorHold(ctx, &ateapipb.RemoveActorHoldRequest{Actor: ref, HoldName: "agent-cost/budget"})
	if err != nil {
		t.Fatalf("RemoveActorHold failed: %v", err)
	}
	if len(removed.GetStatus().GetHolds()) != 0 || removed.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("after RemoveActorHold: holds %v state %s, want none and SUSPENDED", removed.GetStatus().GetHolds(), removed.GetStatus().GetState())
	}
	if _, err := tc.client.RemoveActorHold(ctx, &ateapipb.RemoveActorHoldRequest{Actor: ref, HoldName: "agent-cost/budget"}); err != nil {
		t.Errorf("RemoveActorHold of an absent hold: %v, want no-op", err)
	}

	resumed, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("ResumeActor after removing the hold failed: %v", err)
	}
	if !resumed.GetResumed() {
		t.Errorf("ResumeActor after removing the hold did not resume")
	}
}

func TestAddActorHold_SuspendedActor(t *testing.T) {
	ns := namespaceForTest("ns-hold-suspended")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	createTemplate(t, tc, ns)
	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
	ctx := context.Background()
	ref := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id1"}
	createActorWithLabels(t, tc, "id1", nil)

	if _, err := tc.client.AddActorHold(ctx, &ateapipb.AddActorHoldRequest{Actor: ref, Hold: &ateapipb.ActorHold{Name: "ax/loop"}}); err != nil {
		t.Fatalf("AddActorHold failed: %v", err)
	}
	_, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref})
	assertActorHeld(t, err, "ax/loop")

	got, err := tc.client.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED || got.GetStatus().GetWorkerAssignment() != nil {
		t.Errorf("held actor: state %s, assignment %v; want SUSPENDED with no worker", got.GetStatus().GetState(), got.GetStatus().GetWorkerAssignment())
	}
}

func TestAddActorHold_Validation(t *testing.T) {
	ns := namespaceForTest("ns-hold-validation")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	createTemplate(t, tc, ns)
	ctx := context.Background()
	ref := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id1"}
	createActorWithLabels(t, tc, "id1", nil)

	for _, name := range []string{"", "budget", "/budget", "Not Valid/x"} {
		_, err := tc.client.AddActorHold(ctx, &ateapipb.AddActorHoldRequest{Actor: ref, Hold: &ateapipb.ActorHold{Name: name}})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("AddActorHold(%q): got %v, want InvalidArgument", name, err)
		}
	}
	_, err := tc.client.AddActorHold(ctx, &ateapipb.AddActorHoldRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "missing"},
		Hold:  &ateapipb.ActorHold{Name: "agent-cost/budget"},
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("AddActorHold on a missing actor: got %v, want NotFound", err)
	}
}
