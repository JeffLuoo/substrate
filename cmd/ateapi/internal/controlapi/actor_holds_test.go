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

package controlapi

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/principal"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
)

func TestParsePrefixOwners(t *testing.T) {
	tests := []struct {
		spec    string
		want    PrefixOwners
		wantErr bool
	}{
		{spec: "", want: nil},
		{spec: " agent-cost=spiffe://a , ax=spiffe://b,agent-cost=spiffe://c ", want: PrefixOwners{
			"agent-cost": {"spiffe://a", "spiffe://c"},
			"ax":         {"spiffe://b"},
		}},
		{spec: "agent-cost", wantErr: true},
		{spec: "=spiffe://a", wantErr: true},
		{spec: "agent-cost=", wantErr: true},
		{spec: "agent-cost/x=spiffe://a", wantErr: true},
	}
	for _, tt := range tests {
		got, err := ParsePrefixOwners(tt.spec)
		if (err != nil) != tt.wantErr {
			t.Errorf("ParsePrefixOwners(%q) error = %v, wantErr %v", tt.spec, err, tt.wantErr)
			continue
		}
		if diff := cmp.Diff(tt.want, got); diff != "" {
			t.Errorf("ParsePrefixOwners(%q) mismatch (-want +got):\n%s", tt.spec, diff)
		}
	}
}

func TestCheckMetadataWriter(t *testing.T) {
	const costService = "spiffe://cluster.local/ns/ate-cost/sa/ate-cost"
	s := &RPCService{prefixOwners: PrefixOwners{"agent-cost": {costService}}}
	as := func(id string) context.Context {
		return principal.InjectContext(context.Background(), principal.PrincipalInfo{ID: id, Kind: principal.KindMTLS})
	}
	actorID := resources.ActorSPIFFEID(resources.ActorRef{Atespace: "team-a", Name: "runner"}).String()
	workerID := resources.WorkerForActorSPIFFEID(resources.ActorRef{Atespace: "team-a", Name: "runner"}).String()

	tests := []struct {
		name string
		ctx  context.Context
		keys []string
		want codes.Code
	}{
		{name: "owner writes its prefix", ctx: as(costService), keys: []string{"agent-cost/budget"}, want: codes.OK},
		{name: "other caller on an owned prefix", ctx: as("user:alice"), keys: []string{"agent-cost/budget"}, want: codes.PermissionDenied},
		{name: "unauthenticated caller on an owned prefix", ctx: context.Background(), keys: []string{"agent-cost/budget"}, want: codes.PermissionDenied},
		{name: "any caller on an unowned prefix", ctx: as("user:alice"), keys: []string{"ax/loop", "team"}, want: codes.OK},
		{name: "unauthenticated caller on an unowned prefix", ctx: context.Background(), keys: []string{"ax/loop"}, want: codes.OK},
		{name: "actor inside the sandbox", ctx: as(actorID), keys: []string{"team"}, want: codes.PermissionDenied},
		{name: "worker for the actor", ctx: as(workerID), keys: nil, want: codes.PermissionDenied},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := apierror.Code(s.checkMetadataWriter(tt.ctx, tt.keys...)); got != tt.want {
				t.Errorf("checkMetadataWriter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestActorHeldError(t *testing.T) {
	err := actorHeldError(resources.ActorRef{Atespace: "team-a", Name: "runner"}, []*ateapipb.ActorHold{
		{Name: "agent-cost/budget", Reason: "budget exhausted"},
		{Name: "ax/loop"},
	})
	st, _ := apierror.FromError(err)
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("code = %v, want FailedPrecondition", st.Code())
	}
	if want := "actor team-a/runner held by agent-cost/budget: budget exhausted"; st.Message() != want {
		t.Errorf("message = %q, want %q", st.Message(), want)
	}
	if len(st.Details()) != 1 {
		t.Fatalf("details = %v, want one ErrorInfo", st.Details())
	}
	info := st.Details()[0].(*errdetails.ErrorInfo)
	want := map[string]string{"hold": "agent-cost/budget", "reason": "budget exhausted"}
	if info.GetReason() != ActorHeldReason || info.GetDomain() != ErrorDomain || !cmp.Equal(info.GetMetadata(), want) {
		t.Errorf("ErrorInfo = %v", info)
	}
}
