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
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/url"
	"slices"
	"strings"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/apivalidation"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/actorevent"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/principal"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/util/wait"
)

// Hold error details. A client branches on ErrorInfo.Reason, never on the
// message.
const (
	ActorHeldReason = "ACTOR_HELD"
	ErrorDomain     = "ate.dev"
)

// mutateRetries bounds the read-modify-write retries of a label or hold
// change that loses a version race.
const mutateRetries = 5

var (
	// holdSuspendSyncTimeout bounds how long AddActorHold waits for the
	// actor to suspend before handing the suspend to the background.
	holdSuspendSyncTimeout = 30 * time.Second
	// holdSuspendAsyncTimeout bounds a background suspend for a hold.
	holdSuspendAsyncTimeout = 5 * time.Minute
	// holdSuspendBackoff paces the suspend retries while another operation
	// holds the actor's lease or a resume is still settling.
	holdSuspendBackoff = wait.Backoff{
		Steps:    1 << 20, // bounded by the context instead
		Duration: 100 * time.Millisecond,
		Factor:   2.0,
		Jitter:   0.5,
		Cap:      5 * time.Second,
	}
)

// actorHeldError is the FAILED_PRECONDITION every resume of a held actor
// returns.
func actorHeldError(actorRef resources.ActorRef, holds []*ateapi.ActorHold) error {
	h := holds[0]
	info := &errdetails.ErrorInfo{
		Reason:   ActorHeldReason,
		Domain:   ErrorDomain,
		Metadata: map[string]string{"hold": h.GetName(), "reason": h.GetReason()},
	}
	return apierror.FailedPreconditionWithInfo(info, "actor %s held by %s: %s", actorRef, h.GetName(), h.GetReason())
}

// mapActorReadError maps a store read error to the API error a caller sees.
func mapActorReadError(actorRef resources.ActorRef, err error) error {
	if errors.Is(err, store.ErrNotFound) {
		return apierror.NotFound("Actor %s not found", actorRef)
	}
	return fmt.Errorf("while getting actor %s: %w", actorRef, err)
}

// PrefixOwners restricts the label keys and hold names under a "<prefix>/" to
// the principal IDs listed for that prefix. A prefix with no entry is open to
// every caller the authorizer admits.
type PrefixOwners map[string][]string

// ParsePrefixOwners parses "prefix=principal,prefix=principal". A prefix may
// repeat to list several owners. An empty spec returns nil.
func ParsePrefixOwners(spec string) (PrefixOwners, error) {
	if strings.TrimSpace(spec) == "" {
		return nil, nil
	}
	owners := PrefixOwners{}
	for entry := range strings.SplitSeq(spec, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		prefix, id, ok := strings.Cut(entry, "=")
		prefix, id = strings.TrimSpace(prefix), strings.TrimSpace(id)
		if !ok || prefix == "" || id == "" || strings.Contains(prefix, "/") {
			return nil, fmt.Errorf("invalid prefix owner %q: want <prefix>=<principal>", entry)
		}
		owners[prefix] = append(owners[prefix], id)
	}
	return owners, nil
}

// keyPrefix returns the "<prefix>" of "<prefix>/<key>", or "" for an
// unprefixed key.
func keyPrefix(key string) string {
	prefix, _, ok := strings.Cut(key, "/")
	if !ok {
		return ""
	}
	return prefix
}

// callerID is the authenticated principal ID, or "" when the server runs
// without authentication.
func callerID(ctx context.Context) string {
	p, ok := principal.FromContext(ctx)
	if !ok {
		return ""
	}
	return p.ID
}

// checkMetadataWriter rejects a caller inside a sandbox, and a caller that
// writes a key under a prefix it does not own.
func (s *RPCService) checkMetadataWriter(ctx context.Context, keys ...string) error {
	id := callerID(ctx)
	// Actor and worker-for-actor certificates share the actor trust domain.
	if u, err := url.Parse(id); err == nil && u.Scheme == "spiffe" && u.Host == resources.ActorSPIFFETrustDomain {
		return apierror.PermissionDenied("actor labels and holds cannot be managed from inside a sandbox")
	}
	for _, key := range keys {
		prefix := keyPrefix(key)
		owners, restricted := s.prefixOwners[prefix]
		if prefix == "" || !restricted {
			continue
		}
		if !slices.Contains(owners, id) {
			return apierror.PermissionDenied("caller %q does not own prefix %q of %q", id, prefix, key)
		}
	}
	return nil
}

// mutateActor applies mutate to a fresh read of the actor, retrying when a
// concurrent write wins the version race. mutate reports whether it changed
// anything; an unchanged actor is returned without a write.
func (s *RPCService) mutateActor(ctx context.Context, actorRef resources.ActorRef, mutate func(*ateapi.Actor) (bool, error)) (_ *ateapi.Actor, changed bool, err error) {
	for attempt := 0; ; attempt++ {
		current, err := s.impl.GetActor(ctx, actorRef)
		if err != nil {
			return nil, false, mapActorReadError(actorRef, err)
		}
		probe := proto.CloneOf(current)
		if changed, err := mutate(probe); err != nil {
			return nil, false, err
		} else if !changed {
			return current, false, nil
		}
		stored, err := s.impl.UpdateActor(ctx, actorRef, store.PreconditionFrom(current), func(toUpdate *ateapi.Actor) error {
			_, err := mutate(toUpdate)
			return err
		})
		if apierror.Code(err) == codes.Aborted && attempt < mutateRetries {
			continue
		}
		if err != nil {
			return nil, false, err
		}
		return stored, true, nil
	}
}

func (s *RPCService) SetActorLabels(ctx context.Context, req *ateapi.SetActorLabelsRequest) (*ateapi.Actor, error) {
	if errs := apivalidation.ValidateSetActorLabelsRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	actorRef := resources.ActorRefFromObjectRef(req.GetActor())
	setSpanActorRefAttributes(ctx, actorRef)
	keys := append(slices.Collect(maps.Keys(req.GetLabels())), req.GetDeleteKeys()...)
	if err := s.checkMetadataWriter(ctx, keys...); err != nil {
		return nil, err
	}

	stored, changed, err := s.mutateActor(ctx, actorRef, func(a *ateapi.Actor) (bool, error) {
		before := maps.Clone(a.GetLabels())
		if a.Labels == nil {
			a.Labels = map[string]string{}
		}
		maps.Copy(a.Labels, req.GetLabels())
		for _, k := range req.GetDeleteKeys() {
			delete(a.Labels, k)
		}
		if len(a.Labels) == 0 {
			a.Labels = nil
		}
		return !maps.Equal(before, a.GetLabels()), nil
	})
	if err != nil {
		return nil, err
	}
	if changed {
		logActorLabelsChanged(ctx, stored)
	}
	setSpanActorAttributes(ctx, stored)
	return stored, nil
}

func (s *RPCService) AddActorHold(ctx context.Context, req *ateapi.AddActorHoldRequest) (*ateapi.Actor, error) {
	if errs := apivalidation.ValidateAddActorHoldRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	actorRef := resources.ActorRefFromObjectRef(req.GetActor())
	setSpanActorRefAttributes(ctx, actorRef)
	in := req.GetHold()
	if err := s.checkMetadataWriter(ctx, in.GetName()); err != nil {
		return nil, err
	}
	createdBy := in.GetCreatedBy()
	if createdBy == "" {
		createdBy = callerID(ctx)
	}
	now := timestamppb.Now()

	stored, changed, err := s.mutateActor(ctx, actorRef, func(a *ateapi.Actor) (bool, error) {
		if a.Status == nil {
			a.Status = &ateapi.ActorStatus{}
		}
		for _, h := range a.Status.Holds {
			if h.GetName() == in.GetName() {
				if h.GetReason() == in.GetReason() {
					return false, nil
				}
				h.Reason = in.GetReason()
				return true, nil
			}
		}
		a.Status.Holds = append(a.Status.Holds, &ateapi.ActorHold{
			Name:      in.GetName(),
			Reason:    in.GetReason(),
			CreatedBy: createdBy,
			CreatedAt: now,
		})
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	if changed {
		logActorHoldChanged(ctx, stored, holdNamed(stored, in.GetName()), ateattr.HoldActionAdded)
	}

	// A hold means desired state SUSPENDED, so the hold's owner never calls
	// SuspendActor itself.
	switch stored.GetStatus().GetState() {
	case ateapi.ActorState_ACTOR_STATE_RUNNING, ateapi.ActorState_ACTOR_STATE_RESUMING, ateapi.ActorState_ACTOR_STATE_SUSPENDING:
		syncCtx, cancel := context.WithTimeout(ctx, holdSuspendSyncTimeout)
		suspended, err := s.actorWorkflow.suspendHeld(syncCtx, actorRef)
		cancel()
		if err != nil {
			slog.WarnContext(ctx, "Held actor did not suspend in time; continuing in the background",
				slog.Any("actor", actorRef), slog.Any("err", err))
			s.actorWorkflow.enforceHoldAsync(ctx, actorRef)
		} else if suspended != nil {
			stored = suspended
		}
	}
	setSpanActorAttributes(ctx, stored)
	return stored, nil
}

func (s *RPCService) RemoveActorHold(ctx context.Context, req *ateapi.RemoveActorHoldRequest) (*ateapi.Actor, error) {
	if errs := apivalidation.ValidateRemoveActorHoldRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	actorRef := resources.ActorRefFromObjectRef(req.GetActor())
	setSpanActorRefAttributes(ctx, actorRef)
	if err := s.checkMetadataWriter(ctx, req.GetHoldName()); err != nil {
		return nil, err
	}

	var removed *ateapi.ActorHold
	stored, changed, err := s.mutateActor(ctx, actorRef, func(a *ateapi.Actor) (bool, error) {
		holds := a.GetStatus().GetHolds()
		i := slices.IndexFunc(holds, func(h *ateapi.ActorHold) bool { return h.GetName() == req.GetHoldName() })
		if i < 0 {
			return false, nil
		}
		removed = proto.CloneOf(holds[i])
		a.Status.Holds = slices.Delete(holds, i, i+1)
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	if changed {
		logActorHoldChanged(ctx, stored, removed, ateattr.HoldActionRemoved)
	}
	setSpanActorAttributes(ctx, stored)
	return stored, nil
}

func holdNamed(actor *ateapi.Actor, name string) *ateapi.ActorHold {
	for _, h := range actor.GetStatus().GetHolds() {
		if h.GetName() == name {
			return h
		}
	}
	return &ateapi.ActorHold{Name: name}
}

// logActorLabelsChanged records the actor's labels after a change.
func logActorLabelsChanged(ctx context.Context, actor *ateapi.Actor) {
	attrs := ateattr.ActorLogAttrs(resources.ActorAttributionFromActor(actor))
	attrs = append(attrs, slog.String(string(ateattr.ActorLabelsKey), ateattr.ActorLabelsValue(actor.GetLabels())))
	actorevent.Log(ctx, actorevent.LabelsChanged, attrs)
}

// logActorHoldChanged records a hold added to or removed from actor, with the
// state the actor was in at the commit.
func logActorHoldChanged(ctx context.Context, actor *ateapi.Actor, hold *ateapi.ActorHold, action string) {
	attrs := ateattr.ActorLogAttrs(resources.ActorAttributionFromActor(actor))
	attrs = append(attrs,
		slog.String(string(ateattr.ActorHoldNameKey), hold.GetName()),
		slog.String(string(ateattr.ActorHoldActionKey), action),
		slog.String(string(ateattr.ActorStateKey), ateattr.ActorStateValue(actor.GetStatus().GetState())))
	if hold.GetReason() != "" {
		attrs = append(attrs, slog.String(string(ateattr.ActorHoldReasonKey), hold.GetReason()))
	}
	if hold.GetCreatedBy() != "" {
		attrs = append(attrs, slog.String(string(ateattr.ActorHoldCreatorKey), hold.GetCreatedBy()))
	}
	attrs = appendActorLabels(attrs, actor)
	actorevent.Log(ctx, actorevent.HoldChanged, attrs)
}

// suspendHeld runs the suspend workflow until the actor is asleep or no
// longer held, retrying while another operation holds the actor's lease or a
// resume is still settling. It returns the last actor it read.
func (w *ActorWorkflow) suspendHeld(ctx context.Context, actorRef resources.ActorRef) (*ateapi.Actor, error) {
	var last *ateapi.Actor
	err := wait.ExponentialBackoffWithContext(ctx, holdSuspendBackoff, func(ctx context.Context) (bool, error) {
		actor, err := w.store.GetActor(ctx, actorRef)
		if err != nil {
			return false, mapActorReadError(actorRef, err)
		}
		last = actor
		if len(actor.GetStatus().GetHolds()) == 0 {
			return true, nil
		}
		switch actor.GetStatus().GetState() {
		case ateapi.ActorState_ACTOR_STATE_RUNNING, ateapi.ActorState_ACTOR_STATE_SUSPENDING:
			suspended, err := w.SuspendActor(ctx, actorRef, suspendSync)
			if err == nil {
				last = suspended
				return true, nil
			}
			switch apierror.Code(err) {
			case codes.Aborted, codes.FailedPrecondition, codes.Unavailable:
				return false, nil
			default:
				return false, err
			}
		case ateapi.ActorState_ACTOR_STATE_RESUMING:
			// The resume in flight holds the lease; suspend once it settles.
			return false, nil
		default:
			return true, nil
		}
	})
	return last, err
}

// enforceHoldAsync suspends a held actor in the background. At most one runs
// per actor.
func (w *ActorWorkflow) enforceHoldAsync(ctx context.Context, actorRef resources.ActorRef) {
	key := actorRef.String()
	if _, running := w.holdEnforcing.LoadOrStore(key, struct{}{}); running {
		return
	}
	go func() {
		defer w.holdEnforcing.Delete(key)
		bgCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), holdSuspendAsyncTimeout)
		defer cancel()
		if _, err := w.suspendHeld(bgCtx, actorRef); err != nil {
			slog.WarnContext(bgCtx, "Failed to suspend held actor", slog.Any("actor", actorRef), slog.Any("err", err))
		}
	}()
}
