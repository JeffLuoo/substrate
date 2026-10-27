// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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
	"strings"

	"go.ate.dev/substrate/internal/actorevent"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/volume"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	storagev1listers "k8s.io/client-go/listers/storage/v1"
)

// initialExternalVolumes constructs the actor's volume objects before volume
// creation. A volume has no storage volume ID until it is provisioned.
func initialExternalVolumes(ctx context.Context, scLister storagev1listers.StorageClassLister, template *ateapipb.ActorTemplate) ([]*ateapipb.ExternalVolume, error) {
	if template == nil {
		return nil, apierror.InvalidArgument("template is required")
	}
	var volumes []*ateapipb.ExternalVolume
	for _, vol := range template.GetVolumes() {
		if vol.GetExternalVolumeTemplate() != nil {
			scName := vol.GetExternalVolumeTemplate().GetStorageClassName()
			sc, err := scLister.Get(scName)
			if err != nil {
				if k8serrors.IsNotFound(err) {
					return nil, apierror.FailedPrecondition("StorageClass %q not found", scName)
				}
				return nil, apierror.Internal("failed to get StorageClass %q: %v", scName, err)
			}

			volumes = append(volumes, &ateapipb.ExternalVolume{
				Name:       vol.GetName(),
				VolumeType: sc.Provisioner,
			})
		}
	}
	return volumes, nil
}

// createActorVolumes provisions the external volumes in volumesToCreate that have no storage volume ID, using the provided volume plugin.
// It returns the list of external volumes (with storage IDs), or an error if any creation fails.
// Any volumes processed before or during a failure are returned alongside the error so they can be persisted on the actor.
func createActorVolumes(ctx context.Context, registry VolumePluginRegistry, scLister storagev1listers.StorageClassLister, actorUID string, template *ateapipb.ActorTemplate, volumesToCreate []*ateapipb.ExternalVolume) (resultVolumes []*ateapipb.ExternalVolume, err error) {
	return createActorVolumesForActor(ctx, registry, scLister, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Uid: actorUID},
	}, template, volumesToCreate, false)
}

func createActorVolumesForActor(ctx context.Context, registry VolumePluginRegistry, scLister storagev1listers.StorageClassLister, actor *ateapipb.Actor, template *ateapipb.ActorTemplate, volumesToCreate []*ateapipb.ExternalVolume, labelActorVolumes bool) (resultVolumes []*ateapipb.ExternalVolume, err error) {
	actorUID := actor.GetMetadata().GetUid()
	resultVolumes = make([]*ateapipb.ExternalVolume, 0, len(volumesToCreate))

	var currentIdx int
	defer func() {
		if err != nil {
			// If we encounter an error, append the rest of the volumes to the result with the last
			// known state. This allows the caller to persist the state of the volumes.
			resultVolumes = append(resultVolumes, volumesToCreate[currentIdx:]...)
		}
	}()

	for idx, vol := range volumesToCreate {
		currentIdx = idx

		var specVol *ateapipb.Volume
		volName := vol.GetName()
		for _, tVol := range template.GetVolumes() {
			if tVol.GetName() == volName {
				specVol = tVol
				break
			}
		}
		if specVol == nil || specVol.GetExternalVolumeTemplate() == nil {
			return resultVolumes, apierror.NotFound("volume %q not found in template", volName)
		}

		if vol.GetStorageVolumeId() != "" {
			resultVolumes = append(resultVolumes, vol)
			continue
		}

		actVolID := actorVolumeID(actorUID, volName)

		scName := specVol.GetExternalVolumeTemplate().GetStorageClassName()
		sc, err := scLister.Get(scName)
		if err != nil {
			if k8serrors.IsNotFound(err) {
				return resultVolumes, apierror.FailedPrecondition("StorageClass %q not found", scName)
			}
			return resultVolumes, apierror.Internal("failed to get StorageClass %q: %v", scName, err)
		}

		if sc.Provisioner != vol.GetVolumeType() {
			return resultVolumes, apierror.FailedPrecondition("volume %q has mismatched type %q (expected %q from StorageClass %q)", volName, vol.GetVolumeType(), sc.Provisioner, scName)
		}

		plugin, err := registry.GetPlugin(ctx, vol.GetVolumeType())
		if err != nil {
			return resultVolumes, apierror.FailedPrecondition("failed to get volume plugin for driver %q (StorageClass %q): %v", sc.Provisioner, scName, err)
		}

		params := sc.Parameters
		if labelActorVolumes {
			params = parametersWithActorVolumeLabels(sc.Parameters, actor.GetMetadata().GetAtespace(), actorUID)
		}

		capacityStr := specVol.GetExternalVolumeTemplate().GetCapacity()
		resp, volErr := plugin.CreateVolume(ctx, volume.CreateVolumeRequest{
			Name:       actVolID,
			Capacity:   capacityStr,
			Parameters: params,
		})
		if volErr != nil {
			return resultVolumes, apierror.Internal("failed to create volume %q: %v", specVol.GetName(), volErr)
		}

		if resp.VolumeID == "" {
			// An empty ID would read as not yet provisioned and be created again.
			return resultVolumes, apierror.Internal("volume plugin returned no ID for volume %q", specVol.GetName())
		}

		logActorVolumeProvisioned(ctx, actor, template, volName, actVolID, resp.VolumeID, scName, sc.Provisioner, capacityStr)

		resultVolumes = append(resultVolumes, &ateapipb.ExternalVolume{
			Name:            volName,
			StorageVolumeId: resp.VolumeID,
			VolumeType:      sc.Provisioner,
			VolumeContext:   resp.VolumeContext,
		})
	}
	return resultVolumes, nil
}

// deleteActorVolumes deletes all external volumes in the list.
func deleteActorVolumes(ctx context.Context, registry VolumePluginRegistry, actorUID string, volumes []*ateapipb.ExternalVolume) error {
	return deleteActorVolumesForActor(ctx, registry, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Uid: actorUID},
	}, nil, volumes)
}

func deleteActorVolumesForActor(ctx context.Context, registry VolumePluginRegistry, actor *ateapipb.Actor, template *ateapipb.ActorTemplate, volumes []*ateapipb.ExternalVolume) error {
	actorUID := actor.GetMetadata().GetUid()
	if actorUID == "" {
		return errors.New("actorUID is required")
	}
	var errs []error
	for _, vol := range volumes {
		csiName := actorVolumeID(actorUID, vol.GetName())
		volID := vol.GetStorageVolumeId()
		if volID == "" {
			// If the volume hasn't been successfully created yet, it's possible
			// that it doesn't have a storage volume ID. In that case, fallback
			// to the original requested volID.
			volID = csiName
		}
		// TODO: Standardize volume plugin lookup and error handling across control plane
		// and worker plane (e.g. via a shared helper).
		plugin, err := registry.GetPlugin(ctx, vol.GetVolumeType())
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to get volume plugin for %q: %w", vol.GetVolumeType(), err))
			continue
		}
		if err := plugin.DeleteVolume(ctx, volID); err != nil {
			if status.Code(err) == codes.NotFound {
				slog.WarnContext(ctx, "Volume not found during delete, assuming already deleted", slog.String("volume_id", volID))
				continue
			}
			errs = append(errs, fmt.Errorf("failed to delete volume %q: %w", volID, err))
			continue
		}
		logActorVolumeDeleted(ctx, actor, template, vol.GetName(), csiName, volID, vol.GetVolumeType())
	}
	return errors.Join(errs...)
}

const gceLabelMaxLen = 63

// sanitizeGCELabelValue converts val into a valid GCE resource label value:
// lowercase ASCII letters, digits, underscores, and hyphens, truncated to 63
// characters.
func sanitizeGCELabelValue(val string) string {
	val = strings.ToLower(val)
	var b strings.Builder
	b.Grow(min(len(val), gceLabelMaxLen))
	for _, r := range val {
		if b.Len() >= gceLabelMaxLen {
			break
		}
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// mergeActorVolumeLabels merges sanitized ate_atespace and ate_actor_uid label
// pairs onto an existing comma-separated CSI StorageClass "labels" parameter.
func mergeActorVolumeLabels(existing, atespace, actorUID string) string {
	var parts []string
	for _, seg := range strings.Split(existing, ",") {
		seg = strings.TrimSpace(seg)
		if seg == "" {
			continue
		}
		key, _, _ := strings.Cut(seg, "=")
		key = strings.TrimSpace(key)
		if key == "ate_atespace" || key == "ate_actor_uid" {
			continue
		}
		parts = append(parts, seg)
	}
	if s := sanitizeGCELabelValue(atespace); s != "" {
		parts = append(parts, "ate_atespace="+s)
	}
	if s := sanitizeGCELabelValue(actorUID); s != "" {
		parts = append(parts, "ate_actor_uid="+s)
	}
	return strings.Join(parts, ",")
}

// parametersWithActorVolumeLabels returns a shallow copy of scParams with
// sanitized ate_atespace and ate_actor_uid merged into params["labels"].
// Cloning prevents mutating the shared StorageClass informer cache entry.
func parametersWithActorVolumeLabels(scParams map[string]string, atespace, actorUID string) map[string]string {
	merged := mergeActorVolumeLabels(scParams["labels"], atespace, actorUID)
	if merged == "" {
		return scParams
	}
	out := maps.Clone(scParams)
	if out == nil {
		out = make(map[string]string, 1)
	}
	out["labels"] = merged
	return out
}

func actorAttributionForVolume(actor *ateapipb.Actor, template *ateapipb.ActorTemplate) resources.ActorAttribution {
	attr := resources.ActorAttributionFromActor(actor)
	if attr.TemplateAtespace == "" && template != nil {
		attr.TemplateAtespace = template.GetMetadata().GetAtespace()
	}
	if attr.TemplateName == "" && template != nil {
		attr.TemplateName = template.GetMetadata().GetName()
	}
	return attr
}

func parseVolumeCapacityBytes(capacity string) (int64, bool) {
	if capacity == "" {
		return 0, false
	}
	q, err := resource.ParseQuantity(capacity)
	if err != nil {
		return 0, false
	}
	return q.Value(), true
}

func logActorVolumeProvisioned(ctx context.Context, actor *ateapipb.Actor, template *ateapipb.ActorTemplate, volName, csiName, storageVolumeID, storageClass, driver, capacity string) {
	capBytes, _ := parseVolumeCapacityBytes(capacity)
	attrs := ateattr.ActorLogAttrs(actorAttributionForVolume(actor, template))
	attrs = append(attrs,
		slog.String(string(ateattr.VolumeNameKey), volName),
		slog.String(string(ateattr.VolumeCSINameKey), csiName),
		slog.String(string(ateattr.VolumeStorageIDKey), storageVolumeID),
		slog.String(string(ateattr.VolumeStorageClassKey), storageClass),
		slog.String(string(ateattr.VolumeDriverKey), driver),
		slog.Int64(string(ateattr.VolumeCapacityBytesKey), capBytes),
	)
	attrs = appendActorLabels(attrs, actor)
	actorevent.Log(ctx, actorevent.VolumeProvisioned, attrs)
}

func logActorVolumeDeleted(ctx context.Context, actor *ateapipb.Actor, template *ateapipb.ActorTemplate, volName, csiName, storageVolumeID, driver string) {
	attrs := ateattr.ActorLogAttrs(actorAttributionForVolume(actor, template))
	attrs = append(attrs,
		slog.String(string(ateattr.VolumeNameKey), volName),
		slog.String(string(ateattr.VolumeCSINameKey), csiName),
		slog.String(string(ateattr.VolumeStorageIDKey), storageVolumeID),
		slog.String(string(ateattr.VolumeDriverKey), driver),
	)
	for _, tVol := range template.GetVolumes() {
		if tVol.GetName() == volName && tVol.GetExternalVolumeTemplate() != nil {
			ext := tVol.GetExternalVolumeTemplate()
			if scName := ext.GetStorageClassName(); scName != "" {
				attrs = append(attrs, slog.String(string(ateattr.VolumeStorageClassKey), scName))
			}
			if capBytes, ok := parseVolumeCapacityBytes(ext.GetCapacity()); ok {
				attrs = append(attrs, slog.Int64(string(ateattr.VolumeCapacityBytesKey), capBytes))
			}
			break
		}
	}
	attrs = appendActorLabels(attrs, actor)
	actorevent.Log(ctx, actorevent.VolumeDeleted, attrs)
}

// getMountedActorVolumes filters the actor's volumes and returns only those that are declared and mounted in the ActorTemplate.
func getMountedActorVolumes(ctx context.Context, ref *ateapipb.ObjectRef, volumes []*ateapipb.ExternalVolume, template *ateapipb.ActorTemplate) []*ateapipb.ExternalVolume {
	var mounted []*ateapipb.ExternalVolume
	for _, vol := range volumes {
		// Find the corresponding volume in the ActorTemplate to check if it's mounted
		var matchedTemplateVol *ateapipb.Volume
		for _, tVol := range template.GetVolumes() {
			if vol.GetName() == tVol.GetName() {
				matchedTemplateVol = tVol
				break
			}
		}

		if matchedTemplateVol == nil {
			slog.WarnContext(ctx, "Volume not found in template, skipping", slog.String("volume_id", vol.GetStorageVolumeId()))
			continue
		}

		if !isVolumeMounted(matchedTemplateVol.GetName(), template) {
			slog.InfoContext(ctx, "Volume not mounted in template, skipping", slog.String("volume_id", vol.GetStorageVolumeId()))
			continue
		}
		mounted = append(mounted, vol)
	}
	return mounted
}

func actorVolumeID(actorUID string, volumeName string) string {
	return fmt.Sprintf("substrate-%s-%s", actorUID, volumeName)
}

// detachActorVolumes detaches all mounted external volumes for an actor from its assigned node.
func detachActorVolumes(ctx context.Context, registry VolumePluginRegistry, actor *ateapipb.Actor, template *ateapipb.ActorTemplate, action string) error {
	node := actor.GetStatus().GetAssignedNode()
	if node == "" {
		slog.WarnContext(ctx, fmt.Sprintf("Actor has no assigned node during %s, skipping detach volumes", action), slog.String("actor_id", actor.GetMetadata().GetName()))
		return nil
	}

	ref := &ateapipb.ObjectRef{Atespace: actor.GetMetadata().GetAtespace(), Name: actor.GetMetadata().GetName()}
	// If the template is available, only detach volumes that are actively mounted
	// in the template's containers. If the template is missing/deleted, fall back to
	// attempting detachment for all external volumes recorded on the actor so we do
	// not orphan attached disks on the worker node.
	volumesToDetach := actor.GetStatus().GetExternalVolumes()
	if template != nil {
		volumesToDetach = getMountedActorVolumes(ctx, ref, actor.GetStatus().GetExternalVolumes(), template)
	}
	// Collect errors for all volumes to detach, but continue processing so we attempt to detach all volumes.
	var errs []error
	for _, vol := range volumesToDetach {
		// StorageVolumeId is only populated once the volume is provisioned.
		// Skip volumes that were never created (e.g. creation failed).
		if vol.GetStorageVolumeId() == "" {
			slog.WarnContext(ctx, "Volume has no storage volume ID, skipping detach", slog.String("volume_name", vol.GetName()), slog.String("actor_id", actor.GetMetadata().GetName()))
			continue
		}
		slog.InfoContext(ctx, "Detaching volume from node", slog.String("volume_id", vol.GetStorageVolumeId()), slog.String("node", node))
		plugin, err := registry.GetPlugin(ctx, vol.GetVolumeType())
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to get volume plugin for %q: %w", vol.GetVolumeType(), err))
			continue
		}
		if err := plugin.DetachVolume(ctx, vol.GetStorageVolumeId(), node); err != nil {
			if status.Code(err) == codes.NotFound {
				slog.WarnContext(ctx, "Volume not found during detach, assuming already detached", slog.String("volume_id", vol.GetStorageVolumeId()), slog.String("node", node))
				continue
			}
			errs = append(errs, fmt.Errorf("failed to detach volume %q from node %q: %w", vol.GetStorageVolumeId(), node, err))
		}
	}
	return errors.Join(errs...)
}
