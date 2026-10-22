import {
  MachineId,
  ProjectId,
  ThreadId,
  type MachineId as MachineIdType,
  type ProjectId as ProjectIdType,
  type ScopedProjectRef,
  type ScopedThreadRef,
} from "@vekrel/contracts";

export function scopeProjectRef(
  machineId: MachineIdType,
  projectId: ProjectIdType,
): ScopedProjectRef {
  return { machineId, projectId };
}

export function scopeThreadRef(
  machineId: MachineIdType,
  threadId: ThreadId,
): ScopedThreadRef {
  return { machineId, threadId };
}

function scopedRefKey(ref: ScopedProjectRef | ScopedThreadRef): string {
  const localId = "projectId" in ref ? ref.projectId : ref.threadId;
  return `${ref.machineId}:${localId}`;
}

export function scopedProjectKey(ref: ScopedProjectRef): string {
  return scopedRefKey(ref);
}

export function scopedThreadKey(ref: ScopedThreadRef): string {
  return scopedRefKey(ref);
}

function parseScopedKey(key: string): { machineId: MachineIdType; localId: string } | null {
  const separatorIndex = key.indexOf(":");
  if (separatorIndex <= 0 || separatorIndex >= key.length - 1) {
    return null;
  }
  return {
    machineId: MachineId.make(key.slice(0, separatorIndex)),
    localId: key.slice(separatorIndex + 1),
  };
}

export function parseScopedProjectKey(key: string): ScopedProjectRef | null {
  const parsed = parseScopedKey(key);
  if (!parsed) {
    return null;
  }
  return {
    machineId: parsed.machineId,
    projectId: ProjectId.make(parsed.localId),
  };
}

export function parseScopedThreadKey(key: string): ScopedThreadRef | null {
  const parsed = parseScopedKey(key);
  if (!parsed) {
    return null;
  }
  return {
    machineId: parsed.machineId,
    threadId: ThreadId.make(parsed.localId),
  };
}
