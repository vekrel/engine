import type {
  MachineId,
  OrchestrationV2ShellSnapshot,
  ProjectId,
  ScopedProjectRef,
  ScopedThreadRef,
  ThreadId,
} from "@vekrel/contracts";
import { isProviderNativeSubagentThread, OrchestrationV2ThreadShell } from "@vekrel/contracts";
import * as Schema from "effect/Schema";
import { Atom } from "effect/reactivity";

import type { MachineThreadShell, PendingSubagentRequest } from "./models.ts";
import { presentThreadShell, threadRuntimeIsActive } from "./models.ts";
import { type MachineCatalogState, enabledMachineIds } from "./connections.ts";
import {
  arrayElementsEqual,
  parseProjectRefCollectionKey,
  parseThreadKey,
  projectRefCollectionKey,
  threadKey,
  threadRefsEqual,
} from "./entities.ts";

const EMPTY_THREADS: ReadonlyArray<OrchestrationV2ThreadShell> = Object.freeze([]);
const EMPTY_SCOPED_THREAD_REFS: ReadonlyArray<ScopedThreadRef> = Object.freeze([]);
const EMPTY_THREAD_INDEX: ReadonlyMap<ThreadId, OrchestrationV2ThreadShell> = new Map();
const EMPTY_SUBAGENT_REQUESTS: ReadonlyMap<ThreadId, PendingSubagentRequest> = new Map();
const EMPTY_THREAD_REFS_BY_PROJECT: ReadonlyMap<
  ProjectId,
  ReadonlyArray<ScopedThreadRef>
> = new Map();

// An async question outlives the subagent's run, but once that run ends the
// parent is no longer waiting on the answer, so it stops counting as input.
// The activity run stays set when a follow-up is queued behind a blocked run;
// older servers omit it, so fall back to the latest run's status.
const isChildRunLive = (
  thread: Pick<OrchestrationV2ThreadShell, "activityRunStatus" | "status">,
) =>
  thread.activityRunStatus !== undefined
    ? thread.activityRunStatus !== null
    : thread.status === "preparing" ||
      thread.status === "starting" ||
      thread.status === "running" ||
      thread.status === "waiting";

/** Provider-native
    subagents ask on their parent, so they are skipped. */
function subagentRequest(thread: OrchestrationV2ThreadShell): PendingSubagentRequest | null {
  const kind = thread.pendingRuntimeRequest?.kind;
  if (
    kind === undefined ||
    kind === "auth_refresh" ||
    thread.lineage.relationshipToParent !== "subagent" ||
    isProviderNativeSubagentThread(thread) ||
    !isChildRunLive(thread)
  ) {
    return null;
  }
  return kind === "user_input" ? "user_input" : "approval";
}

export function pendingSubagentRequestsByThread(
  threads: ReadonlyArray<OrchestrationV2ThreadShell>,
): ReadonlyMap<ThreadId, PendingSubagentRequest> {
  const requests = new Map<ThreadId, PendingSubagentRequest>();
  let byId: ReadonlyMap<ThreadId, OrchestrationV2ThreadShell> | undefined;
  for (const child of threads) {
    const request = subagentRequest(child);
    if (request === null) continue;
    byId ??= new Map(threads.map((thread) => [thread.id, thread] as const));
    const seen = new Set<ThreadId>([child.id]);
    let current: OrchestrationV2ThreadShell | undefined = child;
    while (current?.lineage.relationshipToParent === "subagent") {
      const parentId: ThreadId | null = current.lineage.parentThreadId;
      if (parentId === null || seen.has(parentId)) break;
      seen.add(parentId);
      if (requests.get(parentId) !== "approval") requests.set(parentId, request);
      current = byId.get(parentId);
    }
  }
  return requests.size === 0 ? EMPTY_SUBAGENT_REQUESTS : requests;
}

export function childThreadInputsTitle(
  children: ReadonlyArray<Pick<MachineThreadShell, "hasPendingApprovals">>,
): string {
  const approvals = children.filter((child) => child.hasPendingApprovals).length;
  const need = approvals === 0 ? "input" : approvals === children.length ? "approval" : "you";
  return children.length === 1
    ? `Subagent needs ${need}`
    : `${children.length} subagents need ${need}`;
}

const sameWireThreadShell = Schema.toEquivalence(OrchestrationV2ThreadShell);

// While a run streams, the server bumps the shell's timestamps, item counts and
// latest message several times a second. Only the zero crossing of the counts
// says anything (whether the thread has started). Once the run stops every
// field counts again, so snooze wakes and failure times read the current value.
function sameApartFromStreaming(
  previous: MachineThreadShell,
  next: MachineThreadShell,
): boolean {
  if (
    previous.machineId !== next.machineId ||
    previous.pendingSubagentRequest !== next.pendingSubagentRequest ||
    !threadRuntimeIsActive(next.runtime)
  ) {
    return false;
  }
  const before = previous.source;
  const after = next.source;
  return (
    before.itemCount > 0 === after.itemCount > 0 &&
    before.visibleItemCount > 0 === after.visibleItemCount > 0 &&
    sameWireThreadShell(
      {
        ...before,
        updatedAt: after.updatedAt,
        itemCount: after.itemCount,
        visibleItemCount: after.visibleItemCount,
        latestVisibleMessage: after.latestVisibleMessage,
      },
      after,
    )
  );
}

export function createMachineThreadShellAtoms(input: {
  readonly catalogValueAtom: Atom.Atom<MachineCatalogState>;
  readonly snapshotAtom: (
    machineId: MachineId,
  ) => Atom.Atom<OrchestrationV2ShellSnapshot | null>;
}) {
  // Point reads and aggregate lists share values without keeping an atom alive
  // for every listed thread. Replaced source objects can be collected.
  const scopedThreads = new WeakMap<
    OrchestrationV2ThreadShell,
    Map<string, MachineThreadShell>
  >();
  const scopedThread = (
    machineId: MachineId,
    thread: OrchestrationV2ThreadShell,
    subagentRequests: ReadonlyMap<ThreadId, PendingSubagentRequest>,
  ) => {
    let byMachine = scopedThreads.get(thread);
    if (byMachine === undefined) {
      byMachine = new Map();
      scopedThreads.set(thread, byMachine);
    }
    const request = subagentRequests.get(thread.id) ?? null;
    const key = request === null ? machineId : `${machineId}:${request}`;
    let value = byMachine.get(key);
    if (value === undefined) {
      value = presentThreadShell(machineId, thread, request);
      byMachine.set(key, value);
    }
    return value;
  };

  const machineThreadsAtom = Atom.family((machineId: MachineId) =>
    Atom.make(
      (get): ReadonlyArray<OrchestrationV2ThreadShell> =>
        get(input.snapshotAtom(machineId))?.threads ?? EMPTY_THREADS,
    ).pipe(Atom.withLabel(`machine-threads:${machineId}`)),
  );

  const machineThreadIndexAtom = Atom.family((machineId: MachineId) =>
    Atom.make((get): ReadonlyMap<ThreadId, OrchestrationV2ThreadShell> => {
      const threads = get(machineThreadsAtom(machineId));
      if (threads.length === 0) {
        return EMPTY_THREAD_INDEX;
      }
      return new Map(threads.map((thread) => [thread.id, thread] as const));
    }).pipe(Atom.withLabel(`machine-thread-index:${machineId}`)),
  );

  const pendingSubagentRequestsAtom = Atom.family((machineId: MachineId) => {
    let previous = EMPTY_SUBAGENT_REQUESTS;
    return Atom.make((get) => {
      const next = pendingSubagentRequestsByThread(get(machineThreadsAtom(machineId)));
      if (
        next.size === previous.size &&
        [...next].every(([threadId, request]) => previous.get(threadId) === request)
      ) {
        return previous;
      }
      previous = next;
      return next;
    }).pipe(Atom.withLabel(`machine-pending-subagent-requests:${machineId}`));
  });

  const machineThreadRefsAtom = Atom.family((machineId: MachineId) => {
    let previous: ReadonlyArray<ScopedThreadRef> = [];
    return Atom.make((get) => {
      const next = get(machineThreadsAtom(machineId)).map((thread) => ({
        machineId,
        threadId: thread.id,
      }));
      if (threadRefsEqual(previous, next)) {
        return previous;
      }
      previous = next;
      return next;
    }).pipe(Atom.withLabel(`machine-thread-refs:${machineId}`));
  });

  const machineThreadRefsByProjectAtom = Atom.family((machineId: MachineId) => {
    let previous: ReadonlyMap<
      ProjectId,
      ReadonlyArray<ScopedThreadRef>
    > = EMPTY_THREAD_REFS_BY_PROJECT;
    return Atom.make((get) => {
      const grouped = new Map<ProjectId, ScopedThreadRef[]>();
      for (const thread of get(machineThreadsAtom(machineId))) {
        const refs = grouped.get(thread.projectId);
        const ref = { machineId, threadId: thread.id };
        if (refs === undefined) {
          grouped.set(thread.projectId, [ref]);
        } else {
          refs.push(ref);
        }
      }
      if (grouped.size === 0) {
        previous = EMPTY_THREAD_REFS_BY_PROJECT;
        return previous;
      }
      const next = new Map<ProjectId, ReadonlyArray<ScopedThreadRef>>();
      for (const [projectId, refs] of grouped) {
        const previousRefs = previous.get(projectId);
        next.set(
          projectId,
          previousRefs !== undefined && threadRefsEqual(previousRefs, refs) ? previousRefs : refs,
        );
      }
      const previousProjectIds = [...previous.keys()];
      if (
        next.size === previous.size &&
        [...next].every(
          ([projectId, refs], index) =>
            previousProjectIds[index] === projectId && previous.get(projectId) === refs,
        )
      ) {
        return previous;
      }
      previous = next;
      return previous;
    }).pipe(Atom.withLabel(`machine-thread-refs-by-project:${machineId}`));
  });

  const threadShellAtomFamily = Atom.family((key: string) => {
    const ref = parseThreadKey(key);
    return Atom.make((get) => {
      const source = get(machineThreadIndexAtom(ref.machineId)).get(ref.threadId) ?? null;
      return source === null
        ? null
        : scopedThread(
            ref.machineId,
            source,
            get(pendingSubagentRequestsAtom(ref.machineId)),
          );
    }).pipe(Atom.withLabel(`machine-thread-shell:${key}`));
  });

  // Keeps the previous shell while only streaming ticks changed. During a run,
  // its timestamps, item counts and latest message are as of the last kept
  // change; anything that needs them current reads threadShellAtom.
  const quietThreadShellAtomFamily = Atom.family((key: string) => {
    let previous: MachineThreadShell | null = null;
    return Atom.make((get) => {
      const next = get(threadShellAtomFamily(key));
      if (previous !== null && next !== null && sameApartFromStreaming(previous, next)) {
        return previous;
      }
      previous = next;
      return next;
    }).pipe(Atom.withLabel(`machine-quiet-thread-shell:${key}`));
  });

  // Child approvals and questions are hidden with their sidebar rows. Read their
  // shell summaries, without subscribing to every child transcript.
  const childThreadInputsAtomFamily = Atom.family((key: string) => {
    const ref = parseThreadKey(key);
    let previous: ReadonlyArray<MachineThreadShell> = [];
    return Atom.make((get) => {
      const children = new Map<ThreadId, OrchestrationV2ThreadShell[]>();
      for (const thread of get(machineThreadsAtom(ref.machineId))) {
        const parent = thread.lineage.parentThreadId;
        if (parent === null || thread.lineage.relationshipToParent !== "subagent") continue;
        const siblings = children.get(parent);
        if (siblings) siblings.push(thread);
        else children.set(parent, [thread]);
      }
      const subagentRequests = get(pendingSubagentRequestsAtom(ref.machineId));
      const seen = new Set<ThreadId>([ref.threadId]);
      const pending = [ref.threadId];
      const next: MachineThreadShell[] = [];
      for (const parent of pending) {
        for (const child of children.get(parent) ?? []) {
          if (seen.has(child.id)) continue;
          seen.add(child.id);
          pending.push(child.id);
          if (subagentRequest(child) !== null) {
            next.push(scopedThread(ref.machineId, child, subagentRequests));
          }
        }
      }
      // Indicators read only ids, titles and the request kind, so other shell
      // updates keep the old array.
      if (
        previous.length === next.length &&
        previous.every(
          (thread, index) =>
            thread.id === next[index]?.id &&
            thread.title === next[index]?.title &&
            thread.hasPendingApprovals === next[index]?.hasPendingApprovals,
        )
      ) {
        return previous;
      }
      previous = next;
      return next;
    }).pipe(Atom.withLabel(`machine-child-thread-inputs:${key}`));
  });

  const threadShellsForProjectRefsAtomFamily = Atom.family((key: string) => {
    const projectRefs = parseProjectRefCollectionKey(key);
    let previous: ReadonlyArray<MachineThreadShell> = [];
    return Atom.make((get) => {
      const next: MachineThreadShell[] = [];
      const seen = new Set<string>();
      for (const projectRef of projectRefs) {
        const refs =
          get(machineThreadRefsByProjectAtom(projectRef.machineId)).get(
            projectRef.projectId,
          ) ?? EMPTY_SCOPED_THREAD_REFS;
        if (refs.length === 0) continue;
        const threads = get(machineThreadIndexAtom(projectRef.machineId));
        const subagentRequests = get(pendingSubagentRequestsAtom(projectRef.machineId));
        for (const ref of refs) {
          const key = threadKey(ref);
          if (seen.has(key)) {
            continue;
          }
          seen.add(key);
          const thread = threads.get(ref.threadId);
          if (thread !== undefined) {
            next.push(scopedThread(ref.machineId, thread, subagentRequests));
          }
        }
      }
      if (arrayElementsEqual(previous, next)) {
        return previous;
      }
      previous = next;
      return previous;
    }).pipe(Atom.withLabel(`machine-thread-shells-for-projects:${key}`));
  });

  let previousThreadRefs: ReadonlyArray<ScopedThreadRef> = [];
  const threadRefsAtom = Atom.make((get) => {
    const refs: ScopedThreadRef[] = [];
    for (const machineId of enabledMachineIds(get(input.catalogValueAtom))) {
      refs.push(...get(machineThreadRefsAtom(machineId)));
    }
    if (threadRefsEqual(previousThreadRefs, refs)) {
      return previousThreadRefs;
    }
    previousThreadRefs = refs;
    return refs;
  }).pipe(Atom.withLabel("machine-thread-refs"));

  let previousThreadShells: ReadonlyArray<MachineThreadShell> = [];
  const threadShellsAtom = Atom.make((get) => {
    const next: MachineThreadShell[] = [];
    for (const machineId of enabledMachineIds(get(input.catalogValueAtom))) {
      const subagentRequests = get(pendingSubagentRequestsAtom(machineId));
      for (const thread of get(machineThreadsAtom(machineId))) {
        next.push(scopedThread(machineId, thread, subagentRequests));
      }
    }
    if (arrayElementsEqual(previousThreadShells, next)) {
      return previousThreadShells;
    }
    previousThreadShells = next;
    return previousThreadShells;
  }).pipe(Atom.withLabel("machine-thread-shell-list"));

  // Lists that arrange threads read this, so a running thread's streaming
  // ticks reach only what reads its live shell.
  let previousQuietThreadShells: ReadonlyArray<MachineThreadShell> = [];
  const quietThreadShellsAtom = Atom.make((get) => {
    const next: MachineThreadShell[] = [];
    for (const ref of get(threadRefsAtom)) {
      const shell = get(quietThreadShellAtomFamily(threadKey(ref)));
      if (shell !== null) next.push(shell);
    }
    if (arrayElementsEqual(previousQuietThreadShells, next)) return previousQuietThreadShells;
    previousQuietThreadShells = next;
    return next;
  }).pipe(Atom.withLabel("machine-quiet-thread-shell-list"));

  let previousNavigationShells: ReadonlyArray<MachineThreadShell> = [];
  const navigationThreadShellsAtom = Atom.make((get) => {
    const next: MachineThreadShell[] = [];
    for (const machineId of enabledMachineIds(get(input.catalogValueAtom))) {
      const subagentRequests = get(pendingSubagentRequestsAtom(machineId));
      for (const thread of get(machineThreadsAtom(machineId))) {
        if (thread.archivedAt !== null || thread.lineage.relationshipToParent === "subagent")
          continue;
        next.push(scopedThread(machineId, thread, subagentRequests));
      }
    }
    if (arrayElementsEqual(previousNavigationShells, next)) return previousNavigationShells;
    previousNavigationShells = next;
    return next;
  }).pipe(Atom.withLabel("machine-navigation-thread-shells"));

  return {
    machineThreadsAtom,
    machineThreadIndexAtom,
    machineThreadRefsAtom,
    machineThreadRefsByProjectAtom,
    threadRefsAtom,
    threadShellsAtom,
    quietThreadShellsAtom,
    navigationThreadShellsAtom,
    threadShellsForProjectRefsAtom: (refs: ReadonlyArray<ScopedProjectRef>) =>
      threadShellsForProjectRefsAtomFamily(projectRefCollectionKey(refs)),
    threadShellAtom: (ref: ScopedThreadRef) => threadShellAtomFamily(threadKey(ref)),
    quietThreadShellAtom: (ref: ScopedThreadRef) => quietThreadShellAtomFamily(threadKey(ref)),
    childThreadInputsAtom: (ref: ScopedThreadRef) => childThreadInputsAtomFamily(threadKey(ref)),
  };
}
