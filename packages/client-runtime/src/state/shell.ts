import {
  ORCHESTRATION_V2_WS_METHODS,
  type MachineId,
  type OrchestrationV2ShellSnapshot,
  type OrchestrationV2ShellStreamItem,
  type OrchestrationV2ThreadShell,
  type ServerConfig,
  type ThreadId,
  type ThreadPullRequestLink,
} from "@vekrel/contracts";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { AsyncResult, Atom } from "effect/reactivity";

import * as MachineRegistry from "../connection/registry.ts";
import { connectionProjectionPhase } from "../connection/model.ts";
import * as MachineSupervisor from "../connection/supervisor.ts";
import * as ConnectionWakeups from "../connection/wakeups.ts";
import { safeErrorLogAttributes } from "../errors/safeLog.ts";
import * as Persistence from "../platform/persistence.ts";
import { runCachePersistence } from "./cachePersistence.ts";
import { subscribeDynamic } from "../rpc/client.ts";
import type { RpcSession } from "../rpc/session.ts";
import * as ShellSnapshotLoader from "./shellSnapshotHttp.ts";
import type { DeferredShellSnapshot } from "./shellPullRequests.ts";
import {
  applyShellStreamEvent,
  mergeShellSnapshotProjects,
  reuseUnchangedThreadShells,
  sameThreadPullRequests,
} from "./shellReducer.ts";
import { type MachineCatalogState, enabledMachineIds } from "./connections.ts";
import { followStreamInMachine } from "./runtime.ts";

export type MachineShellStatus = "empty" | "cached" | "synchronizing" | "live";

export interface MachineShellState {
  readonly snapshot: Option.Option<OrchestrationV2ShellSnapshot>;
  readonly status: MachineShellStatus;
  readonly error: Option.Option<string>;
}

const EMPTY_SHELL_STATE: MachineShellState = {
  snapshot: Option.none(),
  status: "empty",
  error: Option.none(),
};

function shellStatusForSnapshot(
  snapshot: Option.Option<OrchestrationV2ShellSnapshot>,
): MachineShellStatus {
  return Option.isSome(snapshot) ? "cached" : "empty";
}

function withoutDeferredPullRequests(value: DeferredShellSnapshot): OrchestrationV2ShellSnapshot {
  if (value.loadPullRequests === undefined) return value;
  const { loadPullRequests: _deferred, ...snapshot } = value;
  return snapshot;
}

const SHELL_SYNCHRONIZATION_ERROR_MESSAGE = "Could not synchronize machine data.";

export const makeMachineShellState = Effect.fn("MachineShellState.make")(function* () {
  const supervisor = yield* MachineSupervisor.MachineSupervisor;
  const cache = yield* Persistence.MachineCacheStore;
  const snapshotLoader = yield* ShellSnapshotLoader.ShellSnapshotLoader;
  const wakeups = yield* Effect.serviceOption(ConnectionWakeups.ConnectionWakeups);
  const machineId = supervisor.target.machineId;
  const cached = yield* cache.loadShell(machineId).pipe(
    Effect.catch((error) =>
      Effect.logWarning("Could not load cached machine shell.").pipe(
        Effect.annotateLogs({
          machineId,
          ...safeErrorLogAttributes(error),
        }),
        Effect.as(Option.none<DeferredShellSnapshot>()),
      ),
    ),
  );
  const cachedSnapshot = Option.map(cached, withoutDeferredPullRequests);
  const state = yield* SubscriptionRef.make<MachineShellState>({
    snapshot: cachedSnapshot,
    status: shellStatusForSnapshot(cachedSnapshot),
    error: Option.none(),
  });
  const awaitingCompletion = yield* Ref.make(false);
  const lastAuthoritativeSession = yield* Ref.make<RpcSession | null>(null);
  const activeSubscriptionSession = yield* Ref.make<RpcSession | null>(null);
  const latestLiveSnapshot = yield* Ref.make<Option.Option<OrchestrationV2ShellSnapshot>>(
    Option.none(),
  );
  const persistence = yield* Queue.sliding<OrchestrationV2ShellSnapshot>(1);

  const persistenceLock = yield* Semaphore.make(1);
  let lastPersisted: OrchestrationV2ShellSnapshot | undefined;
  // Rows still waiting for their deferred pull request links, which must not be saved:
  // the cache would keep them without links until the next server refresh.
  let pendingLinkRows: ReadonlySet<OrchestrationV2ThreadShell> = new Set();
  const persistLatest = Effect.fn("MachineShellState.persistLatest")(function* (
    flush: boolean,
  ) {
    while (true) {
      const latest = yield* Ref.get(latestLiveSnapshot);
      if (Option.isNone(latest) || latest.value === lastPersisted) return;
      if (latest.value.threads.some((thread) => pendingLinkRows.has(thread))) return;
      const snapshot = latest.value;
      const saved = yield* cache.saveShell(machineId, snapshot).pipe(
        Effect.as(true),
        Effect.catch((error) =>
          Effect.logWarning("Could not persist machine shell cache.").pipe(
            Effect.annotateLogs({ machineId, ...safeErrorLogAttributes(error) }),
            Effect.as(false),
          ),
        ),
      );
      if (!saved) return;
      lastPersisted = snapshot;
      if (!flush) return;
    }
  }, persistenceLock.withPermit);
  const persist = () => persistLatest(false);
  const flushLiveShellSnapshot = persistLatest(true);

  // Register before scoped worker fibers so reverse finalizer order interrupts
  // those fibers first and this flush sees a stable latestLiveSnapshot.
  yield* Effect.addFinalizer(() => flushLiveShellSnapshot);

  yield* runCachePersistence(persistence, persist).pipe(Effect.forkScoped);

  const setDisconnected = Ref.set(awaitingCompletion, false).pipe(
    Effect.andThen(
      SubscriptionRef.update(state, (current) => ({
        ...current,
        status: shellStatusForSnapshot(current.snapshot),
      })),
    ),
    Effect.andThen(flushLiveShellSnapshot.pipe(Effect.forkScoped)),
    Effect.asVoid,
  );
  const setSynchronizing = SubscriptionRef.update(state, (current) => ({
    ...current,
    status: "synchronizing" as const,
    error: Option.none(),
  }));
  const setReady = SubscriptionRef.update(state, (current) =>
    current.status === "live"
      ? current
      : {
          ...current,
          status: "synchronizing" as const,
          error: Option.none(),
        },
  );
  const setStreamError = (error: unknown) =>
    Ref.set(awaitingCompletion, false).pipe(
      Effect.andThen(Effect.logWarning("Could not synchronize the machine shell.")),
      Effect.annotateLogs({
        machineId,
        ...safeErrorLogAttributes(error),
      }),
      Effect.andThen(
        SubscriptionRef.update(state, (current) => ({
          ...current,
          status: shellStatusForSnapshot(current.snapshot),
          error: Option.some(SHELL_SYNCHRONIZATION_ERROR_MESSAGE),
        })),
      ),
    );

  // Server items and deferred link fills both read the snapshot and write it back; one at a
  // time, so neither can overwrite what the other wrote in between.
  const snapshotWriteLock = yield* Semaphore.make(1);

  const applyItems = Effect.fn("MachineShellState.applyItems")(function* (
    items: ReadonlyArray<OrchestrationV2ShellStreamItem>,
    pendingLinkIds?: ReadonlySet<ThreadId>,
  ) {
    const initial = yield* SubscriptionRef.get(state);
    let waiting = yield* Ref.get(awaitingCompletion);
    let next = initial;
    let receivedSnapshot = false;
    for (const item of items) {
      if (item.kind === "synchronized") {
        waiting = false;
        if (Option.isSome(next.snapshot)) {
          next = { ...next, status: "live", error: Option.none() };
        }
        continue;
      }
      const nextSnapshot =
        item.kind === "snapshot"
          ? mergeShellSnapshotProjects(
              Option.getOrNull(next.snapshot),
              item.snapshot,
              item.resolvedRepositoryIdentityRoots === undefined
                ? undefined
                : {
                    resolvedRepositoryIdentityRoots: item.resolvedRepositoryIdentityRoots,
                  },
            )
          : Option.match(next.snapshot, {
              onNone: () => null,
              onSome: (snapshot) =>
                item.sequence > snapshot.snapshotSequence
                  ? applyShellStreamEvent(snapshot, item)
                  : snapshot,
            });
      if (nextSnapshot === null) continue;
      receivedSnapshot ||= item.kind === "snapshot";
      next = {
        snapshot: Option.some(nextSnapshot),
        status: waiting ? "synchronizing" : "live",
        error: Option.none(),
      };
    }
    yield* Ref.set(awaitingCompletion, waiting);
    if (next === initial) return;
    if (pendingLinkIds !== undefined && Option.isSome(next.snapshot)) {
      pendingLinkRows = new Set(
        next.snapshot.value.threads.filter((thread) => pendingLinkIds.has(thread.id)),
      );
    }
    if (Option.isSome(next.snapshot)) {
      yield* Ref.set(latestLiveSnapshot, next.snapshot);
    }
    yield* SubscriptionRef.set(state, next);
    if (receivedSnapshot) {
      const session = yield* Ref.get(activeSubscriptionSession);
      if (session !== null) {
        yield* Ref.set(lastAuthoritativeSession, session);
      }
    }
    if (next.snapshot !== initial.snapshot && Option.isSome(next.snapshot)) {
      yield* Queue.offer(persistence, next.snapshot.value);
    }
  }, snapshotWriteLock.withPermit);

  const stateScope = yield* Effect.scope;
  // Each full snapshot supersedes the previous one's fill; links from an older snapshot must
  // not land on rows a newer snapshot reused.

  let activeFill: Fiber.Fiber<void> | undefined;
  const cancelActiveFill = Effect.suspend(() => {
    const fill = activeFill;
    activeFill = undefined;
    return fill === undefined ? Effect.void : Fiber.interrupt(fill);
  });
  const fillDeferredPullRequests = Effect.fnUntraced(function* (
    source: DeferredShellSnapshot,
    applied: OrchestrationV2ShellSnapshot,
  ) {
    yield* cancelActiveFill;
    if (source.loadPullRequests === undefined) return;
    const arrivedRows = new Set(applied.threads);
    const arrivedIds = new Set(source.threads.map((thread) => thread.id));
    pendingLinkRows = new Set(applied.threads.filter((thread) => arrivedIds.has(thread.id)));
    activeFill = yield* source.loadPullRequests.pipe(
      Effect.flatMap((linksByThreadId) =>
        writeFilledLinks(linksByThreadId, arrivedRows, arrivedIds),
      ),
      Effect.forkIn(stateScope),
    );
  });
  const writeFilledLinks = Effect.fnUntraced(
    function* (
      linksByThreadId: ReadonlyMap<ThreadId, ReadonlyArray<ThreadPullRequestLink>>,
      arrivedRows: ReadonlySet<OrchestrationV2ThreadShell>,
      arrivedIds: ReadonlySet<ThreadId>,
    ) {
      const filled = yield* SubscriptionRef.modify(
        state,
        (current): [Option.Option<OrchestrationV2ShellSnapshot>, MachineShellState] => {
          if (Option.isNone(current.snapshot)) return [Option.none(), current];
          const snapshot = current.snapshot.value;
          let changed = false;
          const threads = snapshot.threads.map((thread) => {
            if (!arrivedRows.has(thread) || !arrivedIds.has(thread.id)) return thread;
            const links = linksByThreadId.get(thread.id);
            if (sameThreadPullRequests(thread.pullRequests, links)) return thread;
            changed = true;
            if (links === undefined) {
              const { pullRequests: _previous, ...row } = thread;
              return row;
            }
            return { ...thread, pullRequests: links };
          });
          if (!changed) return [Option.none(), current];
          const next = { ...snapshot, threads };
          return [Option.some(next), { ...current, snapshot: Option.some(next) }];
        },
      );
      pendingLinkRows = new Set();
      const live = yield* Ref.get(latestLiveSnapshot);
      if (Option.isNone(live)) return;
      const latest = Option.isSome(filled) ? filled : live;
      yield* Ref.set(latestLiveSnapshot, latest);
      yield* Queue.offer(persistence, latest.value);
    },
    (effect) => snapshotWriteLock.withPermit(effect),
  );
  if (Option.isSome(cached) && Option.isSome(cachedSnapshot)) {
    yield* fillDeferredPullRequests(cached.value, cachedSnapshot.value);
  }

  const foregroundResubscriptions = Option.match(wakeups, {
    onNone: () => Stream.never,
    onSome: (service) =>
      service.changes.pipe(Stream.filter(ConnectionWakeups.shouldResubscribeAfterWakeup)),
  });

  yield* setSynchronizing;
  yield* Effect.forkScoped(
    subscribeDynamic(
      ORCHESTRATION_V2_WS_METHODS.subscribeShell,
      Effect.fn("MachineShellState.makeSubscribeInput")(function* (session) {
        yield* Ref.set(activeSubscriptionSession, session);
        const supportsCompletionMarker = yield* session.initialConfig.pipe(
          Effect.map((config) => config.shellResumeCompletionMarker === true),
          Effect.orElseSucceed(() => false),
        );
        yield* Ref.set(awaitingCompletion, supportsCompletionMarker);
        yield* setSynchronizing;

        // Foreground resubscriptions on the same live session can resume from
        // the in-memory cursor. A new session reloads the authoritative HTTP
        // snapshot so a valid cursor cannot preserve incomplete cached data.
        const hasAuthoritativeSnapshot = (yield* Ref.get(lastAuthoritativeSession)) === session;
        let canResume = hasAuthoritativeSnapshot;
        let current = yield* SubscriptionRef.get(state);
        if (!hasAuthoritativeSnapshot || Option.isNone(current.snapshot)) {
          const prepared = yield* SubscriptionRef.get(supervisor.prepared).pipe(
            Effect.flatMap(
              Option.match({
                onSome: Effect.succeed,
                onNone: () =>
                  SubscriptionRef.changes(supervisor.prepared).pipe(
                    Stream.filter(Option.isSome),
                    Stream.map((value) => value.value),
                    Stream.runHead,
                    Effect.map(Option.getOrThrow),
                  ),
              }),
            ),
          );
          const httpSnapshot = yield* snapshotLoader.load(prepared);
          if (Option.isSome(httpSnapshot)) {
            yield* cancelActiveFill;
            const previous = yield* SubscriptionRef.get(state);
            const snapshot = reuseUnchangedThreadShells(
              Option.getOrNull(previous.snapshot),
              withoutDeferredPullRequests(httpSnapshot.value),
            );
            const pendingLinkIds = new Set(
              httpSnapshot.value.loadPullRequests === undefined
                ? []
                : httpSnapshot.value.threads.map((thread) => thread.id),
            );
            yield* applyItems([{ kind: "snapshot", snapshot }], pendingLinkIds);
            canResume = true;
            current = yield* SubscriptionRef.get(state);
            if (Option.isSome(current.snapshot)) {
              yield* fillDeferredPullRequests(httpSnapshot.value, current.snapshot.value);
            }
          }
        }

        if (!canResume || Option.isNone(current.snapshot)) {
          return supportsCompletionMarker ? { requestCompletionMarker: true as const } : {};
        }
        if (!supportsCompletionMarker) {
          yield* SubscriptionRef.update(state, (value) => ({
            ...value,
            status: "live" as const,
            error: Option.none(),
          }));
        }
        return {
          afterSequence: current.snapshot.value.snapshotSequence,
          ...(supportsCompletionMarker ? { requestCompletionMarker: true as const } : {}),
        };
      }),
      {
        onExpectedFailure: (cause) => setStreamError(Cause.squash(cause)),
        retryExpectedFailureAfter: "250 millis",
        resubscribe: foregroundResubscriptions,
      },
    ).pipe(Stream.runForEachArray((items) => applyItems(items))),
  );
  yield* SubscriptionRef.changes(supervisor.state).pipe(
    Stream.runForEach((connectionState) => {
      switch (connectionProjectionPhase(connectionState)) {
        case "synchronizing":
          return setSynchronizing;
        case "disconnected":
          return setDisconnected;
        case "ready":
          return setReady;
      }
    }),
    Effect.forkScoped,
  );

  return state;
});

function shellStateChanges(machineId: MachineId) {
  return followStreamInMachine(
    machineId,
    Stream.unwrap(makeMachineShellState().pipe(Effect.map(SubscriptionRef.changes))),
  );
}

export interface MachineShellSummary {
  readonly hasSnapshot: boolean;
  readonly hasSynchronizingShell: boolean;
  readonly hasCachedShell: boolean;
  readonly hasLiveShell: boolean;
  readonly firstError: string | null;
}

const EMPTY_MACHINE_SHELL_SUMMARY: MachineShellSummary = Object.freeze({
  hasSnapshot: false,
  hasSynchronizingShell: false,
  hasCachedShell: false,
  hasLiveShell: false,
  firstError: null,
});

const EMPTY_SERVER_CONFIGS: ReadonlyMap<MachineId, ServerConfig> = new Map();

function shellSummariesEqual(
  left: MachineShellSummary,
  right: MachineShellSummary,
): boolean {
  return (
    left.hasSnapshot === right.hasSnapshot &&
    left.hasSynchronizingShell === right.hasSynchronizingShell &&
    left.hasCachedShell === right.hasCachedShell &&
    left.hasLiveShell === right.hasLiveShell &&
    left.firstError === right.firstError
  );
}

function mapsEqual<K, V>(left: ReadonlyMap<K, V>, right: ReadonlyMap<K, V>): boolean {
  if (left.size !== right.size) {
    return false;
  }
  for (const [key, value] of left) {
    if (right.get(key) !== value) {
      return false;
    }
  }
  return true;
}

export function createMachineShellSummaryAtom(input: {
  readonly catalogValueAtom: Atom.Atom<MachineCatalogState>;
  readonly shellStateValueAtom: (machineId: MachineId) => Atom.Atom<MachineShellState>;
}) {
  let previousSummary = EMPTY_MACHINE_SHELL_SUMMARY;
  return Atom.make((get) => {
    let hasSnapshot = false;
    let hasSynchronizingShell = false;
    let hasCachedShell = false;
    let hasLiveShell = false;
    let firstError: string | null = null;

    for (const machineId of enabledMachineIds(get(input.catalogValueAtom))) {
      const state = get(input.shellStateValueAtom(machineId));
      hasSynchronizingShell ||= state.status === "synchronizing";
      hasCachedShell ||= state.status === "cached";
      hasLiveShell ||= state.status === "live";
      if (firstError === null) {
        firstError = Option.getOrNull(state.error);
      }
      if (Option.isNone(state.snapshot)) {
        continue;
      }
      hasSnapshot = true;
    }

    const next: MachineShellSummary = {
      hasSnapshot,
      hasSynchronizingShell,
      hasCachedShell,
      hasLiveShell,
      firstError,
    };
    if (shellSummariesEqual(previousSummary, next)) {
      return previousSummary;
    }
    previousSummary = next;
    return previousSummary;
  }).pipe(Atom.withLabel("machine-shell-summary"));
}

export function createMachineServerConfigsAtom(input: {
  readonly catalogValueAtom: Atom.Atom<MachineCatalogState>;
  readonly serverConfigValueAtom: (machineId: MachineId) => Atom.Atom<ServerConfig | null>;
}) {
  let previousServerConfigs = EMPTY_SERVER_CONFIGS;
  return Atom.make((get) => {
    const next = new Map<MachineId, ServerConfig>();
    for (const machineId of enabledMachineIds(get(input.catalogValueAtom))) {
      const config = get(input.serverConfigValueAtom(machineId));
      if (config !== null) {
        next.set(machineId, config);
      }
    }
    if (mapsEqual(previousServerConfigs, next)) {
      return previousServerConfigs;
    }
    previousServerConfigs = next;
    return previousServerConfigs;
  }).pipe(Atom.withLabel("machine-server-configs"));
}

/**
 * Every shell push replaces the state, so views that gate on load progress read these
 * primitives instead and stay put while threads stream.
 */
function createMachineShellProgressAtoms(
  stateValueAtom: (machineId: MachineId) => Atom.Atom<MachineShellState>,
) {
  return {
    statusAtom: Atom.family((machineId: MachineId) =>
      Atom.make((get) => get(stateValueAtom(machineId)).status).pipe(
        Atom.withLabel(`machine-shell-status:${machineId}`),
      ),
    ),
    hasSnapshotAtom: Atom.family((machineId: MachineId) =>
      Atom.make((get) => Option.isSome(get(stateValueAtom(machineId)).snapshot)).pipe(
        Atom.withLabel(`machine-shell-has-snapshot:${machineId}`),
      ),
    ),
  };
}

export function createMachineShellAtoms<R, E>(
  runtime: Atom.AtomRuntime<
    | MachineRegistry.MachineRegistry
    | Persistence.MachineCacheStore
    | ShellSnapshotLoader.ShellSnapshotLoader
    | R,
    E
  >,
) {
  const stateAtom = Atom.family((machineId: MachineId) =>
    runtime.atom(shellStateChanges(machineId), {
      initialValue: EMPTY_SHELL_STATE,
    }),
  );

  const stateValueAtom = Atom.family((machineId: MachineId) =>
    Atom.make((get) =>
      Option.getOrElse(AsyncResult.value(get(stateAtom(machineId))), () => EMPTY_SHELL_STATE),
    ).pipe(Atom.withLabel(`machine-shell-state-value:${machineId}`)),
  );

  const { statusAtom, hasSnapshotAtom } = createMachineShellProgressAtoms(stateValueAtom);

  return {
    stateAtom,
    stateValueAtom,
    statusAtom,
    hasSnapshotAtom,
  };
}

export * from "./models.ts";
export * from "./shellCommands.ts";
export * from "./shellReducer.ts";
export * from "./shellPullRequests.ts";
export * as ShellSnapshotLoader from "./shellSnapshotHttp.ts";
export * from "./snapshots.ts";
