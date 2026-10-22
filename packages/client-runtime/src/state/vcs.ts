import {
  type MachineId,
  type VcsListRefsInput,
  type VcsListRefsResult,
  type VcsStatusResult,
  WS_METHODS,
} from "@vekrel/contracts";
import { applyGitStatusStreamEvent } from "@vekrel/shared/git";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schedule from "effect/Schedule";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { Atom, AtomRegistry } from "effect/reactivity";

import {
  createMachineRpcCommand,
  createMachineRpcSubscriptionAtomFamily,
  createMachineSubscriptionAtomFamily,
} from "./runtime.ts";
import type { MachineRegistry } from "../connection/registry.ts";
import * as MachineSupervisor from "../connection/supervisor.ts";
import { safeErrorLogAttributes } from "../errors/safeLog.ts";
import * as Persistence from "../platform/persistence.ts";
import { request, subscribe, type MachineRpcInput } from "../rpc/client.ts";
import { followStreamInMachine } from "./runtime.ts";
import { vcsCommandConcurrency, vcsCommandScheduler } from "./vcsCommandScheduler.ts";
import {
  invalidateCachedVcsRefs,
  vcsRefsCacheStateAtom,
  withVcsRefsPersistenceLock,
} from "./vcsRefInvalidation.ts";

const OFFLINE_BRANCH_LIST_LIMIT = 100;
const VCS_REFS_IDLE_TTL_MS = 30_000;
const VCS_STATUS_IDLE_TTL_MS = 10_000;
const VCS_REFS_RETRY_SCHEDULE = Schedule.exponential("1 second").pipe(
  Schedule.modifyDelay(({ duration }) =>
    Effect.succeed(Duration.min(duration, Duration.seconds(30))),
  ),
);

function canUseVcsRefsCache(input: VcsListRefsInput): boolean {
  return (
    input.query === undefined &&
    input.cursor === undefined &&
    input.includeMatchingRemoteRefs === undefined &&
    input.refKind === undefined &&
    input.limit === OFFLINE_BRANCH_LIST_LIMIT
  );
}

export const commitVcsRefsRefresh = Effect.fn("CachedVcsRefsState.commitRefresh")(function* (
  registry: AtomRegistry.AtomRegistry,
  cache: Persistence.MachineCacheStore["Service"],
  input: {
    readonly machineId: MachineId;
    readonly cwd: string;
    readonly refs: VcsListRefsResult;
    readonly expectedRevision: number;
    readonly persist: boolean;
  },
) {
  return yield* withVcsRefsPersistenceLock(
    input.machineId,
    Effect.gen(function* () {
      const stateAtom = vcsRefsCacheStateAtom({ machineId: input.machineId });
      const state = registry.get(stateAtom);
      if (state.revision !== input.expectedRevision) {
        return false;
      }
      let persistedCacheReadable = state.persistedCacheReadable;
      if (input.persist) {
        if (!persistedCacheReadable) {
          persistedCacheReadable = yield* cache.clearVcsRefs(input.machineId).pipe(
            Effect.as(true),
            Effect.catch((error) =>
              Effect.logWarning("Could not recover invalidated cached Git refs.").pipe(
                Effect.annotateLogs({
                  machineId: input.machineId,
                  cwd: input.cwd,
                  ...safeErrorLogAttributes(error),
                }),
                Effect.as(false),
              ),
            ),
          );
        }
        yield* cache.saveVcsRefs(input.machineId, input.cwd, input.refs).pipe(
          Effect.catch((error) =>
            Effect.logWarning("Could not persist cached Git refs.").pipe(
              Effect.annotateLogs({
                machineId: input.machineId,
                cwd: input.cwd,
                ...safeErrorLogAttributes(error),
              }),
            ),
          ),
        );
        if (persistedCacheReadable !== state.persistedCacheReadable) {
          registry.update(stateAtom, (current) =>
            current.revision === input.expectedRevision
              ? { ...current, persistedCacheReadable }
              : current,
          );
        }
      }
      return true;
    }),
  );
});

/**
 * Filtered or paginated lists intentionally stay live-only: treating a
 * partial result as a complete offline list would make branch selection
 * misleading.
 */
export const makeCachedVcsRefsChanges = Effect.fn("CachedVcsRefsState.makeChanges")(function* (
  input: VcsListRefsInput,
  expectedRevision?: number,
  registry?: AtomRegistry.AtomRegistry,
  persistedCacheReadable = true,
) {
  const supervisor = yield* MachineSupervisor.MachineSupervisor;
  const cache = yield* Persistence.MachineCacheStore;
  const machineId = supervisor.target.machineId;
  const useCache = canUseVcsRefsCache(input);
  const cached =
    useCache && persistedCacheReadable
      ? yield* cache.loadVcsRefs(machineId, input.cwd).pipe(
          Effect.catch((error) =>
            Effect.logWarning("Could not load cached Git refs.").pipe(
              Effect.annotateLogs({
                machineId,
                cwd: input.cwd,
                ...safeErrorLogAttributes(error),
              }),
              Effect.as(Option.none<VcsListRefsResult>()),
            ),
          ),
        )
      : Option.none<VcsListRefsResult>();
  const refresh = Effect.fn("CachedVcsRefsState.refresh")(function* () {
    const refs = yield* request(WS_METHODS.vcsListRefs, input).pipe(
      Effect.provideService(MachineSupervisor.MachineSupervisor, supervisor),
    );
    const persist = cache.saveVcsRefs(machineId, input.cwd, refs).pipe(
      Effect.catch((error) =>
        Effect.logWarning("Could not persist cached Git refs.").pipe(
          Effect.annotateLogs({
            machineId,
            cwd: input.cwd,
            ...safeErrorLogAttributes(error),
          }),
        ),
      ),
    );
    if (expectedRevision === undefined || registry === undefined) {
      if (useCache) yield* persist;
      return Option.some(refs);
    }
    const committed = yield* commitVcsRefsRefresh(registry, cache, {
      machineId,
      cwd: input.cwd,
      refs,
      expectedRevision,
      persist: useCache,
    });
    return committed ? Option.some(refs) : Option.none<VcsListRefsResult>();
  });

  const cachedRefs = Stream.fromEffect(Effect.succeed(cached)).pipe(
    Stream.filterMap((refs) =>
      Option.match(refs, {
        onNone: () => Result.failVoid,
        onSome: Result.succeed,
      }),
    ),
  );
  const refreshedRefs = SubscriptionRef.changes(supervisor.session).pipe(
    Stream.map(Option.getOrNull),
    Stream.changes,
    Stream.switchMap((session) =>
      session === null
        ? Stream.empty
        : Stream.fromEffect(
            refresh().pipe(
              // Session removal can precede switchMap cancelling this refresh.
              Effect.catchTags({ MachineRpcUnavailableError: () => Effect.never }),
              Effect.tapError((error) =>
                Effect.logWarning("Could not refresh Git refs.").pipe(
                  Effect.annotateLogs({
                    machineId,
                    cwd: input.cwd,
                    ...safeErrorLogAttributes(error),
                  }),
                ),
              ),
            ),
          ).pipe(
            Stream.retry(VCS_REFS_RETRY_SCHEDULE),
            Stream.filterMap((refs) =>
              Option.match(refs, {
                onNone: () => Result.failVoid,
                onSome: Result.succeed,
              }),
            ),
          ),
    ),
  );

  return Stream.concat(cachedRefs, refreshedRefs);
});

function cachedVcsRefsChanges(
  machineId: MachineId,
  input: VcsListRefsInput,
  expectedRevision: number,
  persistedCacheReadable: boolean,
) {
  return followStreamInMachine(
    machineId,
    Stream.unwrap(
      Effect.gen(function* () {
        const registry = yield* AtomRegistry.AtomRegistry;
        return yield* makeCachedVcsRefsChanges(
          input,
          expectedRevision,
          registry,
          persistedCacheReadable,
        );
      }),
    ),
  );
}

export function createVcsMachineAtoms<R, E>(
  runtime: Atom.AtomRuntime<MachineRegistry | Persistence.MachineCacheStore | R, E>,
) {
  /**
   * One flat family on purpose: families hold entries via WeakRef, so a nested
   * per-machine family can be collected between lookups, dropping every
   * cached page atom and collapsing paginated ref lists mid-scroll.
   */
  const listRefsFamily = Atom.family((key: string) => {
    const [machineId, input] = JSON.parse(key) as [MachineId, VcsListRefsInput];
    return runtime
      .atom((get) => {
        const state = get(vcsRefsCacheStateAtom({ machineId }));
        return cachedVcsRefsChanges(
          machineId,
          input,
          state.revision,
          state.persistedCacheReadable,
        );
      })
      .pipe(
        Atom.setIdleTTL(VCS_REFS_IDLE_TTL_MS),
        Atom.withLabel(`machine-data:vcs:list-refs:${key}`),
      );
  });
  const listRefs = (target: {
    readonly machineId: MachineId;
    readonly input: VcsListRefsInput;
  }) => listRefsFamily(JSON.stringify([target.machineId, target.input]));
  const invalidateRefs = (
    target: { readonly machineId: MachineId; readonly input: { readonly cwd: string } },
    registry: AtomRegistry.AtomRegistry,
  ) =>
    invalidateCachedVcsRefs(registry, {
      machineId: target.machineId,
      cwd: target.input.cwd,
    });

  return {
    listRefs,
    // An uncached read for decisions that must match the repository now, not
    // the persisted list a picker renders from.
    readRefs: createMachineRpcCommand(runtime, {
      label: "machine-data:vcs:read-refs",
      tag: WS_METHODS.vcsListRefs,
    }),
    status: createMachineSubscriptionAtomFamily(runtime, {
      label: "machine-data:vcs:status",
      idleTtlMs: VCS_STATUS_IDLE_TTL_MS,
      subscribe: (input: MachineRpcInput<typeof WS_METHODS.subscribeVcsStatus>) =>
        subscribe(WS_METHODS.subscribeVcsStatus, input).pipe(
          Stream.mapAccum(
            () => null as VcsStatusResult | null,
            (current, event) => {
              const next = applyGitStatusStreamEvent(current, event);
              return [next, [next]] as const;
            },
          ),
        ),
    }),
    pull: createMachineRpcCommand(runtime, {
      label: "machine-data:vcs:pull",
      tag: WS_METHODS.vcsPull,
      scheduler: vcsCommandScheduler,
      concurrency: vcsCommandConcurrency,
      onSettled: invalidateRefs,
    }),
    refreshStatus: createMachineRpcCommand(runtime, {
      label: "machine-data:vcs:refresh-status",
      tag: WS_METHODS.vcsRefreshStatus,
      scheduler: vcsCommandScheduler,
      concurrency: vcsCommandConcurrency,
      onSettled: invalidateRefs,
    }),
    createWorktree: createMachineRpcCommand(runtime, {
      label: "machine-data:vcs:create-worktree",
      tag: WS_METHODS.vcsCreateWorktree,
      scheduler: vcsCommandScheduler,
      concurrency: vcsCommandConcurrency,
      onSettled: invalidateRefs,
    }),
    worktreeSetup: createMachineRpcSubscriptionAtomFamily(runtime, {
      label: "machine-data:vcs:worktree-setup",
      tag: WS_METHODS.subscribeWorktreeSetup,
      idleTtlMs: VCS_STATUS_IDLE_TTL_MS,
    }),
    cancelWorktreeSetup: createMachineRpcCommand(runtime, {
      label: "machine-data:vcs:cancel-worktree-setup",
      tag: WS_METHODS.worktreeSetupCancel,
    }),
    removeWorktree: createMachineRpcCommand(runtime, {
      label: "machine-data:vcs:remove-worktree",
      tag: WS_METHODS.vcsRemoveWorktree,
      scheduler: vcsCommandScheduler,
      concurrency: vcsCommandConcurrency,
      onSettled: invalidateRefs,
    }),
    createRef: createMachineRpcCommand(runtime, {
      label: "machine-data:vcs:create-ref",
      tag: WS_METHODS.vcsCreateRef,
      scheduler: vcsCommandScheduler,
      concurrency: vcsCommandConcurrency,
      onSettled: invalidateRefs,
    }),
    switchRef: createMachineRpcCommand(runtime, {
      label: "machine-data:vcs:switch-ref",
      tag: WS_METHODS.vcsSwitchRef,
      scheduler: vcsCommandScheduler,
      concurrency: vcsCommandConcurrency,
      onSettled: invalidateRefs,
    }),
    init: createMachineRpcCommand(runtime, {
      label: "machine-data:vcs:init",
      tag: WS_METHODS.vcsInit,
      scheduler: vcsCommandScheduler,
      concurrency: vcsCommandConcurrency,
      onSettled: invalidateRefs,
    }),
  };
}

export * from "./gitActions.ts";
export * from "./vcsAction.ts";
export * from "./vcsRef.ts";
