import type { MachineId } from "@vekrel/contracts";
import * as Effect from "effect/Effect";
import * as PartitionedSemaphore from "effect/PartitionedSemaphore";
import { Atom, type AtomRegistry } from "effect/reactivity";

import { safeErrorLogAttributes } from "../errors/safeLog.ts";
import * as Persistence from "../platform/persistence.ts";

export interface VcsRefsInvalidationTarget {
  readonly machineId: MachineId;
}

export interface CachedVcsRefsInvalidationTarget extends VcsRefsInvalidationTarget {
  readonly cwd: string;
}

export interface VcsRefsCacheState {
  readonly revision: number;
  readonly persistedCacheReadable: boolean;
}

const stateByMachine = Atom.family((machineId: MachineId) =>
  Atom.make<VcsRefsCacheState>({
    revision: 0,
    persistedCacheReadable: true,
  }).pipe(Atom.keepAlive, Atom.withLabel(`machine-data:vcs:list-refs-state:${machineId}`)),
);
const persistenceLock = PartitionedSemaphore.makeUnsafe<MachineId>({ permits: 1 });

export function vcsRefsCacheStateAtom(target: VcsRefsInvalidationTarget) {
  return stateByMachine(target.machineId);
}

export function invalidateVcsRefs(
  registry: AtomRegistry.AtomRegistry,
  target: VcsRefsInvalidationTarget,
  persistedCacheReadable?: boolean,
): void {
  registry.update(vcsRefsCacheStateAtom(target), (state) => ({
    revision: state.revision + 1,
    persistedCacheReadable: persistedCacheReadable ?? state.persistedCacheReadable,
  }));
}

export function withVcsRefsPersistenceLock<A, E, R>(
  machineId: MachineId,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> {
  return persistenceLock.withPermit(machineId)(effect);
}

/**
 * Clearing first prevents the restarted stream from rehydrating the invalidated
 * snapshot. A refresh that wins the lock is cleared afterward; one that loses
 * observes the new revision and cannot repersist its pre-mutation result.
 */
export const invalidateCachedVcsRefs = Effect.fn("VcsRefsState.invalidateCached")(function* (
  registry: AtomRegistry.AtomRegistry,
  target: CachedVcsRefsInvalidationTarget,
) {
  const cache = yield* Persistence.MachineCacheStore;
  yield* withVcsRefsPersistenceLock(
    target.machineId,
    Effect.gen(function* () {
      const persistedCacheReadable = yield* cache.clearVcsRefs(target.machineId).pipe(
        Effect.as(true),
        Effect.catch((error) =>
          Effect.logWarning("Could not remove invalidated cached Git refs.").pipe(
            Effect.annotateLogs({
              machineId: target.machineId,
              cwd: target.cwd,
              ...safeErrorLogAttributes(error),
            }),
            Effect.as(false),
          ),
        ),
      );
      invalidateVcsRefs(registry, target, persistedCacheReadable);
    }),
  );
});
