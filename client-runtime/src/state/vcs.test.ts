import { vi } from "vite-plus/test";
import {
  MachineId,
  AuthSourceControlWriteScope,
  WS_METHODS,
  VcsProcessExitError,
  type VcsListRefsResult,
} from "@vekrel/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as TestClock from "effect/testing/TestClock";
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity";

import {
  AVAILABLE_CONNECTION_STATE,
  PrimaryConnectionTarget,
  type PreparedConnection,
  type SupervisorConnectionState,
} from "../connection/model.ts";
import * as MachineRegistry from "../connection/registry.ts";
import * as MachineSupervisor from "../connection/supervisor.ts";
import * as Persistence from "../platform/persistence.ts";
import { MachineRpcUnavailableError } from "../rpc/client.ts";
import type { WsRpcProtocolClient } from "../rpc/protocol.ts";
import type { RpcSession } from "../rpc/session.ts";

import {
  commitVcsRefsRefresh,
  createVcsMachineAtoms,
  makeCachedVcsRefsChanges,
} from "./vcs.ts";
import {
  invalidateCachedVcsRefs,
  invalidateVcsRefs,
  vcsRefsCacheStateAtom,
} from "./vcsRefInvalidation.ts";

const TARGET = new PrimaryConnectionTarget({
  machineId: MachineId.make("machine-1"),
  label: "Test machine",
  httpBaseUrl: "https://machine.example.test",
  wsBaseUrl: "wss://machine.example.test",
});

const CONNECTED_CONNECTION_STATE: SupervisorConnectionState = {
  ...AVAILABLE_CONNECTION_STATE,
  desired: true,
  network: "online",
  phase: "connected",
  attempt: 1,
  generation: 1,
};

const CACHED_REFS: VcsListRefsResult = {
  refs: [
    {
      name: "main",
      current: true,
      isDefault: true,
      worktreePath: "/repo",
    },
  ],
  isRepo: true,
  hasPrimaryRemote: true,
  nextCursor: null,
  totalCount: 1,
};

const LIVE_REFS: VcsListRefsResult = {
  ...CACHED_REFS,
  refs: [
    {
      name: "release",
      current: true,
      isDefault: true,
      worktreePath: "/repo",
    },
  ],
};

function session(client: WsRpcProtocolClient): RpcSession {
  return {
    client,
    initialConfig: Effect.never,
    subscribeServerConfig: (input) => client.subscribeServerConfig(input),
    ready: Effect.void,
    probe: Effect.void,
    closed: Effect.never,
  };
}

function cacheWithRefs(
  refs: Option.Option<VcsListRefsResult>,
  overrides: Partial<Persistence.MachineCacheStore["Service"]> = {},
) {
  return Persistence.MachineCacheStore.of({
    loadShell: () => Effect.succeedNone,
    saveShell: () => Effect.void,
    loadThread: () => Effect.succeedNone,
    saveThread: () => Effect.void,
    removeThread: () => Effect.void,
    loadServerConfig: () => Effect.succeedNone,
    saveServerConfig: () => Effect.void,
    loadVcsRefs: () => Effect.succeed(refs),
    saveVcsRefs: () => Effect.void,
    removeVcsRefs: () => Effect.void,
    clearVcsRefs: () => Effect.void,
    clear: () => Effect.void,
    ...overrides,
  });
}

describe("cached VCS refs", () => {
  it("invalidates all ref streams in the mutated machine", () => {
    const registry = AtomRegistry.make();
    const machine = {
      machineId: TARGET.machineId,
    };
    const otherMachine = {
      machineId: MachineId.make("machine-2"),
    };

    expect(registry.get(vcsRefsCacheStateAtom(machine))).toEqual({
      revision: 0,
      persistedCacheReadable: true,
    });
    expect(registry.get(vcsRefsCacheStateAtom(otherMachine))).toEqual({
      revision: 0,
      persistedCacheReadable: true,
    });

    invalidateVcsRefs(registry, machine);

    expect(registry.get(vcsRefsCacheStateAtom(machine))).toEqual({
      revision: 1,
      persistedCacheReadable: true,
    });
    expect(registry.get(vcsRefsCacheStateAtom(otherMachine))).toEqual({
      revision: 0,
      persistedCacheReadable: true,
    });
    registry.dispose();
  });

  it.effect("does not repersist a refresh superseded by ref invalidation", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* Effect.acquireRelease(Effect.sync(AtomRegistry.make), (registry) =>
          Effect.sync(() => registry.dispose()),
        );
        const saved = yield* Ref.make<ReadonlyArray<VcsListRefsResult>>([]);
        const clears = yield* Ref.make(0);
        const revisionsObservedDuringClear = yield* Ref.make<ReadonlyArray<number>>([]);
        const cache = cacheWithRefs(Option.none(), {
          saveVcsRefs: (_machineId, _cwd, refs) =>
            Ref.update(saved, (current) => [...current, refs]),
          clearVcsRefs: () =>
            Effect.all([
              Ref.update(clears, (count) => count + 1),
              Ref.update(revisionsObservedDuringClear, (current) => [
                ...current,
                registry.get(vcsRefsCacheStateAtom(TARGET)).revision,
              ]),
            ]).pipe(Effect.asVoid),
        });

        yield* invalidateCachedVcsRefs(registry, {
          machineId: TARGET.machineId,
          cwd: "/repo-worktree",
        }).pipe(Effect.provideService(Persistence.MachineCacheStore, cache));

        expect(
          yield* commitVcsRefsRefresh(registry, cache, {
            machineId: TARGET.machineId,
            cwd: "/repo",
            refs: CACHED_REFS,
            expectedRevision: 0,
            persist: true,
          }),
        ).toBe(false);

        expect(yield* Ref.get(saved)).toEqual([]);
        expect(yield* Ref.get(clears)).toBe(1);
        expect(yield* Ref.get(revisionsObservedDuringClear)).toEqual([0]);
        expect(registry.get(vcsRefsCacheStateAtom(TARGET))).toEqual({
          revision: 1,
          persistedCacheReadable: true,
        });

        expect(
          yield* commitVcsRefsRefresh(registry, cache, {
            machineId: TARGET.machineId,
            cwd: "/repo",
            refs: LIVE_REFS,
            expectedRevision: 1,
            persist: true,
          }),
        ).toBe(true);
        expect(yield* Ref.get(saved)).toEqual([LIVE_REFS]);
      }),
    ),
  );

  it.effect("invalidates persisted refs when ref-affecting commands settle", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const expectedError = new MachineRpcUnavailableError({
          machineId: TARGET.machineId,
          message: "pull failed after fetching refs",
        });
        const client = {
          [WS_METHODS.vcsPull]: () => Effect.fail(expectedError),
          [WS_METHODS.vcsRefreshStatus]: () => Effect.void,
        } as unknown as WsRpcProtocolClient;
        const supervisor = MachineSupervisor.MachineSupervisor.of({
          target: TARGET,
          state: yield* SubscriptionRef.make(CONNECTED_CONNECTION_STATE),
          session: yield* SubscriptionRef.make(Option.some(session(client))),
          prepared: yield* SubscriptionRef.make(Option.none<PreparedConnection>()),
          connect: Effect.void,
          disconnect: Effect.void,
          retryNow: Effect.void,
        } satisfies MachineSupervisor.MachineSupervisor["Service"]);
        const run: MachineRegistry.MachineRegistry["Service"]["run"] = (
          _machineId,
          effect,
        ) => Effect.provideService(effect, MachineSupervisor.MachineSupervisor, supervisor);
        const machineRegistry = MachineRegistry.MachineRegistry.of({
          run,
        } as unknown as MachineRegistry.MachineRegistry["Service"]);
        const clears = yield* Ref.make(0);
        const runtime = Atom.runtime(
          Layer.merge(
            Layer.succeed(MachineRegistry.MachineRegistry, machineRegistry),
            Layer.succeed(
              Persistence.MachineCacheStore,
              cacheWithRefs(Option.none(), {
                clearVcsRefs: () => Ref.update(clears, (count) => count + 1),
              }),
            ),
          ),
        );
        const atoms = createVcsMachineAtoms(runtime);
        const registry = yield* Effect.acquireRelease(Effect.sync(AtomRegistry.make), (registry) =>
          Effect.sync(() => registry.dispose()),
        );

        const result = yield* Effect.promise(() =>
          atoms.pull.run(registry, {
            machineId: TARGET.machineId,
            input: { cwd: "/repo" },
          }),
        );

        expect(AsyncResult.isFailure(result)).toBe(true);
        expect(yield* Ref.get(clears)).toBe(1);
        expect(registry.get(vcsRefsCacheStateAtom(TARGET)).revision).toBe(1);

        const refreshResult = yield* Effect.promise(() =>
          atoms.refreshStatus.run(registry, {
            machineId: TARGET.machineId,
            input: { cwd: "/repo" },
          }),
        );

        expect(AsyncResult.isSuccess(refreshResult)).toBe(true);
        expect(yield* Ref.get(clears)).toBe(2);
        expect(registry.get(vcsRefsCacheStateAtom(TARGET)).revision).toBe(2);
      }),
    ),
  );

  it.effect("suppresses persisted snapshots after a machine-wide clear fails", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* Effect.acquireRelease(Effect.sync(AtomRegistry.make), (registry) =>
          Effect.sync(() => registry.dispose()),
        );
        const persisted = yield* Ref.make<Option.Option<VcsListRefsResult>>(
          Option.some(CACHED_REFS),
        );
        const loads = yield* Ref.make(0);
        const clearAttempts = yield* Ref.make(0);
        const cache = cacheWithRefs(Option.none(), {
          loadVcsRefs: () =>
            Ref.update(loads, (count) => count + 1).pipe(Effect.andThen(Ref.get(persisted))),
          saveVcsRefs: (_machineId, _cwd, refs) => Ref.set(persisted, Option.some(refs)),
          clearVcsRefs: () =>
            Ref.updateAndGet(clearAttempts, (count) => count + 1).pipe(
              Effect.flatMap((attempt) =>
                attempt === 1
                  ? Effect.fail(
                      new Persistence.ConnectionPersistenceError({
                        operation: "clear-vcs-refs",
                        message: "storage unavailable",
                      }),
                    )
                  : Ref.set(persisted, Option.none()),
              ),
            ),
        });

        yield* invalidateCachedVcsRefs(registry, {
          machineId: TARGET.machineId,
          cwd: "/repo",
        }).pipe(Effect.provideService(Persistence.MachineCacheStore, cache));

        const state = registry.get(vcsRefsCacheStateAtom(TARGET));
        expect(state).toEqual({
          revision: 1,
          persistedCacheReadable: false,
        });

        const supervisor = MachineSupervisor.MachineSupervisor.of({
          target: TARGET,
          state: yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE),
          session: yield* SubscriptionRef.make(Option.none<RpcSession>()),
          prepared: yield* SubscriptionRef.make(Option.none<PreparedConnection>()),
          connect: Effect.void,
          disconnect: Effect.void,
          retryNow: Effect.void,
        } satisfies MachineSupervisor.MachineSupervisor["Service"]);
        const refs = yield* Stream.unwrap(
          makeCachedVcsRefsChanges(
            { cwd: "/repo", limit: 100 },
            state.revision,
            registry,
            state.persistedCacheReadable,
          ).pipe(
            Effect.provideService(MachineSupervisor.MachineSupervisor, supervisor),
            Effect.provideService(Persistence.MachineCacheStore, cache),
          ),
        ).pipe(Stream.runHead, Effect.forkChild({ startImmediately: true }));

        yield* Effect.yieldNow;
        expect(yield* Ref.get(loads)).toBe(0);
        yield* Fiber.interrupt(refs);
        expect(registry.get(vcsRefsCacheStateAtom(TARGET))).toEqual(state);

        expect(
          yield* commitVcsRefsRefresh(registry, cache, {
            machineId: TARGET.machineId,
            cwd: "/repo",
            refs: LIVE_REFS,
            expectedRevision: state.revision,
            persist: true,
          }),
        ).toBe(true);
        const recoveredState = registry.get(vcsRefsCacheStateAtom(TARGET));
        expect(recoveredState).toEqual({
          revision: 1,
          persistedCacheReadable: true,
        });
        expect(yield* Ref.get(clearAttempts)).toBe(2);

        const recoveredRefs = yield* Stream.unwrap(
          makeCachedVcsRefsChanges(
            { cwd: "/repo", limit: 100 },
            recoveredState.revision,
            registry,
            recoveredState.persistedCacheReadable,
          ).pipe(
            Effect.provideService(MachineSupervisor.MachineSupervisor, supervisor),
            Effect.provideService(Persistence.MachineCacheStore, cache),
          ),
        ).pipe(Stream.runHead);

        expect(Option.getOrThrow(recoveredRefs)).toEqual(LIVE_REFS);
        expect(yield* Ref.get(loads)).toBe(1);
      }),
    ),
  );

  it.effect("loads an unfiltered branch list without a connection", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const supervisor = MachineSupervisor.MachineSupervisor.of({
          target: TARGET,
          state: yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE),
          session: yield* SubscriptionRef.make(Option.none<RpcSession>()),
          prepared: yield* SubscriptionRef.make(Option.none<PreparedConnection>()),
          connect: Effect.void,
          disconnect: Effect.void,
          retryNow: Effect.void,
        } satisfies MachineSupervisor.MachineSupervisor["Service"]);
        const refs = yield* Stream.unwrap(
          makeCachedVcsRefsChanges({ cwd: "/repo", limit: 100 }).pipe(
            Effect.provideService(MachineSupervisor.MachineSupervisor, supervisor),
            Effect.provideService(
              Persistence.MachineCacheStore,
              cacheWithRefs(Option.some(CACHED_REFS)),
            ),
          ),
        ).pipe(Stream.runHead);

        expect(Option.getOrThrow(refs)).toEqual(CACHED_REFS);
      }),
    ),
  );

  it.effect("waits silently for a session while the connection phase is still connected", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const logs: unknown[] = [];
        const logger = Logger.make(({ message }) => logs.push(message));
        const liveSession = yield* SubscriptionRef.make(Option.none<RpcSession>());
        const supervisor = MachineSupervisor.MachineSupervisor.of({
          target: TARGET,
          state: yield* SubscriptionRef.make(CONNECTED_CONNECTION_STATE),
          session: liveSession,
          prepared: yield* SubscriptionRef.make(Option.none<PreparedConnection>()),
          connect: Effect.void,
          disconnect: Effect.void,
          retryNow: Effect.void,
        } satisfies MachineSupervisor.MachineSupervisor["Service"]);
        const refs = yield* Stream.unwrap(
          makeCachedVcsRefsChanges({ cwd: "/repo", limit: 100 }).pipe(
            Effect.provideService(MachineSupervisor.MachineSupervisor, supervisor),
            Effect.provideService(
              Persistence.MachineCacheStore,
              cacheWithRefs(Option.some(CACHED_REFS)),
            ),
          ),
        ).pipe(
          Stream.take(2),
          Stream.runCollect,
          Effect.provide(Logger.layer([logger], { mergeWithExisting: false })),
          Effect.forkChild({ startImmediately: true }),
        );

        yield* TestClock.adjust("1 minute");
        expect(logs).toEqual([]);

        const client = {
          [WS_METHODS.vcsListRefs]: () => Effect.succeed(LIVE_REFS),
        } as unknown as WsRpcProtocolClient;
        yield* SubscriptionRef.set(liveSession, Option.some(session(client)));
        expect(yield* Fiber.join(refs)).toEqual([CACHED_REFS, LIVE_REFS]);
        expect(logs).toEqual([]);
      }),
    ),
  );

  it.effect("cancels an in-flight refresh and restarts when the RPC session changes", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        const interrupted = yield* Deferred.make<void>();
        const calls = yield* Ref.make(0);
        const interruptions = yield* Ref.make(0);
        const client = {
          [WS_METHODS.vcsListRefs]: () =>
            Ref.updateAndGet(calls, (count) => count + 1).pipe(
              Effect.flatMap((count) =>
                count === 1
                  ? Deferred.succeed(started, undefined).pipe(
                      Effect.andThen(Effect.never),
                      Effect.onInterrupt(() =>
                        Ref.update(interruptions, (interruptions) => interruptions + 1).pipe(
                          Effect.andThen(Deferred.succeed(interrupted, undefined)),
                        ),
                      ),
                    )
                  : Effect.succeed(LIVE_REFS),
              ),
            ),
        } as unknown as WsRpcProtocolClient;
        const liveSession = yield* SubscriptionRef.make(Option.some(session(client)));
        const supervisor = MachineSupervisor.MachineSupervisor.of({
          target: TARGET,
          state: yield* SubscriptionRef.make(CONNECTED_CONNECTION_STATE),
          session: liveSession,
          prepared: yield* SubscriptionRef.make(Option.none<PreparedConnection>()),
          connect: Effect.void,
          disconnect: Effect.void,
          retryNow: Effect.void,
        } satisfies MachineSupervisor.MachineSupervisor["Service"]);

        const fiber = yield* Stream.unwrap(
          makeCachedVcsRefsChanges({ cwd: "/repo", limit: 100 }).pipe(
            Effect.provideService(MachineSupervisor.MachineSupervisor, supervisor),
            Effect.provideService(Persistence.MachineCacheStore, cacheWithRefs(Option.none())),
          ),
        ).pipe(Stream.runHead, Effect.forkChild);

        yield* Deferred.await(started);
        yield* SubscriptionRef.set(liveSession, Option.none());
        yield* Deferred.await(interrupted);
        expect(yield* Ref.get(interruptions)).toBe(1);

        yield* SubscriptionRef.set(liveSession, Option.some(session(client)));
        expect(Option.getOrThrow(yield* Fiber.join(fiber))).toEqual(LIVE_REFS);
        expect(yield* Ref.get(calls)).toBe(2);
      }),
    ),
  );

  it.effect("retries Git failures and cancels pending retries when the session is removed", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const calls = yield* Ref.make(0);
        const warnings = yield* Queue.unbounded<unknown>();
        const logger = Logger.make(({ message }) => Queue.offerUnsafe(warnings, message));
        const failure = new VcsProcessExitError({
          operation: "listRefs",
          command: "git",
          cwd: "/repo",
          exitCode: 128,
          detail: "Could not read refs.",
        });
        const client = {
          [WS_METHODS.vcsListRefs]: () =>
            Ref.updateAndGet(calls, (count) => count + 1).pipe(
              Effect.flatMap((count) =>
                count < 3 ? Effect.fail(failure) : Effect.succeed(LIVE_REFS),
              ),
            ),
        } as unknown as WsRpcProtocolClient;
        const liveSession = yield* SubscriptionRef.make(Option.some(session(client)));
        const supervisor = MachineSupervisor.MachineSupervisor.of({
          target: TARGET,
          state: yield* SubscriptionRef.make(CONNECTED_CONNECTION_STATE),
          session: liveSession,
          prepared: yield* SubscriptionRef.make(Option.none<PreparedConnection>()),
          connect: Effect.void,
          disconnect: Effect.void,
          retryNow: Effect.void,
        } satisfies MachineSupervisor.MachineSupervisor["Service"]);
        const refs = yield* Stream.unwrap(
          makeCachedVcsRefsChanges({ cwd: "/repo", limit: 100 }).pipe(
            Effect.provideService(MachineSupervisor.MachineSupervisor, supervisor),
            Effect.provideService(
              Persistence.MachineCacheStore,
              cacheWithRefs(Option.some(CACHED_REFS)),
            ),
          ),
        ).pipe(
          Stream.take(2),
          Stream.runCollect,
          Effect.provide(Logger.layer([logger], { mergeWithExisting: false })),
          Effect.forkChild({ startImmediately: true }),
        );

        yield* Queue.take(warnings);
        expect(yield* Ref.get(calls)).toBe(1);
        yield* TestClock.adjust("1 second");
        expect(yield* Ref.get(calls)).toBe(2);
        expect(yield* Queue.size(warnings)).toBe(1);

        yield* SubscriptionRef.set(liveSession, Option.none());
        yield* TestClock.adjust("1 minute");
        expect(yield* Ref.get(calls)).toBe(2);
        expect(yield* Queue.size(warnings)).toBe(1);

        yield* SubscriptionRef.set(liveSession, Option.some(session(client)));
        expect(yield* Fiber.join(refs)).toEqual([CACHED_REFS, LIVE_REFS]);
        expect(yield* Ref.get(calls)).toBe(3);
      }),
    ),
  );

  it.effect("emits persisted refs before a live refresh", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const client = {
          [WS_METHODS.vcsListRefs]: () => Effect.succeed(LIVE_REFS),
        } as unknown as WsRpcProtocolClient;
        const supervisor = MachineSupervisor.MachineSupervisor.of({
          target: TARGET,
          state: yield* SubscriptionRef.make(CONNECTED_CONNECTION_STATE),
          session: yield* SubscriptionRef.make(Option.some(session(client))),
          prepared: yield* SubscriptionRef.make(Option.none<PreparedConnection>()),
          connect: Effect.void,
          disconnect: Effect.void,
          retryNow: Effect.void,
        } satisfies MachineSupervisor.MachineSupervisor["Service"]);

        const refs = yield* Stream.unwrap(
          makeCachedVcsRefsChanges({ cwd: "/repo", limit: 100 }).pipe(
            Effect.provideService(MachineSupervisor.MachineSupervisor, supervisor),
            Effect.provideService(
              Persistence.MachineCacheStore,
              cacheWithRefs(Option.some(CACHED_REFS)),
            ),
          ),
        ).pipe(Stream.take(2), Stream.runCollect);

        expect(refs).toEqual([CACHED_REFS, LIVE_REFS]);
      }),
    ),
  );
});

vi.mock("./session.ts", () => ({
  createMachineSessionAtoms: () => ({ sessionStateAtom: grantedSessions }),
}));
const grantedSessions = Atom.family((_id: MachineId) =>
  Atom.make(
    AsyncResult.success({
      authenticated: true,
      auth: {
        policy: "remote-reachable",
        bootstrapMethods: [],
        sessionMethods: [],
        sessionCookieName: "test",
      },
      scopes: [AuthSourceControlWriteScope],
    }),
  ),
);
