import {
  type MachineId,
  type ServerConfig,
  type ServerConfigStreamEvent,
  type ServerLifecycleWelcomePayload,
  type ServerLifecycleStreamReadyEvent,
  type ServerSelfUpdateProgressEvent,
  type ServerSelfUpdateResult,
  shouldRecommendDatabaseCompaction,
  WS_METHODS,
} from "@vekrel/contracts";
import * as Cause from "effect/Cause";
import * as Duration from "effect/Duration";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { AsyncResult, Atom } from "effect/reactivity";

import {
  createAtomCommandScheduler,
  createMachineRpcCommand,
  createMachineQueryAtomFamily,
  createMachineRpcQueryAtomFamily,
  createMachineRpcSubscriptionAtomFamily,
  createMachineSubscriptionAtomFamily,
  createRuntimeCommand,
  scheduleAtomCommandEffect,
} from "./runtime.ts";
import * as MachineRegistry from "../connection/registry.ts";
import * as MachineSupervisor from "../connection/supervisor.ts";
import { safeErrorLogAttributes } from "../errors/safeLog.ts";
import * as Persistence from "../platform/persistence.ts";
import { runCachePersistence } from "./cachePersistence.ts";
import {
  isRpcClientError,
  request,
  runStream,
  subscribe,
  subscribeDynamicWithSession,
  type MachineRpcInput,
} from "../rpc/client.ts";
import type { RpcSession } from "../rpc/session.ts";
import { followStreamInMachine } from "./runtime.ts";
import {
  applyServerConfigProjection,
  type ServerConfigProjection,
  withoutMachineThemes,
} from "./serverConfigProjection.ts";

export type { ServerConfigProjection } from "./serverConfigProjection.ts";

export type ServerUpdateStage = "downloading" | "installing" | "resuming";

export type ServerUpdateState =
  | { readonly status: "idle" }
  | {
      readonly status: "running";
      readonly stage: ServerUpdateStage;
      readonly fromVersion: string;
      readonly targetVersion: string;
    }
  | {
      readonly status: "failed";
      readonly stage: ServerUpdateStage;
      readonly fromVersion: string;
      readonly targetVersion: string;
      readonly message: string;
    };

export interface ServerUpdateTarget {
  readonly machineId: MachineId;
  readonly input: MachineRpcInput<typeof WS_METHODS.serverUpdateServer>;
}

const IDLE_SERVER_UPDATE_STATE: ServerUpdateState = { status: "idle" };
const EMPTY_SERVER_UPDATE_STATE_ATOM = Atom.make<ServerUpdateState>(IDLE_SERVER_UPDATE_STATE).pipe(
  Atom.withLabel("machine-data:server:update-state:empty"),
);
export const serverUpdateStateAtom = Atom.family((machineId: MachineId) =>
  Atom.make<ServerUpdateState>(IDLE_SERVER_UPDATE_STATE).pipe(
    Atom.withLabel(`machine-data:server:update-state:${machineId}`),
  ),
);

export class ServerUpdateResumeTimeoutError extends Schema.TaggedError<ServerUpdateResumeTimeoutError>()(
  "ServerUpdateResumeTimeoutError",
  {
    machineId: Schema.String,
    targetVersion: Schema.String,
  },
) {
  override get message(): string {
    return `The server did not resume on t3@${this.targetVersion}.`;
  }
}

export class ServerUpdateProgressIncompleteError extends Schema.TaggedError<ServerUpdateProgressIncompleteError>()(
  "ServerUpdateProgressIncompleteError",
  {
    targetVersion: Schema.String,
  },
) {
  override get message(): string {
    return `The t3@${this.targetVersion} update ended before the server accepted the restart.`;
  }
}

export class ServerUpdateTerminalError extends Schema.TaggedError<ServerUpdateTerminalError>()(
  "ServerUpdateTerminalError",
  {
    targetVersion: Schema.String,
    status: Schema.Literals(["committed", "rolled-back", "failed"]),
    reason: Schema.optional(Schema.String),
  },
) {
  override get message(): string {
    return this.reason ?? `The t3@${this.targetVersion} update ${this.status}.`;
  }
}

// Covers the service launcher's 30-minute trial deadline (PREPARED_TIMEOUT_MS)
// and a final restart of the previous version when the trial rolls back.
const SERVER_UPDATE_RESUME_TIMEOUT = Duration.minutes(32);

export function matchesServerUpdateReadyEvent(
  result: ServerSelfUpdateResult,
  event: ServerLifecycleStreamReadyEvent,
): boolean {
  return result.updateId === undefined
    ? event.payload.machine.serverVersion === result.targetVersion
    : event.payload.updateOutcome?.id === result.updateId;
}

export function matchesServerUpdateResumeEvent(
  result: ServerSelfUpdateResult,
  event: ServerLifecycleStreamReadyEvent,
): boolean {
  return (
    (result.method === "desktop-app" && result.desktopUpdateToken !== undefined) ||
    matchesServerUpdateReadyEvent(result, event)
  );
}

export function validateServerUpdateReadyEvent(
  result: ServerSelfUpdateResult,
  event: ServerLifecycleStreamReadyEvent,
): Effect.Effect<void, ServerUpdateTerminalError> {
  if (result.updateId === undefined) return Effect.void;
  const outcome = event.payload.updateOutcome;
  if (
    outcome?.id === result.updateId &&
    outcome.status === "committed" &&
    outcome.targetVersion === result.targetVersion &&
    event.payload.machine.serverVersion === result.targetVersion
  ) {
    return Effect.void;
  }
  return Effect.fail(
    new ServerUpdateTerminalError({
      targetVersion: result.targetVersion,
      status: outcome?.status ?? "failed",
      reason:
        outcome?.reason ??
        "The service launcher resumed without committing the requested server version.",
    }),
  );
}

/**
 * The sleep before each nudge is the pacer: a connection that fails instantly
 * re-enters backoff immediately and would otherwise spin a tight retry loop.
 *
 * Callers fork this as a child of the update command so it is interrupted as
 * soon as the update settles, whether it succeeds, fails, or times out.
 */
export function nudgeReconnectDuringUpdateRestart(input: {
  readonly stateChanges: Stream.Stream<
    {
      readonly phase: string;
      readonly lastFailure?: { readonly reason: string } | null;
    },
    unknown
  >;
  readonly retryNow: Effect.Effect<void>;
  readonly interval?: Duration.Duration;
}): Effect.Effect<void> {
  return input.stateChanges.pipe(
    Stream.filter(
      (state) =>
        state.phase === "backoff" ||
        (state.phase === "blocked" && state.lastFailure?.reason === "authentication"),
    ),
    Stream.runForEach(() =>
      Effect.sleep(input.interval ?? Duration.seconds(1)).pipe(Effect.andThen(input.retryNow)),
    ),
    Effect.timeoutOption(SERVER_UPDATE_RESUME_TIMEOUT),
    Effect.ignore,
  );
}

export function waitForNextMachineReconnect<E>(
  stateChanges: Stream.Stream<{ readonly phase: string }, E>,
): Effect.Effect<void, E> {
  return stateChanges.pipe(
    Stream.dropWhile((state) => state.phase === "connected"),
    Stream.filter((state) => state.phase === "connected"),
    Stream.runHead,
    Effect.asVoid,
  );
}

export const runDesktopCommitWithReconnectObserver = Effect.fn(
  "runDesktopCommitWithReconnectObserver",
)(function* <EState, ECommit>(
  stateChanges: Stream.Stream<{ readonly phase: string }, EState>,
  commit: Effect.Effect<void, ECommit>,
) {
  const armed = yield* Deferred.make<void>();
  const reconnected = yield* Deferred.make<void>();
  const observer = yield* stateChanges.pipe(
    Stream.tap(() => Deferred.succeed(armed, undefined)),
    waitForNextMachineReconnect,
    Effect.andThen(Deferred.succeed(reconnected, undefined)),
    Effect.forkChild,
  );
  yield* Deferred.await(armed);
  const commitExit = yield* commit.pipe(Effect.exit);
  if (Exit.isSuccess(commitExit)) {
    yield* Fiber.interrupt(observer);
    return;
  }
  if (!isLegacyUpdateHandoffLoss(commitExit.cause)) {
    yield* Fiber.interrupt(observer);
    return yield* Effect.failCause(commitExit.cause);
  }
  yield* Deferred.await(reconnected).pipe(Effect.timeout(SERVER_UPDATE_RESUME_TIMEOUT));
  return yield* Effect.failCause(commitExit.cause);
});

export const waitForDesktopUpdateTarget = Effect.fn("waitForDesktopUpdateTarget")(function* <
  EReady,
  ECommit,
>(
  targetVersion: string,
  nextReady: Effect.Effect<ServerLifecycleStreamReadyEvent, EReady>,
  retryCommit: Effect.Effect<void, ECommit>,
  maxCommitAttempts = 3,
): Effect.fn.Return<ServerLifecycleStreamReadyEvent, EReady | ECommit | ServerUpdateTerminalError> {
  for (let attempt = 1; attempt <= maxCommitAttempts; attempt += 1) {
    const ready = yield* nextReady;
    if (ready.payload.machine.serverVersion === targetVersion) return ready;
    if (attempt === maxCommitAttempts) break;
    const retryExit = yield* retryCommit.pipe(Effect.exit);
    if (Exit.isSuccess(retryExit)) break;
    if (!isLegacyUpdateHandoffLoss(retryExit.cause)) {
      return yield* Effect.failCause(retryExit.cause);
    }
  }
  return yield* new ServerUpdateTerminalError({
    targetVersion,
    status: "failed",
    reason: "The desktop app resumed without installing the prepared update.",
  });
});

export function serverUpdateStateForProgressEvent(
  fromVersion: string,
  targetVersion: string,
  event: ServerSelfUpdateProgressEvent,
): Extract<ServerUpdateState, { status: "running" }> {
  return {
    status: "running",
    stage:
      event.type !== "complete"
        ? event.stage
        : event.result.method === "desktop-app" && event.result.desktopUpdateToken !== undefined
          ? "installing"
          : "resuming",
    fromVersion,
    targetVersion,
  };
}

export function serverUpdateStateForServerVersion(
  state: ServerUpdateState,
  serverVersion: string | null,
): ServerUpdateState {
  return state.status === "idle" ||
    state.status === "running" ||
    serverVersion === null ||
    state.fromVersion === serverVersion
    ? state
    : IDLE_SERVER_UPDATE_STATE;
}

export function serverUpdateFailureMessage(error: unknown): string {
  return error instanceof Error ? error.message : "Server update failed.";
}

function isRpcSocketError(error: unknown): boolean {
  if (!isRpcClientError(error)) {
    return false;
  }
  switch (error.reason._tag) {
    case "SocketReadError":
    case "SocketWriteError":
    case "SocketCloseError":
      return true;
    default:
      return false;
  }
}

export function isLegacyUpdateHandoffLoss(cause: Cause.Cause<unknown>): boolean {
  if (Cause.hasInterruptsOnly(cause)) {
    return true;
  }
  return (
    cause.reasons.length > 0 &&
    cause.reasons.every((reason) => Cause.isFailReason(reason) && isRpcSocketError(reason.error))
  );
}

export function resolveServerUpdateProgressResult<E>(
  targetVersion: string,
  terminal: Option.Option<ServerSelfUpdateResult>,
  streamExit: Exit.Exit<void, E>,
): Effect.Effect<ServerSelfUpdateResult, E | ServerUpdateProgressIncompleteError> {
  if (
    Option.isSome(terminal) &&
    (Exit.isSuccess(streamExit) || isLegacyUpdateHandoffLoss(streamExit.cause))
  ) {
    return Effect.succeed(terminal.value);
  }
  if (Exit.isFailure(streamExit)) {
    return Effect.failCause(streamExit.cause);
  }
  return Effect.fail(new ServerUpdateProgressIncompleteError({ targetVersion }));
}

const cachedConfigSnapshotEvent = (config: ServerConfig): ServerConfigStreamEvent => ({
  version: 1,
  type: "snapshot",
  config,
});

export interface ServerConfigSubscriptionOptions {
  readonly machineThemes?: boolean;
  readonly usageLimitSources?: boolean;
  readonly usageLimitsCommand?: boolean;
}

export const makeMachineServerConfigState = Effect.fn("MachineServerConfigState.make")(
  function* (subscription: ServerConfigSubscriptionOptions) {
    const supervisor = yield* MachineSupervisor.MachineSupervisor;
    const cache = yield* Persistence.MachineCacheStore;
    const machineId = supervisor.target.machineId;
    const cachedConfig = yield* cache.loadServerConfig(machineId).pipe(
      Effect.catch((error) =>
        Effect.logWarning("Could not load cached server configuration.").pipe(
          Effect.annotateLogs({
            machineId,
            ...safeErrorLogAttributes(error),
          }),
          Effect.as(Option.none<ServerConfig>()),
        ),
      ),
    );
    const state = yield* SubscriptionRef.make<Option.Option<ServerConfigProjection>>(
      Option.map(cachedConfig, (cached) => ({
        config: withoutMachineThemes(cached),
        latestEvent: cachedConfigSnapshotEvent(withoutMachineThemes(cached)),
        source: "cache" as const,
      })),
    );
    const persistence = yield* Queue.sliding<ServerConfig>(1);
    const pendingPersistence = yield* Ref.make<Option.Option<ServerConfig>>(Option.none());

    const persist = Effect.fn("MachineServerConfigState.persist")(function* (
      config: ServerConfig,
    ) {
      // A cached stall would read as a fresh freeze on the next launch.
      const { lastEventLoopStall: _stall, ...cacheable } = withoutMachineThemes(config);
      return yield* cache.saveServerConfig(machineId, cacheable).pipe(
        Effect.as(true),
        Effect.catch((error) =>
          Effect.logWarning("Could not persist cached server configuration.").pipe(
            Effect.annotateLogs({
              machineId,
              ...safeErrorLogAttributes(error),
            }),
            Effect.as(false),
          ),
        ),
      );
    });

    const persistPending = Effect.fn("MachineServerConfigState.persistPending")(function* (
      config: ServerConfig,
    ) {
      if (!(yield* persist(config))) {
        return;
      }
      yield* Ref.update(pendingPersistence, (pending) =>
        Option.isSome(pending) && pending.value === config ? Option.none() : pending,
      );
    });

    yield* Effect.addFinalizer(() =>
      Ref.get(pendingPersistence).pipe(
        Effect.flatMap(
          Option.match({
            onNone: () => Effect.void,
            onSome: (config) => persist(config).pipe(Effect.asVoid),
          }),
        ),
      ),
    );

    yield* runCachePersistence(persistence, persistPending).pipe(Effect.forkScoped);

    yield* subscribe(WS_METHODS.subscribeServerConfig, {
      ...(subscription.machineThemes === true ? { machineThemes: true } : {}),
      ...(subscription.usageLimitSources === true ? { usageLimitSources: true } : {}),
      ...(subscription.usageLimitsCommand === true ? { usageLimitsCommand: true } : {}),
    }).pipe(
      Stream.runForEach((event) =>
        Effect.gen(function* () {
          const next = applyServerConfigProjection(yield* SubscriptionRef.get(state), event);
          if (Option.isNone(next)) {
            return;
          }
          yield* Ref.set(pendingPersistence, Option.some(next.value.config));
          yield* SubscriptionRef.set(state, next);
          yield* Queue.offer(persistence, next.value.config);
        }),
      ),
      Effect.forkScoped,
    );

    return state;
  },
);

function serverConfigStateChanges(
  machineId: MachineId,
  subscription: ServerConfigSubscriptionOptions,
) {
  return followStreamInMachine(
    machineId,
    Stream.unwrap(
      makeMachineServerConfigState(subscription).pipe(
        Effect.map((state) =>
          SubscriptionRef.changes(state).pipe(
            Stream.filterMap((projection) =>
              Option.match(projection, {
                onNone: () => Result.failVoid,
                onSome: (value) => Result.succeed(value),
              }),
            ),
          ),
        ),
      ),
    ),
  );
}

export function applyServerWelcomeEvent(
  current: MachineServerWelcomeState,
  session: RpcSession,
  event: {
    readonly type: "welcome" | "ready" | "legacyThreadMigration";
    readonly payload: unknown;
  },
): MachineServerWelcomeState {
  return event.type === "welcome" && current.currentSession === session
    ? {
        ...current,
        welcomeSession: session,
        welcome: event.payload as ServerLifecycleWelcomePayload,
      }
    : current;
}

export interface MachineServerWelcomeState {
  readonly currentSession: RpcSession | null;
  readonly welcomeSession: RpcSession | null;
  readonly welcome: ServerLifecycleWelcomePayload | null;
}

export function resolveServerWelcomeState(
  state: MachineServerWelcomeState,
): ServerLifecycleWelcomePayload | null {
  return state.currentSession === state.welcomeSession ? state.welcome : null;
}

export const makeMachineServerWelcomeState = Effect.fn("MachineServerWelcomeState.make")(
  function* () {
    const supervisor = yield* MachineSupervisor.MachineSupervisor;
    const initialSession = Option.getOrNull(yield* SubscriptionRef.get(supervisor.session));
    const state = yield* SubscriptionRef.make<MachineServerWelcomeState>({
      currentSession: initialSession,
      welcomeSession: null,
      welcome: null,
    });

    const updateWithCurrentSession = Effect.fn(
      "MachineServerWelcomeState.updateWithCurrentSession",
    )(function* (
      update: (
        current: MachineServerWelcomeState,
        currentSession: RpcSession | null,
      ) => MachineServerWelcomeState,
    ) {
      return yield* SubscriptionRef.modifyEffect(state, (current) =>
        SubscriptionRef.get(supervisor.session).pipe(
          Effect.map(
            (latestSession) =>
              [undefined, update(current, Option.getOrNull(latestSession))] as const,
          ),
        ),
      );
    });

    yield* SubscriptionRef.changes(supervisor.session).pipe(
      Stream.runForEach(() =>
        updateWithCurrentSession((current, currentSession) => ({
          ...current,
          currentSession,
        })),
      ),
      Effect.forkScoped,
    );

    yield* subscribeDynamicWithSession(
      WS_METHODS.subscribeServerLifecycle,
      Effect.fn("MachineServerWelcomeState.makeSubscribeInput")(function* (session) {
        yield* updateWithCurrentSession((current, currentSession) =>
          currentSession === session
            ? {
                ...current,
                currentSession,
                welcomeSession: session,
                welcome: null,
              }
            : { ...current, currentSession },
        );
        return {};
      }),
    ).pipe(
      Stream.runForEach(([session, event]) =>
        updateWithCurrentSession((current, currentSession) =>
          applyServerWelcomeEvent(
            {
              ...current,
              currentSession,
            },
            session,
            event,
          ),
        ),
      ),
      Effect.forkScoped,
    );

    return state;
  },
);

function serverWelcomeStateChanges(machineId: MachineId) {
  return followStreamInMachine(
    machineId,
    Stream.unwrap(
      makeMachineServerWelcomeState().pipe(
        Effect.map((state) =>
          SubscriptionRef.changes(state).pipe(Stream.map(resolveServerWelcomeState)),
        ),
      ),
    ),
  );
}

export function resolveServerConfigValue(
  projection: ServerConfigProjection | null,
  initialConfig: ServerConfig | null,
): ServerConfig | null {
  if (
    projection?.source === "live" &&
    (initialConfig === null ||
      projection.config.machine.serverVersion === initialConfig.machine.serverVersion)
  ) {
    return projection.config;
  }
  return initialConfig ?? projection?.config ?? null;
}

export function createServerMachineAtoms<R, E>(
  runtime: Atom.AtomRuntime<
    MachineRegistry.MachineRegistry | Persistence.MachineCacheStore | R,
    E
  >,
  options: {
    readonly initialConfigValueAtom: (
      machineId: MachineId,
    ) => Atom.Atom<ServerConfig | null>;
    readonly machineThemes?: boolean;
    readonly usageLimitSources?: boolean;
    readonly usageLimitsCommand?: boolean;
  },
) {
  const configScheduler = createAtomCommandScheduler();
  const updateScheduler = createAtomCommandScheduler();
  const configConcurrency = {
    mode: "serial" as const,
    key: ({ machineId }: { readonly machineId: string }) => machineId,
  };
  const configProjectionFamily = Atom.family((machineId: MachineId) =>
    runtime
      .atom(
        serverConfigStateChanges(machineId, {
          ...(options.machineThemes === true ? { machineThemes: true } : {}),
          ...(options.usageLimitSources === true ? { usageLimitSources: true } : {}),
          ...(options.usageLimitsCommand === true ? { usageLimitsCommand: true } : {}),
        }),
      )
      .pipe(
        Atom.setIdleTTL(5 * 60_000),
        Atom.withLabel(`machine-data:server:config-projection:${machineId}`),
      ),
  );
  const configProjection = (target: {
    readonly machineId: MachineId;
    readonly input: MachineRpcInput<typeof WS_METHODS.subscribeServerConfig>;
  }) => configProjectionFamily(target.machineId);
  const emptyConfigAtom = Atom.make<ServerConfig | null>(null).pipe(
    Atom.withLabel("machine-data:server:config:empty"),
  );
  const configValueAtom = Atom.family((machineId: MachineId | null) => {
    if (machineId === null) {
      return emptyConfigAtom;
    }
    return Atom.make((get): ServerConfig | null => {
      const projection = Option.getOrNull(
        AsyncResult.value(get(configProjection({ machineId, input: {} }))),
      );
      return resolveServerConfigValue(
        projection,
        get(options.initialConfigValueAtom(machineId)),
      );
    }).pipe(Atom.withLabel(`machine-data:server:config:${machineId}`));
  });
  const updateStateValueAtom = Atom.family((machineId: MachineId) =>
    Atom.make((get) =>
      serverUpdateStateForServerVersion(
        get(serverUpdateStateAtom(machineId)),
        get(configValueAtom(machineId))?.machine.serverVersion ?? null,
      ),
    ).pipe(Atom.withLabel(`machine-data:server:update-state-value:${machineId}`)),
  );
  const updateStateAtom = (machineId: MachineId | null) =>
    machineId === null ? EMPTY_SERVER_UPDATE_STATE_ATOM : updateStateValueAtom(machineId);
  const updateServer = createRuntimeCommand<
    MachineRegistry.MachineRegistry | Persistence.MachineCacheStore | R,
    E,
    ServerUpdateTarget,
    ServerSelfUpdateResult,
    unknown
  >(runtime, {
    label: "machine-data:server:update-server",
    scheduler: updateScheduler,
    concurrency: configConcurrency,
    execute: (target, atomRegistry) => {
      const stateAtom = serverUpdateStateAtom(target.machineId);
      let targetVersion = target.input.targetVersion;
      let fromVersion =
        atomRegistry.get(configValueAtom(target.machineId))?.machine.serverVersion ??
        targetVersion;
      let currentStage: ServerUpdateStage = "downloading";
      let desktopCommitLostTransport = false;
      atomRegistry.set(stateAtom, {
        status: "running",
        stage: currentStage,
        fromVersion,
        targetVersion,
      });

      return Effect.gen(function* () {
        const machineRegistry = yield* MachineRegistry.MachineRegistry;
        const desktopCommitStarting = yield* Deferred.make<void>();
        const desktopReconnectObserverArmed = yield* Deferred.make<void>();
        const desktopReconnected = yield* Deferred.make<void>();
        yield* Deferred.await(desktopCommitStarting).pipe(
          Effect.andThen(
            machineRegistry.stateChanges(target.machineId).pipe(
              Stream.tap(() => Deferred.succeed(desktopReconnectObserverArmed, undefined)),
              Stream.dropWhile((state) => state.phase === "connected"),
              Stream.filter((state) => state.phase === "connected"),
              Stream.runHead,
            ),
          ),
          Effect.andThen(Deferred.succeed(desktopReconnected, undefined)),
          Effect.forkChild,
        );
        const result = yield* scheduleAtomCommandEffect(
          atomRegistry,
          configScheduler,
          configConcurrency,
          target,
          Effect.gen(function* () {
            const currentConfig = atomRegistry.get(configValueAtom(target.machineId));
            fromVersion = currentConfig?.machine.serverVersion ?? targetVersion;
            atomRegistry.set(stateAtom, {
              status: "running",
              stage: currentStage,
              fromVersion,
              targetVersion,
            });

            const supportsProgress =
              currentConfig?.machine.capabilities.serverSelfUpdateProgress === true;
            const updateResult: ServerSelfUpdateResult = supportsProgress
              ? yield* Effect.gen(function* () {
                  const terminal = yield* Ref.make<Option.Option<ServerSelfUpdateResult>>(
                    Option.none(),
                  );
                  const streamExit = yield* machineRegistry
                    .runStream(
                      target.machineId,
                      runStream(WS_METHODS.serverUpdateServerWithProgress, target.input),
                    )
                    .pipe(
                      Stream.runForEach((event) =>
                        Effect.sync(() => {
                          const state = serverUpdateStateForProgressEvent(
                            fromVersion,
                            targetVersion,
                            event,
                          );
                          currentStage = state.stage;
                          atomRegistry.set(stateAtom, state);
                        }).pipe(
                          Effect.andThen(
                            event.type === "complete"
                              ? Ref.set(terminal, Option.some(event.result))
                              : Effect.void,
                          ),
                        ),
                      ),
                      Effect.exit,
                    );
                  return yield* resolveServerUpdateProgressResult(
                    targetVersion,
                    yield* Ref.get(terminal),
                    streamExit,
                  );
                })
              : yield* Effect.gen(function* () {
                  const selfUpdateMethod = currentConfig?.machine.capabilities.serverSelfUpdate;
                  const exit = yield* machineRegistry
                    .run(target.machineId, request(WS_METHODS.serverUpdateServer, target.input))
                    .pipe(Effect.exit);
                  if (Exit.isSuccess(exit)) {
                    return exit.value;
                  }
                  if (
                    (selfUpdateMethod === "boot-service" || selfUpdateMethod === "respawn") &&
                    isLegacyUpdateHandoffLoss(exit.cause)
                  ) {
                    // Older servers can tear down the transport before their
                    // unary acknowledgement arrives. Treat only that transport
                    // loss as a handoff, then prove it by waiting for target ready.
                    return { targetVersion, method: selfUpdateMethod };
                  }
                  return yield* Effect.failCause(exit.cause);
                });

            if (
              updateResult.method === "desktop-app" &&
              updateResult.desktopUpdateToken !== undefined
            ) {
              yield* Deferred.succeed(desktopCommitStarting, undefined);
              yield* Deferred.await(desktopReconnectObserverArmed);
              const commitExit = yield* machineRegistry
                .run(
                  target.machineId,
                  request(WS_METHODS.serverCommitDesktopUpdate, {
                    requestId: updateResult.desktopUpdateToken,
                  }),
                )
                .pipe(Effect.exit);
              if (Exit.isFailure(commitExit) && !isLegacyUpdateHandoffLoss(commitExit.cause)) {
                return yield* Effect.failCause(commitExit.cause);
              }
              desktopCommitLostTransport = Exit.isFailure(commitExit);
            }

            targetVersion = updateResult.targetVersion;

            currentStage = "resuming";
            atomRegistry.set(stateAtom, {
              status: "running",
              stage: currentStage,
              fromVersion,
              targetVersion,
            });
            return updateResult;
          }),
        );

        yield* nudgeReconnectDuringUpdateRestart({
          stateChanges: machineRegistry.stateChanges(target.machineId),
          retryNow: machineRegistry.retryNow(target.machineId),
        }).pipe(Effect.forkChild);

        if (result.method === "desktop-app" && desktopCommitLostTransport) {
          yield* Deferred.await(desktopReconnected).pipe(
            Effect.timeout(SERVER_UPDATE_RESUME_TIMEOUT),
          );
        }

        const waitForReady = machineRegistry
          .followStream(target.machineId, subscribe(WS_METHODS.subscribeServerLifecycle, {}))
          .pipe(
            Stream.filter(
              (event): event is ServerLifecycleStreamReadyEvent =>
                event.type === "ready" && matchesServerUpdateResumeEvent(result, event),
            ),
            Stream.runHead,
            Effect.timeoutOption(SERVER_UPDATE_RESUME_TIMEOUT),
            Effect.map(Option.flatten),
          );
        const nextReady = waitForReady.pipe(
          Effect.flatMap(
            Option.match({
              onNone: () =>
                new ServerUpdateResumeTimeoutError({
                  machineId: target.machineId,
                  targetVersion,
                }),
              onSome: Effect.succeed,
            }),
          ),
        );
        const desktopUpdateToken = result.desktopUpdateToken;
        const resumed =
          result.method === "desktop-app" && desktopUpdateToken !== undefined
            ? yield* waitForDesktopUpdateTarget(
                result.targetVersion,
                nextReady,
                runDesktopCommitWithReconnectObserver(
                  machineRegistry.stateChanges(target.machineId),
                  machineRegistry
                    .run(
                      target.machineId,
                      request(WS_METHODS.serverCommitDesktopUpdate, {
                        requestId: desktopUpdateToken,
                      }),
                    )
                    .pipe(Effect.asVoid),
                ),
              )
            : yield* nextReady;
        yield* validateServerUpdateReadyEvent(result, resumed);

        atomRegistry.set(stateAtom, IDLE_SERVER_UPDATE_STATE);
        return result;
      }).pipe(
        Effect.onExit((exit) =>
          Effect.sync(() => {
            if (Exit.isSuccess(exit)) {
              return;
            }
            if (Cause.hasInterruptsOnly(exit.cause)) {
              atomRegistry.set(stateAtom, IDLE_SERVER_UPDATE_STATE);
              return;
            }
            atomRegistry.set(stateAtom, {
              status: "failed",
              stage: currentStage,
              fromVersion,
              targetVersion,
              message: serverUpdateFailureMessage(Cause.squash(exit.cause)),
            });
          }),
        ),
      );
    },
  });
  const settingsValueAtom = Atom.family((machineId: MachineId) =>
    Atom.make((get) => get(configValueAtom(machineId))?.settings ?? null).pipe(
      Atom.withLabel(`machine-data:server:settings:${machineId}`),
    ),
  );
  const usagePricesAtom = Atom.family((machineId: MachineId) =>
    Atom.make((get) => {
      const overrides = get(settingsValueAtom(machineId))?.usagePriceOverrides ?? {};
      // Only changed prices should trigger another transcript scan. Settings
      // snapshots can recreate the same mapping in a different property order.
      return JSON.stringify(
        Object.keys(overrides)
          .sort()
          .map((model) => {
            const price = overrides[model]!;
            return [
              model,
              price.inputCostPerMillionTokens,
              price.outputCostPerMillionTokens,
              price.cacheReadCostPerMillionTokens,
              price.cacheWriteCostPerMillionTokens,
            ];
          }),
      );
    }).pipe(Atom.withLabel(`machine-data:server:usage-prices:${machineId}`)),
  );
  const usageScanSettingsAtom = Atom.family((machineId: MachineId) =>
    Atom.make((get) => {
      const settings = get(settingsValueAtom(machineId));
      const aliases = settings?.usageModelAliases ?? {};
      return JSON.stringify([
        get(usagePricesAtom(machineId)),
        Object.keys(aliases)
          .sort()
          .map((model) => [model, aliases[model]]),
        settings?.cursorKeychainUsageEnabled ?? false,
      ]);
    }).pipe(Atom.withLabel(`machine-data:server:usage-scan-settings:${machineId}`)),
  );
  const providersValueAtom = Atom.family((machineId: MachineId) =>
    Atom.make((get) => get(configValueAtom(machineId))?.providers ?? null).pipe(
      Atom.withLabel(`machine-data:server:providers:${machineId}`),
    ),
  );
  const welcomeStateFamily = Atom.family((machineId: MachineId) =>
    runtime
      .atom(serverWelcomeStateChanges(machineId), { initialValue: null })
      .pipe(
        Atom.setIdleTTL(5 * 60_000),
        Atom.withLabel(`machine-data:server:welcome-state:${machineId}`),
      ),
  );
  const welcomeFamily = Atom.family((machineId: MachineId) =>
    Atom.make((get) => {
      const result = get(welcomeStateFamily(machineId));
      if (result._tag !== "Success") return result;
      return result.value === null
        ? AsyncResult.initial<ServerLifecycleWelcomePayload, never>(result.waiting)
        : AsyncResult.success(result.value, result);
    }).pipe(Atom.withLabel(`machine-data:server:welcome:${machineId}`)),
  );
  const databaseStorage = createMachineRpcSubscriptionAtomFamily(runtime, {
    label: "machine-data:server:database-storage",
    tag: WS_METHODS.serverGetDatabaseStorage,
    idleTtlMs: 0,
  });
  // Keyed by a JSON array of machine ids so one notice can list every machine.
  const databaseCompactionRecommendations = Atom.family((machineIdsKey: string) => {
    const sources = (JSON.parse(machineIdsKey) as MachineId[]).map((machineId) => ({
      machineId,
      atom: databaseStorage({ machineId, input: {} }),
    }));
    return Atom.make((get) =>
      sources
        .flatMap(({ machineId, atom }) => {
          const result = get(atom);
          return result._tag === "Success" &&
            result.value !== null &&
            shouldRecommendDatabaseCompaction(result.value)
            ? [{ machineId, status: result.value }]
            : [];
        })
        .sort((left, right) => right.status.reclaimableBytes - left.status.reclaimableBytes),
    ).pipe(
      Atom.withLabel(
        `machine-data:server:database-compaction-recommendations:${machineIdsKey}`,
      ),
    );
  });
  const welcome = (target: {
    readonly machineId: MachineId;
    readonly input: MachineRpcInput<typeof WS_METHODS.subscribeServerLifecycle>;
  }) => welcomeFamily(target.machineId);
  const updateSettings = createMachineRpcCommand(runtime, {
    label: "machine-data:server:update-settings",
    tag: WS_METHODS.serverUpdateSettings,
    scheduler: configScheduler,
    concurrency: configConcurrency,
  });

  return {
    configValueAtom,
    runStorageCleanup: createMachineRpcCommand(runtime, {
      label: "machine-data:server:run-storage-cleanup",
      tag: WS_METHODS.serverRunStorageCleanup,
      concurrency: { mode: "singleFlight", key: ({ machineId }) => machineId },
    }),
    databaseStorage,
    databaseCompactionRecommendations,
    storageCleanupReport: createMachineRpcSubscriptionAtomFamily(runtime, {
      label: "machine-data:server:storage-cleanup-report",
      tag: WS_METHODS.serverGetStorageCleanupReport,
      idleTtlMs: 0,
    }),
    updateStateAtom,
    settingsValueAtom,
    providersValueAtom,
    providerAuthState: createMachineRpcSubscriptionAtomFamily(runtime, {
      label: "machine-data:provider:auth-state",
      tag: WS_METHODS.providerAuthSubscribe,
      idleTtlMs: 0,
    }),
    startProviderAuth: createMachineRpcCommand(runtime, {
      label: "machine-data:provider:auth-start",
      tag: WS_METHODS.providerAuthStart,
      concurrency: {
        mode: "singleFlight",
        key: ({ machineId, input }) => JSON.stringify([machineId, input]),
      },
    }),
    respondProviderAuth: createMachineRpcCommand(runtime, {
      label: "machine-data:provider:auth-respond",
      tag: WS_METHODS.providerAuthRespond,
    }),
    completeProviderAuth: createMachineRpcCommand(runtime, {
      label: "machine-data:provider:auth-complete",
      tag: WS_METHODS.providerAuthComplete,
    }),
    chatGptReconnectProfile: createMachineRpcCommand(runtime, {
      label: "machine-data:chatgpt:reconnect-profile",
      tag: WS_METHODS.chatGptReconnectProfile,
    }),
    chatGptImportProfile: createMachineRpcCommand(runtime, {
      label: "machine-data:chatgpt:import-profile",
      tag: WS_METHODS.chatGptImportProfile,
    }),
    chatGptHandoffState: createMachineSubscriptionAtomFamily(runtime, {
      label: "machine-data:chatgpt:handoff",
      sensitiveInput: true,
      // OAuth must not be replayed when the connection recovers.
      subscribe: (input: MachineRpcInput<typeof WS_METHODS.chatGptHandoffSubscribe>) =>
        runStream(WS_METHODS.chatGptHandoffSubscribe, input),
      idleTtlMs: 0,
    }),
    codexAuthCallbackState: createMachineRpcSubscriptionAtomFamily(runtime, {
      label: "machine-data:codex:auth-callback",
      tag: WS_METHODS.codexAuthCallbackSubscribe,
      idleTtlMs: 0,
    }),
    cancelProviderAuth: createMachineRpcCommand(runtime, {
      label: "machine-data:provider:auth-cancel",
      tag: WS_METHODS.providerAuthCancel,
    }),
    logoutProviderAuth: createMachineRpcCommand(runtime, {
      label: "machine-data:provider:auth-logout",
      tag: WS_METHODS.providerAuthLogout,
    }),
    providerInstallState: createMachineRpcSubscriptionAtomFamily(runtime, {
      label: "machine-data:provider:install-state",
      tag: WS_METHODS.providerInstallSubscribe,
      idleTtlMs: 0,
    }),
    startProviderInstall: createMachineRpcCommand(runtime, {
      label: "machine-data:provider:install-start",
      tag: WS_METHODS.providerInstallStart,
      concurrency: {
        mode: "singleFlight",
        key: ({ machineId }) => machineId,
      },
    }),
    cancelProviderInstall: createMachineRpcCommand(runtime, {
      label: "machine-data:provider:install-cancel",
      tag: WS_METHODS.providerInstallCancel,
    }),
    removeProviderInstallation: createMachineRpcCommand(runtime, {
      label: "machine-data:provider:install-remove",
      tag: WS_METHODS.providerInstallRemove,
    }),
    traceDiagnostics: createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:server:trace-diagnostics",
      tag: WS_METHODS.serverGetTraceDiagnostics,
    }),
    processDiagnostics: createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:server:process-diagnostics",
      tag: WS_METHODS.serverGetProcessDiagnostics,
    }),
    hostResources: createMachineQueryAtomFamily(runtime, {
      label: "machine-data:server:host-resources",
      idleTtlMs: 0,
      staleTimeMs: 5_000,
      execute: (input: MachineRpcInput<typeof WS_METHODS.serverGetHostResources>) =>
        request(WS_METHODS.serverGetHostResources, input).pipe(Effect.timeout("5 seconds")),
    }),
    processResourceHistory: createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:server:process-resource-history",
      tag: WS_METHODS.serverGetProcessResourceHistory,
    }),
    scheduledTaskWebhookDeliveries: createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:server:scheduled-task:webhook-deliveries",
      tag: WS_METHODS.scheduledTasksListWebhookDeliveries,
    }),
    scheduledTaskWebhookDelivery: createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:server:scheduled-task:webhook-delivery",
      tag: WS_METHODS.scheduledTasksGetWebhookDelivery,
    }),
    scheduledTasksLive: createMachineRpcSubscriptionAtomFamily(runtime, {
      label: "machine-data:server:scheduled-tasks:live",
      tag: WS_METHODS.scheduledTasksSubscribe,
    }),
    // A cold transcript scan is measured in seconds, so keep the result around
    // long enough that switching windows or re-rendering does not rescan.
    usageSummary: createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:server:usage-summary",
      tag: WS_METHODS.serverGetUsageSummary,
      staleTimeMs: 60_000,
      refreshTrigger: ({ machineId }) => usageScanSettingsAtom(machineId),
      execute: (input, emit) =>
        request(WS_METHODS.serverGetUsageSummary, input).pipe(
          Effect.flatMap((summary) =>
            summary.sources.some((source) => source.refreshing === true)
              ? emit(summary).pipe(
                  Effect.andThen(
                    request(WS_METHODS.serverGetUsageSummary, { ...input, awaitRefresh: true }),
                  ),
                  Effect.catch((error) =>
                    Effect.logWarning("Could not refresh slow usage sources.").pipe(
                      Effect.annotateLogs({ ...safeErrorLogAttributes(error) }),
                      Effect.as(summary),
                    ),
                  ),
                )
              : Effect.succeed(summary),
          ),
        ),
    }),
    resourceTelemetry: createMachineRpcSubscriptionAtomFamily(runtime, {
      label: "machine-data:server:resource-telemetry",
      tag: WS_METHODS.subscribeResourceTelemetry,
      idleTtlMs: 0,
    }),
    resourceTelemetryHistory: createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:server:resource-telemetry-history",
      tag: WS_METHODS.serverGetResourceTelemetryHistory,
      staleTimeMs: 5_000,
    }),
    searchAcpRegistry: createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:server:acp-registry:search",
      tag: WS_METHODS.serverSearchAcpRegistry,
      staleTimeMs: 0,
      idleTtlMs: 0,
    }),
    configProjection,
    welcome,
    legacyThreadMigration: createMachineRpcSubscriptionAtomFamily(runtime, {
      label: "machine-data:server:legacy-thread-migration",
      tag: WS_METHODS.subscribeServerLifecycle,
      transform: (stream) =>
        stream.pipe(
          Stream.filterMap((event) =>
            event.type === "legacyThreadMigration"
              ? Result.succeed(event.payload)
              : Result.failVoid,
          ),
        ),
    }),
    consumeResetCredit: createMachineRpcCommand(runtime, {
      label: "machine-data:server:consume-reset-credit",
      tag: WS_METHODS.providerConsumeResetCredit,
      concurrency: {
        mode: "singleFlight",
        // Both ids are free-form strings; a delimiter could collide.
        key: ({ machineId, input }) => JSON.stringify([machineId, input]),
      },
    }),
    refreshProviders: createMachineRpcCommand(runtime, {
      label: "machine-data:server:refresh-providers",
      tag: WS_METHODS.serverRefreshProviders,
      concurrency: {
        mode: "singleFlight",
        key: ({ machineId, input }) =>
          JSON.stringify([
            machineId,
            input.instanceId ?? null,
            input.cwd ?? null,
            input.fresh ?? false,
            input.refreshModels ?? false,
          ]),
      },
    }),
    updateProvider: createMachineRpcCommand(runtime, {
      label: "machine-data:server:update-provider",
      tag: WS_METHODS.serverUpdateProvider,
      scheduler: configScheduler,
      concurrency: configConcurrency,
    }),
    updateServer,
    upsertKeybinding: createMachineRpcCommand(runtime, {
      label: "machine-data:server:upsert-keybinding",
      tag: WS_METHODS.serverUpsertKeybinding,
      scheduler: configScheduler,
      concurrency: configConcurrency,
    }),
    removeKeybinding: createMachineRpcCommand(runtime, {
      label: "machine-data:server:remove-keybinding",
      tag: WS_METHODS.serverRemoveKeybinding,
      scheduler: configScheduler,
      concurrency: configConcurrency,
    }),
    updateSettings,
    mutateProviderInstance: updateSettings,
    prepareAcpRegistryAgent: createMachineRpcCommand(runtime, {
      label: "machine-data:server:acp-registry:prepare",
      tag: WS_METHODS.serverPrepareAcpRegistryAgent,
      concurrency: {
        mode: "singleFlight",
        key: ({ machineId, input }) => `${machineId}:${input.agentId}`,
      },
    }),
    uninstallAcpRegistryManagedBinary: createMachineRpcCommand(runtime, {
      label: "machine-data:server:acp-registry:uninstall-managed-binary",
      tag: WS_METHODS.serverUninstallAcpRegistryManagedBinary,
      concurrency: {
        mode: "singleFlight",
        key: ({ machineId, input }) => `${machineId}:${input.agentId}`,
      },
    }),
    acceptAcpRegistryUrlAuth: createMachineRpcCommand(runtime, {
      label: "machine-data:server:acp-registry:accept-url-auth",
      tag: WS_METHODS.serverAcceptAcpRegistryUrlAuth,
      concurrency: {
        mode: "singleFlight",
        key: ({ machineId, input }) =>
          `${machineId}:${input.instanceId}:${input.elicitationId}`,
      },
    }),
    listAcpRegistrySessions: createMachineRpcCommand(runtime, {
      label: "machine-data:server:acp-registry:list-sessions",
      tag: WS_METHODS.serverListAcpRegistrySessions,
      concurrency: {
        mode: "singleFlight",
        key: ({ machineId, input }) =>
          `${machineId}:${input.instanceId}:${input.projectId}:${input.cursor ?? "first"}`,
      },
    }),
    importAcpRegistrySession: createMachineRpcCommand(runtime, {
      label: "machine-data:server:acp-registry:import-session",
      tag: WS_METHODS.serverImportAcpRegistrySession,
      concurrency: {
        mode: "singleFlight",
        key: ({ machineId, input }) =>
          `${machineId}:${input.instanceId}:${input.projectId}:${input.sessionId}`,
      },
    }),
    deleteAcpRegistrySession: createMachineRpcCommand(runtime, {
      label: "machine-data:server:acp-registry:delete-session",
      tag: WS_METHODS.serverDeleteAcpRegistrySession,
      concurrency: {
        mode: "singleFlight",
        key: ({ machineId, input }) =>
          `${machineId}:${input.instanceId}:${input.projectId}:${input.sessionId}`,
      },
    }),
    listAcpRegistryProviders: createMachineRpcCommand(runtime, {
      label: "machine-data:server:acp-registry:list-providers",
      tag: WS_METHODS.serverListAcpRegistryProviders,
      concurrency: {
        mode: "singleFlight",
        key: ({ machineId, input }) =>
          `${machineId}:${input.instanceId}:${input.projectId}`,
      },
    }),
    setAcpRegistryProvider: createMachineRpcCommand(runtime, {
      label: "machine-data:server:acp-registry:set-provider",
      tag: WS_METHODS.serverSetAcpRegistryProvider,
      concurrency: {
        mode: "singleFlight",
        key: ({ machineId, input }) =>
          `${machineId}:${input.instanceId}:${input.projectId}:${input.providerId}`,
      },
    }),
    disableAcpRegistryProvider: createMachineRpcCommand(runtime, {
      label: "machine-data:server:acp-registry:disable-provider",
      tag: WS_METHODS.serverDisableAcpRegistryProvider,
      concurrency: {
        mode: "singleFlight",
        key: ({ machineId, input }) =>
          `${machineId}:${input.instanceId}:${input.projectId}:${input.providerId}`,
      },
    }),
    logoutAcpRegistry: createMachineRpcCommand(runtime, {
      label: "machine-data:server:acp-registry:logout",
      tag: WS_METHODS.serverLogoutAcpRegistry,
      concurrency: {
        mode: "singleFlight",
        key: ({ machineId, input }) => `${machineId}:${input.instanceId}`,
      },
    }),
    signalProcess: createMachineRpcCommand(runtime, {
      label: "machine-data:server:signal-process",
      tag: WS_METHODS.serverSignalProcess,
    }),
    upsertScheduledTask: createMachineRpcCommand(runtime, {
      label: "machine-data:server:scheduled-task:upsert",
      tag: WS_METHODS.scheduledTasksUpsert,
      scheduler: configScheduler,
      concurrency: configConcurrency,
    }),
    setScheduledTaskEnabled: createMachineRpcCommand(runtime, {
      label: "machine-data:server:scheduled-task:set-enabled",
      tag: WS_METHODS.scheduledTasksSetEnabled,
      scheduler: configScheduler,
      concurrency: configConcurrency,
    }),
    deleteScheduledTask: createMachineRpcCommand(runtime, {
      label: "machine-data:server:scheduled-task:delete",
      tag: WS_METHODS.scheduledTasksDelete,
      scheduler: configScheduler,
      concurrency: configConcurrency,
    }),
    // Deliberately not on the config lane: run-now blocks until the run is
    // dispatched, and a slow run must not stall settings/keybinding/provider
    // mutations (or other scheduled-task edits) queued behind it.
    runScheduledTaskNow: createMachineRpcCommand(runtime, {
      label: "machine-data:server:scheduled-task:run-now",
      tag: WS_METHODS.scheduledTasksRunNow,
    }),
    rotateScheduledTaskWebhookToken: createMachineRpcCommand(runtime, {
      label: "machine-data:server:scheduled-task:rotate-webhook-token",
      tag: WS_METHODS.scheduledTasksRotateWebhookToken,
      scheduler: configScheduler,
      concurrency: configConcurrency,
    }),
    // Off the config lane: answering a card must not queue behind settings
    // edits. One answer per card at a time.
    answerSecretRequest: createMachineRpcCommand(runtime, {
      label: "machine-data:server:secrets:answer-request",
      tag: WS_METHODS.secretsAnswerRequest,
      concurrency: {
        mode: "singleFlight",
        key: ({ machineId, input }) =>
          JSON.stringify([machineId, input.threadId, input.turnItemId]),
      },
    }),
    refreshUsageRates: createMachineRpcCommand(runtime, {
      label: "machine-data:server:refresh-usage-rates",
      tag: WS_METHODS.serverRefreshUsageRates,
      concurrency: {
        mode: "singleFlight",
        key: ({ machineId }) => machineId,
      },
    }),
    checkOtlpEndpoint: createMachineRpcCommand(runtime, {
      label: "machine-data:server:check-otlp-endpoint",
      tag: WS_METHODS.serverCheckOtlpEndpoint,
    }),
    retryResourceTelemetry: createMachineRpcCommand(runtime, {
      label: "machine-data:server:retry-resource-telemetry",
      tag: WS_METHODS.serverRetryResourceTelemetry,
      concurrency: {
        mode: "singleFlight",
        key: ({ machineId }) => machineId,
      },
    }),
  };
}
