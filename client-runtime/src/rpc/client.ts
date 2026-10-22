import {
  MachineAuthorizationError,
  clientRpcRequiredScopes,
  authScopeRequiredResponse,
  type MachineId,
  type ClientGuardedRpcTag,
  ORCHESTRATION_V2_WS_METHODS,
  WS_METHODS,
} from "@vekrel/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { RpcClientError } from "effect/rpc";

import * as MachineSupervisor from "../connection/supervisor.ts";
import type { WsRpcProtocolClient } from "../rpc/protocol.ts";
import type { RpcSession } from "../rpc/session.ts";

export class MachineRpcUnavailableError extends Schema.TaggedError<MachineRpcUnavailableError>()(
  "MachineRpcUnavailableError",
  {
    machineId: Schema.String,
    message: Schema.String,
  },
) {}

export interface MachineRpcRequestObservation {
  readonly machineId: string;
  readonly machineLabel: string;
  readonly method: string;
}

export class MachineRpcRequestObserver extends Context.Reference<{
  readonly observe: (
    request: MachineRpcRequestObservation,
  ) => Effect.Effect<Effect.Effect<void>>;
}>("@vekrel/client-runtime/rpc/MachineRpcRequestObserver", {
  defaultValue: () => ({
    observe: () => Effect.succeed(Effect.void),
  }),
}) {}

export type MachineRpcTag = keyof WsRpcProtocolClient & string;
type RpcMethod<TTag extends MachineRpcTag> = WsRpcProtocolClient[TTag];

export type MachineSubscriptionRpcTag =
  | typeof WS_METHODS.codexAuthCallbackSubscribe
  | typeof WS_METHODS.providerAuthSubscribe
  | typeof WS_METHODS.providerInstallSubscribe
  | typeof ORCHESTRATION_V2_WS_METHODS.subscribeShell
  | typeof ORCHESTRATION_V2_WS_METHODS.subscribeThread
  | typeof ORCHESTRATION_V2_WS_METHODS.subscribeCommandOutput
  | typeof ORCHESTRATION_V2_WS_METHODS.searchThreadStream
  | typeof WS_METHODS.subscribeAuthAccess
  | typeof WS_METHODS.subscribeServerConfig
  | typeof WS_METHODS.subscribeServerLifecycle
  | typeof WS_METHODS.scheduledTasksSubscribe
  | typeof WS_METHODS.serverGetStorageCleanupReport
  | typeof WS_METHODS.serverGetDatabaseStorage
  | typeof WS_METHODS.subscribeTerminalEvents
  | typeof WS_METHODS.subscribeTerminalMetadata
  | typeof WS_METHODS.subscribePreviewEvents
  | typeof WS_METHODS.subscribeDiscoveredLocalServers
  | typeof WS_METHODS.subscribeDeviceState
  | typeof WS_METHODS.subscribeResourceTelemetry
  | typeof WS_METHODS.pullRequestsSubscribeRefreshes
  | typeof WS_METHODS.subscribeVcsStatus
  | typeof WS_METHODS.subscribeWorktreeSetup
  | typeof WS_METHODS.subscribeProjectClones
  | typeof WS_METHODS.terminalAttach
  | typeof WS_METHODS.terminalObserve;

export type MachineStreamCommandRpcTag =
  | typeof WS_METHODS.chatGptHandoffSubscribe
  | typeof WS_METHODS.cloudInstallRelayClient
  | typeof WS_METHODS.serverUpdateServerWithProgress
  | typeof WS_METHODS.gitRunStackedAction;

export type MachineStreamRpcTag =
  | MachineSubscriptionRpcTag
  | MachineStreamCommandRpcTag;

export type MachineUnaryRpcTag = Exclude<MachineRpcTag, MachineStreamRpcTag>;

export interface MachineRpcSubscriptionObservation {
  readonly machineId: string;
  readonly method: MachineSubscriptionRpcTag;
  readonly input: unknown;
}

export class MachineRpcSubscriptionObserver extends Context.Reference<{
  readonly observe: (
    subscription: MachineRpcSubscriptionObservation,
  ) => Effect.Effect<Effect.Effect<void>>;
}>("@vekrel/client-runtime/rpc/MachineRpcSubscriptionObserver", {
  defaultValue: () => ({
    observe: () => Effect.succeed(Effect.void),
  }),
}) {}

export const isRpcClientError = Schema.is(RpcClientError.RpcClientError);
const isMachineAuthorizationError = Schema.is(MachineAuthorizationError);

const MAX_EXPECTED_FAILURE_RETRY_DELAY_MS = 30_000;

export type MachineRpcInput<TTag extends MachineRpcTag> = Parameters<RpcMethod<TTag>>[0];

export type MachineRpcSuccess<TTag extends MachineUnaryRpcTag> =
  RpcMethod<TTag> extends (input: any, options?: any) => Effect.Effect<infer A, any, any>
    ? A
    : never;

export type MachineRpcFailure<TTag extends MachineUnaryRpcTag> =
  RpcMethod<TTag> extends (input: any, options?: any) => Effect.Effect<any, infer E, any>
    ? E
    : never;

export type MachineRpcStreamValue<TTag extends MachineStreamRpcTag> =
  RpcMethod<TTag> extends (input: any, options?: any) => Stream.Stream<infer A, any, any>
    ? A
    : never;

export type MachineRpcStreamFailure<TTag extends MachineStreamRpcTag> =
  RpcMethod<TTag> extends (input: any, options?: any) => Stream.Stream<any, infer E, any>
    ? E
    : never;

const currentSession = Effect.fn("MachineRpc.currentSession")(function* () {
  const supervisor = yield* MachineSupervisor.MachineSupervisor;
  return yield* SubscriptionRef.get(supervisor.session).pipe(
    Effect.flatMap(
      Option.match({
        onNone: () =>
          Effect.fail(
            new MachineRpcUnavailableError({
              machineId: supervisor.target.machineId,
              message: `${supervisor.target.label} is not connected.`,
            }),
          ),
        onSome: Effect.succeed,
      }),
    ),
  );
});

export const getInitialServerConfig = Effect.fn("MachineRpc.getInitialServerConfig")(
  function* () {
    const session = yield* currentSession();
    return yield* session.initialConfig;
  },
);

/** Installed by the shared command boundary. A missing boundary denies protected writes. */
export class RpcPermissionGuard extends Context.Reference<{
  readonly authorize: (
    machineId: MachineId,
    method: string,
    input: unknown,
  ) => Effect.Effect<void, MachineAuthorizationError>;
}>("@vekrel/client-runtime/rpc/RpcPermissionGuard", {
  defaultValue: () => ({
    authorize: (_machineId, method, input) => {
      const scope = clientRpcRequiredScopes(method, input)[0];
      return scope === undefined
        ? Effect.void
        : Effect.fail(
            new MachineAuthorizationError({
              ...authScopeRequiredResponse(scope),
              message: `This connection requires ${scope}.`,
            }),
          );
    },
  }),
}) {}

const authorizeRequest = Effect.fn("MachineRpc.authorize")(function* (
  method: string,
  input: unknown,
) {
  if (clientRpcRequiredScopes(method, input).length === 0) return;
  const supervisor = yield* MachineSupervisor.MachineSupervisor;
  const guard = yield* RpcPermissionGuard;
  yield* guard.authorize(supervisor.target.machineId, method, input);
});

export const requestGuarded = Effect.fn("MachineRpc.request")(function* <
  TTag extends MachineUnaryRpcTag,
>(tag: TTag, input: MachineRpcInput<TTag>) {
  const supervisor = yield* MachineSupervisor.MachineSupervisor;
  yield* Effect.annotateCurrentSpan({
    "machine.id": supervisor.target.machineId,
    "rpc.method": tag,
  });
  const session = yield* currentSession();
  yield* authorizeRequest(tag, input);
  const observer = yield* MachineRpcRequestObserver;
  const method = session.client[tag] as (
    input: MachineRpcInput<TTag>,
  ) => Effect.Effect<MachineRpcSuccess<TTag>, MachineRpcFailure<TTag>>;
  const completeObservation = yield* observer.observe({
    machineId: supervisor.target.machineId,
    machineLabel: supervisor.target.label,
    method: tag,
  });
  return yield* method(input).pipe(Effect.ensuring(completeObservation));
});

export function runStreamGuarded<TTag extends MachineStreamCommandRpcTag>(
  tag: TTag,
  input: MachineRpcInput<TTag>,
): Stream.Stream<
  MachineRpcStreamValue<TTag>,
  | MachineRpcStreamFailure<TTag>
  | MachineRpcUnavailableError
  | MachineAuthorizationError,
  MachineSupervisor.MachineSupervisor
> {
  return Stream.unwrap(
    authorizeRequest(tag, input).pipe(
      Effect.andThen(currentSession()),
      Effect.map((session) => {
        const method = session.client[tag] as (
          input: MachineRpcInput<TTag>,
        ) => Stream.Stream<MachineRpcStreamValue<TTag>, MachineRpcStreamFailure<TTag>>;
        return method(input);
      }),
    ),
  ).pipe(
    Stream.withSpan("MachineRpc.runStream", {
      attributes: { "rpc.method": tag },
    }),
  );
}

interface SubscriptionOptions<TTag extends MachineSubscriptionRpcTag> {
  readonly onDefect?: (
    cause: Cause.Cause<MachineRpcStreamFailure<TTag>>,
  ) => Effect.Effect<void, never, never>;
  readonly onExpectedFailure?: (
    cause: Cause.Cause<MachineRpcStreamFailure<TTag>>,
  ) => Effect.Effect<void, never, never>;
  readonly retryExpectedFailureAfter?: Duration.Input;
  readonly resubscribe?: Stream.Stream<unknown, never, never>;
}

function subscribeDynamicMapped<TTag extends MachineSubscriptionRpcTag, A>(
  tag: TTag,
  makeInput: (session: RpcSession) => Effect.Effect<MachineRpcInput<TTag>>,
  mapStream: (
    session: RpcSession,
    stream: Stream.Stream<MachineRpcStreamValue<TTag>, MachineRpcStreamFailure<TTag>>,
  ) => Stream.Stream<A, MachineRpcStreamFailure<TTag>>,
  options?: SubscriptionOptions<TTag>,
): Stream.Stream<
  A,
  MachineRpcStreamFailure<TTag>,
  MachineSupervisor.MachineSupervisor
> {
  return Stream.unwrap(
    Effect.gen(function* () {
      const supervisor = yield* MachineSupervisor.MachineSupervisor;
      const observer = yield* MachineRpcSubscriptionObserver;
      const sessionChanges = SubscriptionRef.changes(supervisor.session);
      const sessions =
        options?.resubscribe === undefined
          ? sessionChanges
          : Stream.merge(
              sessionChanges,
              options.resubscribe.pipe(
                Stream.mapEffect(() => SubscriptionRef.get(supervisor.session)),
              ),
            );
      return sessions.pipe(
        Stream.switchMap(
          Option.match({
            onNone: () => Stream.empty,
            onSome: (session) => {
              const method = (
                tag === WS_METHODS.subscribeServerConfig
                  ? session.subscribeServerConfig
                  : session.client[tag]
              ) as (
                input: MachineRpcInput<TTag>,
              ) => Stream.Stream<
                MachineRpcStreamValue<TTag>,
                MachineRpcStreamFailure<TTag>
              >;
              let expectedFailureRetries = 0;
              const subscribeToSession = (): Stream.Stream<A, MachineRpcStreamFailure<TTag>> =>
                Stream.suspend(() =>
                  Stream.unwrap(
                    Effect.gen(function* () {
                      const input = yield* makeInput(session);
                      const completeObservation = yield* observer.observe({
                        machineId: supervisor.target.machineId,
                        method: tag,
                        input,
                      });
                      const stream = mapStream(session, method(input)).pipe(
                        Stream.onFirst(() =>
                          Effect.sync(() => {
                            expectedFailureRetries = 0;
                          }),
                        ),
                      );
                      return stream.pipe(Stream.ensuring(completeObservation));
                    }),
                  ).pipe(
                    Stream.tapCause((cause) =>
                      options?.onDefect !== undefined &&
                      cause.reasons.some(
                        (reason) =>
                          reason._tag === "Die" ||
                          (reason._tag === "Fail" &&
                            isRpcClientError(reason.error) &&
                            reason.error.reason._tag === "RpcClientDefect"),
                      )
                        ? options.onDefect(cause)
                        : Effect.void,
                    ),
                    Stream.catchCause((cause) => {
                      const hasOnlyExpectedFailures =
                        cause.reasons.length > 0 &&
                        cause.reasons.every((reason) => reason._tag === "Fail");
                      const isTransportFailure =
                        hasOnlyExpectedFailures &&
                        cause.reasons.every(
                          (reason) => reason._tag === "Fail" && isRpcClientError(reason.error),
                        );
                      if (isTransportFailure) {
                        return Stream.fromEffect(
                          Effect.logWarning(
                            "Durable RPC subscription lost its transport; waiting for the next session.",
                            {
                              cause: Cause.pretty(cause),
                              method: tag,
                              machineId: supervisor.target.machineId,
                            },
                          ),
                        ).pipe(Stream.drain);
                      }
                      if (hasOnlyExpectedFailures && options?.onExpectedFailure !== undefined) {
                        const handled = Stream.fromEffect(options.onExpectedFailure(cause)).pipe(
                          Stream.drain,
                        );
                        const isAuthorizationFailure = cause.reasons.some(
                          (reason) =>
                            reason._tag === "Fail" && isMachineAuthorizationError(reason.error),
                        );
                        if (
                          options.retryExpectedFailureAfter === undefined ||
                          isAuthorizationFailure
                        ) {
                          return handled;
                        }
                        const retryDelay = Duration.millis(
                          Math.min(
                            Duration.toMillis(options.retryExpectedFailureAfter) *
                              2 ** expectedFailureRetries,
                            MAX_EXPECTED_FAILURE_RETRY_DELAY_MS,
                          ),
                        );
                        expectedFailureRetries += 1;
                        return handled.pipe(
                          Stream.concat(
                            Stream.fromEffect(Effect.sleep(retryDelay)).pipe(Stream.drain),
                          ),
                          Stream.concat(subscribeToSession()),
                        );
                      }
                      return Stream.failCause(cause);
                    }),
                  ),
                );
              return subscribeToSession();
            },
          }),
        ),
      );
    }),
  ).pipe(
    Stream.withSpan("MachineRpc.subscribe", {
      attributes: { "rpc.method": tag },
    }),
  );
}

export function subscribeDynamic<TTag extends MachineSubscriptionRpcTag>(
  tag: TTag,
  makeInput: (session: RpcSession) => Effect.Effect<MachineRpcInput<TTag>>,
  options?: SubscriptionOptions<TTag>,
): Stream.Stream<
  MachineRpcStreamValue<TTag>,
  MachineRpcStreamFailure<TTag>,
  MachineSupervisor.MachineSupervisor
> {
  return subscribeDynamicMapped(tag, makeInput, (_session, stream) => stream, options);
}

/** Tags each value before `switchMap` can buffer it across a session change. */
export function subscribeDynamicWithSession<TTag extends MachineSubscriptionRpcTag>(
  tag: TTag,
  makeInput: (session: RpcSession) => Effect.Effect<MachineRpcInput<TTag>>,
  options?: SubscriptionOptions<TTag>,
): Stream.Stream<
  readonly [session: RpcSession, value: MachineRpcStreamValue<TTag>],
  MachineRpcStreamFailure<TTag>,
  MachineSupervisor.MachineSupervisor
> {
  return subscribeDynamicMapped(
    tag,
    makeInput,
    (session, stream) => stream.pipe(Stream.map((value) => [session, value] as const)),
    options,
  );
}

export function subscribe<TTag extends MachineSubscriptionRpcTag>(
  tag: TTag,
  input: MachineRpcInput<TTag>,
  options?: SubscriptionOptions<TTag>,
): Stream.Stream<
  MachineRpcStreamValue<TTag>,
  MachineRpcStreamFailure<TTag>,
  MachineSupervisor.MachineSupervisor
> {
  return subscribeDynamic(tag, () => Effect.succeed(input), options);
}

export const request = <TTag extends Exclude<MachineUnaryRpcTag, ClientGuardedRpcTag>>(
  tag: TTag,
  input: MachineRpcInput<TTag>,
) => requestGuarded(tag, input);

export const runStream = <
  TTag extends Exclude<MachineStreamCommandRpcTag, ClientGuardedRpcTag>,
>(
  tag: TTag,
  input: MachineRpcInput<TTag>,
) => runStreamGuarded(tag, input);
