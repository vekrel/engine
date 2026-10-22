import type { AuthSessionState, MachineId, ServerConfig } from "@vekrel/contracts";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { HttpClient } from "effect/http";
import { AsyncResult, Atom } from "effect/reactivity";

import * as RemoteMachineAuthorization from "../authorization/service.ts";
import * as MachineRegistry from "../connection/registry.ts";
import type { PreparedConnection } from "../connection/model.ts";
import * as MachineSupervisor from "../connection/supervisor.ts";
import * as ManagedRelay from "../relay/managedRelay.ts";
import { safeErrorLogAttributes } from "../errors/safeLog.ts";
import { executeAuthenticatedMachineHttpRequest } from "./machineHttpAuth.ts";
import { followStreamInMachine } from "./machineStreams.ts";

/** @public Required to name the error in consumers' inferred session results. */
export class SessionHttpClientUnavailable extends Data.TaggedError(
  "SessionHttpClientUnavailable",
) {}

function initialConfigOption<E>(
  initialConfig: Effect.Effect<ServerConfig, E>,
): Effect.Effect<Option.Option<ServerConfig>> {
  return initialConfig.pipe(
    Effect.asSome,
    Effect.catch((error) =>
      Effect.logWarning("Could not load the initial machine configuration.").pipe(
        Effect.annotateLogs({ ...safeErrorLogAttributes(error) }),
        Effect.as(Option.none<ServerConfig>()),
      ),
    ),
  );
}

const DEFAULT_SESSION_STATE_TIMEOUT_MS = 6_000;

export const fetchMachineSessionState = Effect.fn(
  "clientRuntime.state.fetchMachineSessionState",
)(function* (input: {
  readonly prepared: PreparedConnection;
  readonly signer: Option.Option<ManagedRelay.ManagedRelayDpopSigner["Service"]>;
  readonly remoteAuthorization?: Option.Option<
    RemoteMachineAuthorization.RemoteMachineAuthorization["Service"]
  >;
  readonly timeoutMs?: number;
}) {
  return yield* executeAuthenticatedMachineHttpRequest({
    ...input,
    group: "auth",
    method: "GET",
    url: (urls) => urls.session(),
    timeoutMs: input.timeoutMs ?? DEFAULT_SESSION_STATE_TIMEOUT_MS,
    request: ({ client, headers }) => client.session({ headers }),
    // This endpoint returns 200 with authenticated:false for expired credentials.
    isUnauthorizedResponse: (response) => !response.authenticated,
  });
});

function makeMachineSessionAtoms<R, E>(
  runtime: Atom.AtomRuntime<MachineRegistry.MachineRegistry | R, E>,
) {
  const initialConfigAtom = Atom.family((machineId: MachineId) =>
    runtime.atom(
      followStreamInMachine(
        machineId,
        Stream.unwrap(
          MachineSupervisor.MachineSupervisor.pipe(
            Effect.map((supervisor) =>
              SubscriptionRef.changes(supervisor.session).pipe(
                Stream.mapEffect(
                  Option.match({
                    onNone: () => Effect.succeed(Option.none<ServerConfig>()),
                    onSome: (session) => initialConfigOption(session.initialConfig),
                  }),
                ),
              ),
            ),
          ),
        ),
      ),
      { initialValue: Option.none() },
    ),
  );

  // This is only the bootstrap config captured when a transport session is
  // established. Consumers that need current provider/settings state must use
  // createServerMachineAtoms(...).configValueAtom instead.
  const initialConfigValueAtom = Atom.family((machineId: MachineId) =>
    Atom.make((get): ServerConfig | null =>
      Option.getOrNull(
        Option.getOrElse(AsyncResult.value(get(initialConfigAtom(machineId))), () =>
          Option.none(),
        ),
      ),
    ).pipe(Atom.withLabel(`machine-config-value:${machineId}`)),
  );

  const preparedConnectionAtom = Atom.family((machineId: MachineId) =>
    runtime.atom(
      followStreamInMachine(
        machineId,
        Stream.unwrap(
          MachineSupervisor.MachineSupervisor.pipe(
            Effect.map((supervisor) => SubscriptionRef.changes(supervisor.prepared)),
          ),
        ),
      ),
      { initialValue: Option.none<PreparedConnection>() },
    ),
  );

  const preparedConnectionValueAtom = Atom.family((machineId: MachineId) =>
    Atom.make((get) =>
      Option.getOrElse(AsyncResult.value(get(preparedConnectionAtom(machineId))), () =>
        Option.none<PreparedConnection>(),
      ),
    ).pipe(Atom.withLabel(`machine-prepared-connection:${machineId}`)),
  );

  const sessionStateAtom = Atom.family((machineId: MachineId) =>
    runtime
      .atom((get) => {
        const prepared = Option.getOrNull(get(preparedConnectionValueAtom(machineId)));
        if (prepared === null) {
          return Effect.never;
        }
        return Effect.gen(function* () {
          const signer = yield* Effect.serviceOption(ManagedRelay.ManagedRelayDpopSigner);
          const remoteAuthorization = yield* Effect.serviceOption(
            RemoteMachineAuthorization.RemoteMachineAuthorization,
          );
          const client = yield* Effect.serviceOption(HttpClient.HttpClient);
          if (Option.isNone(client)) return yield* new SessionHttpClientUnavailable();
          return yield* fetchMachineSessionState({
            prepared,
            signer,
            remoteAuthorization,
          }).pipe(Effect.provideService(HttpClient.HttpClient, client.value));
        });
      })
      .pipe(
        Atom.swr({ staleTime: 30_000, revalidateOnMount: true }),
        Atom.setIdleTTL(5 * 60_000),
        Atom.withLabel(`machine-session-state:${machineId}`),
      ),
  );

  const sessionStateValueAtom = Atom.family((machineId: MachineId) =>
    Atom.make(
      (get): AuthSessionState | null =>
        Option.getOrNull(AsyncResult.value(get(sessionStateAtom(machineId)))) ?? null,
    ).pipe(Atom.withLabel(`machine-session-state-value:${machineId}`)),
  );

  return {
    initialConfigAtom,
    initialConfigValueAtom,
    preparedConnectionAtom,
    preparedConnectionValueAtom,
    sessionStateAtom,
    sessionStateValueAtom,
  };
}

const sessionAtomsByRuntime = new WeakMap<object, ReturnType<typeof makeMachineSessionAtoms>>();

export function createMachineSessionAtoms<R, E>(
  runtime: Atom.AtomRuntime<MachineRegistry.MachineRegistry | R, E>,
): ReturnType<typeof makeMachineSessionAtoms<R, E>> {
  const existing = sessionAtomsByRuntime.get(runtime);
  if (existing) return existing as ReturnType<typeof makeMachineSessionAtoms<R, E>>;
  const atoms = makeMachineSessionAtoms(runtime);
  sessionAtomsByRuntime.set(runtime, atoms);
  return atoms;
}
