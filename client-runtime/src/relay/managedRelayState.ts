import type {
  RelayClientMachineRecord,
  RelayMachineStatusResponse,
} from "@vekrel/contracts/relay";
import type { MachineId } from "@vekrel/contracts";
import {
  RelayMachineConnectScope,
  RelayMachineStatusScope,
} from "@vekrel/contracts/relay";
import { decodeRelayJwt } from "@vekrel/shared/relayJwt";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity";

import { findErrorTraceId } from "../errors/errorTrace.ts";
import * as ManagedRelay from "./managedRelay.ts";
import { relayProtectedErrorMessage } from "./errorPresentation.ts";

const DEFAULT_STALE_TIME_MS = 15_000;
const DEFAULT_IDLE_TTL_MS = 5 * 60_000;
const ACCOUNT_TOKEN_EXPIRY_SKEW_MS = 5_000;
const isManagedRelayRequestFailedError = Schema.is(ManagedRelay.ManagedRelayRequestFailedError);

export interface ManagedRelaySession {
  readonly accountId: string;
  readonly readToken: () => Effect.Effect<string | null, ManagedRelaySessionError>;
}

export interface ManagedRelaySessionInput {
  readonly accountId: string;
  readonly readToken: () => Promise<string | null>;
}

interface ManagedRelaySessionControl {
  readonly updateReadToken: (readToken: ManagedRelaySessionInput["readToken"]) => void;
}

export interface ManagedRelaySnapshotState<A> {
  readonly data: A | null;
  readonly error: string | null;
  readonly errorTraceId: string | null;
  readonly isPending: boolean;
}

export interface ManagedRelayQueryEvent {
  readonly operation: "machines" | "devices" | "machine-status";
  readonly stage: "account-token" | "relay-request" | "validation";
  readonly phase: "start" | "success" | "failure";
  readonly accountId: string;
  readonly machineId?: string;
  readonly message?: string;
  readonly traceId?: string | null;
}

export class ManagedRelaySessionError extends Data.TaggedError("ManagedRelaySessionError")<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

export class ManagedRelaySnapshotError extends Data.TaggedError("ManagedRelaySnapshotError")<{
  readonly message: string;
}> {}

export const managedRelaySessionAtom = Atom.make<ManagedRelaySession | null>(null).pipe(
  Atom.keepAlive,
  Atom.withLabel("managed-relay:session"),
);

const managedRelaySessionControls = new WeakMap<ManagedRelaySession, ManagedRelaySessionControl>();

export function createManagedRelaySession(input: ManagedRelaySessionInput): ManagedRelaySession {
  let cachedToken: { readonly token: string; readonly expiresAtMillis: number } | null = null;
  let pendingToken: Promise<string | null> | null = null;
  let readToken = input.readToken;
  let tokenProviderGeneration = 0;

  const readCachedToken = async (nowMillis: number): Promise<string | null> => {
    if (cachedToken && cachedToken.expiresAtMillis > nowMillis + ACCOUNT_TOKEN_EXPIRY_SKEW_MS) {
      return cachedToken.token;
    }
    if (pendingToken) {
      return await pendingToken;
    }

    const operationGeneration = tokenProviderGeneration;
    const operation = readToken().then((token) => {
      if (operationGeneration !== tokenProviderGeneration) {
        return token;
      }
      if (!token) {
        cachedToken = null;
        return null;
      }
      try {
        const expiresAtSeconds = decodeRelayJwt(token).exp;
        cachedToken =
          typeof expiresAtSeconds === "number"
            ? { token, expiresAtMillis: expiresAtSeconds * 1_000 }
            : null;
      } catch {
        cachedToken = null;
      }
      return token;
    });
    pendingToken = operation;
    try {
      return await operation;
    } finally {
      if (pendingToken === operation) {
        pendingToken = null;
      }
    }
  };

  const session: ManagedRelaySession = {
    accountId: input.accountId,
    readToken: Effect.fn("clientRuntime.managedRelaySession.readToken")(function* () {
      const nowMillis = yield* Clock.currentTimeMillis;
      return yield* Effect.tryPromise({
        try: () => readCachedToken(nowMillis),
        catch: (cause) =>
          new ManagedRelaySessionError({
            message: "Could not obtain the Vekrel Connect session token.",
            cause,
          }),
      });
    }),
  };
  managedRelaySessionControls.set(session, {
    updateReadToken: (nextReadToken) => {
      readToken = nextReadToken;
      tokenProviderGeneration += 1;
      pendingToken = null;
    },
  });
  return session;
}

export function setManagedRelaySession(
  registry: AtomRegistry.AtomRegistry,
  input: ManagedRelaySessionInput | null,
): void {
  const current = registry.get(managedRelaySessionAtom);
  if (input === null) {
    if (current !== null) {
      registry.set(managedRelaySessionAtom, null);
    }
    return;
  }
  if (current?.accountId === input.accountId) {
    const control = managedRelaySessionControls.get(current);
    if (control) {
      // The auth provider can replace its token reader during routine same-account refreshes.
      // Keep the session stable so those refreshes do not invalidate queries or reconnect leases.
      control.updateReadToken(input.readToken);
      return;
    }
  }
  registry.set(managedRelaySessionAtom, createManagedRelaySession(input));
}

export function managedRelayAccountChanges(
  registry: AtomRegistry.AtomRegistry,
): Stream.Stream<string | null> {
  return AtomRegistry.toStream(registry, managedRelaySessionAtom).pipe(
    Stream.map((session) => session?.accountId ?? null),
    Stream.changes,
    Stream.drop(1),
  );
}

function readSessionToken(
  session: ManagedRelaySession,
): Effect.Effect<string, ManagedRelaySessionError> {
  return session.readToken().pipe(
    Effect.filterOrFail(
      (token): token is string => Boolean(token),
      () =>
        new ManagedRelaySessionError({
          message: "The Vekrel Connect session token is unavailable.",
        }),
    ),
  );
}

export const deregisterManagedRelayMachine = Effect.fn(
  "clientRuntime.managedRelaySession.deregisterMachine",
)(function* (
  registry: AtomRegistry.AtomRegistry,
  input: { readonly accountId: string; readonly machineId: MachineId },
) {
  const session = registry.get(managedRelaySessionAtom);
  if (!session || session.accountId !== input.accountId) {
    return yield* new ManagedRelaySessionError({
      message: "Sign in to Vekrel Connect before deregistering a machine.",
    });
  }
  const accountToken = yield* readSessionToken(session);
  const relay = yield* ManagedRelay.ManagedRelayClient;
  yield* relay.unlinkMachine({ accountToken, machineId: input.machineId });
});

function requireToken(
  get: Atom.AtomContext,
  accountId: string,
): Effect.Effect<string, ManagedRelaySessionError> {
  const session = get(managedRelaySessionAtom);
  if (!session || session.accountId !== accountId) {
    return Effect.fail(
      new ManagedRelaySessionError({
        message: "Sign in to Vekrel Connect before loading relay data.",
      }),
    );
  }
  return readSessionToken(session);
}

function statusKey(input: {
  readonly accountId: string;
  readonly machine: RelayClientMachineRecord;
}): string {
  return JSON.stringify(input);
}

function parseStatusKey(key: string): {
  readonly accountId: string;
  readonly machine: RelayClientMachineRecord;
} {
  return JSON.parse(key) as {
    readonly accountId: string;
    readonly machine: RelayClientMachineRecord;
  };
}

function endpointMatches(
  left: RelayClientMachineRecord["endpoint"],
  right: RelayClientMachineRecord["endpoint"],
): boolean {
  return (
    left.httpBaseUrl === right.httpBaseUrl &&
    left.wsBaseUrl === right.wsBaseUrl &&
    left.providerKind === right.providerKind
  );
}

function validateMachineStatus(
  machine: RelayClientMachineRecord,
  status: RelayMachineStatusResponse,
): Effect.Effect<RelayMachineStatusResponse, ManagedRelaySnapshotError> {
  if (status.machineId !== machine.machineId) {
    return Effect.fail(
      new ManagedRelaySnapshotError({
        message: "Relay returned status for a different machine.",
      }),
    );
  }
  if (!endpointMatches(status.endpoint, machine.endpoint)) {
    return Effect.fail(
      new ManagedRelaySnapshotError({
        message: "Relay returned status for a different endpoint.",
      }),
    );
  }
  if (status.descriptor && status.descriptor.machineId !== machine.machineId) {
    return Effect.fail(
      new ManagedRelaySnapshotError({
        message: "Relay returned status descriptor for a different machine.",
      }),
    );
  }
  return Effect.succeed(status);
}

export function readManagedRelaySnapshotState<A>(
  result: AsyncResult.AsyncResult<A, unknown>,
): ManagedRelaySnapshotState<A> {
  let error: string | null = null;
  let errorTraceId: string | null = null;
  if (result._tag === "Failure") {
    const cause = Cause.squash(result.cause);
    error =
      isManagedRelayRequestFailedError(cause) && cause.relayError
        ? relayProtectedErrorMessage(cause.relayError)
        : cause instanceof Error
          ? cause.message
          : "Could not load Vekrel Connect data.";
    errorTraceId = findErrorTraceId(cause);
  }
  return {
    data: Option.getOrNull(AsyncResult.value(result)),
    error,
    errorTraceId,
    isPending: result.waiting,
  };
}

export function createManagedRelayQueryManager(
  runtime: Atom.AtomRuntime<ManagedRelay.ManagedRelayClient>,
  options?: {
    readonly staleTimeMs?: number;
    readonly idleTtlMs?: number;
    readonly onQueryEvent?: (event: ManagedRelayQueryEvent) => void;
  },
) {
  const staleTime = options?.staleTimeMs ?? DEFAULT_STALE_TIME_MS;
  const idleTtl = options?.idleTtlMs ?? DEFAULT_IDLE_TTL_MS;
  const observe = Effect.fn("managedRelayState.observe")(function* <A, E, R>(
    input: Omit<ManagedRelayQueryEvent, "phase" | "message" | "traceId">,
    effect: Effect.Effect<A, E, R>,
  ): Effect.fn.Return<A, E, R> {
    options?.onQueryEvent?.({ ...input, phase: "start" });
    return yield* effect.pipe(
      Effect.onExit((exit) =>
        Effect.sync(() => {
          if (exit._tag === "Success") {
            options?.onQueryEvent?.({ ...input, phase: "success" });
            return;
          }
          const error = Cause.squash(exit.cause);
          options?.onQueryEvent?.({
            ...input,
            phase: "failure",
            message: error instanceof Error ? error.message : String(error),
            traceId: findErrorTraceId(error),
          });
        }),
      ),
    );
  });

  const machinesAtom = Atom.family((accountId: string) =>
    runtime
      .atom((get) =>
        Effect.gen(function* () {
          const base = { operation: "machines" as const, accountId };
          const accountToken = yield* observe(
            { ...base, stage: "account-token" },
            requireToken(get, accountId),
          );
          const relay = yield* ManagedRelay.ManagedRelayClient;
          return yield* observe(
            { ...base, stage: "relay-request" },
            relay.listMachines({ accountToken }),
          );
        }),
      )
      .pipe(
        Atom.swr({ staleTime, revalidateOnMount: true }),
        Atom.setIdleTTL(idleTtl),
        Atom.withLabel(`managed-relay:machines:${accountId}`),
      ),
  );

  const devicesAtom = Atom.family((accountId: string) =>
    runtime
      .atom((get) =>
        Effect.gen(function* () {
          const base = { operation: "devices" as const, accountId };
          const accountToken = yield* observe(
            { ...base, stage: "account-token" },
            requireToken(get, accountId),
          );
          const relay = yield* ManagedRelay.ManagedRelayClient;
          return yield* observe(
            { ...base, stage: "relay-request" },
            relay.listDevices({ accountToken }),
          );
        }),
      )
      .pipe(
        Atom.swr({ staleTime, revalidateOnMount: true }),
        Atom.setIdleTTL(idleTtl),
        Atom.withLabel(`managed-relay:devices:${accountId}`),
      ),
  );

  const machineStatusAtom = Atom.family((key: string) => {
    const { accountId, machine } = parseStatusKey(key);
    return runtime
      .atom((get) =>
        Effect.gen(function* () {
          const base = {
            operation: "machine-status" as const,
            accountId,
            machineId: machine.machineId,
          };
          const accountToken = yield* observe(
            { ...base, stage: "account-token" },
            requireToken(get, accountId),
          );
          const relay = yield* ManagedRelay.ManagedRelayClient;
          const status = yield* observe(
            { ...base, stage: "relay-request" },
            relay.getMachineStatus({
              accountToken,
              scopes: [RelayMachineStatusScope, RelayMachineConnectScope],
              machineId: machine.machineId,
            }),
          );
          return yield* observe(
            { ...base, stage: "validation" },
            validateMachineStatus(machine, status),
          );
        }),
      )
      .pipe(
        Atom.swr({ staleTime, revalidateOnMount: true }),
        Atom.setIdleTTL(idleTtl),
        Atom.withLabel(`managed-relay:machine-status:${key}`),
      );
  });

  return {
    machinesAtom,
    devicesAtom,
    machineStatusAtom: (input: {
      readonly accountId: string;
      readonly machine: RelayClientMachineRecord;
    }) => machineStatusAtom(statusKey(input)),
    refreshMachines(registry: AtomRegistry.AtomRegistry, accountId: string): void {
      registry.refresh(machinesAtom(accountId));
    },
    refreshDevices(registry: AtomRegistry.AtomRegistry, accountId: string): void {
      registry.refresh(devicesAtom(accountId));
    },
    refreshMachineStatus(
      registry: AtomRegistry.AtomRegistry,
      input: {
        readonly accountId: string;
        readonly machine: RelayClientMachineRecord;
      },
    ): void {
      registry.refresh(machineStatusAtom(statusKey(input)));
    },
  };
}
