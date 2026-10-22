import type {
  RelayClientMachineRecord,
  RelayMachineStatusResponse,
} from "@vekrel/contracts/relay";
import { decodeRelayJwt } from "@vekrel/shared/relayJwt";
import {
  RelayMachineConnectScope,
  RelayMachineStatusScope,
} from "@vekrel/contracts/relay";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";

import * as ManagedRelay from "./managedRelay.ts";
import * as ClientCapabilities from "../platform/capabilities.ts";
import * as Connectivity from "../connection/connectivity.ts";
import { mapManagedRelayError } from "../connection/errors.ts";
import { ConnectionBlockedError, type ConnectionAttemptError } from "../connection/model.ts";
import * as ConnectionWakeups from "../connection/wakeups.ts";

export type RelayMachineAvailability = "checking" | "online" | "offline" | "error";

export interface RelayDiscoveredMachine {
  readonly machine: RelayClientMachineRecord;
  readonly availability: RelayMachineAvailability;
  readonly status: Option.Option<RelayMachineStatusResponse>;
  readonly error: Option.Option<ConnectionAttemptError>;
}

export interface RelayMachineDiscoveryState {
  readonly machines: ReadonlyMap<string, RelayDiscoveredMachine>;
  readonly refreshing: boolean;
  readonly offline: boolean;
  readonly error: Option.Option<ConnectionAttemptError>;
}

export class RelayMachineDiscovery extends Context.Service<
  RelayMachineDiscovery,
  {
    readonly state: SubscriptionRef.SubscriptionRef<RelayMachineDiscoveryState>;
    readonly refresh: Effect.Effect<void>;
  }
>()("@vekrel/client-runtime/relay/discovery/RelayMachineDiscovery") {}

export const EMPTY_RELAY_MACHINE_DISCOVERY_STATE: RelayMachineDiscoveryState = {
  machines: new Map(),
  refreshing: false,
  offline: false,
  error: Option.none(),
};

function validateStatus(
  machine: RelayClientMachineRecord,
  status: RelayMachineStatusResponse,
): Effect.Effect<RelayMachineStatusResponse, ConnectionAttemptError> {
  if (status.machineId !== machine.machineId) {
    return Effect.fail(
      new ConnectionBlockedError({
        reason: "configuration",
        detail: "Relay returned status for a different machine.",
      }),
    );
  }
  if (
    status.endpoint.httpBaseUrl !== machine.endpoint.httpBaseUrl ||
    status.endpoint.wsBaseUrl !== machine.endpoint.wsBaseUrl ||
    status.endpoint.providerKind !== machine.endpoint.providerKind
  ) {
    return Effect.fail(
      new ConnectionBlockedError({
        reason: "configuration",
        detail: "Relay returned status for a different machine endpoint.",
      }),
    );
  }
  if (
    status.descriptor !== undefined &&
    status.descriptor.machineId !== machine.machineId
  ) {
    return Effect.fail(
      new ConnectionBlockedError({
        reason: "configuration",
        detail: "Relay returned a descriptor for a different machine.",
      }),
    );
  }
  return Effect.succeed(status);
}

function relayAccountId(clerkToken: string): Option.Option<string> {
  try {
    return Option.fromNullishOr(decodeRelayJwt(clerkToken).sub).pipe(
      Option.filter((subject) => subject.length > 0),
    );
  } catch {
    return Option.none();
  }
}

/** @public Service construction is part of the canonical Effect module API. */
export const make = Effect.fn("RelayMachineDiscovery.make")(function* () {
  const relay = yield* ManagedRelay.ManagedRelayClient;
  const session = yield* ClientCapabilities.CloudSession;
  const connectivity = yield* Connectivity.Connectivity;
  const wakeups = yield* ConnectionWakeups.ConnectionWakeups;
  const state = yield* SubscriptionRef.make(EMPTY_RELAY_MACHINE_DISCOVERY_STATE);
  const refreshLock = yield* Semaphore.make(1);
  const hasRefreshed = yield* Ref.make(false);
  const accountGeneration = yield* Ref.make(0);
  const activeAccountId = yield* Ref.make<Option.Option<string>>(Option.none());
  const refreshGeneration = yield* Ref.make(0);
  const offlineReportFingerprints = yield* Ref.make<ReadonlyMap<string, string>>(new Map());

  const clearOfflineReport = Effect.fn("RelayMachineDiscovery.clearOfflineReport")(function* (
    machineId: string,
  ) {
    yield* Ref.update(offlineReportFingerprints, (current) => {
      if (!current.has(machineId)) {
        return current;
      }
      const next = new Map(current);
      next.delete(machineId);
      return next;
    });
  });

  const updateMachine = Effect.fn("RelayMachineDiscovery.updateMachine")(function* (
    generation: number,
    machineId: string,
    update: (current: RelayDiscoveredMachine) => RelayDiscoveredMachine,
  ) {
    if ((yield* Ref.get(accountGeneration)) !== generation) {
      return;
    }
    yield* SubscriptionRef.update(state, (current) => {
      const entry = current.machines.get(machineId);
      if (entry === undefined) {
        return current;
      }
      const machines = new Map(current.machines);
      machines.set(machineId, update(entry));
      return { ...current, machines };
    });
  });

  const refreshStatus = Effect.fn("RelayMachineDiscovery.refreshStatus")(function* (
    generation: number,
    clerkToken: string,
    machine: RelayClientMachineRecord,
  ) {
    const result = yield* relay
      .getMachineStatus({
        clerkToken,
        scopes: [RelayMachineStatusScope, RelayMachineConnectScope],
        machineId: machine.machineId,
      })
      .pipe(
        Effect.mapError(mapManagedRelayError),
        Effect.flatMap((status) => validateStatus(machine, status)),
        Effect.result,
      );

    if (result._tag === "Success") {
      if (result.success.status === "offline") {
        const fingerprint = `${result.success.endpoint.httpBaseUrl}\n${result.success.error ?? ""}`;
        const shouldReport = yield* Ref.modify(offlineReportFingerprints, (current) => {
          if (current.get(machine.machineId) === fingerprint) {
            return [false, current];
          }
          return [true, new Map(current).set(machine.machineId, fingerprint)];
        });
        if (shouldReport) {
          yield* Effect.logWarning("Relay machine health check reported offline", {
            machineId: result.success.machineId,
            endpoint: result.success.endpoint.httpBaseUrl,
            message: result.success.error,
            traceId: result.success.traceId,
          });
        }
      } else {
        yield* clearOfflineReport(machine.machineId);
      }
      yield* updateMachine(generation, machine.machineId, (current) => ({
        ...current,
        availability: result.success.status,
        status: Option.some(result.success),
        error: Option.none(),
      }));
      return;
    }

    yield* clearOfflineReport(machine.machineId);
    yield* updateMachine(generation, machine.machineId, (current) => ({
      ...current,
      availability: "error",
      error: Option.some(result.failure),
    }));
  });

  const refresh = refreshLock.withPermits(1)(
    Effect.gen(function* () {
      yield* Ref.set(hasRefreshed, true);
      if ((yield* connectivity.status) === "offline") {
        yield* SubscriptionRef.update(state, (current) => ({
          ...current,
          refreshing: false,
          offline: true,
        }));
        return;
      }

      let generation = yield* Ref.get(accountGeneration);
      yield* Ref.set(refreshGeneration, generation);
      yield* SubscriptionRef.set(state, {
        machines: new Map(),
        refreshing: true,
        offline: false,
        error: Option.none(),
      });

      // Signed out is the idle state, not a failure: the proactive refresh on
      // credentials-changed also runs on sign-out and must settle back to a
      // clean empty list. Only the session-level "no credentials" error is
      // benign — relay-side auth failures (expired/invalid tokens) happen
      // after this point and must surface as errors.
      const tokenResult = yield* Effect.result(session.clerkToken);
      if (tokenResult._tag === "Failure") {
        const failure = tokenResult.failure;
        if (failure._tag === "ConnectionBlockedError" && failure.reason === "authentication") {
          if ((yield* Ref.get(accountGeneration)) !== generation) {
            return;
          }
          yield* SubscriptionRef.update(state, (current) => ({
            ...current,
            refreshing: false,
          }));
          return;
        }
        return yield* failure;
      }
      const clerkToken = tokenResult.success;
      if ((yield* Ref.get(accountGeneration)) !== generation) {
        return;
      }
      const accountId = relayAccountId(clerkToken);
      const previousAccountId = yield* Ref.get(activeAccountId);
      if (
        Option.isSome(previousAccountId) &&
        (!Option.isSome(accountId) || previousAccountId.value !== accountId.value)
      ) {
        generation = yield* Ref.updateAndGet(accountGeneration, (current) => current + 1);
        yield* Ref.set(refreshGeneration, generation);
      }
      yield* Ref.set(activeAccountId, accountId);

      const machines = yield* relay
        .listMachines({ clerkToken })
        .pipe(Effect.mapError(mapManagedRelayError));
      if ((yield* Ref.get(accountGeneration)) !== generation) {
        return;
      }
      const next = new Map<string, RelayDiscoveredMachine>();
      for (const machine of machines) {
        next.set(machine.machineId, {
          machine,
          availability: "checking",
          status: Option.none(),
          error: Option.none(),
        });
      }
      yield* SubscriptionRef.update(state, (current) => ({
        ...current,
        machines: next,
      }));

      yield* Effect.forEach(
        machines,
        (machine) => refreshStatus(generation, clerkToken, machine),
        {
          concurrency: "unbounded",
          discard: true,
        },
      );
      if ((yield* Ref.get(accountGeneration)) !== generation) {
        return;
      }
      yield* SubscriptionRef.update(state, (current) => ({
        ...current,
        refreshing: false,
      }));
    }).pipe(
      Effect.catch((error) =>
        Effect.gen(function* () {
          const generation = yield* Ref.get(refreshGeneration);
          if ((yield* Ref.get(accountGeneration)) !== generation) {
            return;
          }
          yield* SubscriptionRef.update(state, (current) => ({
            ...current,
            refreshing: false,
            error: Option.some(error),
          }));
        }),
      ),
    ),
  );

  yield* connectivity.changes.pipe(
    Stream.changes,
    Stream.runForEach((networkStatus) =>
      networkStatus === "offline"
        ? SubscriptionRef.update(state, (current) => ({
            ...current,
            refreshing: false,
            offline: true,
          }))
        : Ref.get(hasRefreshed).pipe(
            Effect.flatMap((shouldRefresh) => (shouldRefresh ? refresh : Effect.void)),
          ),
    ),
    Effect.forkScoped,
  );
  yield* wakeups.changes.pipe(
    Stream.runForEach((reason) =>
      reason === "credentials-changed"
        ? Effect.gen(function* () {
            yield* Ref.update(accountGeneration, (current) => current + 1);
            yield* Ref.set(activeAccountId, Option.none());
            yield* Ref.set(offlineReportFingerprints, new Map());
            yield* SubscriptionRef.set(state, EMPTY_RELAY_MACHINE_DISCOVERY_STATE);
            yield* refresh.pipe(Effect.forkScoped);
          })
        : Effect.void,
    ),
    Effect.forkScoped,
  );

  return RelayMachineDiscovery.of({ state, refresh });
});

export const layer = Layer.effect(RelayMachineDiscovery, make());
