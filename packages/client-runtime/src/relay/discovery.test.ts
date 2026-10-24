import { MachineId } from "@vekrel/contracts";
import type {
  RelayClientMachineRecord,
  RelayMachineStatusResponse,
} from "@vekrel/contracts/relay";
import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";

import * as ManagedRelay from "./managedRelay.ts";
import * as ClientCapabilities from "../platform/capabilities.ts";
import * as Connectivity from "../connection/connectivity.ts";
import { ConnectionBlockedError, type NetworkStatus } from "../connection/model.ts";
import * as ConnectionWakeups from "../connection/wakeups.ts";
import * as RelayMachineDiscovery from "./discovery.ts";
import { NETWORK_BLOCKING_HINT } from "../errors/network.ts";

const machines = [
  {
    machineId: MachineId.make("machine-1"),
    label: "Machine One",
    endpoint: {
      httpBaseUrl: "https://one.example.test",
      wsBaseUrl: "wss://one.example.test",
      providerKind: "cloudflare_tunnel",
    },
    linkedAt: "2026-06-01T00:00:00.000Z",
  },
  {
    machineId: MachineId.make("machine-2"),
    label: "Machine Two",
    endpoint: {
      httpBaseUrl: "https://two.example.test",
      wsBaseUrl: "wss://two.example.test",
      providerKind: "cloudflare_tunnel",
    },
    linkedAt: "2026-06-01T00:00:00.000Z",
  },
] satisfies ReadonlyArray<RelayClientMachineRecord>;

function status(
  machine: RelayClientMachineRecord,
  value: "online" | "offline",
): RelayMachineStatusResponse {
  return {
    machineId: machine.machineId,
    endpoint: machine.endpoint,
    status: value,
    checkedAt: "2026-06-01T00:00:00.000Z",
  };
}

const makeHarness = Effect.fn("RelayDiscoveryTest.makeHarness")(function* () {
  const networkStatus = yield* SubscriptionRef.make<NetworkStatus>("online");
  const listCalls = yield* Ref.make(0);
  const listFailure = yield* Ref.make<ManagedRelay.ManagedRelayClientError | null>(null);
  const secondListCall = yield* Deferred.make<void>();
  const accountToken = yield* Ref.make<string | null>("account-token");
  const sessionIdentity = { accountId: "account-1" };
  const wakeups = yield* SubscriptionRef.make<{
    readonly sequence: number;
    readonly reason: "application-active" | "credentials-changed";
  }>({
    sequence: 0,
    reason: "application-active",
  });
  const statusRequests = yield* Ref.make(
    new Map<
      string,
      Deferred.Deferred<RelayMachineStatusResponse, ManagedRelay.ManagedRelayClientError>
    >(),
  );
  for (const machine of machines) {
    const request = yield* Deferred.make<
      RelayMachineStatusResponse,
      ManagedRelay.ManagedRelayClientError
    >();
    yield* Ref.update(statusRequests, (current) => {
      const next = new Map(current);
      next.set(machine.machineId, request);
      return next;
    });
  }

  const client = ManagedRelay.ManagedRelayClient.of({
    relayUrl: "https://relay.example.test",
    listMachines: () =>
      Effect.gen(function* () {
        const count = yield* Ref.updateAndGet(listCalls, (current) => current + 1);
        if (count >= 2) {
          yield* Deferred.succeed(secondListCall, undefined);
        }
        const failure = yield* Ref.get(listFailure);
        if (failure) {
          return yield* failure;
        }
        return machines;
      }),
    getMachineStatus: ({ machineId }) =>
      Ref.get(statusRequests).pipe(
        Effect.flatMap((requests) => Deferred.await(requests.get(machineId)!)),
      ),
    listDevices: () => Effect.die("unused"),
    createMachineLinkChallenge: () => Effect.die("unused"),
    linkMachine: () => Effect.die("unused"),
    unlinkMachine: () => Effect.die("unused"),
    connectMachine: () => Effect.die("unused"),
    registerDevice: () => Effect.die("unused"),
    unregisterDevice: () => Effect.die("unused"),
    registerLiveActivity: () => Effect.die("unused"),
    getAgentActivitySnapshot: () => Effect.die("unused"),
    resetTokenCache: Effect.void,
  } satisfies ManagedRelay.ManagedRelayClient["Service"]);
  const connectivity = Connectivity.Connectivity.of({
    status: SubscriptionRef.get(networkStatus),
    changes: SubscriptionRef.changes(networkStatus),
  });
  const layer = RelayMachineDiscovery.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.succeed(ManagedRelay.ManagedRelayClient, client),
        Layer.succeed(
          ClientCapabilities.CloudSession,
          ClientCapabilities.CloudSession.of({
            identity: Ref.get(accountToken).pipe(
              Effect.map((token) =>
                token === null ? Option.none() : Option.some(sessionIdentity),
              ),
            ),
            accountToken: Ref.get(accountToken).pipe(
              Effect.filterOrFail(
                (token) => token !== null,
                () =>
                  new ConnectionBlockedError({
                    reason: "authentication",
                    detail: "Signed out.",
                  }),
              ),
            ),
          }),
        ),
        Layer.succeed(Connectivity.Connectivity, connectivity),
        Layer.succeed(
          ConnectionWakeups.ConnectionWakeups,
          ConnectionWakeups.ConnectionWakeups.of({
            changes: SubscriptionRef.changes(wakeups).pipe(
              Stream.drop(1),
              Stream.map((event) => event.reason),
            ),
          }),
        ),
      ),
    ),
  );

  return {
    layer,
    listCalls,
    listFailure,
    accountToken,
    networkStatus,
    secondListCall,
    statusRequests,
    wake: (reason: "application-active" | "credentials-changed") =>
      SubscriptionRef.update(wakeups, (event) => ({
        sequence: event.sequence + 1,
        reason,
      })),
  };
});

describe("RelayMachineDiscovery", () => {
  it.effect("publishes each machine status as soon as that lookup completes", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      yield* Effect.gen(function* () {
        const discovery = yield* RelayMachineDiscovery.RelayMachineDiscovery;
        const refreshFiber = yield* Effect.forkChild(discovery.refresh);

        const checking = yield* SubscriptionRef.changes(discovery.state).pipe(
          Stream.filter((state) => state.machines.size === 2),
          Stream.runHead,
          Effect.map(Option.getOrThrow),
        );
        expect(
          [...checking.machines.values()].every((entry) => entry.availability === "checking"),
        ).toBe(true);

        const requests = yield* Ref.get(harness.statusRequests);
        yield* Deferred.succeed(
          requests.get(machines[1]!.machineId)!,
          status(machines[1]!, "online"),
        );

        const partiallyResolved = yield* SubscriptionRef.changes(discovery.state).pipe(
          Stream.filter(
            (state) =>
              state.machines.get(machines[1]!.machineId)?.availability === "online",
          ),
          Stream.runHead,
          Effect.map(Option.getOrThrow),
        );
        expect(
          partiallyResolved.machines.get(machines[0]!.machineId)?.availability,
        ).toBe("checking");

        yield* Deferred.succeed(
          requests.get(machines[0]!.machineId)!,
          status(machines[0]!, "offline"),
        );
        yield* Fiber.join(refreshFiber);

        const complete = yield* SubscriptionRef.get(discovery.state);
        expect(complete.machines.get(machines[0]!.machineId)?.availability).toBe(
          "offline",
        );
        expect(complete.refreshing).toBe(false);
      }).pipe(Effect.provide(harness.layer));
    }),
  );

  it.effect(
    "preserves discovered rows while offline and refreshes after connectivity returns",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        yield* Effect.gen(function* () {
          const discovery = yield* RelayMachineDiscovery.RelayMachineDiscovery;
          const requests = yield* Ref.get(harness.statusRequests);
          for (const machine of machines) {
            yield* Deferred.succeed(
              requests.get(machine.machineId)!,
              status(machine, "online"),
            );
          }
          yield* discovery.refresh;

          const offlineFiber = yield* SubscriptionRef.changes(discovery.state).pipe(
            Stream.filter((state) => state.offline),
            Stream.runHead,
            Effect.forkChild,
          );
          yield* SubscriptionRef.set(harness.networkStatus, "offline");
          yield* Fiber.join(offlineFiber);
          expect((yield* SubscriptionRef.get(discovery.state)).machines.size).toBe(2);

          yield* SubscriptionRef.set(harness.networkStatus, "online");
          yield* Deferred.await(harness.secondListCall);
          expect(yield* Ref.get(harness.listCalls)).toBe(2);
        }).pipe(Effect.provide(harness.layer));
      }),
  );

  it.effect("publishes listing failures without rejecting the refresh command", () =>
    Effect.gen(function* () {
      const networkStatus = yield* SubscriptionRef.make<NetworkStatus>("online");
      const client = ManagedRelay.ManagedRelayClient.of({
        relayUrl: "https://relay.example.test",
        listMachines: () =>
          Effect.fail(
            new ManagedRelay.ManagedRelayRequestTimeoutError({
              activity: "Relay machine listing",
              timeoutMs: ManagedRelay.MANAGED_RELAY_REQUEST_TIMEOUT_MS,
              traceId: null,
            }),
          ),
        getMachineStatus: () => Effect.die("unused"),
        listDevices: () => Effect.die("unused"),
        createMachineLinkChallenge: () => Effect.die("unused"),
        linkMachine: () => Effect.die("unused"),
        unlinkMachine: () => Effect.die("unused"),
        connectMachine: () => Effect.die("unused"),
        registerDevice: () => Effect.die("unused"),
        unregisterDevice: () => Effect.die("unused"),
        registerLiveActivity: () => Effect.die("unused"),
        getAgentActivitySnapshot: () => Effect.die("unused"),
        resetTokenCache: Effect.void,
      } satisfies ManagedRelay.ManagedRelayClient["Service"]);
      const layer = RelayMachineDiscovery.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.succeed(ManagedRelay.ManagedRelayClient, client),
            Layer.succeed(ClientCapabilities.CloudSession, {
              identity: Effect.succeedSome({ accountId: "account-1" }),
              accountToken: Effect.succeed("account-token"),
            }),
            Layer.succeed(Connectivity.Connectivity, {
              status: SubscriptionRef.get(networkStatus),
              changes: SubscriptionRef.changes(networkStatus),
            }),
            Layer.succeed(
              ConnectionWakeups.ConnectionWakeups,
              ConnectionWakeups.ConnectionWakeups.of({ changes: Stream.never }),
            ),
          ),
        ),
      );

      yield* Effect.gen(function* () {
        const discovery = yield* RelayMachineDiscovery.RelayMachineDiscovery;
        yield* discovery.refresh;

        const state = yield* SubscriptionRef.get(discovery.state);
        expect(state.refreshing).toBe(false);
        expect(Option.getOrThrow(state.error)).toMatchObject({
          _tag: "ConnectionTransientError",
          reason: "timeout",
          message: `Relay machine listing timed out. ${NETWORK_BLOCKING_HINT}`,
        });
      }).pipe(Effect.provide(layer));
    }),
  );

  it.effect("clears previously discovered rows when a refresh fails", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      yield* Effect.gen(function* () {
        const discovery = yield* RelayMachineDiscovery.RelayMachineDiscovery;
        const requests = yield* Ref.get(harness.statusRequests);
        for (const machine of machines) {
          yield* Deferred.succeed(
            requests.get(machine.machineId)!,
            status(machine, "online"),
          );
        }
        yield* discovery.refresh;
        expect((yield* SubscriptionRef.get(discovery.state)).machines.size).toBe(2);

        yield* Ref.set(
          harness.listFailure,
          new ManagedRelay.ManagedRelayRequestFailedError({
            action: "list relay-managed machines",
            cause: new Error("Relay request failed."),
          }),
        );
        yield* discovery.refresh;

        const failed = yield* SubscriptionRef.get(discovery.state);
        expect(failed.machines.size).toBe(0);
        expect(Option.isSome(failed.error)).toBe(true);
      }).pipe(Effect.provide(harness.layer));
    }),
  );

  it.effect("refreshes proactively when credentials change before any manual refresh", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      yield* Effect.gen(function* () {
        const discovery = yield* RelayMachineDiscovery.RelayMachineDiscovery;
        const requests = yield* Ref.get(harness.statusRequests);
        for (const machine of machines) {
          yield* Deferred.succeed(
            requests.get(machine.machineId)!,
            status(machine, "online"),
          );
        }

        // Let the scoped wakeup subscription start before emitting, mirroring
        // a real sign-in which always happens long after service start.
        yield* Effect.yieldNow;

        yield* harness.wake("credentials-changed");
        const populated = yield* SubscriptionRef.changes(discovery.state).pipe(
          Stream.filter(
            (state) => state.machines.size === machines.length && !state.refreshing,
          ),
          Stream.runHead,
          Effect.map(Option.getOrThrow),
        );
        expect(Option.isNone(populated.error)).toBe(true);
        expect(yield* Ref.get(harness.listCalls)).toBe(1);
      }).pipe(Effect.provide(harness.layer), Effect.scoped);
    }),
  );

  it.effect("settles to a clean empty state when refreshed while signed out", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      yield* Effect.gen(function* () {
        const discovery = yield* RelayMachineDiscovery.RelayMachineDiscovery;
        yield* Ref.set(harness.accountToken, null);
        yield* discovery.refresh;

        const state = yield* SubscriptionRef.get(discovery.state);
        expect(state.machines.size).toBe(0);
        expect(state.refreshing).toBe(false);
        expect(Option.isNone(state.error)).toBe(true);
      }).pipe(Effect.provide(harness.layer));
    }),
  );

  it.effect("does not republish stale rows after sign-out invalidates an in-flight refresh", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      yield* Effect.gen(function* () {
        const discovery = yield* RelayMachineDiscovery.RelayMachineDiscovery;
        const refreshFiber = yield* Effect.forkChild(discovery.refresh);
        yield* SubscriptionRef.changes(discovery.state).pipe(
          Stream.filter((state) => state.machines.size === machines.length),
          Stream.runHead,
        );

        yield* Ref.set(harness.accountToken, null);
        yield* harness.wake("credentials-changed");
        yield* SubscriptionRef.changes(discovery.state).pipe(
          Stream.filter((state) => state.machines.size === 0),
          Stream.runHead,
        );

        const requests = yield* Ref.get(harness.statusRequests);
        for (const machine of machines) {
          yield* Deferred.succeed(
            requests.get(machine.machineId)!,
            status(machine, "online"),
          );
        }
        yield* Fiber.join(refreshFiber);
        yield* Effect.yieldNow;

        expect((yield* SubscriptionRef.get(discovery.state)).machines.size).toBe(0);
      }).pipe(Effect.provide(harness.layer), Effect.scoped);
    }),
  );
});
