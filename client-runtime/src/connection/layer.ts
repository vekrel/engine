import type { RelayMachineStatusResponse } from "@vekrel/contracts/relay";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as Option from "effect/Option";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { orchestrationProtocolCompatibilityError } from "./compatibility.ts";

import * as ConnectionResolver from "./resolver.ts";
import * as ConnectionDriver from "./driver.ts";
import * as MachineRegistry from "./registry.ts";
import * as ConnectionOnboarding from "./onboarding.ts";
import { connectionRoutes, hasRelayRoute } from "./routes.ts";
import * as PlatformConnectionSource from "../platform/source.ts";
import * as RelayMachineDiscovery from "../relay/discovery.ts";
import * as RemoteMachineAuthorization from "../authorization/service.ts";
import * as RpcSession from "../rpc/session.ts";

export const watchDiscoveredCompatibility = Effect.fn("connection.watchDiscoveredCompatibility")(
  function* () {
    const registry = yield* MachineRegistry.MachineRegistry;
    const discovery = yield* RelayMachineDiscovery.RelayMachineDiscovery;
    const seenChecks = new Map<string, RelayMachineStatusResponse>();
    yield* Stream.merge(
      SubscriptionRef.changes(discovery.state),
      SubscriptionRef.changes(registry.entries),
    ).pipe(
      Stream.runForEach(() =>
        Effect.gen(function* () {
          const current = yield* SubscriptionRef.get(discovery.state);
          if (!current.refreshing) {
            for (const machineId of seenChecks.keys()) {
              if (!current.machines.has(machineId)) seenChecks.delete(machineId);
            }
          }
          const registered = yield* SubscriptionRef.get(registry.entries);
          for (const entry of current.machines.values()) {
            const status = Option.getOrNull(entry.status);
            const descriptor = status?.descriptor;
            if (status === null || descriptor === undefined) continue;
            const machineId = entry.machine.machineId;
            // Discovery describes the server behind the relay route. A direct
            // connection (the desktop's own server, a saved URL, SSH) can reach
            // a different server with the same machine id, such as a
            // preview app that shares the home directory.
            const saved = registered.get(machineId);
            if (
              saved === undefined ||
              connectionRoutes(saved).length !== 1 ||
              !hasRelayRoute(saved)
            )
              continue;
            const previous = seenChecks.get(machineId);
            const fresh =
              previous?.checkedAt !== status.checkedAt ||
              (previous.descriptor?.orchestrationProtocolVersion ?? 1) !==
                (descriptor.orchestrationProtocolVersion ?? 1) ||
              previous.descriptor?.serverVersion !== descriptor.serverVersion;
            const error = orchestrationProtocolCompatibilityError(descriptor);
            // A replayed health result must not clear a newer socket rejection.
            if (error !== null || fresh) yield* registry.setCompatibility(machineId, error);
            seenChecks.set(machineId, status);
          }
        }).pipe(
          Effect.catch((error) =>
            Effect.logWarning("Could not apply discovered machine compatibility.", { error }),
          ),
        ),
      ),
    );
  },
);

export function layerWithOptions(options: RpcSession.RpcSessionOptions) {
  const layerDriver = ConnectionDriver.layer.pipe(
    Layer.provide(Layer.mergeAll(ConnectionResolver.layer, RpcSession.layer(options))),
  );
  const layerRegistry = MachineRegistry.layer.pipe(Layer.provide(layerDriver));
  const layerOnboarding = ConnectionOnboarding.layer.pipe(Layer.provide(layerRegistry));
  const layerConnectionServices = Layer.mergeAll(
    layerRegistry,
    RelayMachineDiscovery.layer,
    layerOnboarding,
    ConnectionResolver.layer,
  );
  const layerConnectionStartup = Layer.effectDiscard(
    Effect.gen(function* () {
      const registry = yield* MachineRegistry.MachineRegistry;
      const platformSource = yield* PlatformConnectionSource.PlatformConnectionSource;
      yield* watchDiscoveredCompatibility().pipe(Effect.forkScoped);
      yield* registry.start;
      yield* platformSource.registrations.pipe(
        Stream.runForEach(registry.reconcilePlatform),
        Effect.forkScoped,
      );
    }).pipe(Effect.withSpan("clientRuntime.connection.application.start")),
  );
  return layerConnectionStartup.pipe(
    Layer.provideMerge(layerConnectionServices),
    Layer.provideMerge(RemoteMachineAuthorization.layer),
  );
}

export const layer = layerWithOptions({});
