import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { AsyncResult, Atom } from "effect/reactivity";

import * as RelayMachineDiscovery from "../relay/discovery.ts";
import { createRuntimeCommand } from "./runtime.ts";

export function createRelayMachineDiscoveryAtoms<R, E>(
  runtime: Atom.AtomRuntime<RelayMachineDiscovery.RelayMachineDiscovery | R, E>,
) {
  const stateAtom = runtime.atom(
    Stream.unwrap(
      RelayMachineDiscovery.RelayMachineDiscovery.pipe(
        Effect.map((discovery) => SubscriptionRef.changes(discovery.state)),
      ),
    ),
    { initialValue: RelayMachineDiscovery.EMPTY_RELAY_MACHINE_DISCOVERY_STATE },
  );
  const stateValueAtom = Atom.make((get) =>
    Option.getOrElse(
      AsyncResult.value(get(stateAtom)),
      () => RelayMachineDiscovery.EMPTY_RELAY_MACHINE_DISCOVERY_STATE,
    ),
  ).pipe(Atom.withLabel("relay-machine-discovery-value"));
  const refresh = createRuntimeCommand(runtime, {
    label: "relay-machine-discovery:refresh",
    concurrency: { mode: "singleFlight", key: () => "refresh" },
    execute: (_input: void) =>
      RelayMachineDiscovery.RelayMachineDiscovery.pipe(
        Effect.flatMap((discovery) => discovery.refresh),
      ),
  });

  return {
    stateAtom,
    stateValueAtom,
    refresh,
  };
}
