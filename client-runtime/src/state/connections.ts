import type { MachineId as MachineIdType } from "@vekrel/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { AsyncResult, Atom } from "effect/reactivity";

import * as MachineRegistry from "../connection/registry.ts";
import type { ConnectionCatalogEntry } from "../connection/catalog.ts";
import { AVAILABLE_CONNECTION_STATE } from "../connection/model.ts";
import * as MachineSupervisor from "../connection/supervisor.ts";
import {
  GitHubRoutingPermissions,
  type GitHubRoutingPermission,
  type StoredGitHubRoutingPermission,
} from "../connection/githubRoutingPermissions.ts";
import {
  createAtomCommandScheduler,
  createRuntimeCommand,
  followStreamInMachine,
} from "./runtime.ts";

export interface MachineCatalogState {
  readonly isReady: boolean;
  readonly entries: ReadonlyMap<MachineIdType, ConnectionCatalogEntry>;
}

export function* enabledMachineIds(
  catalog: MachineCatalogState,
): Generator<MachineIdType> {
  for (const [machineId, entry] of catalog.entries) {
    if (entry.enabled && entry.unsupportedReason === undefined) {
      yield machineId;
    }
  }
}

const EMPTY_MACHINE_CATALOG_STATE: MachineCatalogState = Object.freeze({
  isReady: false,
  entries: new Map(),
});

export function createMachineCatalogAtoms<R, E>(
  runtime: Atom.AtomRuntime<MachineRegistry.MachineRegistry | R, E>,
) {
  const commandScheduler = createAtomCommandScheduler();
  const serial = { mode: "serial" as const, key: () => "machine-catalog" };
  const catalogAtom = runtime.atom(
    Stream.unwrap(
      MachineRegistry.MachineRegistry.pipe(
        Effect.map((registry) =>
          SubscriptionRef.changes(registry.entries).pipe(
            Stream.map((entries) => ({
              isReady: true,
              entries,
            })),
          ),
        ),
      ),
    ),
    { initialValue: EMPTY_MACHINE_CATALOG_STATE },
  );

  const catalogValueAtom = Atom.make((get) =>
    Option.getOrElse(AsyncResult.value(get(catalogAtom)), () => EMPTY_MACHINE_CATALOG_STATE),
  ).pipe(Atom.withLabel("machine-catalog-value"));

  const githubRoutingPermissionsAtom = runtime.atom(
    Stream.unwrap(GitHubRoutingPermissions.pipe(Effect.map((permissions) => permissions.changes))),
    { initialValue: [] as ReadonlyArray<StoredGitHubRoutingPermission> },
  );
  const githubRoutingPermissionsValueAtom = Atom.make((get) =>
    Option.getOrElse(AsyncResult.value(get(githubRoutingPermissionsAtom)), () => []),
  ).pipe(Atom.withLabel("machine-github-routing-permissions"));
  const setGitHubRoutingPermission = createRuntimeCommand(runtime, {
    label: "machine-catalog:github-routing-permission",
    scheduler: commandScheduler,
    concurrency: serial,
    execute: Effect.fn(function* (input: {
      readonly machineId: MachineIdType;
      readonly permission: GitHubRoutingPermission;
    }) {
      const registry = yield* MachineRegistry.MachineRegistry;
      const entry = (yield* SubscriptionRef.get(registry.entries)).get(input.machineId);
      if (entry === undefined)
        return yield* new MachineRegistry.MachineNotRegisteredError({
          machineId: input.machineId,
        });
      const permissions = yield* GitHubRoutingPermissions;
      yield* permissions.set(entry, input.permission);
    }),
  });

  const networkStatusAtom = runtime.atom(
    Stream.unwrap(
      MachineRegistry.MachineRegistry.pipe(
        Effect.map((registry) => SubscriptionRef.changes(registry.networkStatus)),
      ),
    ),
    { initialValue: "unknown" as const },
  );

  const networkStatusValueAtom = Atom.make((get) =>
    Option.getOrElse(AsyncResult.value(get(networkStatusAtom)), () => "unknown" as const),
  ).pipe(Atom.withLabel("machine-network-status-value"));

  const stateAtom = Atom.family((machineId: MachineIdType) =>
    runtime.atom(
      followStreamInMachine(
        machineId,
        Stream.unwrap(
          MachineSupervisor.MachineSupervisor.pipe(
            Effect.map((supervisor) => SubscriptionRef.changes(supervisor.state)),
          ),
        ),
      ),
      { initialValue: AVAILABLE_CONNECTION_STATE },
    ),
  );

  const register = createRuntimeCommand(runtime, {
    label: "machine-catalog:register",
    scheduler: commandScheduler,
    concurrency: serial,
    execute: (
      target: Parameters<MachineRegistry.MachineRegistry["Service"]["register"]>[0],
    ) =>
      MachineRegistry.MachineRegistry.pipe(
        Effect.flatMap((registry) => registry.register(target)),
      ),
  });
  const remove = createRuntimeCommand(runtime, {
    label: "machine-catalog:remove",
    scheduler: commandScheduler,
    concurrency: serial,
    execute: (machineId: MachineIdType) =>
      MachineRegistry.MachineRegistry.pipe(
        Effect.flatMap((registry) => registry.remove(machineId)),
      ),
  });
  const removeRelayMachines = createRuntimeCommand(runtime, {
    label: "machine-catalog:remove-relay-machines",
    scheduler: commandScheduler,
    concurrency: serial,
    execute: (_input: void) =>
      MachineRegistry.MachineRegistry.pipe(
        Effect.flatMap((registry) => registry.removeRelayMachines()),
      ),
  });
  const setEnabled = createRuntimeCommand(runtime, {
    label: "machine-catalog:set-enabled",
    scheduler: commandScheduler,
    concurrency: serial,
    execute: (input: { readonly machineId: MachineIdType; readonly enabled: boolean }) =>
      MachineRegistry.MachineRegistry.pipe(
        Effect.flatMap((registry) => registry.setEnabled(input.machineId, input.enabled)),
      ),
  });
  const removeRoute = createRuntimeCommand(runtime, {
    label: "machine-catalog:remove-route",
    scheduler: commandScheduler,
    concurrency: serial,
    execute: (input: { readonly machineId: MachineIdType; readonly routeId: string }) =>
      MachineRegistry.MachineRegistry.pipe(
        Effect.flatMap((registry) => registry.removeRoute(input.machineId, input.routeId)),
      ),
  });
  const reorderRoutes = createRuntimeCommand(runtime, {
    label: "machine-catalog:reorder-routes",
    scheduler: commandScheduler,
    concurrency: serial,
    execute: (input: {
      readonly machineId: MachineIdType;
      readonly routeIds: ReadonlyArray<string>;
    }) =>
      MachineRegistry.MachineRegistry.pipe(
        Effect.flatMap((registry) => registry.reorderRoutes(input.machineId, input.routeIds)),
      ),
  });
  const retryNow = createRuntimeCommand(runtime, {
    label: "machine-catalog:retry-now",
    scheduler: commandScheduler,
    concurrency: serial,
    execute: (machineId: MachineIdType) =>
      MachineRegistry.MachineRegistry.pipe(
        Effect.flatMap((registry) => registry.retryNow(machineId)),
      ),
  });

  return {
    catalogAtom,
    catalogValueAtom,
    githubRoutingPermissionsValueAtom,
    setGitHubRoutingPermission,
    networkStatusAtom,
    networkStatusValueAtom,
    stateAtom,
    register,
    remove,
    removeRoute,
    reorderRoutes,
    removeRelayMachines,
    retryNow,
    setEnabled,
  };
}
