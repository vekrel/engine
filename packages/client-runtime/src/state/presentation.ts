import {
  resolveMachineKind,
  type MachineKind,
  type MachineId,
  type ServerConfig,
  type ServerProvider,
} from "@vekrel/contracts";
import * as Option from "effect/Option";
import { AsyncResult, Atom } from "effect/reactivity";

import { AVAILABLE_CONNECTION_STATE, type SupervisorConnectionState } from "../connection/model.ts";
import {
  connectionCatalogDisplayUrl,
  presentMachineConnection,
  type MachineConnectionPhase,
  type MachineConnectionPresentation,
  type MachinePresentation,
  type MachineServerConfig,
} from "../connection/presentation.ts";
import { type MachineCatalogState, enabledMachineIds } from "./connections.ts";
import { hasRelayRoute } from "../connection/routes.ts";

function mapsEqual<K, V>(left: ReadonlyMap<K, V>, right: ReadonlyMap<K, V>): boolean {
  if (left.size !== right.size) {
    return false;
  }
  for (const [key, value] of left) {
    if (right.get(key) !== value) {
      return false;
    }
  }
  return true;
}

function sameConnection(
  left: MachineConnectionPresentation,
  right: MachineConnectionPresentation,
): boolean {
  return left.phase === right.phase && left.error === right.error && left.traceId === right.traceId;
}

function withoutProviders(
  config: ServerConfig | null,
  previous: MachineServerConfig | null,
): MachineServerConfig | null {
  if (config === null) {
    return null;
  }
  const { providers: _providers, ...next } = config;
  if (previous === null) {
    return next;
  }
  const keys = Object.keys(next) as Array<keyof MachineServerConfig>;
  return keys.length === Object.keys(previous).length &&
    keys.every((key) => key in previous && previous[key] === next[key])
    ? previous
    : next;
}

export interface MachinePresentationWithProviders extends Omit<
  MachinePresentation,
  "serverConfig"
> {
  readonly serverConfig: ServerConfig | null;
}

export function createMachinePresentationAtoms<E>(input: {
  readonly catalogValueAtom: Atom.Atom<MachineCatalogState>;
  readonly stateAtom: (
    machineId: MachineId,
  ) => Atom.Atom<AsyncResult.AsyncResult<SupervisorConnectionState, E>>;
  readonly serverConfigValueAtom: (machineId: MachineId) => Atom.Atom<ServerConfig | null>;
}) {
  const presentationAtom = Atom.family((machineId: MachineId) =>
    Atom.make((get) => {
      const entry = get(input.catalogValueAtom).entries.get(machineId);
      if (entry === undefined) {
        return null;
      }
      const state = Option.getOrElse(
        AsyncResult.value(get(input.stateAtom(machineId))),
        () => AVAILABLE_CONNECTION_STATE,
      );
      const previous = Option.getOrNull(get.self<MachinePresentation | null>());
      const connection: MachineConnectionPresentation =
        entry.unsupportedReason === undefined
          ? presentMachineConnection(state)
          : { phase: "unsupported", error: entry.unsupportedReason, traceId: null };
      const next = {
        entry,
        connection:
          previous !== null && sameConnection(previous.connection, connection)
            ? previous.connection
            : connection,
        serverConfig: withoutProviders(
          get(input.serverConfigValueAtom(machineId)),
          previous?.serverConfig ?? null,
        ),
      } satisfies MachinePresentation;
      return previous !== null &&
        previous.entry === next.entry &&
        previous.connection === next.connection &&
        previous.serverConfig === next.serverConfig
        ? previous
        : next;
    }).pipe(Atom.withLabel(`machine-presentation:${machineId}`)),
  );

  let previous: ReadonlyMap<MachineId, MachinePresentation> = new Map();
  const presentationsAtom = Atom.make((get) => {
    const next = new Map<MachineId, MachinePresentation>();
    for (const machineId of get(input.catalogValueAtom).entries.keys()) {
      const presentation = get(presentationAtom(machineId));
      if (presentation !== null) {
        next.set(machineId, presentation);
      }
    }
    if (mapsEqual(previous, next)) {
      return previous;
    }
    previous = next;
    return previous;
  }).pipe(Atom.withLabel("machine-presentations"));

  // Enabled machines only, like the full server-configs map, but without providers, so
  // capability checks on every sidebar row and markdown link skip provider status pushes.
  const serverConfigsAtom = Atom.make((get) => {
    const next = new Map<MachineId, MachineServerConfig>();
    for (const machineId of enabledMachineIds(get(input.catalogValueAtom))) {
      const serverConfig = get(presentationAtom(machineId))?.serverConfig;
      if (serverConfig != null) {
        next.set(machineId, serverConfig);
      }
    }
    const previous = Option.getOrNull(
      get.self<ReadonlyMap<MachineId, MachineServerConfig>>(),
    );
    return previous !== null && mapsEqual(previous, next) ? previous : next;
  }).pipe(Atom.withLabel("machine-server-configs-without-providers"));

  const providersByMachineAtom = Atom.make((get) => {
    const next = new Map<MachineId, ReadonlyArray<ServerProvider>>();
    for (const machineId of get(input.catalogValueAtom).entries.keys()) {
      const providers = get(input.serverConfigValueAtom(machineId))?.providers;
      if (providers !== undefined) {
        next.set(machineId, providers);
      }
    }
    const previous = Option.getOrNull(
      get.self<ReadonlyMap<MachineId, ReadonlyArray<ServerProvider>>>(),
    );
    return previous !== null && mapsEqual(previous, next) ? previous : next;
  }).pipe(Atom.withLabel("machine-providers"));

  // Enabled machines only, like serverConfigsAtom: a disabled connection keeps its cached
  // config, and its accounts would otherwise count in usage limits. Usage limits ride on provider
  // statuses, so these surfaces rebuild on every push by design.
  const presentationsWithProvidersAtom = Atom.make((get) => {
    const previous = Option.getOrNull(
      get.self<ReadonlyMap<MachineId, MachinePresentationWithProviders>>(),
    );
    const next = new Map<MachineId, MachinePresentationWithProviders>();
    for (const machineId of enabledMachineIds(get(input.catalogValueAtom))) {
      const presentation = get(presentationAtom(machineId));
      if (presentation === null) continue;
      const serverConfig = get(input.serverConfigValueAtom(machineId));
      const prior = previous?.get(machineId);
      next.set(
        machineId,
        prior !== undefined &&
          prior.entry === presentation.entry &&
          prior.connection === presentation.connection &&
          prior.serverConfig === serverConfig
          ? prior
          : { ...presentation, serverConfig },
      );
    }
    return previous !== null && mapsEqual(previous, next) ? previous : next;
  }).pipe(Atom.withLabel("machine-presentations-with-providers"));

  return {
    presentationAtom,
    presentationsAtom,
    serverConfigsAtom,
    providersByMachineAtom,
    presentationsWithProvidersAtom,
  };
}

export interface MachineConnectionSummary {
  readonly machineId: MachineId;
  readonly machineLabel: string;
  readonly displayUrl: string;
  readonly isRelayManaged: boolean;
  readonly isEnabled: boolean;
  readonly connectionState: MachineConnectionPhase;
  readonly connectionError: string | null;
  readonly connectionErrorTraceId: string | null;
}

export function projectMachineConnectionSummary(
  machineId: MachineId,
  machine: MachinePresentation,
): MachineConnectionSummary {
  return {
    machineId,
    machineLabel: machine.entry.target.label,
    displayUrl: connectionCatalogDisplayUrl(machine.entry) ?? "",
    isRelayManaged: hasRelayRoute(machine.entry),
    isEnabled: machine.entry.enabled,
    connectionState: machine.connection.phase,
    connectionError: machine.connection.error,
    connectionErrorTraceId: machine.connection.traceId,
  };
}

export function createMachineSummaryAtoms(input: {
  readonly catalogValueAtom: Atom.Atom<MachineCatalogState>;
  readonly presentationAtom: (
    machineId: MachineId,
  ) => Atom.Atom<MachinePresentation | null>;
}) {
  const machineIdsAtom = Atom.make((get) => {
    const next = [...get(input.catalogValueAtom).entries.keys()];
    const previous = Option.getOrNull(get.self<ReadonlyArray<MachineId>>());
    return previous !== null &&
      previous.length === next.length &&
      next.every((id, index) => id === previous[index])
      ? previous
      : next;
  });
  const identitiesAtom = Atom.make((get) => {
    const next = [...get(input.catalogValueAtom).entries].map(([machineId, entry]) => ({
      machineId,
      label: entry.target.label,
    }));
    const previous = Option.getOrNull(
      get.self<ReadonlyArray<{ readonly machineId: MachineId; readonly label: string }>>(),
    );
    return previous !== null &&
      previous.length === next.length &&
      next.every(
        (value, index) =>
          value.machineId === previous[index]?.machineId &&
          value.label === previous[index]?.label,
      )
      ? previous
      : next;
  });
  const machineAtom = Atom.family((machineId: MachineId) =>
    Atom.make((get) => {
      const presentation = get(input.presentationAtom(machineId));
      if (presentation === null) return null;
      const next = projectMachineConnectionSummary(machineId, presentation);
      const previous = Option.getOrNull(get.self<MachineConnectionSummary | null>());
      if (
        previous !== null &&
        previous.machineId === next.machineId &&
        previous.machineLabel === next.machineLabel &&
        previous.displayUrl === next.displayUrl &&
        previous.isRelayManaged === next.isRelayManaged &&
        previous.isEnabled === next.isEnabled &&
        previous.connectionState === next.connectionState &&
        previous.connectionError === next.connectionError &&
        previous.connectionErrorTraceId === next.connectionErrorTraceId
      )
        return previous;
      return next;
    }),
  );
  const machinesAtom = Atom.make((get) => {
    const next: Array<MachineConnectionSummary> = [];
    for (const machineId of get(input.catalogValueAtom).entries.keys()) {
      const machine = get(machineAtom(machineId));
      if (machine !== null) next.push(machine);
    }
    const previous = Option.getOrNull(get.self<ReadonlyArray<MachineConnectionSummary>>());
    return previous !== null &&
      previous.length === next.length &&
      next.every((value, index) => value === previous[index])
      ? previous
      : next;
  }).pipe(Atom.withLabel("machine-connection-summaries"));
  const connectedMachineIdsAtom = Atom.make((get) => {
    const next = get(machineIdsAtom).filter(
      (id) => get(machineAtom(id))?.connectionState === "connected",
    );
    const previous = Option.getOrNull(get.self<ReadonlyArray<MachineId>>());
    return previous !== null &&
      previous.length === next.length &&
      next.every((id, index) => id === previous[index])
      ? previous
      : next;
  });
  const machineByIdAtom = Atom.make((get) => {
    const next = new Map(
      get(machineIdsAtom).map(
        (machineId) =>
          [
            machineId,
            resolveMachineKind(
              get(input.presentationAtom(machineId))?.serverConfig ?? null,
            ),
          ] as const,
      ),
    );
    const previous = Option.getOrNull(
      get.self<ReadonlyMap<MachineId, MachineKind>>(),
    );
    return previous !== null && mapsEqual(previous, next) ? previous : next;
  });
  const pullRequestsSupportedAtom = Atom.make((get) =>
    get(machineIdsAtom).some(
      (machineId) =>
        get(input.presentationAtom(machineId))?.serverConfig?.machine.capabilities
          .pullRequests === true,
    ),
  );
  return {
    machineIdsAtom,
    connectedMachineIdsAtom,
    identitiesAtom,
    machinesAtom,
    machineByIdAtom,
    pullRequestsSupportedAtom,
  };
}
