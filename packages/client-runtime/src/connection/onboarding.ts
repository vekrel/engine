import type { DesktopSshMachineTarget, MachineId } from "@vekrel/contracts";
import { resolveRemotePairingTarget } from "@vekrel/shared/remote";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as HttpClient from "effect/http/HttpClient";

import { bootstrapRemoteBearerSession } from "../authorization/remote.ts";
import { deriveWsBaseUrl, normalizeHttpBaseUrl } from "../machine/endpoint.ts";
import { fetchRemoteMachineDescriptor } from "../machine/descriptor.ts";
import * as ClientCapabilities from "../platform/capabilities.ts";
import {
  BearerConnectionCredential,
  BearerConnectionProfile,
  BearerConnectionRegistration,
  type ConnectionCatalogEntry,
  type ConnectionCredential,
  SshConnectionProfile,
  SshConnectionRegistration,
} from "./catalog.ts";
import * as ConnectionCredentialStore from "./credentialStore.ts";
import { mapRemoteMachineError } from "./errors.ts";
import {
  BearerConnectionTarget,
  ConnectionBlockedError,
  SshConnectionTarget,
  type ConnectionAttemptError,
} from "./model.ts";
import * as Persistence from "../platform/persistence.ts";
import * as MachineRegistry from "./registry.ts";
import { orchestrationProtocolCompatibilityError } from "./compatibility.ts";
import { connectionRoutes, routeEntry, sshTargetKey } from "./routes.ts";

export interface PairingConnectionInput {
  readonly pairingUrl?: string;
  readonly host?: string;
  readonly pairingCode?: string;
  readonly expectedMachineId?: MachineId;
}

export interface SshConnectionInput {
  readonly target: DesktopSshMachineTarget;
  readonly label?: string;
  readonly expectedMachineId?: MachineId;
}

export interface BearerConnectionUpdateInput {
  readonly machineId: MachineId;
  readonly label: string;
  readonly httpBaseUrl: string;
}

export class ConnectionOnboarding extends Context.Service<
  ConnectionOnboarding,
  {
    readonly registerPairing: (
      input: PairingConnectionInput,
    ) => Effect.Effect<
      MachineId,
      ConnectionAttemptError | Persistence.ConnectionPersistenceError
    >;
    readonly registerSsh: (
      input: SshConnectionInput,
    ) => Effect.Effect<
      MachineId,
      ConnectionAttemptError | Persistence.ConnectionPersistenceError
    >;
    readonly updateBearer: (
      input: BearerConnectionUpdateInput,
    ) => Effect.Effect<void, ConnectionAttemptError | Persistence.ConnectionPersistenceError>;
  }
>()("@vekrel/client-runtime/connection/onboarding/ConnectionOnboarding") {}

const resolvePairingTarget = Effect.fn("clientRuntime.connection.onboarding.resolvePairingTarget")(
  function* (input: PairingConnectionInput) {
    return yield* Effect.try({
      try: () => resolveRemotePairingTarget(input),
      catch: (cause) =>
        new ConnectionBlockedError({
          reason: "configuration",
          detail: cause instanceof Error ? cause.message : "The pairing details are invalid.",
        }),
    });
  },
);

function bearerConnectionId(machineId: MachineId, httpBaseUrl: string): string {
  return `bearer:${machineId}:${new URL(httpBaseUrl).origin}`;
}

function differentMachineError(label: string) {
  return new ConnectionBlockedError({
    reason: "configuration",
    detail: `That address reaches ${label}, a different machine. Add it as its own machine instead.`,
  });
}

export const preparePairingRegistration = Effect.fn(
  "clientRuntime.connection.onboarding.preparePairingRegistration",
)(function* (input: PairingConnectionInput) {
  const target = yield* resolvePairingTarget(input);
  const presentation = yield* ClientCapabilities.ClientPresentation;
  const descriptor = yield* fetchRemoteMachineDescriptor({
    httpBaseUrl: target.httpBaseUrl,
  }).pipe(Effect.mapError(mapRemoteMachineError));
  // Checked before redeeming the one-time code, so a wrong link is not spent.
  if (
    input.expectedMachineId !== undefined &&
    descriptor.machineId !== input.expectedMachineId
  ) {
    return yield* differentMachineError(descriptor.label);
  }
  const compatibilityError = orchestrationProtocolCompatibilityError(descriptor);
  // An outdated server is still saved so it can be updated from this client.
  if (compatibilityError !== null && compatibilityError.serverUpdateRequired !== true) {
    return yield* compatibilityError;
  }
  const access = yield* bootstrapRemoteBearerSession({
    httpBaseUrl: target.httpBaseUrl,
    credential: target.credential,
    clientMetadata: presentation.metadata,
  }).pipe(Effect.mapError(mapRemoteMachineError));
  const connectionId = bearerConnectionId(descriptor.machineId, target.httpBaseUrl);

  return new BearerConnectionRegistration({
    target: new BearerConnectionTarget({
      machineId: descriptor.machineId,
      label: descriptor.label,
      connectionId,
    }),
    profile: new BearerConnectionProfile({
      connectionId,
      machineId: descriptor.machineId,
      label: descriptor.label,
      httpBaseUrl: target.httpBaseUrl,
      wsBaseUrl: target.wsBaseUrl,
    }),
    credential: new BearerConnectionCredential({
      token: access.access_token,
    }),
  });
});

const registerPairingConnection = Effect.fn(
  "clientRuntime.connection.onboarding.registerPairingConnection",
)(function* (input: PairingConnectionInput) {
  const registration = yield* preparePairingRegistration(input);
  const registry = yield* MachineRegistry.MachineRegistry;
  if (!(yield* registry.register(registration))) return registration.target.machineId;
  yield* registry.setCompatibility(registration.target.machineId, null);
  yield* registry
    .setEnabled(registration.target.machineId, true)
    .pipe(Effect.catchTags({ MachineNotRegisteredError: () => Effect.void }));
  return registration.target.machineId;
});

const isBearerCredential = Schema.is(BearerConnectionCredential);
const isBearerProfile = Schema.is(BearerConnectionProfile);

const updateBearerConnection = Effect.fn(
  "clientRuntime.connection.onboarding.updateBearerConnection",
)(function* (input: BearerConnectionUpdateInput) {
  const registry = yield* MachineRegistry.MachineRegistry;
  const credentials = yield* ConnectionCredentialStore.ConnectionCredentialStore;
  const saved = (yield* SubscriptionRef.get(registry.entries)).get(input.machineId);
  const route =
    saved === undefined
      ? undefined
      : connectionRoutes(saved).find(
          (candidate) => candidate.target._tag === "BearerConnectionTarget",
        );
  const entry = saved === undefined || route === undefined ? saved : routeEntry(saved, route);
  const credential =
    entry?.target._tag === "BearerConnectionTarget"
      ? yield* credentials.get(entry.target.connectionId)
      : Option.none();
  const registration = yield* prepareBearerConnectionUpdate({
    input,
    entry: Option.fromUndefinedOr(entry),
    credential,
  });
  yield* registry.register(registration);
});

export const prepareBearerConnectionUpdate = Effect.fn(
  "clientRuntime.connection.onboarding.prepareBearerConnectionUpdate",
)(function* (options: {
  readonly input: BearerConnectionUpdateInput;
  readonly entry: Option.Option<ConnectionCatalogEntry>;
  readonly credential: Option.Option<ConnectionCredential>;
}) {
  const entry = Option.getOrNull(options.entry);
  if (
    entry === undefined ||
    entry === null ||
    entry.target._tag !== "BearerConnectionTarget" ||
    Option.isNone(entry.profile) ||
    !isBearerProfile(entry.profile.value)
  ) {
    return yield* new ConnectionBlockedError({
      reason: "configuration",
      detail: "Only saved bearer machines can be edited.",
    });
  }

  const credential = options.credential;
  if (Option.isNone(credential) || !isBearerCredential(credential.value)) {
    return yield* new ConnectionBlockedError({
      reason: "authentication",
      detail: "The saved bearer credential is unavailable.",
    });
  }

  const label = options.input.label.trim();
  if (label === "") {
    return yield* new ConnectionBlockedError({
      reason: "configuration",
      detail: "Machine label cannot be empty.",
    });
  }
  const httpBaseUrl = yield* Effect.try({
    try: () => normalizeHttpBaseUrl(options.input.httpBaseUrl),
    catch: (cause) =>
      new ConnectionBlockedError({
        reason: "configuration",
        detail: cause instanceof Error ? cause.message : "The machine URL is invalid.",
      }),
  });
  const connectionId = entry.target.connectionId;
  return new BearerConnectionRegistration({
    target: new BearerConnectionTarget({
      machineId: options.input.machineId,
      label,
      connectionId,
    }),
    profile: new BearerConnectionProfile({
      connectionId,
      machineId: options.input.machineId,
      label,
      httpBaseUrl,
      wsBaseUrl: deriveWsBaseUrl(httpBaseUrl),
    }),
    credential: credential.value,
  });
});

export const prepareSshRegistration = Effect.fn(
  "clientRuntime.connection.onboarding.prepareSshRegistration",
)(function* (input: SshConnectionInput) {
  const gateway = yield* ClientCapabilities.SshMachineGateway;
  const provisioned = yield* gateway.provision(input.target, input.expectedMachineId);
  const connectionId = `ssh:${provisioned.machineId}:${sshTargetKey(provisioned.bootstrap.target)}`;
  const label = input.label?.trim() || provisioned.label || provisioned.bootstrap.target.alias;

  return new SshConnectionRegistration({
    target: new SshConnectionTarget({
      machineId: provisioned.machineId,
      label,
      connectionId,
    }),
    profile: new SshConnectionProfile({
      connectionId,
      machineId: provisioned.machineId,
      label,
      target: provisioned.bootstrap.target,
    }),
    credential: new BearerConnectionCredential({ token: provisioned.bearerToken }),
  });
});

const registerSshConnection = Effect.fn(
  "clientRuntime.connection.onboarding.registerSshConnection",
)(function* (input: SshConnectionInput) {
  const registration = yield* prepareSshRegistration(input);
  if (
    input.expectedMachineId !== undefined &&
    registration.target.machineId !== input.expectedMachineId
  ) {
    return yield* differentMachineError(registration.target.label);
  }
  const registry = yield* MachineRegistry.MachineRegistry;
  yield* registry.register(registration);
  return registration.target.machineId;
});

/** @public Service construction is part of the canonical Effect module API. */
export const make = Effect.gen(function* () {
  const registry = yield* MachineRegistry.MachineRegistry;
  const presentation = yield* ClientCapabilities.ClientPresentation;
  const httpClient = yield* HttpClient.HttpClient;
  const ssh = yield* ClientCapabilities.SshMachineGateway;
  const credentials = yield* ConnectionCredentialStore.ConnectionCredentialStore;

  return ConnectionOnboarding.of({
    registerPairing: (input) =>
      registerPairingConnection(input).pipe(
        Effect.provideService(MachineRegistry.MachineRegistry, registry),
        Effect.provideService(ClientCapabilities.ClientPresentation, presentation),
        Effect.provideService(HttpClient.HttpClient, httpClient),
      ),
    registerSsh: (input) =>
      registerSshConnection(input).pipe(
        Effect.provideService(MachineRegistry.MachineRegistry, registry),
        Effect.provideService(ClientCapabilities.SshMachineGateway, ssh),
      ),
    updateBearer: (input) =>
      updateBearerConnection(input).pipe(
        Effect.provideService(MachineRegistry.MachineRegistry, registry),
        Effect.provideService(ConnectionCredentialStore.ConnectionCredentialStore, credentials),
      ),
  });
});

export const layer = Layer.effect(ConnectionOnboarding, make);
