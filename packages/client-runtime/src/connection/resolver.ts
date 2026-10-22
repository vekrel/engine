import type {
  AuthClientPresentationMetadata,
  ExecutionMachineDescriptor,
} from "@vekrel/contracts";
import { withRelayClientTracing } from "@vekrel/shared/relayTracing";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as HttpClient from "effect/http/HttpClient";

import {
  appendClientConnectionParams,
  type ClientOwnerParams,
  readClientOwnerParams,
} from "../authorization/remote.ts";
import * as RemoteMachineAuthorization from "../authorization/service.ts";
import * as ClientCapabilities from "../platform/capabilities.ts";
import {
  BearerConnectionCredential,
  BearerConnectionProfile,
  type ConnectionCatalogEntry,
  SshConnectionProfile,
} from "./catalog.ts";
import * as ConnectionCredentialStore from "./credentialStore.ts";
import {
  credentialMissingError,
  machineMismatchError,
  mapRemoteMachineError,
  profileMissingError,
} from "./errors.ts";
import {
  GitHubRoutingPermissions,
  gitHubRoutingConnectionKey,
} from "./githubRoutingPermissions.ts";
import type {
  BearerConnectionTarget,
  ConnectionTarget,
  PreparedConnection,
  PrimaryConnectionTarget,
  RelayConnectionTarget,
  SshConnectionTarget,
} from "./model.ts";
import { ConnectionBlockedError, type ConnectionAttemptError } from "./model.ts";
import * as ConnectionProfileStore from "./profileStore.ts";
import {
  appendOrchestrationProtocol,
  orchestrationProtocolCompatibilityError,
} from "./compatibility.ts";
import { credentialConnectionId } from "./routes.ts";
import { fetchRemoteMachineDescriptor } from "../machine/descriptor.ts";

export class ConnectionResolver extends Context.Service<
  ConnectionResolver,
  {
    readonly prepare: (
      entry: ConnectionCatalogEntry,
    ) => Effect.Effect<PreparedConnection, ConnectionAttemptError>;
    /**
     * Authorizes a socket without the orchestration protocol gate, for hosts
     * too old to connect normally. Only update RPCs may run over it.
     */
    readonly prepareForUpdate: (entry: ConnectionCatalogEntry) => Effect.Effect<
      {
        readonly prepared: PreparedConnection;
        readonly descriptor: ExecutionMachineDescriptor;
      },
      ConnectionAttemptError
    >;
  }
>()("@vekrel/client-runtime/connection/resolver/ConnectionResolver") {}

const isBearerProfile = Schema.is(BearerConnectionProfile);
const isSshProfile = Schema.is(SshConnectionProfile);
const isBearerCredential = Schema.is(BearerConnectionCredential);

function primarySocketUrl(
  target: PrimaryConnectionTarget,
  clientMetadata: AuthClientPresentationMetadata | undefined,
  owners: ClientOwnerParams,
): string {
  const url = new URL(target.wsBaseUrl);
  if (url.pathname === "" || url.pathname === "/") {
    url.pathname = "/ws";
  }
  appendClientConnectionParams(url, clientMetadata, "direct", owners);
  return url.toString();
}

const makePrimaryBroker = Effect.fn("clientRuntime.connection.broker.makePrimary")(function* () {
  const auth = yield* ClientCapabilities.PrimaryMachineAuth;
  const presentation = yield* ClientCapabilities.ClientPresentation;
  const remote = yield* RemoteMachineAuthorization.RemoteMachineAuthorization;

  return Effect.fn("clientRuntime.connection.broker.primary")(function* (
    target: PrimaryConnectionTarget,
  ) {
    const bearerToken = yield* auth.bearerToken;
    if (Option.isNone(bearerToken)) {
      return {
        machineId: target.machineId,
        label: target.label,
        httpBaseUrl: target.httpBaseUrl,
        socketUrl: primarySocketUrl(target, presentation.metadata, yield* readClientOwnerParams),
        httpAuthorization: null,
        target,
      } satisfies PreparedConnection;
    }

    const authorized = yield* remote.authorizeBearer({
      expectedMachineId: target.machineId,
      httpBaseUrl: target.httpBaseUrl,
      wsBaseUrl: target.wsBaseUrl,
      bearerToken: bearerToken.value,
      connectionMethod: "direct",
    });
    return {
      ...authorized,
      target,
    } satisfies PreparedConnection;
  });
});

const makeBearerBroker = Effect.fn("clientRuntime.connection.broker.makeBearer")(function* () {
  const credentials = yield* ConnectionCredentialStore.ConnectionCredentialStore;
  const remote = yield* RemoteMachineAuthorization.RemoteMachineAuthorization;

  return Effect.fn("clientRuntime.connection.broker.bearer")(function* (
    entry: ConnectionCatalogEntry & { readonly target: BearerConnectionTarget },
  ) {
    const target = entry.target;
    const profile = yield* Effect.fromOption(entry.profile, () =>
      profileMissingError(target.connectionId),
    );
    if (!isBearerProfile(profile)) {
      return yield* new ConnectionBlockedError({
        reason: "configuration",
        detail: `Connection profile ${target.connectionId} is not a bearer connection.`,
      });
    }
    if (profile.machineId !== target.machineId) {
      return yield* machineMismatchError({
        expected: target.machineId,
        actual: profile.machineId,
      });
    }
    if (profile.authorization === "t3-connect") {
      const authorized = yield* remote.authorizeDpop({
        expectedMachineId: target.machineId,
        directEndpoint: { httpBaseUrl: profile.httpBaseUrl, wsBaseUrl: profile.wsBaseUrl },
      });
      return { ...authorized, target } satisfies PreparedConnection;
    }
    const credential = yield* credentials.get(credentialConnectionId(target.connectionId)).pipe(
      Effect.flatMap(
        Option.match({
          onNone: () => Effect.fail(credentialMissingError(target.connectionId)),
          onSome: Effect.succeed,
        }),
      ),
    );
    if (!isBearerCredential(credential)) {
      return yield* credentialMissingError(target.connectionId);
    }
    const authorized = yield* remote.authorizeBearer({
      expectedMachineId: target.machineId,
      httpBaseUrl: profile.httpBaseUrl,
      wsBaseUrl: profile.wsBaseUrl,
      bearerToken: credential.token,
      connectionMethod: "direct",
    });
    return {
      machineId: authorized.machineId,
      label: authorized.label,
      httpBaseUrl: authorized.httpBaseUrl,
      socketUrl: authorized.socketUrl,
      httpAuthorization: authorized.httpAuthorization,
      target,
    } satisfies PreparedConnection;
  });
});

const makeRelayBroker = Effect.fn("clientRuntime.connection.broker.makeRelay")(function* () {
  const remote = yield* RemoteMachineAuthorization.RemoteMachineAuthorization;

  return Effect.fnUntraced(
    function* (target: RelayConnectionTarget) {
      const authorized = yield* remote.authorizeDpop({
        expectedMachineId: target.machineId,
      });
      return {
        machineId: authorized.machineId,
        label: authorized.label,
        httpBaseUrl: authorized.httpBaseUrl,
        socketUrl: authorized.socketUrl,
        httpAuthorization: authorized.httpAuthorization,
        target,
      } satisfies PreparedConnection;
    },
    Effect.withSpan("clientRuntime.connection.broker.relay"),
    withRelayClientTracing,
  );
});

const makeSshBroker = Effect.fn("clientRuntime.connection.broker.makeSsh")(function* () {
  const profiles = yield* ConnectionProfileStore.ConnectionProfileStore;
  const credentials = yield* ConnectionCredentialStore.ConnectionCredentialStore;
  const ssh = yield* ClientCapabilities.SshMachineGateway;
  const remote = yield* RemoteMachineAuthorization.RemoteMachineAuthorization;

  return Effect.fn("clientRuntime.connection.broker.ssh")(function* (
    entry: ConnectionCatalogEntry & { readonly target: SshConnectionTarget },
  ) {
    const target = entry.target;
    const profile = yield* Effect.fromOption(entry.profile, () =>
      profileMissingError(target.connectionId),
    );
    if (!isSshProfile(profile)) {
      return yield* new ConnectionBlockedError({
        reason: "configuration",
        detail: `Connection profile ${target.connectionId} is not an SSH connection.`,
      });
    }
    if (profile.machineId !== target.machineId) {
      return yield* machineMismatchError({
        expected: target.machineId,
        actual: profile.machineId,
      });
    }
    // Each pairing adds a session on the host, so connects reuse the saved token.
    const saved = Option.getOrUndefined(yield* credentials.get(target.connectionId));
    const savedToken = saved !== undefined && isBearerCredential(saved) ? saved.token : undefined;
    const prepared = yield* ssh.prepare({
      connectionId: target.connectionId,
      expectedMachineId: target.machineId,
      target: profile.target,
      ...(savedToken === undefined ? {} : { bearerToken: savedToken }),
    });
    if (prepared.bearerToken !== savedToken) {
      yield* credentials.put(
        target.connectionId,
        new BearerConnectionCredential({ token: prepared.bearerToken }),
      );
    }
    const preparedProfile = new SshConnectionProfile({
      connectionId: profile.connectionId,
      machineId: profile.machineId,
      label: profile.label,
      target: prepared.bootstrap.target,
    });
    if (
      gitHubRoutingConnectionKey(entry) !==
      gitHubRoutingConnectionKey({ ...entry, profile: Option.some(preparedProfile) })
    ) {
      const permissions = yield* GitHubRoutingPermissions;
      yield* permissions.forget(target.machineId);
    }
    yield* profiles.put(preparedProfile);
    const authorized = yield* remote.authorizeBearer({
      expectedMachineId: target.machineId,
      httpBaseUrl: prepared.bootstrap.httpBaseUrl,
      wsBaseUrl: prepared.bootstrap.wsBaseUrl,
      bearerToken: prepared.bearerToken,
      connectionMethod: "ssh",
    });
    return {
      machineId: authorized.machineId,
      label: authorized.label,
      httpBaseUrl: authorized.httpBaseUrl,
      socketUrl: authorized.socketUrl,
      httpAuthorization: authorized.httpAuthorization,
      target,
    } satisfies PreparedConnection;
  });
});

/** @public Service construction is part of the canonical Effect module API. */
export const make = Effect.gen(function* () {
  const primary = yield* makePrimaryBroker();
  const bearer = yield* makeBearerBroker();
  const relay = yield* makeRelayBroker();
  const ssh = yield* makeSshBroker();
  const httpClient = yield* HttpClient.HttpClient;

  const authorize = Effect.fn("clientRuntime.connection.broker.authorize")(function* (
    entry: ConnectionCatalogEntry,
  ) {
    const target: ConnectionTarget = entry.target;
    yield* Effect.annotateCurrentSpan({
      "connection.machine.id": target.machineId,
      "connection.target.kind": target._tag,
    });
    const prepared = yield* (() => {
      switch (target._tag) {
        case "PrimaryConnectionTarget":
          return primary(target);
        case "BearerConnectionTarget":
          return bearer({ ...entry, target });
        case "RelayConnectionTarget":
          return relay(target);
        case "SshConnectionTarget":
          return ssh({ ...entry, target });
      }
    })();
    const descriptor = yield* fetchRemoteMachineDescriptor({
      httpBaseUrl: prepared.httpBaseUrl,
    }).pipe(
      Effect.mapError(mapRemoteMachineError),
      Effect.provideService(HttpClient.HttpClient, httpClient),
    );
    if (descriptor.machineId !== target.machineId) {
      return yield* machineMismatchError({
        expected: target.machineId,
        actual: descriptor.machineId,
      });
    }
    return { prepared, descriptor };
  });

  const prepare = Effect.fn("clientRuntime.connection.broker.prepare")(function* (
    entry: ConnectionCatalogEntry,
  ) {
    const { prepared, descriptor } = yield* authorize(entry);
    const compatibilityError = orchestrationProtocolCompatibilityError(descriptor);
    if (compatibilityError !== null) {
      return yield* compatibilityError;
    }
    return {
      ...prepared,
      socketUrl: appendOrchestrationProtocol(prepared.socketUrl),
    };
  });

  return ConnectionResolver.of({ prepare, prepareForUpdate: authorize });
});

export const layer = Layer.effect(ConnectionResolver, make);
