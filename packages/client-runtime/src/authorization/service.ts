import {
  type ClientConnectionMethod,
  MachineId,
  type ExecutionMachineDescriptor,
} from "@vekrel/contracts";
import { RelayMachineConnectScope } from "@vekrel/contracts/relay";
import { withRelayClientTracing } from "@vekrel/shared/relayTracing";
import {
  exchangeRemoteDpopAccessToken,
  type RemoteMachineAuthError,
  resolveRemoteDpopWebSocketConnectionUrl,
  resolveRemoteWebSocketConnectionUrl,
} from "./remote.ts";
import {
  machineMismatchError,
  mapManagedRelayError,
  mapRemoteDpopMachineError,
  mapRemoteMachineError,
} from "../connection/errors.ts";
import {
  ConnectionBlockedError,
  ConnectionTransientError,
  type ConnectionAttemptError,
} from "../connection/model.ts";
import { fetchRemoteMachineDescriptor } from "../machine/descriptor.ts";
import { machineEndpointUrl } from "../machine/endpoint.ts";
import * as ClientCapabilities from "../platform/capabilities.ts";
import * as ManagedRelay from "../relay/managedRelay.ts";
import * as TokenStore from "./tokenStore.ts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as HttpClient from "effect/http/HttpClient";

import {
  DPOP_ACCESS_TOKEN_REFRESH_SKEW_MS,
  type PreparedHttpAuthorization,
} from "../connection/model.ts";

export interface AuthorizedRemoteMachine {
  readonly machineId: MachineId;
  readonly label: string;
  readonly httpBaseUrl: string;
  readonly socketUrl: string;
  readonly httpAuthorization: PreparedHttpAuthorization;
}

export interface AuthorizedRemoteHttpMachine {
  readonly machineId: MachineId;
  readonly label: string;
  readonly httpBaseUrl: string;
  readonly httpAuthorization: Extract<PreparedHttpAuthorization, { readonly _tag: "Dpop" }>;
}

export class RemoteMachineAuthorization extends Context.Service<
  RemoteMachineAuthorization,
  {
    readonly authorizeBearer: (input: {
      readonly expectedMachineId: MachineId;
      readonly httpBaseUrl: string;
      readonly wsBaseUrl: string;
      readonly bearerToken: string;
      readonly connectionMethod: ClientConnectionMethod;
    }) => Effect.Effect<AuthorizedRemoteMachine, ConnectionAttemptError>;
    readonly authorizeDpop: (input: {
      readonly expectedMachineId: MachineId;
      /**
       * The access token is not bound to an origin, and each proof is minted
       * for the URL it signs, so a token from the relay also works on the
       * machine's LAN or tailnet address.
       */
      readonly directEndpoint?: { readonly httpBaseUrl: string; readonly wsBaseUrl: string };
    }) => Effect.Effect<AuthorizedRemoteMachine, ConnectionAttemptError>;
    readonly authorizeDpopHttp: (input: {
      readonly expectedMachineId: MachineId;
      readonly rejectedAccessToken?: string;
    }) => Effect.Effect<AuthorizedRemoteHttpMachine, ConnectionAttemptError>;
  }
>()("@vekrel/client-runtime/authorization/service/RemoteMachineAuthorization") {}

const CACHED_ENDPOINT_SOCKET_TIMEOUT_MS = 3_000;
const BEARER_DESCRIPTOR_CACHE_TTL_MS = 10_000;
const DPOP_AUTHORIZATION_TIMEOUT_MS = 30_000;

function mapDpopSocketError(error: RemoteMachineAuthError | ConnectionAttemptError) {
  return error._tag === "ConnectionTransientError" || error._tag === "ConnectionBlockedError"
    ? error
    : mapRemoteDpopMachineError(error);
}

const fetchDescriptor = Effect.fn("clientRuntime.connection.remote.fetchDescriptor")(function* (
  httpBaseUrl: string,
  connectionMethod: ClientConnectionMethod,
) {
  return yield* fetchRemoteMachineDescriptor({ httpBaseUrl }).pipe(
    Effect.mapError((error) => mapRemoteMachineError(error, connectionMethod)),
  );
});

export const make = Effect.gen(function* () {
  const serviceScope = yield* Scope.Scope;
  const signer = yield* ManagedRelay.ManagedRelayDpopSigner;
  const relay = yield* ManagedRelay.ManagedRelayClient;
  const cloudSession = yield* ClientCapabilities.CloudSession;
  const deviceIdentity = yield* ClientCapabilities.RelayDeviceIdentity;
  const presentation = yield* ClientCapabilities.ClientPresentation;
  const tokenStore = yield* TokenStore.RemoteDpopAccessTokenStore;
  const httpClient = yield* HttpClient.HttpClient;
  const tokenLock = yield* Semaphore.make(1);
  const tokenOwners = new Map<
    MachineId,
    { readonly accessToken: string; readonly identity: ClientCapabilities.CloudSessionIdentity }
  >();
  const pendingTokens = new Map<
    MachineId,
    {
      readonly id: object;
      readonly identity: ClientCapabilities.CloudSessionIdentity;
      readonly thumbprint: string;
      readonly token: Effect.Effect<TokenStore.RemoteDpopAccessToken, ConnectionAttemptError>;
    }
  >();
  const bearerDescriptors = yield* Ref.make<
    ReadonlyMap<
      MachineId,
      {
        readonly httpBaseUrl: string;
        readonly descriptor: ExecutionMachineDescriptor;
        readonly validatedAtEpochMs: number;
      }
    >
  >(new Map());

  /**
   * A saved LAN address can belong to another machine on a different network.
   */
  const verifyDirectEndpoint = Effect.fn("clientRuntime.connection.remote.verifyDirectEndpoint")(
    function* (input: {
      readonly expectedMachineId: MachineId;
      readonly httpBaseUrl: string;
      readonly connectionMethod: ClientConnectionMethod;
    }) {
      const now = yield* Clock.currentTimeMillis;
      const cachedDescriptor = (yield* Ref.get(bearerDescriptors)).get(input.expectedMachineId);
      const canReuseDescriptor =
        cachedDescriptor?.httpBaseUrl === input.httpBaseUrl &&
        cachedDescriptor.validatedAtEpochMs + BEARER_DESCRIPTOR_CACHE_TTL_MS > now;
      const descriptor = canReuseDescriptor
        ? cachedDescriptor.descriptor
        : yield* fetchDescriptor(input.httpBaseUrl, input.connectionMethod).pipe(
            Effect.provideService(HttpClient.HttpClient, httpClient),
          );
      if (descriptor.machineId !== input.expectedMachineId) {
        return yield* machineMismatchError({
          expected: input.expectedMachineId,
          actual: descriptor.machineId,
        });
      }
      if (!canReuseDescriptor) {
        yield* Ref.update(bearerDescriptors, (current) => {
          const next = new Map(current);
          next.set(input.expectedMachineId, {
            httpBaseUrl: input.httpBaseUrl,
            descriptor,
            validatedAtEpochMs: now,
          });
          return next;
        });
      }
      return descriptor;
    },
  );

  const authorizeBearer = Effect.fn("clientRuntime.connection.remote.authorizeBearer")(
    function* (input: {
      readonly expectedMachineId: Parameters<
        RemoteMachineAuthorization["Service"]["authorizeBearer"]
      >[0]["expectedMachineId"];
      readonly httpBaseUrl: string;
      readonly wsBaseUrl: string;
      readonly bearerToken: string;
      readonly connectionMethod: ClientConnectionMethod;
    }) {
      const descriptor = yield* verifyDirectEndpoint(input);
      const socketUrl = yield* resolveRemoteWebSocketConnectionUrl({
        wsBaseUrl: input.wsBaseUrl,
        httpBaseUrl: input.httpBaseUrl,
        bearerToken: input.bearerToken,
        clientMetadata: presentation.metadata,
        connectionMethod: input.connectionMethod,
      }).pipe(
        Effect.mapError(mapRemoteMachineError),
        Effect.provideService(HttpClient.HttpClient, httpClient),
      );
      return {
        machineId: descriptor.machineId,
        label: descriptor.label,
        httpBaseUrl: input.httpBaseUrl,
        socketUrl,
        httpAuthorization: {
          _tag: "Bearer" as const,
          token: input.bearerToken,
        },
      };
    },
  );

  const createDpopSocketUrl = Effect.fn("clientRuntime.connection.remote.createDpopSocketUrl")(
    function* (
      token: TokenStore.RemoteDpopAccessToken,
      timeoutMs?: number,
      endpoint: { readonly httpBaseUrl: string; readonly wsBaseUrl: string } = token.endpoint,
    ) {
      const ticketProof = yield* signer
        .createProof({
          method: "POST",
          url: machineEndpointUrl(endpoint.httpBaseUrl, "/api/auth/websocket-ticket"),
          accessToken: token.accessToken,
        })
        .pipe(
          Effect.mapError(
            () =>
              new ConnectionBlockedError({
                reason: "configuration",
                detail: "Could not create the websocket authorization proof.",
              }),
          ),
        );
      return yield* resolveRemoteDpopWebSocketConnectionUrl({
        wsBaseUrl: endpoint.wsBaseUrl,
        httpBaseUrl: endpoint.httpBaseUrl,
        accessToken: token.accessToken,
        dpopProof: ticketProof,
        clientMetadata: presentation.metadata,
        connectionMethod: "relay",
        ...(timeoutMs === undefined ? {} : { timeoutMs }),
      }).pipe(Effect.provideService(HttpClient.HttpClient, httpClient));
    },
  );

  const sessionChanged = () =>
    new ConnectionBlockedError({
      reason: "authentication",
      detail: "Your cloud sign-in changed. Sign in again to authorize the machine.",
    });

  // A missing identity before the attempt captures one means the cloud session
  // is still loading, not a sign-out. Only a captured identity that goes away
  // or changes is evidence of one.
  const sessionPending = () =>
    new ConnectionTransientError({
      reason: "session-pending",
      detail: "Waiting for the Vekrel Connect sign-in to load.",
    });

  const assertSession = Effect.fnUntraced(function* (
    identity: ClientCapabilities.CloudSessionIdentity,
  ) {
    const current = yield* cloudSession.identity;
    if (Option.isNone(current) || current.value !== identity) {
      return yield* sessionChanged();
    }
  });

  // Called under tokenLock so a late rejection cannot remove a newer credential.
  const removeRejectedToken = Effect.fnUntraced(function* (
    machineId: MachineId,
    accessToken: string,
  ) {
    const cached = yield* tokenStore.get(machineId);
    if (Option.isSome(cached) && cached.value.accessToken === accessToken) {
      yield* tokenStore.remove(machineId);
      tokenOwners.delete(machineId);
    }
  });

  const obtainBootstrap = Effect.fn("relay.connection.bootstrap.obtain")(function* (
    machineId: MachineId,
    identity: ClientCapabilities.CloudSessionIdentity,
  ) {
    yield* assertSession(identity);
    const clerkToken = yield* cloudSession.clerkToken.pipe(
      Effect.withSpan("relay.connection.cloudSessionToken.resolve"),
    );
    const deviceId = yield* deviceIdentity.deviceId.pipe(
      Effect.withSpan("relay.connection.deviceIdentity.resolve"),
    );
    yield* assertSession(identity);
    const connected = yield* relay
      .connectMachine({
        clerkToken,
        scopes: [RelayMachineConnectScope],
        machineId,
        ...(Option.isSome(deviceId) ? { deviceId: deviceId.value } : {}),
      })
      .pipe(Effect.mapError(mapManagedRelayError));
    if (connected.machineId !== machineId) {
      return yield* machineMismatchError({
        expected: machineId,
        actual: connected.machineId,
      });
    }
    yield* assertSession(identity);
    return connected;
  });

  const exchangeDpopToken = Effect.fn("clientRuntime.connection.remote.exchangeDpopToken")(
    function* (
      machineId: MachineId,
      thumbprint: string,
      identity: ClientCapabilities.CloudSessionIdentity,
    ) {
      const bootstrap = yield* obtainBootstrap(machineId, identity);
      const descriptor = yield* fetchDescriptor(bootstrap.endpoint.httpBaseUrl, "relay").pipe(
        Effect.provideService(HttpClient.HttpClient, httpClient),
        Effect.withSpan("machine.authorization.descriptor"),
      );
      if (descriptor.machineId !== machineId) {
        return yield* machineMismatchError({
          expected: machineId,
          actual: descriptor.machineId,
        });
      }
      const bootstrapProof = yield* signer
        .createProof({
          method: "POST",
          url: machineEndpointUrl(bootstrap.endpoint.httpBaseUrl, "/oauth/token"),
        })
        .pipe(
          Effect.mapError(
            () =>
              new ConnectionBlockedError({
                reason: "configuration",
                detail: "Could not create the machine authorization proof.",
              }),
          ),
        );
      yield* assertSession(identity);
      const access = yield* exchangeRemoteDpopAccessToken({
        httpBaseUrl: bootstrap.endpoint.httpBaseUrl,
        credential: bootstrap.credential,
        dpopProof: bootstrapProof,
        clientMetadata: presentation.metadata,
      }).pipe(
        Effect.mapError(mapRemoteDpopMachineError),
        Effect.provideService(HttpClient.HttpClient, httpClient),
        Effect.withSpan("machine.authorization.accessToken.exchange"),
      );
      const issuedAt = yield* Clock.currentTimeMillis;
      return new TokenStore.RemoteDpopAccessToken({
        machineId: descriptor.machineId,
        accountId: identity.accountId,
        label: descriptor.label,
        endpoint: bootstrap.endpoint,
        accessToken: access.access_token,
        expiresAtEpochMs: issuedAt + access.expires_in * 1_000,
        dpopThumbprint: thumbprint,
      });
    },
  );

  const getDpopToken = Effect.fn("clientRuntime.connection.remote.getDpopToken")(function* (
    input: Parameters<RemoteMachineAuthorization["Service"]["authorizeDpopHttp"]>[0],
  ) {
    const session = yield* cloudSession.identity;
    if (Option.isNone(session)) {
      return yield* sessionPending();
    }
    const identity = session.value;
    const thumbprint = yield* signer.thumbprint.pipe(
      Effect.mapError(
        () =>
          new ConnectionBlockedError({
            reason: "configuration",
            detail: "Could not load the machine authorization key.",
          }),
      ),
      Effect.withSpan("machine.authorization.dpopKey.resolve"),
    );
    const selected = yield* tokenLock.withPermits(1)(
      Effect.gen(function* () {
        yield* assertSession(identity);
        const now = yield* Clock.currentTimeMillis;
        const cached = yield* tokenStore
          .get(input.expectedMachineId)
          .pipe(Effect.withSpan("machine.authorization.accessToken.cache"));
        const owner = tokenOwners.get(input.expectedMachineId);
        if (
          Option.isSome(cached) &&
          cached.value.machineId === input.expectedMachineId &&
          cached.value.accountId === identity.accountId &&
          cached.value.dpopThumbprint === thumbprint &&
          cached.value.expiresAtEpochMs > now + DPOP_ACCESS_TOKEN_REFRESH_SKEW_MS &&
          cached.value.accessToken !== input.rejectedAccessToken &&
          (owner?.accessToken !== cached.value.accessToken || owner.identity === identity)
        ) {
          tokenOwners.set(input.expectedMachineId, {
            accessToken: cached.value.accessToken,
            identity,
          });
          return { token: Effect.succeed(cached.value), fromCache: true };
        }
        if (input.rejectedAccessToken !== undefined) {
          yield* removeRejectedToken(input.expectedMachineId, input.rejectedAccessToken);
        }
        const pending = pendingTokens.get(input.expectedMachineId);
        if (pending?.identity === identity && pending.thumbprint === thumbprint) {
          return { token: pending.token, fromCache: false };
        }

        const id = {};
        const fiber = yield* exchangeDpopToken(
          input.expectedMachineId,
          thumbprint,
          identity,
        ).pipe(
          Effect.flatMap((token) =>
            tokenLock.withPermits(1)(
              Effect.gen(function* () {
                yield* assertSession(identity);
                yield* tokenStore
                  .put(token)
                  .pipe(Effect.withSpan("machine.authorization.accessToken.persist"));
                const current = yield* cloudSession.identity;
                if (Option.isNone(current) || current.value !== identity) {
                  yield* removeRejectedToken(input.expectedMachineId, token.accessToken);
                  return yield* sessionChanged();
                }
                tokenOwners.set(input.expectedMachineId, {
                  accessToken: token.accessToken,
                  identity,
                });
                if (pendingTokens.get(input.expectedMachineId)?.id === id) {
                  pendingTokens.delete(input.expectedMachineId);
                }
                return token;
              }),
            ),
          ),
          Effect.timeoutOrElse({
            duration: DPOP_AUTHORIZATION_TIMEOUT_MS,
            orElse: () =>
              Effect.fail(
                new ConnectionTransientError({
                  reason: "timeout",
                  detail: "Timed out renewing the machine credential.",
                }),
              ),
          }),
          Effect.ensuring(
            tokenLock.withPermits(1)(
              Effect.sync(() => {
                if (pendingTokens.get(input.expectedMachineId)?.id === id) {
                  pendingTokens.delete(input.expectedMachineId);
                }
              }),
            ),
          ),
          // One caller's timeout must not cancel renewal for other HTTP requests.
          Effect.forkIn(serviceScope),
        );
        const token = Fiber.join(fiber);
        pendingTokens.set(input.expectedMachineId, { id, identity, thumbprint, token });
        return { token, fromCache: false };
      }),
    );
    yield* Effect.annotateCurrentSpan({
      "connection.remote_token_cache": selected.fromCache ? "hit" : "miss",
    });
    const token = yield* selected.token;
    yield* assertSession(identity);
    return { token, fromCache: selected.fromCache, identity };
  });

  const httpAuthorization = (
    token: TokenStore.RemoteDpopAccessToken,
    httpBaseUrl = token.endpoint.httpBaseUrl,
  ): AuthorizedRemoteHttpMachine => ({
    machineId: token.machineId,
    label: token.label,
    httpBaseUrl,
    httpAuthorization: {
      _tag: "Dpop",
      accessToken: token.accessToken,
      expiresAtEpochMs: token.expiresAtEpochMs,
    },
  });

  const authorizeDpop = Effect.fn("clientRuntime.connection.remote.authorizeDpop")(function* (
    input: Parameters<RemoteMachineAuthorization["Service"]["authorizeDpop"]>[0],
  ) {
    const endpoint = input.directEndpoint;
    if (endpoint !== undefined) {
      yield* verifyDirectEndpoint({
        expectedMachineId: input.expectedMachineId,
        httpBaseUrl: endpoint.httpBaseUrl,
        connectionMethod: "direct",
      });
    }
    const authorized = (token: TokenStore.RemoteDpopAccessToken, socketUrl: string) => ({
      ...httpAuthorization(token, endpoint?.httpBaseUrl),
      socketUrl,
    });
    let selected = yield* getDpopToken(input);
    if (selected.fromCache) {
      const cachedSocket = yield* createDpopSocketUrl(
        selected.token,
        CACHED_ENDPOINT_SOCKET_TIMEOUT_MS,
        endpoint,
      ).pipe(Effect.result);
      if (Result.isSuccess(cachedSocket)) {
        yield* assertSession(selected.identity);
        return authorized(selected.token, cachedSocket.success);
      }
      if (cachedSocket.failure._tag === "ConnectionBlockedError") {
        return yield* mapDpopSocketError(cachedSocket.failure);
      }
      // An unreachable direct address says nothing about the token, so keep
      // it for the T3 Connect route rather than forcing a relay round trip.
      if (endpoint !== undefined && cachedSocket.failure._tag !== "MachineAuthInvalidError") {
        return yield* mapDpopSocketError(cachedSocket.failure);
      }
      yield* assertSession(selected.identity);
      selected = yield* getDpopToken({
        ...input,
        rejectedAccessToken: selected.token.accessToken,
      });
    }
    const socket = yield* createDpopSocketUrl(selected.token, undefined, endpoint).pipe(
      Effect.result,
    );
    if (Result.isFailure(socket)) {
      if (endpoint === undefined || socket.failure._tag === "ConnectionBlockedError") {
        yield* tokenLock.withPermits(1)(
          removeRejectedToken(input.expectedMachineId, selected.token.accessToken),
        );
      }
      return yield* mapDpopSocketError(socket.failure);
    }
    yield* assertSession(selected.identity);
    return authorized(selected.token, socket.success);
  });

  return RemoteMachineAuthorization.of({
    authorizeBearer,
    authorizeDpop: (input) =>
      authorizeDpop(input).pipe(Effect.withSpan("machine.authorization")),
    authorizeDpopHttp: (input) =>
      getDpopToken(input).pipe(
        Effect.map(({ token }) => httpAuthorization(token)),
        Effect.withSpan("machine.authorization.http"),
        withRelayClientTracing,
      ),
  });
});

export const layer = Layer.effect(RemoteMachineAuthorization, make);
