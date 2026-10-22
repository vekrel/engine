// @effect-diagnostics-next-line nodeBuiltinImport:off -- Effect's Crypto has no createPublicKey.
import * as NodeCrypto from "node:crypto";
import {
  AuthStandardClientScopes,
  MachineAuthenticatedPrincipal,
  type MachineCloudLinkStateResult,
  type MachineCloudPreferencesRequest,
  type MachineCloudRelayConfigResult,
} from "@vekrel/contracts";
import {
  RelayCloudMachineHealthProofPayload,
  type RelayCloudMachineHealthRequest,
  RelayCloudMintCredentialProofPayload,
  type RelayCloudMintCredentialRequest,
  type RelayMachineConfigRequest,
  type RelayMachineHealthResponse,
  type RelayMachineHealthResponseProofPayload,
  type RelayMachineLinkProof,
  type RelayMachineLinkProofPayload,
  RelayMachineLinkChallengeResponse,
  RelayMachineLinkResponse,
  type RelayMachineMintResponse,
  type RelayMachineMintResponseProofPayload,
  type RelayLinkProofRequest,
  type RelayManagedEndpointOrigin,
  type RelayManagedEndpointRecoveryProofPayload,
  RelayManagedEndpointRecoveryRegistrationResponse,
  RelayManagedEndpointRecoveryResponse,
  type RelayManagedEndpointRuntimeConfig,
  RelayOkResponse,
} from "@vekrel/contracts/relay";
import { withRelayClientTracing } from "@vekrel/shared/relayTracing";
import {
  normalizeRelayIssuer,
  RELAY_HEALTH_REQUEST_TYP,
  RELAY_HEALTH_RESPONSE_TYP,
  RELAY_LINK_PROOF_TYP,
  RELAY_MANAGED_TUNNEL_RECOVERY_TYP,
  RELAY_MINT_REQUEST_TYP,
  RELAY_MINT_RESPONSE_TYP,
  signRelayJwt,
  verifyRelayJwt,
} from "@vekrel/shared/relayJwt";
import { isSecureRelayUrl } from "@vekrel/shared/relayUrl";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import type * as PlatformError from "effect/PlatformError";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import {
  HttpClient,
  HttpClientRequest,
  HttpClientResponse,
  type HttpServerRequest,
} from "effect/http";
import type * as HttpClientError from "effect/http/HttpClientError";
import * as HttpServer from "effect/http/HttpServer";

import * as MachineAuth from "../auth/MachineAuth.ts";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerConfig from "../config.ts";
import * as ServerMachine from "../machine/ServerMachine.ts";
import * as AgentAwarenessRelay from "../relay/AgentAwarenessRelay.ts";
import { makeRelayMachineClient } from "../relay/relayMachineClient.ts";
import { loopbackUrlHostForAddress } from "../startupAccess.ts";
import {
  type CliDesiredLinkMode,
  readCliDesiredCloudLink,
  readCliDesiredLinkMode,
  setCliDesiredCloudLink,
} from "./CliState.ts";
import * as CliTokenManager from "./CliTokenManager.ts";
import {
  CLOUD_ENDPOINT_CONFIRMED_ORIGIN,
  CLOUD_ENDPOINT_RUNTIME_CONFIG,
  CLOUD_LINK_REVOKED,
  CLOUD_LINKED_USER_ID,
  CLOUD_MINT_PUBLIC_KEY,
  decodeConfirmedOrigin,
  decodeRuntimeConfig,
  encodeConfirmedOriginJson,
  encodeEndpointRuntimeConfigJson,
  HOLD_WEBHOOKS_WHILE_OFFLINE_SECRET,
  PUBLISH_AGENT_ACTIVITY_SECRET,
  readRelayConnection,
  RELAY_MACHINE_CREDENTIAL_SECRET,
  RELAY_ISSUER_SECRET,
  RELAY_URL_SECRET,
} from "./config.ts";
import { getOrCreateMachineKeyPairFromSecretStore } from "./machineKeys.ts";
import * as ConnectReachability from "./ConnectReachability.ts";
import * as ManagedEndpointRuntime from "./ManagedEndpointRuntime.ts";
import { relayUrlConfig } from "./publicConfig.ts";
import {
  filterRelayResponse,
  relayRequestError,
  type RelayRequestError,
  shouldRetryRelayRequest,
} from "./relayResponse.ts";
import {
  hasBoundedCloudProofLifetime,
  hasExactScope,
  hasForwardedAuthorityHeaders,
  isAllowedEndpointOrigin,
  isSupportedLinkProviderKind,
  linkProofScopes,
  managedEndpointRuntimeConfigsMatch,
  parseManagedEndpointLocalOrigin,
  requestAbsoluteUrl,
} from "./linkChecks.ts";
import { desktopUpdateRestartPending, pendingUpdateHandoffExists } from "./updateHandoff.ts";

const CLOUD_MINT_NONCE_PREFIX = "cloud-mint-nonce-";
const CLOUD_MINT_JTI_PREFIX = "cloud-mint-jti-";
const CLOUD_HEALTH_NONCE_PREFIX = "cloud-health-nonce-";
const CLOUD_HEALTH_JTI_PREFIX = "cloud-health-jti-";
export const CLOUD_REPLAY_MARKER_PREFIXES = [
  CLOUD_MINT_NONCE_PREFIX,
  CLOUD_MINT_JTI_PREFIX,
  CLOUD_HEALTH_NONCE_PREFIX,
  CLOUD_HEALTH_JTI_PREFIX,
] as const;
const MANAGED_ENDPOINT_PROVISION_REQUEST_TIMEOUT = Duration.minutes(2);
const RELAY_CONFIG_FIELD_MESSAGES = {
  relayUrl: "Relay URL must be a secure absolute HTTPS URL.",
  relayIssuer: "Relay issuer must be a secure absolute HTTPS URL.",
  machineCredential: "Relay machine credential is required.",
  cloudUserId: "Cloud user id is required.",
  cloudMintPublicKey: "Cloud mint public key must be a valid Ed25519 public key.",
} as const;

export class CloudLinkRelayConfigInvalidError extends Schema.TaggedError<CloudLinkRelayConfigInvalidError>()(
  "CloudLinkRelayConfigInvalidError",
  {
    field: Schema.Literals([
      "relayUrl",
      "relayIssuer",
      "machineCredential",
      "cloudUserId",
      "cloudMintPublicKey",
    ]),
  },
) {
  override get message(): string {
    return RELAY_CONFIG_FIELD_MESSAGES[this.field];
  }
}

export class CloudLinkOriginInvalidError extends Schema.TaggedError<CloudLinkOriginInvalidError>()(
  "CloudLinkOriginInvalidError",
  { origin: Schema.Literals(["endpoint", "local"]) },
) {
  override get message(): string {
    return this.origin === "endpoint"
      ? "Invalid managed endpoint origin."
      : "Could not resolve local machine origin.";
  }
}

export class CloudLinkNotLinkedError extends Schema.TaggedError<CloudLinkNotLinkedError>()(
  "CloudLinkNotLinkedError",
  {},
) {
  override get message(): string {
    return "Link this machine to T3 Connect first.";
  }
}

export class CloudLinkAccountMismatchError extends Schema.TaggedError<CloudLinkAccountMismatchError>()(
  "CloudLinkAccountMismatchError",
  {},
) {
  override get message(): string {
    return "This machine is already linked to a different cloud account. Unlink it before switching accounts.";
  }
}

export class CloudLinkAuthorizationMissingError extends Schema.TaggedError<CloudLinkAuthorizationMissingError>()(
  "CloudLinkAuthorizationMissingError",
  {},
) {
  override get message(): string {
    return "Run `t3 connect link` to authorize this machine.";
  }
}

const SignedRelayRequest = Schema.Literals(["health", "mint"]);

export class CloudLinkProofRejectedError extends Schema.TaggedError<CloudLinkProofRejectedError>()(
  "CloudLinkProofRejectedError",
  { request: SignedRelayRequest },
) {
  override get message(): string {
    return `Invalid cloud ${this.request} request.`;
  }
}

export class CloudLinkProofReplayedError extends Schema.TaggedError<CloudLinkProofReplayedError>()(
  "CloudLinkProofReplayedError",
  { request: SignedRelayRequest },
) {
  override get message(): string {
    return `Cloud ${this.request} request was already consumed.`;
  }
}

export class CloudLinkTunnelSupersededError extends Schema.TaggedError<CloudLinkTunnelSupersededError>()(
  "CloudLinkTunnelSupersededError",
  {},
) {
  override get message(): string {
    return "The managed tunnel configuration changed during registration.";
  }
}

export class CloudLinkEndpointUnavailableError extends Schema.TaggedError<CloudLinkEndpointUnavailableError>()(
  "CloudLinkEndpointUnavailableError",
  {
    reason: Schema.Literals(["runtime-not-started", "origin-unconfirmed"]),
    endpointRuntimeStatus: Schema.Unknown,
  },
) {
  override get message(): string {
    return this.reason === "runtime-not-started"
      ? "Managed endpoint runtime could not be started."
      : "Managed endpoint origin could not be confirmed.";
  }
}

const INTERNAL_OPERATION_MESSAGES = {
  "relay-url-unconfigured":
    "VEKREL_RELAY_URL must be configured as a secure absolute HTTPS origin.",
  "generate-link-proof": "Could not generate machine link proof.",
  "persist-relay-config": "Could not persist machine relay configuration.",
  "register-endpoint-origin": "Could not register the managed endpoint origin.",
  "resolve-server-origin": "Could not resolve the local server origin.",
  "persist-desired-link": "Could not persist desired T3 Connect link state.",
  "sign-recovery-proof": "Could not sign the managed tunnel recovery request.",
  "unsupported-recovered-tunnel":
    "T3 Connect returned an unsupported managed tunnel configuration.",
  "persist-recovered-tunnel": "Could not persist the recovered managed tunnel configuration.",
  "read-relay-config": "Could not read machine relay configuration.",
  "remove-relay-config": "Could not remove machine relay configuration.",
  "update-webhook-settings": "Could not update T3 Connect webhook settings.",
  "read-preferences": "Could not read machine cloud preferences.",
  "persist-preferences": "Could not persist machine cloud preferences.",
  "answer-health": "Could not answer cloud health request.",
  "issue-credential": "Could not issue cloud connection credential.",
} as const;

export class CloudLinkInternalError extends Schema.TaggedError<CloudLinkInternalError>()(
  "CloudLinkInternalError",
  {
    operation: Schema.Literals(
      Object.keys(INTERNAL_OPERATION_MESSAGES) as [
        keyof typeof INTERNAL_OPERATION_MESSAGES,
        ...Array<keyof typeof INTERNAL_OPERATION_MESSAGES>,
      ],
    ),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return INTERNAL_OPERATION_MESSAGES[this.operation];
  }
}

const internalError =
  (operation: CloudLinkInternalError["operation"]) =>
  (cause: unknown): Effect.Effect<never, CloudLinkInternalError> =>
    Effect.fail(new CloudLinkInternalError({ operation, cause }));

const isPermanentLinkError = Schema.is(
  Schema.Union([
    CloudLinkRelayConfigInvalidError,
    CloudLinkOriginInvalidError,
    CloudLinkNotLinkedError,
    CloudLinkAccountMismatchError,
    CloudLinkAuthorizationMissingError,
    CloudLinkProofRejectedError,
    CloudLinkProofReplayedError,
    CloudLinkTunnelSupersededError,
  ]),
);

export const shouldRetryCloudLink = (error: unknown): boolean =>
  shouldRetryRelayRequest(error) &&
  !isPermanentLinkError(error) &&
  !(CliTokenManager.isCloudCliCredentialRefreshError(error) && error.requiresLogin);

const rollbackFailed = (cause: unknown) =>
  Effect.logWarning("Could not roll back a T3 Connect preference", { cause });

const requireRelayUrl = relayUrlConfig.pipe(
  Effect.mapError(
    (cause) => new CloudLinkInternalError({ operation: "relay-url-unconfigured", cause }),
  ),
);

function bytesToString(bytes: Uint8Array): string {
  return new TextDecoder().decode(bytes);
}

function stringToBytes(value: string): Uint8Array {
  return new TextEncoder().encode(value);
}

export function consumeCloudReplayGuards(input: {
  readonly secrets: ServerSecretStore.ServerSecretStore["Service"];
  readonly names: ReadonlyArray<string>;
  readonly value: Uint8Array;
}) {
  return Effect.forEach(
    input.names,
    (name) =>
      input.secrets.create(name, input.value).pipe(
        Effect.as(true),
        Effect.catchIf(ServerSecretStore.isSecretStoreError, (error) =>
          ServerSecretStore.isSecretAlreadyExistsError(error)
            ? Effect.succeed(false)
            : Effect.fail(error),
        ),
      ),
    { concurrency: input.names.length },
  ).pipe(Effect.map((created) => created.every(Boolean)));
}

function validateCloudMintPublicKey(
  publicKey: string,
): Effect.Effect<void, CloudLinkRelayConfigInvalidError> {
  const invalid = new CloudLinkRelayConfigInvalidError({ field: "cloudMintPublicKey" });
  return Effect.try({
    try: () => NodeCrypto.createPublicKey(publicKey.replace(/\\n/g, "\n")),
    catch: () => invalid,
  }).pipe(
    Effect.flatMap((key) =>
      key.asymmetricKeyType === "ed25519" ? Effect.void : Effect.fail(invalid),
    ),
  );
}

function invalidRelayConfigField(
  payload: RelayMachineConfigRequest,
): CloudLinkRelayConfigInvalidError["field"] | null {
  if (!isSecureRelayUrl(payload.relayUrl)) return "relayUrl";
  if (payload.relayIssuer !== undefined && !isSecureRelayUrl(payload.relayIssuer)) {
    return "relayIssuer";
  }
  if (payload.machineCredential.trim().length === 0) return "machineCredential";
  if (payload.cloudUserId.trim().length === 0) return "cloudUserId";
  return null;
}

function validateRelayConfigPayload(
  payload: RelayMachineConfigRequest,
): Effect.Effect<void, CloudLinkRelayConfigInvalidError> {
  const field = invalidRelayConfigField(payload);
  return field === null
    ? Effect.void
    : Effect.fail(new CloudLinkRelayConfigInvalidError({ field }));
}

function normalizePemForSignedPayload(value: string): string {
  return value.trim();
}

const decodeCloudHealthProof = Schema.decodeUnknownEffect(RelayCloudMachineHealthProofPayload);
const decodeCloudMintProof = Schema.decodeUnknownEffect(RelayCloudMintCredentialProofPayload);

type ManagedTunnelRecoveryProofInput = {
  readonly machineId: RelayManagedEndpointRecoveryProofPayload["machineId"];
  readonly cloudUserId: string;
  readonly relayUrl: string;
} & (
  | {
      readonly action: "register";
      readonly tunnelId: string;
      readonly origin: RelayManagedEndpointOrigin;
    }
  | { readonly action: "recover"; readonly origin: RelayManagedEndpointOrigin }
);

export type CloudLinkError =
  | CloudLinkAccountMismatchError
  | CloudLinkAuthorizationMissingError
  | CloudLinkEndpointUnavailableError
  | CloudLinkInternalError
  | CloudLinkNotLinkedError
  | CloudLinkOriginInvalidError
  | CloudLinkProofRejectedError
  | CloudLinkProofReplayedError
  | CloudLinkRelayConfigInvalidError
  | CloudLinkTunnelSupersededError;

type CloudLinkBackgroundError =
  | CloudLinkEndpointUnavailableError
  | CloudLinkInternalError
  | CloudLinkOriginInvalidError
  | RelayRequestError
  | ServerSecretStore.SecretStoreError
  | Schema.SchemaError
  | PlatformError.PlatformError;

type ManagedTunnelRegistration =
  | { readonly status: "not_linked" | "superseded" | "revoked" }
  | { readonly status: "recovery_required"; readonly config: RelayManagedEndpointRuntimeConfig }
  | {
      readonly status: "ready";
      readonly endpointRuntimeStatus: ManagedEndpointRuntime.CloudManagedEndpointRuntimeStatus;
    };

export class CloudLink extends Context.Service<
  CloudLink,
  {
    /**
     * Signs the proof the relay asks for to link this machine. The request
     * must reach this server directly. Desktop bootstrap sessions may use the
     * WSL IP; all other sessions must use the loopback origin they name.
     */
    readonly linkProof: (
      request: RelayLinkProofRequest,
      httpRequest: HttpServerRequest.HttpServerRequest,
    ) => Effect.Effect<
      RelayMachineLinkProof,
      | CloudLinkOriginInvalidError
      | CloudLinkInternalError
      | MachineAuth.ServerAuthInternalError,
      MachineAuthenticatedPrincipal
    >;
    readonly applyRelayConfig: (
      payload: RelayMachineConfigRequest,
    ) => Effect.Effect<
      MachineCloudRelayConfigResult,
      | CloudLinkAccountMismatchError
      | CloudLinkEndpointUnavailableError
      | CloudLinkInternalError
      | CloudLinkOriginInvalidError
      | CloudLinkRelayConfigInvalidError
      | CloudLinkTunnelSupersededError
      | MachineAuth.ServerAuthInternalError
      | RelayRequestError
    >;
    readonly linkState: () => Effect.Effect<
      MachineCloudLinkStateResult,
      CloudLinkInternalError
    >;
    readonly unlink: () => Effect.Effect<MachineCloudRelayConfigResult, CloudLinkInternalError>;
    readonly updatePreferences: (
      input: MachineCloudPreferencesRequest,
    ) => Effect.Effect<
      MachineCloudLinkStateResult,
      CloudLinkNotLinkedError | CloudLinkInternalError
    >;
    readonly answerHealthRequest: (
      request: RelayCloudMachineHealthRequest,
    ) => Effect.Effect<
      RelayMachineHealthResponse,
      | CloudLinkProofRejectedError
      | CloudLinkProofReplayedError
      | CloudLinkInternalError
      | MachineAuth.ServerAuthInternalError
    >;
    readonly mintCredential: (
      request: RelayCloudMintCredentialRequest,
    ) => Effect.Effect<
      RelayMachineMintResponse,
      | CloudLinkProofRejectedError
      | CloudLinkProofReplayedError
      | CloudLinkInternalError
      | MachineAuth.ServerAuthInternalError
    >;
    readonly reconcileDesiredLink: (
      localOrigin: string,
    ) => Effect.Effect<
      CliDesiredLinkMode,
      | CloudLinkBackgroundError
      | CloudLinkAccountMismatchError
      | CloudLinkAuthorizationMissingError
      | CloudLinkRelayConfigInvalidError
      | MachineAuth.ServerAuthInternalError
      | CliTokenManager.CloudCliTokenManagerError
    >;
    readonly reconcileDesiredLinkIfStillDesired: (
      localOrigin: string,
    ) => Effect.Effect<
      CliDesiredLinkMode | null,
      | CloudLinkBackgroundError
      | CloudLinkAccountMismatchError
      | CloudLinkAuthorizationMissingError
      | CloudLinkRelayConfigInvalidError
      | MachineAuth.ServerAuthInternalError
      | CliTokenManager.CloudCliTokenManagerError
    >;
    readonly registerManagedTunnelRecovery: (
      localOrigin: string,
      options?: { readonly retryRuntimeFailures?: boolean },
    ) => Effect.Effect<ManagedTunnelRegistration, CloudLinkBackgroundError>;
    readonly recoverManagedTunnel: (
      localOrigin: string,
      expectedConfig?: RelayManagedEndpointRuntimeConfig,
      options?: { readonly retryRuntimeFailures?: boolean },
    ) => Effect.Effect<boolean, CloudLinkBackgroundError>;
    readonly startManagedTunnelIfOriginConfirmed: (
      localOrigin: string,
      options?: { readonly requireConfirmedOrigin?: boolean },
    ) => Effect.Effect<
      boolean,
      | CloudLinkEndpointUnavailableError
      | CloudLinkOriginInvalidError
      | ServerSecretStore.SecretStoreError
    >;
    readonly releaseManagedTunnelOnShutdown: () => Effect.Effect<
      boolean,
      | CloudLinkBackgroundError
      | CliTokenManager.CloudCliTokenManagerError
      | HttpClientError.HttpClientError
    >;
  }
>()("vekrel-engine/cloud/CloudLink") {}

const make = Effect.gen(function* () {
  const secrets = yield* ServerSecretStore.ServerSecretStore;
  const machine = yield* ServerMachine.ServerMachine;
  const endpointRuntime = yield* ManagedEndpointRuntime.CloudManagedEndpointRuntime;
  const reachability = yield* ConnectReachability.ConnectReachability;
  const machineAuth = yield* MachineAuth.MachineAuth;
  const cliTokenManager = yield* CliTokenManager.CloudCliTokenManager;
  const httpClient = yield* HttpClient.HttpClient;
  const httpServer = yield* HttpServer.HttpServer;
  const awarenessRelay = yield* AgentAwarenessRelay.AgentAwarenessRelay;
  const crypto = yield* Crypto.Crypto;
  const config = yield* ServerConfig.ServerConfig;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;

  const withSecrets = Effect.provideService(ServerSecretStore.ServerSecretStore, secrets);
  const withRuntimeFiles = <A, E>(
    effect: Effect.Effect<A, E, ServerConfig.ServerConfig | FileSystem.FileSystem | Path.Path>,
  ) =>
    effect.pipe(
      Effect.provideService(ServerConfig.ServerConfig, config),
      Effect.provideService(FileSystem.FileSystem, fileSystem),
      Effect.provideService(Path.Path, path),
    );

  const validateLinkedCloudUser = (
    cloudUserId: string,
  ): Effect.Effect<void, MachineAuth.ServerAuthInternalError | CloudLinkAccountMismatchError> =>
    secrets.get(CLOUD_LINKED_USER_ID).pipe(
      Effect.mapError(
        (cause) =>
          new MachineAuth.ServerAuthLinkedCloudAccountVerificationError({
            cause,
          }),
      ),
      Effect.flatMap((existing) => {
        if (Option.isNone(existing)) {
          return Effect.void;
        }
        const existingCloudUserId = bytesToString(existing.value);
        return existingCloudUserId === cloudUserId
          ? Effect.void
          : Effect.fail(new CloudLinkAccountMismatchError({}));
      }),
    );

  const readInstalledCloudUserId: Effect.Effect<string, MachineAuth.ServerAuthInternalError> =
    secrets.get(CLOUD_LINKED_USER_ID).pipe(
      Effect.mapError(
        (cause) =>
          new MachineAuth.ServerAuthLinkedCloudAccountReadError({
            cause,
          }),
      ),
      Effect.flatMap((bytes) =>
        Option.isSome(bytes)
          ? Effect.succeed(bytesToString(bytes.value))
          : Effect.fail(new MachineAuth.ServerAuthLinkedCloudAccountMissingError({})),
      ),
    );

  const makeCloudLinkProof = Effect.fn("machine.cloud.makeLinkProof")(function* (
    request: RelayLinkProofRequest,
    requestUrl: string,
    allowNonLoopbackRequestHost = false,
  ) {
    const keyPair = yield* getOrCreateMachineKeyPairFromSecretStore(secrets);
    if (
      !isSupportedLinkProviderKind(request) ||
      !isAllowedEndpointOrigin({
        origin: request.origin,
        requestUrl,
        allowNonLoopbackRequestHost,
      })
    ) {
      return yield* new CloudLinkOriginInvalidError({ origin: "endpoint" });
    }
    const now = yield* DateTime.now;
    const expiresAt = DateTime.add(now, { minutes: 5 });
    const nowSeconds = Math.floor(now.epochMilliseconds / 1_000);
    const descriptor = yield* machine.getDescriptor;
    const payload = {
      iss: `t3-env:${descriptor.machineId}`,
      aud: normalizeRelayIssuer(request.relayIssuer),
      sub: descriptor.machineId,
      jti: yield* crypto.randomUUIDv4(),
      iat: nowSeconds,
      exp: Math.floor(expiresAt.epochMilliseconds / 1_000),
      challenge: request.challenge,
      descriptor,
      machineId: descriptor.machineId,
      machinePublicKey: normalizePemForSignedPayload(keyPair.publicKey),
      endpoint: request.endpoint,
      origin: request.origin,
      scopes: linkProofScopes(request),
    } satisfies RelayMachineLinkProofPayload;
    return yield* signRelayJwt({
      privateKey: keyPair.privateKey,
      typ: RELAY_LINK_PROOF_TYP,
      payload,
    }).pipe(
      Effect.mapError(
        (cause) =>
          new MachineAuth.ServerAuthCloudLinkJwtSigningError({
            cause,
          }),
      ),
    );
  });

  const linkProof = Effect.fn("machine.cloud.linkProof")(
    function* (request: RelayLinkProofRequest, httpRequest: HttpServerRequest.HttpServerRequest) {
      const requestUrl = requestAbsoluteUrl(httpRequest);
      if (requestUrl === null || hasForwardedAuthorityHeaders(httpRequest)) {
        return yield* new CloudLinkOriginInvalidError({ origin: "endpoint" });
      }
      const principal = yield* MachineAuthenticatedPrincipal;
      // WSL desktop requests use the distro IP; only the server-issued bootstrap subject qualifies.
      return yield* makeCloudLinkProof(
        request,
        requestUrl,
        principal.subject === "desktop-bootstrap",
      );
    },
    Effect.catchIf(ServerSecretStore.isSecretStoreError, internalError("generate-link-proof")),
    Effect.catchTags({ PlatformError: internalError("generate-link-proof") }),
  );

  const activateManagedTunnel = Effect.fn("machine.cloud.activateManagedTunnel")(
    function* (input: {
      readonly config: RelayManagedEndpointRuntimeConfig;
      readonly configJson: string;
      readonly origin: RelayManagedEndpointOrigin;
    }) {
      return yield* endpointRuntime.withLinkStateLock(
        Effect.gen(function* () {
          const currentConfig = yield* secrets.get(CLOUD_ENDPOINT_RUNTIME_CONFIG);
          if (
            Option.isNone(currentConfig) ||
            bytesToString(currentConfig.value) !== input.configJson
          ) {
            return null;
          }
          const status = yield* endpointRuntime.applyConfig(input.config);
          if (status.status !== "running") {
            return yield* new CloudLinkEndpointUnavailableError({
              reason: "runtime-not-started",
              endpointRuntimeStatus: status,
            });
          }
          const marker = yield* encodeConfirmedOriginJson({
            config: input.config,
            origin: input.origin,
          });
          yield* secrets.set(CLOUD_ENDPOINT_CONFIRMED_ORIGIN, stringToBytes(marker));
          return status;
        }),
      );
    },
  );

  const activateManagedTunnelWithRetry = (
    input: {
      readonly config: RelayManagedEndpointRuntimeConfig;
      readonly configJson: string;
      readonly origin: RelayManagedEndpointOrigin;
    },
    retryRuntimeFailures: boolean,
  ) => {
    const activate = activateManagedTunnel(input);
    return retryRuntimeFailures
      ? activate.pipe(
          Effect.retry({
            while: (error) =>
              error._tag === "CloudLinkEndpointUnavailableError" &&
              ManagedEndpointRuntime.isRetryableManagedEndpointRuntimeStatus(
                error.endpointRuntimeStatus,
              ),
            schedule: Schedule.exponential("1 second").pipe(
              Schedule.modifyDelay(({ duration }) =>
                Effect.succeed(Duration.min(duration, Duration.seconds(30))),
              ),
              Schedule.jittered,
            ),
          }),
        )
      : activate;
  };

  const startManagedTunnelIfOriginConfirmed = Effect.fn(
    "machine.cloud.startManagedCloudTunnelIfOriginConfirmed",
  )(function* (localOrigin: string, options?: { readonly requireConfirmedOrigin?: boolean }) {
    const requireConfirmedOrigin = options?.requireConfirmedOrigin ?? true;
    const parsedOrigin = yield* Effect.try({
      try: () => parseManagedEndpointLocalOrigin(localOrigin),
      catch: () => new CloudLinkOriginInvalidError({ origin: "local" }),
    });
    return yield* endpointRuntime.withLinkStateLock(
      Effect.gen(function* () {
        const [runtimeBytes, markerBytes] = yield* Effect.all([
          secrets.get(CLOUD_ENDPOINT_RUNTIME_CONFIG),
          secrets.get(CLOUD_ENDPOINT_CONFIRMED_ORIGIN),
        ]);
        if (Option.isNone(runtimeBytes)) return false;
        const config = Option.getOrNull(decodeRuntimeConfig(bytesToString(runtimeBytes.value)));
        if (config === null || config.providerKind !== "cloudflare_tunnel") return false;
        // With the marker required, only a config the relay already confirmed on
        // this port may start. Without it, startup is falling back after the
        // relay stayed unreachable: an unconfirmed origin may send traffic to a
        // stale port, but that beats no remote access at all.
        if (requireConfirmedOrigin) {
          if (Option.isNone(markerBytes)) return false;
          const marker = Option.getOrNull(decodeConfirmedOrigin(bytesToString(markerBytes.value)));
          if (
            marker === null ||
            !managedEndpointRuntimeConfigsMatch(marker.config, config) ||
            marker.origin.localHttpHost !== parsedOrigin.origin.localHttpHost ||
            marker.origin.localHttpPort !== parsedOrigin.origin.localHttpPort
          ) {
            return false;
          }
        }
        const status = yield* endpointRuntime.applyConfig(config);
        if (status.status !== "running") {
          return yield* new CloudLinkEndpointUnavailableError({
            reason: "runtime-not-started",
            endpointRuntimeStatus: status,
          });
        }
        return true;
      }),
    );
  });

  const applyCloudRelayConfig = Effect.fn("machine.cloud.applyRelayConfig")(function* (
    payload: RelayMachineConfigRequest,
    options?: {
      readonly lockHeld?: boolean;
      readonly confirmedOrigin?: RelayManagedEndpointOrigin;
    },
  ) {
    const apply = Effect.gen(function* () {
      yield* validateRelayConfigPayload(payload);
      yield* validateLinkedCloudUser(payload.cloudUserId);
      yield* validateCloudMintPublicKey(payload.cloudMintPublicKey);
      // Reject unsupported runtimes before touching the connector so a bad
      // payload cannot stop a healthy tunnel on its way to a 503.
      if (
        payload.endpointRuntime !== null &&
        payload.endpointRuntime.providerKind !== "cloudflare_tunnel"
      ) {
        return yield* new CloudLinkEndpointUnavailableError({
          reason: "runtime-not-started",
          endpointRuntimeStatus: {
            status: "unsupported",
            providerKind: payload.endpointRuntime.providerKind,
          },
        });
      }
      const endpointRuntimeJson = payload.endpointRuntime
        ? yield* encodeEndpointRuntimeConfigJson(payload.endpointRuntime)
        : null;
      yield* endpointRuntime.applyConfig(null);
      yield* secrets.remove(CLOUD_ENDPOINT_CONFIRMED_ORIGIN);
      // The stored runtime config is what reports the link as managed and what
      // startup respawns, so drop it first and store it last. A write that
      // fails part-way then leaves a link that reads as not managed, and the
      // next relink repairs it instead of trusting a stale tunnel config.
      yield* secrets.remove(CLOUD_ENDPOINT_RUNTIME_CONFIG);

      yield* secrets.set(RELAY_URL_SECRET, stringToBytes(payload.relayUrl));
      yield* secrets.set(
        RELAY_ISSUER_SECRET,
        stringToBytes(payload.relayIssuer ?? payload.relayUrl),
      );
      yield* secrets.set(CLOUD_LINKED_USER_ID, stringToBytes(payload.cloudUserId));
      yield* secrets.set(
        RELAY_MACHINE_CREDENTIAL_SECRET,
        stringToBytes(payload.machineCredential),
      );
      yield* secrets.remove(CLOUD_LINK_REVOKED);
      yield* secrets.set(CLOUD_MINT_PUBLIC_KEY, stringToBytes(payload.cloudMintPublicKey));
      yield* awarenessRelay.requestCatchUp();
      if (endpointRuntimeJson !== null) {
        yield* secrets.set(CLOUD_ENDPOINT_RUNTIME_CONFIG, stringToBytes(endpointRuntimeJson));
      }
      if (payload.endpointRuntime === null || options?.confirmedOrigin === undefined) {
        return {
          ok: true,
          endpointRuntimeStatus: { status: "disabled" },
        } satisfies MachineCloudRelayConfigResult;
      }
      const endpointRuntimeStatus = yield* endpointRuntime.applyConfig(payload.endpointRuntime);
      if (endpointRuntimeStatus.status !== "running") {
        return yield* new CloudLinkEndpointUnavailableError({
          reason: "runtime-not-started",
          endpointRuntimeStatus,
        });
      }
      const marker = yield* encodeConfirmedOriginJson({
        config: payload.endpointRuntime,
        origin: options.confirmedOrigin,
      });
      yield* secrets.set(CLOUD_ENDPOINT_CONFIRMED_ORIGIN, stringToBytes(marker));
      return { ok: true, endpointRuntimeStatus } satisfies MachineCloudRelayConfigResult;
    });
    return yield* options?.lockHeld ? apply : endpointRuntime.withLinkStateLock(apply);
  });

  const relayClientRequest = <A>(input: {
    readonly url: string;
    readonly token: string;
    readonly payload: unknown;
    readonly schema: Schema.Decoder<A>;
    readonly timeout?: Duration.Input;
  }) =>
    HttpClientRequest.post(input.url).pipe(
      HttpClientRequest.bearerToken(input.token),
      HttpClientRequest.bodyJson(input.payload),
      Effect.flatMap(httpClient.execute),
      Effect.flatMap(filterRelayResponse),
      Effect.flatMap(HttpClientResponse.schemaBodyJson(input.schema)),
      Effect.timeout(input.timeout ?? "10 seconds"),
      Effect.mapError(relayRequestError),
      withRelayClientTracing,
    );

  const reconcileDesiredCloudLinkWith = Effect.fn("machine.cloud.reconcileDesiredLinkWith")(
    function* (localOrigin: string) {
      const parsedOrigin = yield* Effect.try({
        try: () => parseManagedEndpointLocalOrigin(localOrigin),
        catch: () => new CloudLinkOriginInvalidError({ origin: "local" }),
      });
      const token = yield* cliTokenManager.getExisting.pipe(
        Effect.flatMap(
          Option.match({
            onNone: () => Effect.fail(new CloudLinkAuthorizationMissingError({})),
            onSome: Effect.succeed,
          }),
        ),
      );
      const mode = yield* readCliDesiredLinkMode.pipe(withSecrets);
      const managedTunnelsEnabled = mode !== "publish_only";
      const relayUrl = yield* requireRelayUrl;
      const challenge = yield* relayClientRequest({
        url: `${relayUrl}/v1/client/machine-link-challenges`,
        token: token.accessToken,
        payload: {
          notificationsEnabled: true,
          liveActivitiesEnabled: true,
          managedTunnelsEnabled,
        },
        schema: RelayMachineLinkChallengeResponse,
      });
      const proof = yield* makeCloudLinkProof(
        {
          challenge: challenge.challenge,
          relayIssuer: relayUrl,
          endpoint: {
            httpBaseUrl: parsedOrigin.httpBaseUrl,
            wsBaseUrl: parsedOrigin.wsBaseUrl,
            providerKind: managedTunnelsEnabled ? "cloudflare_tunnel" : "manual",
          },
          origin: parsedOrigin.origin,
        },
        parsedOrigin.httpBaseUrl,
      );
      const link = yield* relayClientRequest({
        url: `${relayUrl}/v1/client/machine-links`,
        token: token.accessToken,
        payload: {
          proof,
          notificationsEnabled: true,
          liveActivitiesEnabled: true,
          managedTunnelsEnabled,
        },
        schema: RelayMachineLinkResponse,
        timeout: MANAGED_ENDPOINT_PROVISION_REQUEST_TIMEOUT,
      });
      yield* setCliDesiredCloudLink(true, mode).pipe(withSecrets);
      yield* applyCloudRelayConfig(
        {
          relayUrl,
          relayIssuer: link.relayIssuer,
          cloudUserId: link.cloudUserId,
          machineCredential: link.machineCredential,
          cloudMintPublicKey: link.cloudMintPublicKey,
          endpointRuntime: link.endpointRuntime,
        },
        {
          lockHeld: true,
          confirmedOrigin: parsedOrigin.origin,
        },
      );
      return mode;
    },
    Effect.catchIf(ServerSecretStore.isSecretStoreError, internalError("persist-desired-link")),
  );

  const reconcileDesiredLink = Effect.fn("machine.cloud.reconcileDesiredLink")(function* (
    localOrigin: string,
  ) {
    return yield* endpointRuntime.withLinkStateLock(reconcileDesiredCloudLinkWith(localOrigin));
  });

  const reconcileDesiredLinkIfStillDesired = Effect.fn(
    "machine.cloud.reconcileDesiredLinkIfStillDesired",
  )(function* (localOrigin: string) {
    return yield* endpointRuntime.withLinkStateLock(
      Effect.gen(function* () {
        if (!(yield* readCliDesiredCloudLink.pipe(withSecrets))) {
          return null;
        }
        return yield* reconcileDesiredCloudLinkWith(localOrigin);
      }),
    );
  });

  const makeManagedTunnelRecoveryProof = Effect.fn(
    "machine.cloud.makeManagedTunnelRecoveryProof",
  )(function* (input: ManagedTunnelRecoveryProofInput) {
    const keyPair = yield* getOrCreateMachineKeyPairFromSecretStore(secrets);
    const configuredIssuer = yield* secrets.get(RELAY_ISSUER_SECRET);
    const now = yield* DateTime.now;
    const issuedAt = Math.floor(now.epochMilliseconds / 1_000);
    const claims = {
      iss: `t3-env:${input.machineId}`,
      aud: normalizeRelayIssuer(
        Option.isSome(configuredIssuer) ? bytesToString(configuredIssuer.value) : input.relayUrl,
      ),
      sub: input.machineId,
      jti: yield* crypto.randomUUIDv4(),
      iat: issuedAt,
      exp: issuedAt + 60,
      machineId: input.machineId,
      cloudUserId: input.cloudUserId,
    };
    const payload =
      input.action === "register"
        ? {
            ...claims,
            action: "register" as const,
            tunnelId: input.tunnelId,
            origin: input.origin,
          }
        : { ...claims, action: "recover" as const, origin: input.origin };

    return yield* signRelayJwt({
      privateKey: keyPair.privateKey,
      typ: RELAY_MANAGED_TUNNEL_RECOVERY_TYP,
      payload,
    }).pipe(
      Effect.mapError(
        (cause) => new CloudLinkInternalError({ operation: "sign-recovery-proof", cause }),
      ),
    );
  });

  /**
   * Nothing stored is removed: the relay also answers 401 for a skewed clock,
   * and the next accepted registration clears the mark. Returns false, without
   * marking, when a relink replaced the credential while the rejected request
   * was in flight.
   */
  const markLinkRevoked = (credential: string, cause: RelayRequestError) =>
    endpointRuntime.withLinkStateLock(
      Effect.gen(function* () {
        const current = yield* secrets.get(RELAY_MACHINE_CREDENTIAL_SECRET);
        if (Option.isNone(current) || bytesToString(current.value) !== credential) {
          return false;
        }
        yield* Effect.logWarning(
          "T3 Connect no longer accepts this machine's link. Link it again to restore remote access.",
          { cause },
        );
        yield* secrets.set(CLOUD_LINK_REVOKED, stringToBytes("true"));
        return true;
      }),
    );

  const registerManagedTunnelRecovery = Effect.fn(
    "machine.cloud.registerManagedCloudTunnelRecovery",
  )(function* (localOrigin: string, options?: { readonly retryRuntimeFailures?: boolean }) {
    const [runtimeConfig, relayUrl, cloudUserId, machineCredential] = yield* Effect.all([
      secrets.get(CLOUD_ENDPOINT_RUNTIME_CONFIG),
      secrets.get(RELAY_URL_SECRET),
      secrets.get(CLOUD_LINKED_USER_ID),
      secrets.get(RELAY_MACHINE_CREDENTIAL_SECRET),
    ]);
    if (
      Option.isNone(runtimeConfig) ||
      Option.isNone(relayUrl) ||
      Option.isNone(cloudUserId) ||
      Option.isNone(machineCredential)
    ) {
      return { status: "not_linked" as const };
    }

    const config = Option.getOrNull(decodeRuntimeConfig(bytesToString(runtimeConfig.value)));
    if (config?.providerKind !== "cloudflare_tunnel") {
      return { status: "not_linked" as const };
    }

    const parsedOrigin = yield* Effect.try({
      try: () => parseManagedEndpointLocalOrigin(localOrigin),
      catch: () => new CloudLinkOriginInvalidError({ origin: "local" }),
    });
    if (config.tunnelId === undefined) {
      return { status: "recovery_required" as const, config };
    }
    const origin = parsedOrigin.origin;
    const machineId = yield* machine.getMachineId;
    const relayUrlValue = bytesToString(relayUrl.value);
    const cloudUserIdValue = bytesToString(cloudUserId.value);
    const proof = yield* makeManagedTunnelRecoveryProof({
      action: "register",
      machineId,
      cloudUserId: cloudUserIdValue,
      relayUrl: relayUrlValue,
      tunnelId: config.tunnelId,
      origin,
    });
    const credential = bytesToString(machineCredential.value);
    const registered = yield* relayClientRequest({
      url: `${relayUrlValue}/v1/machines/${encodeURIComponent(machineId)}/tunnel/recovery`,
      token: credential,
      payload: {
        cloudUserId: cloudUserIdValue,
        tunnelId: config.tunnelId,
        origin,
        proof,
      },
      schema: RelayManagedEndpointRecoveryRegistrationResponse,
    }).pipe(
      Effect.catchTags({
        RelayRequestError: (error) =>
          error.rejection === "unauthorized"
            ? markLinkRevoked(credential, error).pipe(
                Effect.map((marked) => (marked ? ("revoked" as const) : ("superseded" as const))),
              )
            : Effect.fail(error),
      }),
    );
    if (registered === "revoked" || registered === "superseded") {
      return { status: registered };
    }
    yield* secrets.remove(CLOUD_LINK_REVOKED);
    if (registered.status === "recovery_required") {
      return { status: registered.status, config };
    }
    const endpointRuntimeStatus = yield* activateManagedTunnelWithRetry(
      {
        config,
        configJson: bytesToString(runtimeConfig.value),
        origin,
      },
      options?.retryRuntimeFailures === true,
    );
    return endpointRuntimeStatus === null
      ? { status: "superseded" as const }
      : { status: "ready" as const, endpointRuntimeStatus };
  });

  const recoverManagedTunnel = Effect.fn("machine.cloud.recoverManagedCloudTunnel")(function* (
    localOrigin: string,
    expectedConfig?: RelayManagedEndpointRuntimeConfig,
    options?: { readonly retryRuntimeFailures?: boolean },
  ) {
    const [runtimeConfig, relayUrl, cloudUserId, machineCredential] = yield* Effect.all([
      secrets.get(CLOUD_ENDPOINT_RUNTIME_CONFIG),
      secrets.get(RELAY_URL_SECRET),
      secrets.get(CLOUD_LINKED_USER_ID),
      secrets.get(RELAY_MACHINE_CREDENTIAL_SECRET),
    ]);
    if (
      Option.isNone(runtimeConfig) ||
      Option.isNone(relayUrl) ||
      Option.isNone(cloudUserId) ||
      Option.isNone(machineCredential)
    ) {
      return false;
    }
    if (expectedConfig !== undefined) {
      const current = Option.getOrNull(decodeRuntimeConfig(bytesToString(runtimeConfig.value)));
      if (
        current === null ||
        current.providerKind !== expectedConfig.providerKind ||
        current.connectorToken !== expectedConfig.connectorToken ||
        current.tunnelId !== expectedConfig.tunnelId ||
        current.tunnelName !== expectedConfig.tunnelName
      ) {
        return false;
      }
    }

    const parsedOrigin = yield* Effect.try({
      try: () => parseManagedEndpointLocalOrigin(localOrigin),
      catch: () => new CloudLinkOriginInvalidError({ origin: "local" }),
    });

    const machineId = yield* machine.getMachineId;
    const relayUrlValue = bytesToString(relayUrl.value);
    const cloudUserIdValue = bytesToString(cloudUserId.value);
    const origin = parsedOrigin.origin;
    const proof = yield* makeManagedTunnelRecoveryProof({
      action: "recover",
      machineId,
      cloudUserId: cloudUserIdValue,
      relayUrl: relayUrlValue,
      origin,
    });
    const credential = bytesToString(machineCredential.value);
    const recovered = yield* relayClientRequest({
      url: `${relayUrlValue}/v1/machines/${encodeURIComponent(machineId)}/tunnel`,
      token: credential,
      payload: {
        cloudUserId: cloudUserIdValue,
        origin,
        proof,
      },
      schema: RelayManagedEndpointRecoveryResponse,
      timeout: MANAGED_ENDPOINT_PROVISION_REQUEST_TIMEOUT,
    }).pipe(
      Effect.tapError((error) =>
        error.rejection === "unauthorized" ? markLinkRevoked(credential, error) : Effect.void,
      ),
    );
    if (recovered.endpointRuntime.providerKind !== "cloudflare_tunnel") {
      return yield* new CloudLinkInternalError({ operation: "unsupported-recovered-tunnel" });
    }

    const encoded = yield* encodeEndpointRuntimeConfigJson(recovered.endpointRuntime).pipe(
      Effect.mapError(
        (cause) => new CloudLinkInternalError({ operation: "persist-recovered-tunnel", cause }),
      ),
    );
    const stored = yield* endpointRuntime.withLinkStateLock(
      Effect.gen(function* () {
        const currentConfig = yield* secrets.get(CLOUD_ENDPOINT_RUNTIME_CONFIG);
        if (
          Option.isNone(currentConfig) ||
          bytesToString(currentConfig.value) !== bytesToString(runtimeConfig.value)
        ) {
          return false;
        }
        yield* secrets.set(CLOUD_ENDPOINT_RUNTIME_CONFIG, stringToBytes(encoded));
        yield* secrets.remove(CLOUD_ENDPOINT_CONFIRMED_ORIGIN);
        yield* secrets.remove(CLOUD_LINK_REVOKED);
        return true;
      }),
    );
    if (!stored) return false;
    const status = yield* activateManagedTunnelWithRetry(
      {
        config: recovered.endpointRuntime,
        configJson: encoded,
        origin,
      },
      options?.retryRuntimeFailures === true,
    );
    return status !== null;
  });

  const applyRelayConfig = Effect.fn("machine.cloud.relayConfig")(
    function* (payload: RelayMachineConfigRequest) {
      const result = yield* applyCloudRelayConfig(payload);
      if (payload.endpointRuntime?.providerKind === "cloudflare_tunnel") {
        const address = httpServer.address;
        if (typeof address === "string" || !("port" in address)) {
          return yield* new CloudLinkInternalError({ operation: "resolve-server-origin" });
        }
        const registration = yield* registerManagedTunnelRecovery(
          `http://${loopbackUrlHostForAddress(address)}:${address.port}`,
        ).pipe(
          Effect.retry({
            times: 2,
            while: (error) =>
              shouldRetryCloudLink(error) && error._tag !== "CloudLinkEndpointUnavailableError",
          }),
        );
        if (registration.status === "superseded") {
          return yield* new CloudLinkTunnelSupersededError({});
        }
        if (registration.status === "recovery_required") {
          yield* endpointRuntime.requestRecovery(registration.config);
        }
        if (registration.status !== "ready") {
          return yield* new CloudLinkEndpointUnavailableError({
            reason: "origin-unconfirmed",
            endpointRuntimeStatus: { status: "disabled" },
          });
        }
        return {
          ok: true,
          endpointRuntimeStatus: registration.endpointRuntimeStatus,
        } satisfies MachineCloudRelayConfigResult;
      }
      return result;
    },
    Effect.catchIf(ServerSecretStore.isSecretStoreError, internalError("persist-relay-config")),
    Effect.catchTags({
      SchemaError: internalError("persist-relay-config"),
      PlatformError: internalError("register-endpoint-origin"),
    }),
  );

  // Cloudflare bills per provisioned tunnel, so a machine that goes offline
  // must not leave its tunnel behind.
  const releaseManagedTunnelOnShutdown = Effect.fn(
    "machine.cloud.releaseManagedTunnelOnShutdown",
  )(function* () {
    const runtimeConfig = yield* secrets.get(CLOUD_ENDPOINT_RUNTIME_CONFIG);
    if (Option.isNone(runtimeConfig)) {
      return false;
    }
    // Only CLI-desired managed links release eagerly because this request uses
    // CLI authorization. Web/mobile links register startup recovery with their
    // machine credential, and the relay reaper removes them after they are
    // down for the configured grace period. Unlink still deletes either kind.
    if (
      !(yield* readCliDesiredCloudLink.pipe(withSecrets)) ||
      (yield* readCliDesiredLinkMode.pipe(withSecrets)) !== "managed"
    ) {
      return false;
    }
    // A shutdown that hands off to a pending update is not the machine going
    // offline: the service launcher or the desktop app immediately brings a
    // server back (the new version, or the old one after a rollback). Deleting
    // the tunnel here forces that server to provision a replacement UUID, and the
    // public hostname's route to the new tunnel takes 1-2 minutes to propagate —
    // the dominant cost of an update restart.
    if (
      (yield* withRuntimeFiles(desktopUpdateRestartPending)) ||
      (yield* withRuntimeFiles(pendingUpdateHandoffExists))
    ) {
      yield* Effect.logInfo("Keeping the managed tunnel across the update restart");
      return false;
    }
    const token = yield* cliTokenManager.getExisting;
    if (Option.isNone(token)) {
      return false;
    }
    // The link belongs to the relay it was installed against, so target the
    // persisted URL: VEKREL_RELAY_URL may have changed since the link was made.
    const relayUrl = yield* secrets.get(RELAY_URL_SECRET);
    if (Option.isNone(relayUrl)) {
      return false;
    }
    const machineId = yield* machine.getMachineId;
    yield* endpointRuntime.applyConfig(null);
    const response = yield* HttpClientRequest.delete(
      `${bytesToString(relayUrl.value)}/v1/client/machine-links/${encodeURIComponent(machineId)}/tunnel`,
    ).pipe(
      HttpClientRequest.bearerToken(token.value.accessToken),
      httpClient.execute,
      Effect.flatMap(filterRelayResponse),
      Effect.flatMap(HttpClientResponse.schemaBodyJson(RelayOkResponse)),
      withRelayClientTracing,
    );
    // ok:false means the relay skipped deletion because a concurrent provision
    // owns the recorded tunnel now — leave the stored config alone.
    if (!response.ok) {
      return false;
    }
    // The connector token died with the tunnel. Drop the stored config so the
    // next start waits for the link reconcile instead of respawning the relay
    // client with a dead token. Only dropped while it is still the config this
    // shutdown released — a fast restart may already have reconciled and stored
    // a fresh config for its replacement tunnel, and that one must survive this
    // finalizer.
    const storedConfig = yield* secrets.get(CLOUD_ENDPOINT_RUNTIME_CONFIG);
    if (
      Option.isSome(storedConfig) &&
      bytesToString(storedConfig.value) === bytesToString(runtimeConfig.value)
    ) {
      yield* secrets.remove(CLOUD_ENDPOINT_RUNTIME_CONFIG);
      yield* secrets.remove(CLOUD_ENDPOINT_CONFIRMED_ORIGIN);
    }
    return true;
  });

  const readCloudLinkState = Effect.fn("machine.cloud.readLinkState")(function* () {
    const [
      cloudUserId,
      relayUrl,
      relayIssuer,
      endpointRuntimeConfig,
      publishAgentActivity,
      holdWebhooks,
      revoked,
      latestReachability,
    ] = yield* Effect.all(
      [
        secrets.get(CLOUD_LINKED_USER_ID),
        secrets.get(RELAY_URL_SECRET),
        secrets.get(RELAY_ISSUER_SECRET),
        secrets.get(CLOUD_ENDPOINT_RUNTIME_CONFIG),
        secrets.get(PUBLISH_AGENT_ACTIVITY_SECRET),
        secrets.get(HOLD_WEBHOOKS_WHILE_OFFLINE_SECRET),
        secrets.get(CLOUD_LINK_REVOKED),
        reachability.latest,
      ],
      { concurrency: 8 },
    );
    return {
      linked: Option.isSome(cloudUserId),
      linkRevoked: Option.isSome(cloudUserId) && Option.isSome(revoked),
      cloudUserId: Option.isSome(cloudUserId) ? bytesToString(cloudUserId.value) : null,
      relayUrl: Option.isSome(relayUrl) ? bytesToString(relayUrl.value) : null,
      relayIssuer: Option.isSome(relayIssuer) ? bytesToString(relayIssuer.value) : null,
      managedTunnelActive: Option.isSome(endpointRuntimeConfig),
      publishAgentActivity: Option.isSome(publishAgentActivity)
        ? bytesToString(publishAgentActivity.value) === "true"
        : false,
      holdWebhooksWhileOffline:
        Option.isSome(holdWebhooks) && bytesToString(holdWebhooks.value) === "true",
      ...(Option.isSome(latestReachability) ? { reachability: latestReachability.value } : {}),
    } satisfies MachineCloudLinkStateResult;
  });

  const linkState = Effect.fn("machine.cloud.linkState")(
    function* () {
      return yield* readCloudLinkState();
    },
    Effect.catchIf(ServerSecretStore.isSecretStoreError, internalError("read-relay-config")),
  );

  const unlink = Effect.fn("machine.cloud.unlink")(
    function* () {
      return yield* endpointRuntime.withLinkStateLock(
        Effect.gen(function* () {
          const endpointRuntimeStatus = yield* endpointRuntime.applyConfig(null);
          yield* Effect.all(
            [
              secrets.remove(CLOUD_LINKED_USER_ID),
              secrets.remove(RELAY_URL_SECRET),
              secrets.remove(RELAY_ISSUER_SECRET),
              secrets.remove(RELAY_MACHINE_CREDENTIAL_SECRET),
              secrets.remove(CLOUD_MINT_PUBLIC_KEY),
              secrets.remove(CLOUD_ENDPOINT_RUNTIME_CONFIG),
              secrets.remove(CLOUD_ENDPOINT_CONFIRMED_ORIGIN),
              secrets.remove(PUBLISH_AGENT_ACTIVITY_SECRET),
              secrets.remove(HOLD_WEBHOOKS_WHILE_OFFLINE_SECRET),
              secrets.remove(CLOUD_LINK_REVOKED),
            ],
            { concurrency: 10 },
          );
          yield* setCliDesiredCloudLink(false).pipe(withSecrets);
          return { ok: true, endpointRuntimeStatus } satisfies MachineCloudRelayConfigResult;
        }),
      );
    },
    Effect.catchIf(ServerSecretStore.isSecretStoreError, internalError("remove-relay-config")),
  );

  const pushHoldWebhooksWhileOffline = Effect.fn("CloudPreferences.pushHoldWebhooksWhileOffline")(
    function* (holdWebhooksWhileOffline: boolean) {
      const connection = yield* readRelayConnection.pipe(withSecrets);
      if (connection === null) {
        return yield* new CloudLinkNotLinkedError({});
      }
      const machineId = yield* machine.getMachineId;
      const client = yield* makeRelayMachineClient(connection);
      yield* client.server
        .updateLinkPreferences({
          params: { machineId },
          payload: { holdWebhooksWhileOffline },
        })
        .pipe(Effect.timeout("10 seconds"), Effect.catch(internalError("update-webhook-settings")));
    },
  );

  const savePreference = (name: string, value: boolean) =>
    secrets
      .set(name, stringToBytes(String(value)))
      .pipe(Effect.catch(internalError("persist-preferences")));

  const preferencesLock = yield* Semaphore.make(1);

  const savePreferences = Effect.fn("CloudPreferences.update")(
    function* (input: MachineCloudPreferencesRequest) {
      // Both current values are read up front; a failed read stops here, before
      // anything changes, because a guessed value would be the rollback target.
      const readCurrent = (name: string) =>
        secrets.get(name).pipe(Effect.catch(internalError("read-preferences")));
      const previousActivity = yield* readCurrent(PUBLISH_AGENT_ACTIVITY_SECRET);
      const previousHold = yield* readCurrent(HOLD_WEBHOOKS_WHILE_OFFLINE_SECRET);
      yield* savePreference(PUBLISH_AGENT_ACTIVITY_SECRET, input.publishAgentActivity);
      if (input.holdWebhooksWhileOffline !== undefined) {
        const next = input.holdWebhooksWhileOffline;
        const previous = Option.match(previousHold, {
          onNone: () => false,
          onSome: (bytes) => bytesToString(bytes) === "true",
        });
        yield* pushHoldWebhooksWhileOffline(next).pipe(
          Effect.andThen(
            savePreference(HOLD_WEBHOOKS_WHILE_OFFLINE_SECRET, next).pipe(
              Effect.tapError(() =>
                previous === next
                  ? Effect.void
                  : pushHoldWebhooksWhileOffline(previous).pipe(Effect.catch(rollbackFailed)),
              ),
            ),
          ),
          Effect.tapError(() =>
            Option.match(previousActivity, {
              onNone: () => secrets.remove(PUBLISH_AGENT_ACTIVITY_SECRET),
              onSome: (bytes) => secrets.set(PUBLISH_AGENT_ACTIVITY_SECRET, bytes),
            }).pipe(Effect.catch(rollbackFailed)),
          ),
        );
      }
      yield* awarenessRelay.requestCatchUp();
    },
    // A client that disconnects mid-update must not stop it between a save and
    // its rollback, so an update always finishes or undoes itself.
    Effect.uninterruptible,
    preferencesLock.withPermits(1),
  );

  const updatePreferences = Effect.fn("machine.cloud.preferences")(
    function* (input: MachineCloudPreferencesRequest) {
      yield* savePreferences(input);
      return yield* readCloudLinkState();
    },
    Effect.catchIf(ServerSecretStore.isSecretStoreError, internalError("read-preferences")),
  );

  const answerHealthRequest = Effect.fn("machine.cloud.health")(
    function* (request: RelayCloudMachineHealthRequest) {
      const cloudMintPublicKey = yield* secrets
        .get(CLOUD_MINT_PUBLIC_KEY)
        .pipe(
          Effect.flatMap((bytes) =>
            Option.isSome(bytes)
              ? Effect.succeed(bytesToString(bytes.value))
              : Effect.fail(new MachineAuth.ServerAuthCloudMintPublicKeyMissingError({})),
          ),
        );
      const relayIssuer = yield* secrets
        .get(RELAY_ISSUER_SECRET)
        .pipe(
          Effect.flatMap((bytes) =>
            Option.isSome(bytes)
              ? Effect.succeed(bytesToString(bytes.value))
              : secrets
                  .get(RELAY_URL_SECRET)
                  .pipe(
                    Effect.flatMap((fallbackBytes) =>
                      Option.isSome(fallbackBytes)
                        ? Effect.succeed(bytesToString(fallbackBytes.value))
                        : Effect.fail(
                            new MachineAuth.ServerAuthCloudRelayIssuerMissingError({}),
                          ),
                    ),
                  ),
          ),
        );
      const machineId = yield* machine.getMachineId;
      const linkedCloudUserId = yield* readInstalledCloudUserId;
      const now = yield* DateTime.now;
      const nowSeconds = Math.floor(now.epochMilliseconds / 1_000);
      const proofOption = yield* verifyRelayJwt({
        publicKey: cloudMintPublicKey,
        token: request.proof,
        typ: RELAY_HEALTH_REQUEST_TYP,
        issuer: normalizeRelayIssuer(relayIssuer),
        audience: `t3-env:${machineId}`,
        nowEpochSeconds: nowSeconds,
      }).pipe(Effect.flatMap(decodeCloudHealthProof), Effect.option);
      if (
        Option.isNone(proofOption) ||
        proofOption.value.machineId !== machineId ||
        proofOption.value.sub !== linkedCloudUserId ||
        !hasBoundedCloudProofLifetime({ ...proofOption.value, nowSeconds }) ||
        !hasExactScope({ scopes: proofOption.value.scope, expected: "machine:status" })
      ) {
        return yield* new CloudLinkProofRejectedError({ request: "health" });
      }
      const proof = proofOption.value;

      const jtiSecretName = `${CLOUD_HEALTH_JTI_PREFIX}${proof.jti}`;
      const nonceSecretName = `${CLOUD_HEALTH_NONCE_PREFIX}${proof.nonce}`;
      const consumedReplayGuards = yield* consumeCloudReplayGuards({
        secrets,
        names: [jtiSecretName, nonceSecretName],
        value: stringToBytes(DateTime.formatIso(now)),
      });
      if (!consumedReplayGuards) {
        return yield* new CloudLinkProofReplayedError({ request: "health" });
      }

      const keyPair = yield* getOrCreateMachineKeyPairFromSecretStore(secrets);
      const descriptor = yield* machine.getDescriptor;
      const responseExpiresAt = DateTime.add(now, { minutes: 5 });
      const responsePayload = {
        iss: `t3-env:${machineId}`,
        aud: normalizeRelayIssuer(relayIssuer),
        sub: machineId,
        jti: yield* crypto.randomUUIDv4(),
        iat: nowSeconds,
        exp: Math.floor(responseExpiresAt.epochMilliseconds / 1_000),
        machineId,
        requestNonce: proof.nonce,
        status: "online",
        descriptor,
        checkedAt: DateTime.formatIso(now),
      } satisfies RelayMachineHealthResponseProofPayload;
      const responseProof = yield* signRelayJwt({
        privateKey: keyPair.privateKey,
        typ: RELAY_HEALTH_RESPONSE_TYP,
        payload: responsePayload,
      }).pipe(
        Effect.mapError(
          (cause) =>
            new MachineAuth.ServerAuthCloudHealthJwtSigningError({
              cause,
            }),
        ),
      );
      return {
        machineId,
        status: "online",
        descriptor,
        checkedAt: responsePayload.checkedAt,
        proof: responseProof,
      } satisfies RelayMachineHealthResponse;
    },
    Effect.catchIf(ServerSecretStore.isSecretStoreError, internalError("answer-health")),
    Effect.catchTags({ PlatformError: internalError("answer-health") }),
  );

  const mintCredential = Effect.fn("machine.cloud.mintCredential")(
    function* (request: RelayCloudMintCredentialRequest) {
      const cloudMintPublicKey = yield* secrets
        .get(CLOUD_MINT_PUBLIC_KEY)
        .pipe(
          Effect.flatMap((bytes) =>
            Option.isSome(bytes)
              ? Effect.succeed(bytesToString(bytes.value))
              : Effect.fail(new MachineAuth.ServerAuthCloudMintPublicKeyMissingError({})),
          ),
        );
      const relayIssuer = yield* secrets
        .get(RELAY_ISSUER_SECRET)
        .pipe(
          Effect.flatMap((bytes) =>
            Option.isSome(bytes)
              ? Effect.succeed(bytesToString(bytes.value))
              : secrets
                  .get(RELAY_URL_SECRET)
                  .pipe(
                    Effect.flatMap((fallbackBytes) =>
                      Option.isSome(fallbackBytes)
                        ? Effect.succeed(bytesToString(fallbackBytes.value))
                        : Effect.fail(
                            new MachineAuth.ServerAuthCloudRelayIssuerMissingError({}),
                          ),
                    ),
                  ),
          ),
        );
      const machineId = yield* machine.getMachineId;
      const linkedCloudUserId = yield* readInstalledCloudUserId;
      const now = yield* DateTime.now;
      const nowSeconds = Math.floor(now.epochMilliseconds / 1_000);
      const proofOption = yield* verifyRelayJwt({
        publicKey: cloudMintPublicKey,
        token: request.proof,
        typ: RELAY_MINT_REQUEST_TYP,
        issuer: normalizeRelayIssuer(relayIssuer),
        audience: `t3-env:${machineId}`,
        nowEpochSeconds: nowSeconds,
      }).pipe(Effect.flatMap(decodeCloudMintProof), Effect.option);
      if (
        Option.isNone(proofOption) ||
        proofOption.value.machineId !== machineId ||
        proofOption.value.sub !== linkedCloudUserId ||
        proofOption.value.cnf.jkt !== proofOption.value.clientProofKeyThumbprint ||
        !hasBoundedCloudProofLifetime({ ...proofOption.value, nowSeconds }) ||
        !hasExactScope({ scopes: proofOption.value.scope, expected: "machine:connect" })
      ) {
        return yield* new CloudLinkProofRejectedError({ request: "mint" });
      }
      const proof = proofOption.value;

      const jtiSecretName = `${CLOUD_MINT_JTI_PREFIX}${proof.jti}`;
      const nonceSecretName = `${CLOUD_MINT_NONCE_PREFIX}${proof.nonce}`;
      const consumedReplayGuards = yield* consumeCloudReplayGuards({
        secrets,
        names: [jtiSecretName, nonceSecretName],
        value: stringToBytes(DateTime.formatIso(now)),
      });
      if (!consumedReplayGuards) {
        return yield* new CloudLinkProofReplayedError({ request: "mint" });
      }

      const keyPair = yield* getOrCreateMachineKeyPairFromSecretStore(secrets);
      const issued = yield* machineAuth.createPairingLink({
        scopes: AuthStandardClientScopes,
        subject: "cloud-connect",
        ttl: Duration.minutes(2),
        label: "T3 Connect connect",
        proofKeyThumbprint: proof.clientProofKeyThumbprint,
      });
      const responsePayload = {
        iss: `t3-env:${machineId}`,
        aud: normalizeRelayIssuer(relayIssuer),
        sub: machineId,
        jti: yield* crypto.randomUUIDv4(),
        iat: nowSeconds,
        exp: Math.floor(issued.expiresAt.epochMilliseconds / 1_000),
        machineId,
        clientProofKeyThumbprint: proof.clientProofKeyThumbprint,
        requestNonce: proof.nonce,
        credential: issued.credential,
      } satisfies RelayMachineMintResponseProofPayload;
      const responseProof = yield* signRelayJwt({
        privateKey: keyPair.privateKey,
        typ: RELAY_MINT_RESPONSE_TYP,
        payload: responsePayload,
      }).pipe(
        Effect.mapError(
          (cause) =>
            new MachineAuth.ServerAuthCloudMintJwtSigningError({
              cause,
            }),
        ),
      );
      return {
        credential: issued.credential,
        expiresAt: DateTime.formatIso(issued.expiresAt),
        proof: responseProof,
      } satisfies RelayMachineMintResponse;
    },
    Effect.catchIf(ServerSecretStore.isSecretStoreError, internalError("issue-credential")),
    Effect.catchTags({ PlatformError: internalError("issue-credential") }),
  );

  return CloudLink.of({
    linkProof,
    applyRelayConfig,
    linkState,
    unlink,
    updatePreferences,
    answerHealthRequest,
    mintCredential,
    reconcileDesiredLink,
    reconcileDesiredLinkIfStillDesired,
    registerManagedTunnelRecovery,
    recoverManagedTunnel,
    startManagedTunnelIfOriginConfirmed,
    releaseManagedTunnelOnShutdown,
  });
});

export const layer = Layer.effect(CloudLink, make);
