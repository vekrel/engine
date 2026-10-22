import * as Context from "effect/Context";
import * as Schema from "effect/Schema";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiEndpoint from "effect/http-api/HttpApiEndpoint";
import * as HttpApiGroup from "effect/http-api/HttpApiGroup";
import * as HttpApiMiddleware from "effect/http-api/HttpApiMiddleware";
import * as HttpApiSchema from "effect/http-api/HttpApiSchema";
import * as HttpApiSecurity from "effect/http-api/HttpApiSecurity";
import * as OpenApi from "effect/http-api/OpenApi";

import {
  DpopFailureReason,
  MachineId,
  ThreadId,
  TrimmedNonEmptyString,
} from "./baseSchemas.ts";
import { ExecutionMachineDescriptor } from "./machine.ts";

export const RelayAgentAwarenessPlatform = Schema.Literals(["ios", "android"]);
export type RelayAgentAwarenessPlatform = typeof RelayAgentAwarenessPlatform.Type;

export const RelayAgentAwarenessPhase = Schema.Literals([
  "starting",
  "running",
  "waiting_for_approval",
  "waiting_for_input",
  "completed",
  "failed",
  "stale",
]);
export type RelayAgentAwarenessPhase = typeof RelayAgentAwarenessPhase.Type;

export const RelayAgentAwarenessPreferences = Schema.Struct({
  liveActivitiesEnabled: Schema.Boolean,
  notificationsEnabled: Schema.Boolean,
  notifyOnApproval: Schema.Boolean,
  notifyOnInput: Schema.Boolean,
  notifyOnCompletion: Schema.Boolean,
  notifyOnFailure: Schema.Boolean,
});
export type RelayAgentAwarenessPreferences = typeof RelayAgentAwarenessPreferences.Type;

export const RelayApnsEnvironment = Schema.Literals(["sandbox", "production"]);
export type RelayApnsEnvironment = typeof RelayApnsEnvironment.Type;

export const RelayDeviceRegistrationRequest = Schema.Struct({
  deviceId: TrimmedNonEmptyString,
  label: TrimmedNonEmptyString,
  platform: RelayAgentAwarenessPlatform,
  iosMajorVersion: Schema.optional(Schema.Int.check(Schema.isGreaterThanOrEqualTo(18))),
  androidApiLevel: Schema.optional(Schema.Int.check(Schema.isGreaterThanOrEqualTo(24))),
  appVersion: Schema.optional(TrimmedNonEmptyString),
  // APNs routing for this install: the topic must match the app's bundle id (dev/preview/prod
  // variants differ) and development-signed builds receive sandbox tokens.
  bundleId: Schema.optional(TrimmedNonEmptyString),
  apsEnvironment: Schema.optional(RelayApnsEnvironment),
  pushToken: Schema.optional(TrimmedNonEmptyString),
  pushToStartToken: Schema.optional(TrimmedNonEmptyString),
  preferences: RelayAgentAwarenessPreferences,
}).check(
  Schema.makeFilter((device) =>
    device.platform === "ios"
      ? device.iosMajorVersion !== undefined
      : device.androidApiLevel !== undefined &&
        device.iosMajorVersion === undefined &&
        device.apsEnvironment === undefined &&
        device.pushToStartToken === undefined,
  ),
);
export type RelayDeviceRegistrationRequest = typeof RelayDeviceRegistrationRequest.Type;

export const RelayClientDeviceRecord = Schema.Struct({
  deviceId: TrimmedNonEmptyString,
  label: TrimmedNonEmptyString,
  platform: RelayAgentAwarenessPlatform,
  iosMajorVersion: Schema.NullOr(Schema.Int.check(Schema.isGreaterThanOrEqualTo(18))),
  androidApiLevel: Schema.optional(Schema.NullOr(Schema.Int)),
  appVersion: Schema.NullOr(TrimmedNonEmptyString),
  notifications: Schema.Struct({
    enabled: Schema.Boolean,
    notifyOnApproval: Schema.Boolean,
    notifyOnInput: Schema.Boolean,
    notifyOnCompletion: Schema.Boolean,
    notifyOnFailure: Schema.Boolean,
  }),
  liveActivities: Schema.Struct({
    enabled: Schema.Boolean,
  }),
  updatedAt: TrimmedNonEmptyString,
});
export type RelayClientDeviceRecord = typeof RelayClientDeviceRecord.Type;

export const RelayListDevicesResponse = Schema.Struct({
  devices: Schema.Array(RelayClientDeviceRecord),
});
export type RelayListDevicesResponse = typeof RelayListDevicesResponse.Type;

// Installed clients decode v1 as iOS-only. Keep that response contract frozen.
export const RelayListDevicesResponseV1 = Schema.Struct({
  devices: Schema.Array(
    Schema.Struct({
      ...RelayClientDeviceRecord.fields,
      platform: Schema.Literal("ios"),
      iosMajorVersion: Schema.Int.check(Schema.isGreaterThanOrEqualTo(18)),
    }),
  ),
});

export const RelayLiveActivityRegistrationRequest = Schema.Struct({
  deviceId: TrimmedNonEmptyString,
  activityPushToken: TrimmedNonEmptyString,
});
export type RelayLiveActivityRegistrationRequest = typeof RelayLiveActivityRegistrationRequest.Type;

export const RelayDeviceUnregistrationParams = Schema.Struct({
  deviceId: TrimmedNonEmptyString,
});
export type RelayDeviceUnregistrationParams = typeof RelayDeviceUnregistrationParams.Type;

export const RelayAgentActivityState = Schema.Struct({
  machineId: MachineId,
  threadId: ThreadId,
  projectTitle: TrimmedNonEmptyString,
  threadTitle: TrimmedNonEmptyString,
  phase: RelayAgentAwarenessPhase,
  headline: TrimmedNonEmptyString,
  detail: Schema.optional(TrimmedNonEmptyString),
  modelTitle: TrimmedNonEmptyString,
  updatedAt: TrimmedNonEmptyString,
  deepLink: TrimmedNonEmptyString,
});
export type RelayAgentActivityState = typeof RelayAgentActivityState.Type;

export const RelayAgentActivityAggregateRow = Schema.Struct({
  machineId: MachineId,
  threadId: ThreadId,
  projectTitle: TrimmedNonEmptyString,
  threadTitle: TrimmedNonEmptyString,
  modelTitle: TrimmedNonEmptyString,
  phase: RelayAgentAwarenessPhase,
  status: TrimmedNonEmptyString,
  updatedAt: TrimmedNonEmptyString,
  deepLink: TrimmedNonEmptyString,
});
export type RelayAgentActivityAggregateRow = typeof RelayAgentActivityAggregateRow.Type;

export const RelayAgentActivityAggregateState = Schema.Struct({
  title: TrimmedNonEmptyString,
  subtitle: TrimmedNonEmptyString,
  activeCount: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  updatedAt: TrimmedNonEmptyString,
  activities: Schema.Array(RelayAgentActivityAggregateRow),
});
export type RelayAgentActivityAggregateState = typeof RelayAgentActivityAggregateState.Type;

export const RelayManagedEndpointProviderKind = Schema.Literals([
  "manual",
  "cloudflare_tunnel",
  "t3_relay",
]);
export type RelayManagedEndpointProviderKind = typeof RelayManagedEndpointProviderKind.Type;

export const RelayManagedEndpoint = Schema.Struct({
  httpBaseUrl: TrimmedNonEmptyString,
  wsBaseUrl: TrimmedNonEmptyString,
  providerKind: RelayManagedEndpointProviderKind,
});
export type RelayManagedEndpoint = typeof RelayManagedEndpoint.Type;

export const RelayManagedEndpointOrigin = Schema.Struct({
  localHttpHost: TrimmedNonEmptyString,
  localHttpPort: Schema.Int.check(
    Schema.isGreaterThanOrEqualTo(1),
    Schema.isLessThanOrEqualTo(65_535),
  ),
});
export type RelayManagedEndpointOrigin = typeof RelayManagedEndpointOrigin.Type;

export const RelayManagedEndpointRuntimeConfig = Schema.Struct({
  providerKind: RelayManagedEndpointProviderKind,
  connectorToken: TrimmedNonEmptyString,
  tunnelId: Schema.optional(TrimmedNonEmptyString),
  tunnelName: Schema.optional(TrimmedNonEmptyString),
});
export type RelayManagedEndpointRuntimeConfig = typeof RelayManagedEndpointRuntimeConfig.Type;

export const RelayManagedEndpointRecoveryRequest = Schema.Struct({
  cloudUserId: TrimmedNonEmptyString,
  origin: RelayManagedEndpointOrigin,
  proof: TrimmedNonEmptyString,
});
export type RelayManagedEndpointRecoveryRequest = typeof RelayManagedEndpointRecoveryRequest.Type;

export const RelayManagedEndpointRecoveryRegistrationRequest = Schema.Struct({
  cloudUserId: TrimmedNonEmptyString,
  tunnelId: TrimmedNonEmptyString,
  origin: RelayManagedEndpointOrigin,
  proof: TrimmedNonEmptyString,
});
export type RelayManagedEndpointRecoveryRegistrationRequest =
  typeof RelayManagedEndpointRecoveryRegistrationRequest.Type;

export const RelayManagedEndpointRecoveryRegistrationResponse = Schema.Struct({
  status: Schema.Literals(["ready", "recovery_required"]),
});
export type RelayManagedEndpointRecoveryRegistrationResponse =
  typeof RelayManagedEndpointRecoveryRegistrationResponse.Type;

export const RelayManagedEndpointRecoveryResponse = Schema.Struct({
  endpoint: RelayManagedEndpoint,
  endpointRuntime: RelayManagedEndpointRuntimeConfig,
});
export type RelayManagedEndpointRecoveryResponse = typeof RelayManagedEndpointRecoveryResponse.Type;

export const RelayLinkProofRequest = Schema.Struct({
  challenge: Schema.String,
  relayIssuer: Schema.String,
  endpoint: RelayManagedEndpoint,
  origin: RelayManagedEndpointOrigin,
});
export type RelayLinkProofRequest = typeof RelayLinkProofRequest.Type;

export const RelayMachineConfigRequest = Schema.Struct({
  relayUrl: Schema.String,
  relayIssuer: Schema.optional(Schema.String),
  cloudUserId: Schema.String,
  machineCredential: Schema.String,
  cloudMintPublicKey: Schema.String,
  endpointRuntime: Schema.NullOr(RelayManagedEndpointRuntimeConfig),
});
export type RelayMachineConfigRequest = typeof RelayMachineConfigRequest.Type;

const RelaySignedJwtRegisteredClaims = {
  iss: TrimmedNonEmptyString,
  aud: TrimmedNonEmptyString,
  sub: TrimmedNonEmptyString,
  jti: TrimmedNonEmptyString,
  iat: Schema.Int,
  exp: Schema.Int,
} as const;

export const RelayManagedEndpointRecoveryProofPayload = Schema.Union([
  Schema.Struct({
    ...RelaySignedJwtRegisteredClaims,
    action: Schema.Literal("register"),
    machineId: MachineId,
    cloudUserId: TrimmedNonEmptyString,
    tunnelId: TrimmedNonEmptyString,
    origin: RelayManagedEndpointOrigin,
  }),
  Schema.Struct({
    ...RelaySignedJwtRegisteredClaims,
    action: Schema.Literal("recover"),
    machineId: MachineId,
    cloudUserId: TrimmedNonEmptyString,
    origin: RelayManagedEndpointOrigin,
  }),
]);
export type RelayManagedEndpointRecoveryProofPayload =
  typeof RelayManagedEndpointRecoveryProofPayload.Type;

export const RelayAgentActivityPublishProofPayload = Schema.Struct({
  ...RelaySignedJwtRegisteredClaims,
  machineId: MachineId,
  threadId: ThreadId,
  state: Schema.NullOr(RelayAgentActivityState),
});
export type RelayAgentActivityPublishProofPayload =
  typeof RelayAgentActivityPublishProofPayload.Type;
export type RelayAgentActivityPublishProof = string;

export const RelayAgentActivityPublishRequest = Schema.Struct({
  state: Schema.NullOr(RelayAgentActivityState).annotate({
    description: "Current agent-awareness state, or null to remove the published state.",
  }),
  proof: TrimmedNonEmptyString.annotate({
    description: "Machine-signed JWT covering this published activity state.",
  }),
}).annotate({ description: "Publishes a signed agent-awareness update from a machine." });
export type RelayAgentActivityPublishRequest = typeof RelayAgentActivityPublishRequest.Type;

export const RelayMachineLinkScope = Schema.Literals([
  "agent_activity_notifications",
  "managed_tunnels",
]);
export type RelayMachineLinkScope = typeof RelayMachineLinkScope.Type;

export const RelayMachineLinkProofPayload = Schema.Struct({
  ...RelaySignedJwtRegisteredClaims,
  challenge: TrimmedNonEmptyString,
  descriptor: ExecutionMachineDescriptor,
  machineId: MachineId,
  machinePublicKey: TrimmedNonEmptyString,
  endpoint: RelayManagedEndpoint,
  origin: RelayManagedEndpointOrigin,
  scopes: Schema.Array(RelayMachineLinkScope),
});
export type RelayMachineLinkProofPayload = typeof RelayMachineLinkProofPayload.Type;

export const RelayMachineLinkProof = TrimmedNonEmptyString;
export type RelayMachineLinkProof = typeof RelayMachineLinkProof.Type;

export const RelayMachineLinkChallengeRequest = Schema.Struct({
  notificationsEnabled: Schema.Boolean.annotate({
    description: "Whether this link may deliver push notifications.",
  }),
  liveActivitiesEnabled: Schema.Boolean.annotate({
    description: "Whether this link may update Live Activities.",
  }),
  managedTunnelsEnabled: Schema.Boolean.annotate({
    description: "Whether the relay should provision a managed tunnel for this machine.",
  }),
}).annotate({ description: "Requested capabilities for a new machine-link challenge." });
export type RelayMachineLinkChallengeRequest = typeof RelayMachineLinkChallengeRequest.Type;

export const RelayMachineLinkChallengeResponse = Schema.Struct({
  challenge: TrimmedNonEmptyString,
  expiresAt: TrimmedNonEmptyString,
});
export type RelayMachineLinkChallengeResponse =
  typeof RelayMachineLinkChallengeResponse.Type;

export const RelayMachineLinkRequest = Schema.Struct({
  deviceId: Schema.optional(
    TrimmedNonEmptyString.annotate({
      description: "Optional client device identifier associated with this link.",
    }),
  ),
  proof: RelayMachineLinkProof.annotate({
    description: "Machine-signed proof bound to a previously issued link challenge.",
  }),
  notificationsEnabled: Schema.Boolean,
  liveActivitiesEnabled: Schema.Boolean,
  managedTunnelsEnabled: Schema.Boolean,
}).annotate({ description: "Links an authenticated cloud user to a T3 machine." });
export type RelayMachineLinkRequest = typeof RelayMachineLinkRequest.Type;

export const RelayMachineLinkResponse = Schema.Struct({
  ok: Schema.Boolean,
  cloudUserId: TrimmedNonEmptyString,
  machineId: MachineId,
  endpoint: RelayManagedEndpoint,
  endpointRuntime: Schema.NullOr(RelayManagedEndpointRuntimeConfig),
  relayIssuer: TrimmedNonEmptyString,
  machineCredential: TrimmedNonEmptyString,
  cloudMintPublicKey: TrimmedNonEmptyString,
});
export type RelayMachineLinkResponse = typeof RelayMachineLinkResponse.Type;

export const RelayMachineLinkProofInvalidReason = Schema.Literals([
  "invalid_signature_or_scope",
  "descriptor_mismatch",
  "replayed_nonce",
  "challenge_invalid",
  "origin_not_allowed",
  "endpoint_not_secure",
]);
export type RelayMachineLinkProofInvalidReason =
  typeof RelayMachineLinkProofInvalidReason.Type;

export const RelayMachineLinkFailedReason = Schema.Literals([
  "link_persistence_failed",
  "credential_persistence_failed",
  "replay_persistence_failed",
  "internal_error",
]);
export type RelayMachineLinkFailedReason = typeof RelayMachineLinkFailedReason.Type;

export const RelayMachineLinkUnavailableReason = Schema.Literals([
  "managed_endpoint_not_configured",
  "managed_endpoint_provisioning_failed",
]);
export type RelayMachineLinkUnavailableReason =
  typeof RelayMachineLinkUnavailableReason.Type;

export const RelayMachineEndpointUnavailableReason = Schema.Literals([
  "endpoint_request_failed",
  "endpoint_response_invalid",
]);
export type RelayMachineEndpointUnavailableReason =
  typeof RelayMachineEndpointUnavailableReason.Type;

/**
 * Which part of the path from the relay to the machine failed. `tunnel_down`
 * means Cloudflare has no live connector for the tunnel; `origin_unreachable`
 * means the tunnel answered but T3 Code behind it did not. Optional on the wire
 * so old relays and old clients keep working.
 */
export const RelayMachineEndpointFailure = Schema.Literals([
  "tunnel_down",
  "origin_unreachable",
  "dns",
  "tls",
  "refused",
  "timeout",
  "invalid_response",
  "unknown",
]);
export type RelayMachineEndpointFailure = typeof RelayMachineEndpointFailure.Type;

export const RelayAgentActivityPublishProofInvalidReason = Schema.Literals([
  "invalid_signature_or_payload",
  "replayed_nonce",
]);
export type RelayAgentActivityPublishProofInvalidReason =
  typeof RelayAgentActivityPublishProofInvalidReason.Type;

export const RelayAuthInvalidReason = Schema.Literals([
  "missing_bearer",
  "invalid_bearer",
  "invalid_dpop",
  "not_authorized",
]);
export type RelayAuthInvalidReason = typeof RelayAuthInvalidReason.Type;

export const RelayDpopFailureReason = DpopFailureReason;
export type RelayDpopFailureReason = typeof RelayDpopFailureReason.Type;

export const RelayInternalErrorReason = Schema.Literals([
  "database_unavailable",
  "persistence_failed",
  "upstream_unavailable",
  "internal_error",
]);
export type RelayInternalErrorReason = typeof RelayInternalErrorReason.Type;

export class RelayAuthInvalidError extends Schema.TaggedError<RelayAuthInvalidError>()(
  "RelayAuthInvalidError",
  {
    code: Schema.Literal("auth_invalid"),
    reason: RelayAuthInvalidReason,
    dpopFailureReason: Schema.optionalKey(RelayDpopFailureReason),
    traceId: TrimmedNonEmptyString,
  },
  { httpApiStatus: 401 },
) {
  override get message(): string {
    return `Relay authentication failed: ${this.reason}`;
  }
}

export class RelayMachineLinkProofExpiredError extends Schema.TaggedError<RelayMachineLinkProofExpiredError>()(
  "RelayMachineLinkProofExpiredError",
  {
    code: Schema.Literal("machine_link_proof_expired"),
    traceId: TrimmedNonEmptyString,
  },
  { httpApiStatus: 401 },
) {
  override get message(): string {
    return "Relay machine link proof expired";
  }
}

export class RelayMachineLinkProofInvalidError extends Schema.TaggedError<RelayMachineLinkProofInvalidError>()(
  "RelayMachineLinkProofInvalidError",
  {
    code: Schema.Literal("machine_link_proof_invalid"),
    reason: RelayMachineLinkProofInvalidReason,
    traceId: TrimmedNonEmptyString,
  },
  { httpApiStatus: 400 },
) {
  override get message(): string {
    return `Relay machine link proof is invalid: ${this.reason}`;
  }
}

export const RelayMachineConnectNotAuthorizedReason = Schema.Literals([
  "client_proof_key_thumbprint_missing",
  "machine_link_not_found",
  "endpoint_provider_not_managed",
  "managed_endpoint_allocation_not_found",
  "managed_endpoint_base_domain_not_configured",
  "managed_endpoint_allocation_not_ready",
  "managed_endpoint_hostname_invalid",
  "managed_endpoint_mismatch",
]);
export type RelayMachineConnectNotAuthorizedReason =
  typeof RelayMachineConnectNotAuthorizedReason.Type;

export class RelayMachineConnectNotAuthorizedError extends Schema.TaggedError<RelayMachineConnectNotAuthorizedError>()(
  "RelayMachineConnectNotAuthorizedError",
  {
    code: Schema.Literal("machine_connect_not_authorized"),
    reason: Schema.optional(RelayMachineConnectNotAuthorizedReason),
    traceId: TrimmedNonEmptyString,
  },
  { httpApiStatus: 403 },
) {
  override get message(): string {
    return this.reason
      ? `Relay machine connection is not authorized: ${this.reason}`
      : "Relay machine connection is not authorized";
  }
}

export class RelayMachineEndpointUnavailableError extends Schema.TaggedError<RelayMachineEndpointUnavailableError>()(
  "RelayMachineEndpointUnavailableError",
  {
    code: Schema.Literal("machine_endpoint_unavailable"),
    reason: RelayMachineEndpointUnavailableReason,
    endpointFailure: Schema.optionalKey(RelayMachineEndpointFailure),
    endpointStatus: Schema.optionalKey(Schema.Number),
    traceId: TrimmedNonEmptyString,
  },
  { httpApiStatus: 502 },
) {
  override get message(): string {
    return `Relay machine endpoint is unavailable: ${this.reason}`;
  }
}

export class RelayMachineEndpointTimedOutError extends Schema.TaggedError<RelayMachineEndpointTimedOutError>()(
  "RelayMachineEndpointTimedOutError",
  {
    code: Schema.Literal("machine_endpoint_timed_out"),
    traceId: TrimmedNonEmptyString,
  },
  { httpApiStatus: 504 },
) {
  override get message(): string {
    return "Relay machine endpoint request timed out";
  }
}

export class RelayMachineLinkFailedError extends Schema.TaggedError<RelayMachineLinkFailedError>()(
  "RelayMachineLinkFailedError",
  {
    code: Schema.Literal("machine_link_failed"),
    reason: RelayMachineLinkFailedReason,
    traceId: TrimmedNonEmptyString,
  },
  { httpApiStatus: 500 },
) {
  override get message(): string {
    return `Relay machine link failed: ${this.reason}`;
  }
}

export class RelayMachineLinkUnavailableError extends Schema.TaggedError<RelayMachineLinkUnavailableError>()(
  "RelayMachineLinkUnavailableError",
  {
    code: Schema.Literal("machine_link_unavailable"),
    reason: RelayMachineLinkUnavailableReason,
    provisioningStage: Schema.optionalKey(TrimmedNonEmptyString),
    traceId: TrimmedNonEmptyString,
  },
  { httpApiStatus: 503 },
) {
  override get message(): string {
    return `Relay machine link is unavailable: ${this.reason}`;
  }
}

export class RelayMachineLinkLimitExceededError extends Schema.TaggedError<RelayMachineLinkLimitExceededError>()(
  "RelayMachineLinkLimitExceededError",
  {
    code: Schema.Literal("machine_link_limit_exceeded"),
    maxTunnels: Schema.Number,
    traceId: TrimmedNonEmptyString,
  },
  { httpApiStatus: 403 },
) {
  override get message(): string {
    return `Relay managed tunnel limit reached: this account allows at most ${this.maxTunnels} tunnels`;
  }
}

export class RelayAgentActivityPublishProofExpiredError extends Schema.TaggedError<RelayAgentActivityPublishProofExpiredError>()(
  "RelayAgentActivityPublishProofExpiredError",
  {
    code: Schema.Literal("agent_activity_publish_proof_expired"),
    traceId: TrimmedNonEmptyString,
  },
  { httpApiStatus: 401 },
) {
  override get message(): string {
    return "Relay agent activity publish proof expired";
  }
}

export class RelayAgentActivityPublishProofInvalidError extends Schema.TaggedError<RelayAgentActivityPublishProofInvalidError>()(
  "RelayAgentActivityPublishProofInvalidError",
  {
    code: Schema.Literal("agent_activity_publish_proof_invalid"),
    reason: RelayAgentActivityPublishProofInvalidReason,
    traceId: TrimmedNonEmptyString,
  },
  { httpApiStatus: 401 },
) {
  override get message(): string {
    return `Relay agent activity publish proof is invalid: ${this.reason}`;
  }
}

export class RelayInternalError extends Schema.TaggedError<RelayInternalError>()(
  "RelayInternalError",
  {
    code: Schema.Literal("internal_error"),
    reason: RelayInternalErrorReason,
    traceId: TrimmedNonEmptyString,
  },
  { httpApiStatus: 500 },
) {
  override get message(): string {
    return `Relay internal error: ${this.reason}`;
  }
}

export const RelayProtectedError = Schema.Union([
  RelayAuthInvalidError,
  RelayMachineLinkProofExpiredError,
  RelayMachineLinkProofInvalidError,
  RelayMachineConnectNotAuthorizedError,
  RelayMachineEndpointUnavailableError,
  RelayMachineEndpointTimedOutError,
  RelayMachineLinkFailedError,
  RelayMachineLinkUnavailableError,
  RelayMachineLinkLimitExceededError,
  RelayAgentActivityPublishProofExpiredError,
  RelayAgentActivityPublishProofInvalidError,
  RelayInternalError,
]);
export type RelayProtectedError = typeof RelayProtectedError.Type;

const RelayAuthAndInternalErrors = [RelayAuthInvalidError, RelayInternalError] as const;

const RelayMachineLinkErrors = [
  RelayAuthInvalidError,
  RelayMachineLinkProofExpiredError,
  RelayMachineLinkProofInvalidError,
  RelayMachineLinkUnavailableError,
  RelayMachineLinkLimitExceededError,
  RelayMachineLinkFailedError,
  RelayInternalError,
] as const;

const RelayMachineConnectErrors = [
  RelayAuthInvalidError,
  RelayMachineConnectNotAuthorizedError,
  RelayMachineEndpointUnavailableError,
  RelayMachineEndpointTimedOutError,
  RelayInternalError,
] as const;

const RelayAgentActivityPublishErrors = [
  RelayAuthInvalidError,
  RelayAgentActivityPublishProofExpiredError,
  RelayAgentActivityPublishProofInvalidError,
  RelayInternalError,
] as const;

export class RelayClientPrincipal extends Context.Service<
  RelayClientPrincipal,
  {
    readonly userId: string;
    readonly token: string;
    readonly proofKeyThumbprint?: string;
    readonly dpopScopes?: ReadonlyArray<RelayDpopAccessTokenScope>;
  }
>()("@vekrel/contracts/relay/RelayClientPrincipal") {}

export class RelayMachinePrincipal extends Context.Service<
  RelayMachinePrincipal,
  {
    readonly machineId: string;
    readonly machinePublicKey: string;
  }
>()("@vekrel/contracts/relay/RelayMachinePrincipal") {}

const RelayClientBearerAuthorization = HttpApiSecurity.http({ scheme: "bearer" }).pipe(
  HttpApiSecurity.annotate(
    OpenApi.Description,
    "Clerk session or OAuth bearer token for the signed-in T3 Connect user.",
  ),
);

export class RelayClientAuth extends HttpApiMiddleware.Service<
  RelayClientAuth,
  { provides: RelayClientPrincipal }
>()("RelayClientAuth", {
  error: RelayAuthInvalidError,
  security: { clientBearer: RelayClientBearerAuthorization },
}) {}

const RelayMachineBearerAuthorization = HttpApiSecurity.http({ scheme: "bearer" }).pipe(
  HttpApiSecurity.annotate(
    OpenApi.Description,
    "Relay-issued machine credential installed when the machine is linked.",
  ),
);

export class RelayMachineAuth extends HttpApiMiddleware.Service<
  RelayMachineAuth,
  { provides: RelayMachinePrincipal }
>()("RelayMachineAuth", {
  error: [RelayAuthInvalidError, RelayInternalError],
  security: { machineBearer: RelayMachineBearerAuthorization },
}) {}

const RelayDpopAuthorization = HttpApiSecurity.http({ scheme: "DPoP" }).pipe(
  HttpApiSecurity.annotate(
    OpenApi.Description,
    "DPoP-bound access token. Requests must also include the DPoP proof JWT header.",
  ),
);

export class RelayDpopClientAuth extends HttpApiMiddleware.Service<
  RelayDpopClientAuth,
  { provides: RelayClientPrincipal }
>()("RelayDpopClientAuth", {
  error: RelayAuthInvalidError,
  security: { relayDpop: RelayDpopAuthorization },
}) {}

export const RelayClientMachineRecord = Schema.Struct({
  machineId: MachineId,
  label: TrimmedNonEmptyString,
  endpoint: RelayManagedEndpoint,
  linkedAt: TrimmedNonEmptyString,
});
export type RelayClientMachineRecord = typeof RelayClientMachineRecord.Type;

export const RelayListMachinesResponse = Schema.Struct({
  machines: Schema.Array(RelayClientMachineRecord),
});
export type RelayListMachinesResponse = typeof RelayListMachinesResponse.Type;

export const RelayMachineConnectRequest = Schema.Struct({
  deviceId: Schema.optional(
    TrimmedNonEmptyString.annotate({
      description: "Optional client device identifier requesting the connection.",
    }),
  ),
  clientKeyThumbprint: Schema.optional(
    TrimmedNonEmptyString.annotate({
      description: "Deprecated alias for clientProofKeyThumbprint.",
    }),
  ),
  clientProofKeyThumbprint: Schema.optional(
    TrimmedNonEmptyString.annotate({
      description: "JWK thumbprint that the minted machine credential must be bound to.",
    }),
  ),
}).annotate({ description: "Requests a short-lived credential for connecting to a machine." });
export type RelayMachineConnectRequest = typeof RelayMachineConnectRequest.Type;

export const RelayMachineConnectScope = "machine:connect" as const;
export const RelayMachineStatusScope = "machine:status" as const;
export const RelayMobileRegistrationScope = "mobile:registration" as const;
export const RelayDpopAccessTokenScope = Schema.Literals([
  RelayMachineConnectScope,
  RelayMachineStatusScope,
  RelayMobileRegistrationScope,
]);
export type RelayDpopAccessTokenScope = typeof RelayDpopAccessTokenScope.Type;

export const RelayDpopTokenExchangeGrantType =
  "urn:ietf:params:oauth:grant-type:token-exchange" as const;
export const RelayJwtSubjectTokenType = "urn:ietf:params:oauth:token-type:jwt" as const;
export const RelayAccessTokenType = "urn:ietf:params:oauth:token-type:access_token" as const;
export const RelayPublicClientId = Schema.Literals(["t3-mobile", "t3-web"]);
export type RelayPublicClientId = typeof RelayPublicClientId.Type;
export const RelayMobileClientId = "t3-mobile" as const;
export const RelayWebClientId = "t3-web" as const;

export const RelayDpopAccessTokenRequest = Schema.Struct({
  grant_type: Schema.Literal(RelayDpopTokenExchangeGrantType),
  subject_token: TrimmedNonEmptyString.annotate({
    description: "Clerk bearer token for the signed-in cloud user.",
  }),
  subject_token_type: Schema.Literal(RelayJwtSubjectTokenType),
  requested_token_type: Schema.Literal(RelayAccessTokenType),
  resource: TrimmedNonEmptyString.annotate({
    description: "Relay issuer URL that will receive the DPoP-bound access token.",
  }),
  scope: TrimmedNonEmptyString.annotate({
    description: "Space-separated relay scopes requested by the client.",
  }),
  client_id: RelayPublicClientId,
})
  .annotate({ description: "OAuth token exchange request for a DPoP-bound relay access token." })
  .pipe(HttpApiSchema.asFormUrlEncoded());
export type RelayDpopAccessTokenRequest = typeof RelayDpopAccessTokenRequest.Type;

export const RelayDpopAccessTokenResponse = Schema.Struct({
  access_token: TrimmedNonEmptyString,
  issued_token_type: Schema.Literal(RelayAccessTokenType),
  token_type: Schema.Literal("DPoP"),
  expires_in: Schema.Int.check(Schema.isGreaterThan(0)),
  scope: TrimmedNonEmptyString,
});
export type RelayDpopAccessTokenResponse = typeof RelayDpopAccessTokenResponse.Type;

export const RelayBearerRequestHeaders = Schema.Struct({
  authorization: TrimmedNonEmptyString,
});

export const RelayDpopProofRequestHeaders = Schema.Struct({
  dpop: TrimmedNonEmptyString,
});

export const RelayDpopRequestHeaders = Schema.Struct({
  authorization: TrimmedNonEmptyString,
  dpop: TrimmedNonEmptyString,
});

export const RelayAuthorizationServerMetadata = Schema.Struct({
  issuer: TrimmedNonEmptyString,
  token_endpoint: TrimmedNonEmptyString,
  grant_types_supported: Schema.Array(Schema.Literal(RelayDpopTokenExchangeGrantType)),
  token_endpoint_auth_methods_supported: Schema.Array(Schema.Literal("none")),
  dpop_signing_alg_values_supported: Schema.Array(Schema.Literal("ES256")),
  scopes_supported: Schema.Array(RelayDpopAccessTokenScope),
});

export const RelayProtectedResourceMetadata = Schema.Struct({
  resource: TrimmedNonEmptyString,
  authorization_servers: Schema.Array(TrimmedNonEmptyString),
  scopes_supported: Schema.Array(RelayDpopAccessTokenScope),
  dpop_bound_access_tokens_required: Schema.Boolean,
  dpop_signing_alg_values_supported: Schema.Array(Schema.Literal("ES256")),
});

export const RelayMachineUnlinkParams = Schema.Struct({
  machineId: MachineId,
});
export type RelayMachineUnlinkParams = typeof RelayMachineUnlinkParams.Type;

export const RelayMachineConnectResponse = Schema.Struct({
  machineId: MachineId,
  endpoint: RelayManagedEndpoint,
  credential: TrimmedNonEmptyString,
  expiresAt: TrimmedNonEmptyString,
});
export type RelayMachineConnectResponse = typeof RelayMachineConnectResponse.Type;

export const RelayMachineStatusValue = Schema.Literals(["online", "offline"]);
export type RelayMachineStatusValue = typeof RelayMachineStatusValue.Type;

const RelayMachineOfflineReason = Schema.Literals(["tunnel_released"]);
type RelayMachineOfflineReason = typeof RelayMachineOfflineReason.Type;

export const RelayMachineStatusResponse = Schema.Struct({
  machineId: MachineId,
  endpoint: RelayManagedEndpoint,
  status: RelayMachineStatusValue,
  checkedAt: TrimmedNonEmptyString,
  descriptor: Schema.optional(ExecutionMachineDescriptor),
  error: Schema.optional(TrimmedNonEmptyString),
  endpointFailure: Schema.optional(RelayMachineEndpointFailure),
  endpointStatus: Schema.optional(Schema.Number),
  offlineReason: Schema.optional(RelayMachineOfflineReason),
  traceId: Schema.optional(TrimmedNonEmptyString),
});
export type RelayMachineStatusResponse = typeof RelayMachineStatusResponse.Type;

export const RelayCloudMintCredentialProofPayload = Schema.Struct({
  ...RelaySignedJwtRegisteredClaims,
  machineId: MachineId,
  clientProofKeyThumbprint: TrimmedNonEmptyString,
  cnf: Schema.Struct({
    jkt: TrimmedNonEmptyString,
  }),
  deviceId: Schema.optional(TrimmedNonEmptyString),
  nonce: TrimmedNonEmptyString,
  scope: Schema.Array(Schema.Literal("machine:connect")),
});
export type RelayCloudMintCredentialProofPayload = typeof RelayCloudMintCredentialProofPayload.Type;

export const RelayCloudMintCredentialProof = TrimmedNonEmptyString;
export type RelayCloudMintCredentialProof = typeof RelayCloudMintCredentialProof.Type;

export const RelayCloudMintCredentialRequest = Schema.Struct({
  proof: RelayCloudMintCredentialProof,
});
export type RelayCloudMintCredentialRequest = typeof RelayCloudMintCredentialRequest.Type;

export const RelayCloudMachineHealthProofPayload = Schema.Struct({
  ...RelaySignedJwtRegisteredClaims,
  machineId: MachineId,
  nonce: TrimmedNonEmptyString,
  scope: Schema.Array(Schema.Literal("machine:status")),
});
export type RelayCloudMachineHealthProofPayload =
  typeof RelayCloudMachineHealthProofPayload.Type;

/**
 * Sent with every webhook the relay forwards, signed with the relay's mint key
 * (`x-t3-relay-delivery`). The machine trusts the relay's delivery id,
 * receive time and trace context only when this verifies, since its webhook
 * URL can also be called directly.
 */
export const RelayHookDeliveryProofPayload = Schema.Struct({
  ...RelaySignedJwtRegisteredClaims,
  machineId: MachineId,
  deliveryId: TrimmedNonEmptyString,
  receivedAt: TrimmedNonEmptyString,
  hookId: TrimmedNonEmptyString,
});
export type RelayHookDeliveryProofPayload = typeof RelayHookDeliveryProofPayload.Type;

export const RelayCloudMachineHealthProof = TrimmedNonEmptyString;
export type RelayCloudMachineHealthProof = typeof RelayCloudMachineHealthProof.Type;

export const RelayCloudMachineHealthRequest = Schema.Struct({
  proof: RelayCloudMachineHealthProof,
});
export type RelayCloudMachineHealthRequest = typeof RelayCloudMachineHealthRequest.Type;

export const RelayMachineHealthResponseProofPayload = Schema.Struct({
  ...RelaySignedJwtRegisteredClaims,
  machineId: MachineId,
  requestNonce: TrimmedNonEmptyString,
  status: Schema.Literal("online"),
  descriptor: ExecutionMachineDescriptor,
  checkedAt: TrimmedNonEmptyString,
});
export type RelayMachineHealthResponseProofPayload =
  typeof RelayMachineHealthResponseProofPayload.Type;

export const RelayMachineHealthResponse = Schema.Struct({
  machineId: MachineId,
  status: Schema.Literal("online"),
  descriptor: ExecutionMachineDescriptor,
  checkedAt: TrimmedNonEmptyString,
  proof: TrimmedNonEmptyString,
});
export type RelayMachineHealthResponse = typeof RelayMachineHealthResponse.Type;

export const RelayMachineMintResponseProofPayload = Schema.Struct({
  ...RelaySignedJwtRegisteredClaims,
  machineId: MachineId,
  clientProofKeyThumbprint: TrimmedNonEmptyString,
  requestNonce: TrimmedNonEmptyString,
  credential: TrimmedNonEmptyString,
});
export type RelayMachineMintResponseProofPayload =
  typeof RelayMachineMintResponseProofPayload.Type;

export const RelayMachineMintResponse = Schema.Struct({
  credential: TrimmedNonEmptyString,
  expiresAt: TrimmedNonEmptyString,
  proof: TrimmedNonEmptyString,
});
export type RelayMachineMintResponse = typeof RelayMachineMintResponse.Type;

export const RelayDeliveryKind = Schema.Literals([
  "live_activity_start",
  "live_activity_update",
  "live_activity_end",
  "push_notification",
]);
export type RelayDeliveryKind = typeof RelayDeliveryKind.Type;

export const RelayDeliveryResult = Schema.Struct({
  deviceId: TrimmedNonEmptyString,
  kind: RelayDeliveryKind,
  ok: Schema.Boolean,
  queued: Schema.optional(Schema.Boolean),
  apnsStatus: Schema.NullOr(Schema.Number),
  apnsReason: Schema.NullOr(Schema.String),
  apnsId: Schema.NullOr(Schema.String),
});
export type RelayDeliveryResult = typeof RelayDeliveryResult.Type;

export const RelayOkResponse = Schema.Struct({
  ok: Schema.Boolean,
});
export type RelayOkResponse = typeof RelayOkResponse.Type;

export const RelayPublishResponse = Schema.Struct({
  ok: Schema.Boolean,
  deliveries: Schema.Array(RelayDeliveryResult),
});
export type RelayPublishResponse = typeof RelayPublishResponse.Type;

export const RelayHealthResponse = Schema.Struct({
  ok: Schema.Boolean,
  service: Schema.Literal("relay"),
});
export type RelayHealthResponse = typeof RelayHealthResponse.Type;

const RelayHealthGroup = HttpApiGroup.make("health")
  .add(
    HttpApiEndpoint.get("health", "/health", {
      success: RelayHealthResponse,
      error: RelayInternalError,
    }).annotate(OpenApi.Summary, "Check relay health"),
  )
  .annotate(OpenApi.Description, "Service health and readiness.");

const RelayMetadataGroup = HttpApiGroup.make("metadata")
  .add(
    HttpApiEndpoint.get("authorizationServer", "/.well-known/oauth-authorization-server", {
      success: RelayAuthorizationServerMetadata,
    }).annotate(OpenApi.Summary, "Read OAuth authorization-server metadata"),
    HttpApiEndpoint.get("protectedResource", "/.well-known/oauth-protected-resource", {
      success: RelayProtectedResourceMetadata,
    }).annotate(OpenApi.Summary, "Read OAuth protected-resource metadata"),
  )
  .annotate(OpenApi.Description, "OAuth and DPoP discovery metadata.");

export const RelayRegisterDeviceEndpoint = HttpApiEndpoint.post(
  "registerDevice",
  "/v1/mobile/devices",
  {
    headers: RelayDpopRequestHeaders,
    payload: RelayDeviceRegistrationRequest,
    success: RelayOkResponse,
    error: RelayAuthAndInternalErrors,
  },
).annotate(OpenApi.Summary, "Register or update a mobile device");

export const RelayRegisterLiveActivityEndpoint = HttpApiEndpoint.post(
  "registerLiveActivity",
  "/v1/mobile/live-activities",
  {
    headers: RelayDpopRequestHeaders,
    payload: RelayLiveActivityRegistrationRequest,
    success: RelayOkResponse,
    error: RelayAuthAndInternalErrors,
  },
).annotate(OpenApi.Summary, "Register a Live Activity push token");

export const RelayAgentActivitySnapshotResponse = Schema.Struct({
  aggregate: Schema.NullOr(RelayAgentActivityAggregateState),
});
export type RelayAgentActivitySnapshotResponse = typeof RelayAgentActivitySnapshotResponse.Type;

export const RelayAgentActivitySnapshotEndpoint = HttpApiEndpoint.get(
  "getAgentActivitySnapshot",
  "/v1/mobile/agent-activity",
  {
    headers: RelayDpopRequestHeaders,
    success: RelayAgentActivitySnapshotResponse,
    error: RelayAuthAndInternalErrors,
  },
).annotate(OpenApi.Summary, "Read the current Live Activity aggregate");

export const RelayUnregisterDeviceEndpoint = HttpApiEndpoint.delete(
  "unregisterDevice",
  "/v1/mobile/devices/:deviceId",
  {
    headers: RelayDpopRequestHeaders,
    params: RelayDeviceUnregistrationParams,
    success: RelayOkResponse,
    error: RelayAuthAndInternalErrors,
  },
).annotate(OpenApi.Summary, "Unregister a mobile device");

const RelayMobileGroup = HttpApiGroup.make("mobile")
  .add(
    RelayRegisterDeviceEndpoint,
    RelayRegisterLiveActivityEndpoint,
    RelayAgentActivitySnapshotEndpoint,
    RelayUnregisterDeviceEndpoint,
  )
  .annotate(OpenApi.Description, "Mobile push-notification and Live Activity registration.")
  .middleware(RelayDpopClientAuth);

const RelayClientGroup = HttpApiGroup.make("client")
  .add(
    HttpApiEndpoint.get("listMachines", "/v1/machines", {
      headers: RelayBearerRequestHeaders,
      success: RelayListMachinesResponse,
      error: RelayAuthAndInternalErrors,
    }).annotate(OpenApi.Summary, "List linked machines"),
    HttpApiEndpoint.get("listDevices", "/v1/client/devices", {
      headers: RelayBearerRequestHeaders,
      success: RelayListDevicesResponseV1,
      error: RelayAuthAndInternalErrors,
    }).annotate(OpenApi.Summary, "List registered iOS devices (legacy clients)"),
    HttpApiEndpoint.get("listDevicesV2", "/v2/client/devices", {
      headers: RelayBearerRequestHeaders,
      success: RelayListDevicesResponse,
      error: RelayAuthAndInternalErrors,
    }).annotate(OpenApi.Summary, "List registered mobile devices"),
    HttpApiEndpoint.post("linkMachine", "/v1/client/machine-links", {
      headers: RelayBearerRequestHeaders,
      payload: RelayMachineLinkRequest,
      success: RelayMachineLinkResponse,
      error: RelayMachineLinkErrors,
    }).annotate(OpenApi.Summary, "Link a machine"),
    HttpApiEndpoint.post(
      "createMachineLinkChallenge",
      "/v1/client/machine-link-challenges",
      {
        headers: RelayBearerRequestHeaders,
        payload: RelayMachineLinkChallengeRequest,
        success: RelayMachineLinkChallengeResponse,
        error: RelayAuthAndInternalErrors,
      },
    ).annotate(OpenApi.Summary, "Create a machine-link challenge"),
    HttpApiEndpoint.delete("unlinkMachine", "/v1/client/machine-links/:machineId", {
      headers: RelayBearerRequestHeaders,
      params: RelayMachineUnlinkParams,
      success: RelayOkResponse,
      error: RelayAuthAndInternalErrors,
    }).annotate(OpenApi.Summary, "Unlink a machine"),
    HttpApiEndpoint.delete(
      "releaseMachineTunnel",
      "/v1/client/machine-links/:machineId/tunnel",
      {
        headers: RelayBearerRequestHeaders,
        params: RelayMachineUnlinkParams,
        success: RelayOkResponse,
        error: RelayAuthAndInternalErrors,
      },
    )
      .annotate(OpenApi.Summary, "Release a machine's managed tunnel")
      .annotate(
        OpenApi.Description,
        "Deletes the provisioned Cloudflare tunnel while keeping the machine link and its hostname reservation, so a later link re-provisions the tunnel under the same URL. Machines call this when they shut down; Cloudflare bills per provisioned tunnel, so idle tunnels should not outlive their machine.",
      ),
  )
  .annotate(OpenApi.Description, "Cloud-user machine links and registered devices.")
  .middleware(RelayClientAuth);

export const RelayExchangeDpopAccessTokenEndpoint = HttpApiEndpoint.post(
  "exchangeDpopAccessToken",
  "/v1/client/dpop-token",
  {
    headers: RelayDpopProofRequestHeaders,
    payload: RelayDpopAccessTokenRequest,
    success: RelayDpopAccessTokenResponse,
    error: RelayAuthAndInternalErrors,
  },
)
  .annotate(OpenApi.Summary, "Exchange a Clerk token for a DPoP access token")
  .annotate(
    OpenApi.Description,
    "Bootstrap endpoint. Send the DPoP proof JWT in the dpop header and the Clerk token in subject_token. The returned access token is bound to the proof key.",
  );

const RelayTokenGroup = HttpApiGroup.make("token")
  .add(RelayExchangeDpopAccessTokenEndpoint)
  .annotate(OpenApi.Description, "OAuth token exchange for DPoP-bound client access.");

export const RelayConnectMachineEndpoint = HttpApiEndpoint.post(
  "connectMachine",
  "/v1/machines/:machineId/connect",
  {
    headers: RelayDpopRequestHeaders,
    params: Schema.Struct({
      machineId: MachineId,
    }),
    payload: RelayMachineConnectRequest,
    success: RelayMachineConnectResponse,
    error: RelayMachineConnectErrors,
  },
).annotate(OpenApi.Summary, "Connect to a machine");

export const RelayGetMachineStatusEndpoint = HttpApiEndpoint.post(
  "getMachineStatus",
  "/v1/machines/:machineId/status",
  {
    headers: RelayDpopRequestHeaders,
    params: Schema.Struct({
      machineId: MachineId,
    }),
    success: RelayMachineStatusResponse,
    error: RelayMachineConnectErrors,
  },
).annotate(OpenApi.Summary, "Check machine status");

const RelayDpopClientGroup = HttpApiGroup.make("dpopClient")
  .add(RelayConnectMachineEndpoint, RelayGetMachineStatusEndpoint)
  .annotate(OpenApi.Description, "DPoP-authenticated client access to linked machines.")
  .middleware(RelayDpopClientAuth);

export const RelayMachineLinkPreferencesRequest = Schema.Struct({
  holdWebhooksWhileOffline: Schema.Boolean,
});
export type RelayMachineLinkPreferencesRequest =
  typeof RelayMachineLinkPreferencesRequest.Type;

export const RelayWakeHeldHooksResponse = Schema.Struct({
  pending: Schema.Boolean,
});
export type RelayWakeHeldHooksResponse = typeof RelayWakeHeldHooksResponse.Type;

const RelayServerGroup = HttpApiGroup.make("server")
  .add(
    HttpApiEndpoint.post(
      "registerManagedEndpointRecovery",
      "/v1/machines/:machineId/tunnel/recovery",
      {
        params: Schema.Struct({
          machineId: MachineId,
        }),
        payload: RelayManagedEndpointRecoveryRegistrationRequest,
        success: RelayManagedEndpointRecoveryRegistrationResponse,
        error: RelayAuthAndInternalErrors,
      },
    ).annotate(OpenApi.Summary, "Register managed tunnel recovery without provisioning"),
    HttpApiEndpoint.post("recoverManagedEndpoint", "/v1/machines/:machineId/tunnel", {
      params: Schema.Struct({
        machineId: MachineId,
      }),
      payload: RelayManagedEndpointRecoveryRequest,
      success: RelayManagedEndpointRecoveryResponse,
      error: RelayAuthAndInternalErrors,
    }).annotate(OpenApi.Summary, "Recover a machine's managed tunnel"),
    HttpApiEndpoint.post(
      "publishAgentActivity",
      "/v1/machines/:machineId/threads/:threadId/agent-activity",
      {
        params: Schema.Struct({
          machineId: MachineId,
          threadId: ThreadId,
        }),
        payload: RelayAgentActivityPublishRequest,
        success: RelayPublishResponse,
        error: RelayAgentActivityPublishErrors,
      },
    ).annotate(OpenApi.Summary, "Publish agent activity"),
    HttpApiEndpoint.post(
      "updateLinkPreferences",
      "/v1/machines/:machineId/link-preferences",
      {
        params: Schema.Struct({ machineId: MachineId }),
        payload: RelayMachineLinkPreferencesRequest,
        success: RelayMachineLinkPreferencesRequest,
        error: RelayAuthAndInternalErrors,
      },
    ).annotate(OpenApi.Summary, "Update a machine's link preferences"),
    HttpApiEndpoint.post("wakeHeldHooks", "/v1/machines/:machineId/hooks/wake", {
      params: Schema.Struct({ machineId: MachineId }),
      success: RelayWakeHeldHooksResponse,
      error: RelayAuthAndInternalErrors,
    }).annotate(
      OpenApi.Summary,
      "Deliver webhook requests held while the machine was offline now",
    ),
  )
  .annotate(OpenApi.Description, "Machine-authenticated activity publication.")
  .middleware(RelayMachineAuth);

/**
 * Public, stateless webhook forwarding to a machine's managed tunnel.
 * Unauthenticated: the token in the path is the machine's credential, and
 * the relay only routes and forwards. Raw, so the body reaches the
 * machine byte for byte and signatures still verify there. `endpointKey`
 * names one managed endpoint (its tunnel's hash of user and machine), not
 * the machine id, which any account can link.
 */
const RelayHookParams = Schema.Struct({
  endpointKey: Schema.String,
  hookId: Schema.String,
  token: Schema.String,
});
const RELAY_HOOK_PATH = "/v1/hooks/:endpointKey/:hookId/:token";
const relayHookEndpoint = { params: RelayHookParams } as const;
const RelayHooksGroup = HttpApiGroup.make("hooks")
  .add(
    HttpApiEndpoint.post("forwardPost", RELAY_HOOK_PATH, relayHookEndpoint),
    HttpApiEndpoint.put("forwardPut", RELAY_HOOK_PATH, relayHookEndpoint),
    HttpApiEndpoint.patch("forwardPatch", RELAY_HOOK_PATH, relayHookEndpoint),
    HttpApiEndpoint.get("forwardGet", RELAY_HOOK_PATH, relayHookEndpoint),
  )
  .annotate(OpenApi.Description, "Forward webhook requests to a machine.");

export const RelayApi = HttpApi.make("RelayApi")
  .add(
    RelayHealthGroup,
    RelayMetadataGroup,
    RelayMobileGroup,
    RelayClientGroup,
    RelayTokenGroup,
    RelayDpopClientGroup,
    RelayServerGroup,
    RelayHooksGroup,
  )
  .annotate(OpenApi.Title, "T3 Code Relay API")
  .annotate(OpenApi.Version, "1.0.0")
  .annotate(
    OpenApi.Description,
    "Control-plane API for linking T3 machines, connecting authorized clients, and publishing agent activity.",
  );
export type RelayApi = typeof RelayApi;
