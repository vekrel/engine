import * as Context from "effect/Context";
import type * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiEndpoint from "effect/http-api/HttpApiEndpoint";
import * as HttpApiGroup from "effect/http-api/HttpApiGroup";
import * as HttpApiMiddleware from "effect/http-api/HttpApiMiddleware";
import * as HttpApiSchema from "effect/http-api/HttpApiSchema";
import * as HttpServerRespondable from "effect/http/HttpServerRespondable";
import * as HttpServerResponse from "effect/http/HttpServerResponse";

import {
  AuthAccessTokenResult,
  AuthBrowserSessionRequest,
  AuthBrowserSessionResult,
  AuthClientSession,
  AuthCreatePairingCredentialInput,
  AuthMcpApprovalDecisionRequest,
  AuthMcpApprovalDetails,
  AuthMcpApprovalError,
  AuthMcpApprovalRedirect,
  AuthMcpAuthorizationRequest,
  AuthMcpAuthorizationServerMetadata,
  AuthMcpClientRegistration,
  AuthMcpProtectedResourceMetadata,
  AuthMcpRegisteredClient,
  AuthMcpRegistrationError,
  AuthMcpTokenError,
  AuthMcpTokenRequest,
  AuthMcpTokenResult,
  AuthPairingCredentialResult,
  AuthPairingLink,
  AuthRevokeClientSessionInput,
  AuthRevokePairingLinkInput,
  AuthMachineScope,
  AuthTokenExchangeRequest,
  AuthSessionState,
  AuthWebSocketTicketResult,
  ServerAuthSessionMethod,
} from "./auth.ts";
import {
  ExecutionMachineDescriptor,
  ORCHESTRATION_PROTOCOL_HEADER,
  ORCHESTRATION_PROTOCOL_VERSION_TEXT,
} from "./machine.ts";
import {
  DpopFailureReason,
  AuthSessionId,
  ThreadId,
  TrimmedNonEmptyString,
} from "./baseSchemas.ts";
import {
  OrchestrationV2ShellSnapshot,
  OrchestrationV2ThreadBoundedSnapshot,
  OrchestrationV2ThreadDetailSnapshot,
  OrchestrationV2ThreadHistoryPage,
  OrchestrationV2ThreadTranscript,
} from "./orchestrationV2.ts";
import { Project, ProjectMutation, ProjectSnapshot } from "./project.ts";
import {
  PullRequestDiffInput,
  PullRequestDiffResult,
  PullRequestOperationError,
  PullRequestUnavailableError,
} from "./pullRequest.ts";
import {
  RelayCloudMachineHealthRequest,
  RelayCloudMintCredentialRequest,
  RelayMachineConfigRequest,
  RelayMachineHealthResponse,
  RelayMachineLinkProof,
  RelayMachineMintResponse,
  RelayLinkProofRequest,
} from "./relay.ts";

const OptionalBearerHeaders = Schema.Struct({
  authorization: Schema.optionalKey(Schema.String),
  dpop: Schema.optionalKey(Schema.String),
});

const OrchestrationProtocolHeaders = Schema.Struct({
  authorization: Schema.optionalKey(Schema.String),
  dpop: Schema.optionalKey(Schema.String),
  [ORCHESTRATION_PROTOCOL_HEADER]: Schema.Literal(ORCHESTRATION_PROTOCOL_VERSION_TEXT),
});

const OptionalDpopProofHeaders = Schema.Struct({
  dpop: Schema.optionalKey(Schema.String),
});

export const MachineRequestInvalidReason = Schema.Literals([
  "invalid_scope",
  "scope_not_granted",
  "invalid_command",
  "invalid_history_cursor",
  "thread_transcript_too_large",
]);
export type MachineRequestInvalidReason = typeof MachineRequestInvalidReason.Type;

export const MachineAuthInvalidReason = Schema.Literals([
  "missing_credential",
  "invalid_credential",
]);
export type MachineAuthInvalidReason = typeof MachineAuthInvalidReason.Type;

export const MachineOperationForbiddenReason = Schema.Literals([
  "current_session_revoke_not_allowed",
]);
export type MachineOperationForbiddenReason = typeof MachineOperationForbiddenReason.Type;

export const MachineInternalErrorReason = Schema.Literals([
  "bootstrap_validation_failed",
  "browser_session_issuance_failed",
  "browser_session_cookie_failed",
  "access_token_issuance_failed",
  "websocket_ticket_issuance_failed",
  "pairing_credential_issuance_failed",
  "pairing_links_load_failed",
  "pairing_link_revoke_failed",
  "client_sessions_load_failed",
  "client_session_revoke_failed",
  "project_snapshot_failed",
  "project_mutation_failed",
  "orchestration_snapshot_failed",
  "orchestration_thread_snapshot_failed",
  "orchestration_thread_bounded_snapshot_failed",
  "orchestration_thread_history_failed",
  "internal_error",
]);
export type MachineInternalErrorReason = typeof MachineInternalErrorReason.Type;

export class MachineRequestInvalidError extends Schema.TaggedError<MachineRequestInvalidError>()(
  "MachineRequestInvalidError",
  {
    code: Schema.Literal("invalid_request"),
    reason: MachineRequestInvalidReason,
    traceId: TrimmedNonEmptyString,
  },
  { httpApiStatus: 400 },
) {
  [HttpServerRespondable.symbol]() {
    return HttpServerResponse.schemaJson(MachineRequestInvalidError)(this, { status: 400 });
  }

  override get message(): string {
    return `The machine rejected the request (${this.reason}).`;
  }
}

export class MachineAuthInvalidError extends Schema.TaggedError<MachineAuthInvalidError>()(
  "MachineAuthInvalidError",
  {
    code: Schema.Literal("auth_invalid"),
    reason: MachineAuthInvalidReason,
    dpopFailureReason: Schema.optionalKey(DpopFailureReason),
    traceId: TrimmedNonEmptyString,
  },
  { httpApiStatus: 401 },
) {
  [HttpServerRespondable.symbol]() {
    return HttpServerResponse.schemaJson(MachineAuthInvalidError)(this, { status: 401 });
  }

  override get message(): string {
    return `The machine rejected this client's credentials (${this.reason}).`;
  }
}

export class MachineScopeRequiredError extends Schema.TaggedError<MachineScopeRequiredError>()(
  "MachineScopeRequiredError",
  {
    code: Schema.Literal("insufficient_scope"),
    requiredScope: AuthMachineScope,
    requiredPermission: Schema.optionalKey(Schema.String),
    traceId: TrimmedNonEmptyString,
  },
  { httpApiStatus: 403 },
) {
  [HttpServerRespondable.symbol]() {
    return HttpServerResponse.schemaJson(MachineScopeRequiredError)(this, { status: 403 });
  }

  override get message(): string {
    return `This request needs the ${this.requiredPermission ?? this.requiredScope} scope, which this client does not have.`;
  }
}

export class MachineOperationForbiddenError extends Schema.TaggedError<MachineOperationForbiddenError>()(
  "MachineOperationForbiddenError",
  {
    code: Schema.Literal("operation_forbidden"),
    reason: MachineOperationForbiddenReason,
    traceId: TrimmedNonEmptyString,
  },
  { httpApiStatus: 403 },
) {
  [HttpServerRespondable.symbol]() {
    return HttpServerResponse.schemaJson(MachineOperationForbiddenError)(this, { status: 403 });
  }

  override get message(): string {
    return `The machine refused this operation (${this.reason}).`;
  }
}

export class MachineInternalError extends Schema.TaggedError<MachineInternalError>()(
  "MachineInternalError",
  {
    code: Schema.Literal("internal_error"),
    reason: MachineInternalErrorReason,
    traceId: TrimmedNonEmptyString,
  },
  { httpApiStatus: 500 },
) {
  [HttpServerRespondable.symbol]() {
    return HttpServerResponse.schemaJson(MachineInternalError)(this, { status: 500 });
  }

  override get message(): string {
    return `The machine failed to answer this request (${this.reason}).`;
  }
}

export const MachineResourceNotFoundReason = Schema.Literals(["thread_not_found"]);
export type MachineResourceNotFoundReason = typeof MachineResourceNotFoundReason.Type;

export class MachineResourceNotFoundError extends Schema.TaggedError<MachineResourceNotFoundError>()(
  "MachineResourceNotFoundError",
  {
    code: Schema.Literal("not_found"),
    reason: MachineResourceNotFoundReason,
    traceId: TrimmedNonEmptyString,
  },
  { httpApiStatus: 404 },
) {
  [HttpServerRespondable.symbol]() {
    return HttpServerResponse.schemaJson(MachineResourceNotFoundError)(this, { status: 404 });
  }

  override get message(): string {
    return `The machine could not find what this request named (${this.reason}).`;
  }
}

export const MachineHttpCommonError = Schema.Union([
  MachineRequestInvalidError,
  MachineAuthInvalidError,
  MachineScopeRequiredError,
  MachineOperationForbiddenError,
  MachineResourceNotFoundError,
  MachineInternalError,
]);
export type MachineHttpCommonError = typeof MachineHttpCommonError.Type;

const MachineAuthenticationErrors = [MachineAuthInvalidError, MachineInternalError] as const;

export class MachineHttpBadRequestError extends Schema.TaggedError<MachineHttpBadRequestError>()(
  "MachineHttpBadRequestError",
  {
    message: Schema.String,
  },
  { httpApiStatus: 400 },
) {
  [HttpServerRespondable.symbol]() {
    return HttpServerResponse.schemaJson(MachineHttpBadRequestError)(this, { status: 400 });
  }
}

export class MachineHttpUnauthorizedError extends Schema.TaggedError<MachineHttpUnauthorizedError>()(
  "MachineHttpUnauthorizedError",
  {
    message: Schema.String,
  },
  { httpApiStatus: 401 },
) {
  [HttpServerRespondable.symbol]() {
    return HttpServerResponse.schemaJson(MachineHttpUnauthorizedError)(this, { status: 401 });
  }
}

export class MachineHttpForbiddenError extends Schema.TaggedError<MachineHttpForbiddenError>()(
  "MachineHttpForbiddenError",
  {
    message: Schema.String,
  },
  { httpApiStatus: 403 },
) {
  [HttpServerRespondable.symbol]() {
    return HttpServerResponse.schemaJson(MachineHttpForbiddenError)(this, { status: 403 });
  }
}

export class MachineHttpInternalServerError extends Schema.TaggedError<MachineHttpInternalServerError>()(
  "MachineHttpInternalServerError",
  {
    message: Schema.String,
  },
  { httpApiStatus: 500 },
) {
  [HttpServerRespondable.symbol]() {
    return HttpServerResponse.schemaJson(MachineHttpInternalServerError)(this, { status: 500 });
  }
}

export class MachineHttpConflictError extends Schema.TaggedError<MachineHttpConflictError>()(
  "MachineHttpConflictError",
  {
    message: Schema.String,
  },
  { httpApiStatus: 409 },
) {
  [HttpServerRespondable.symbol]() {
    return HttpServerResponse.schemaJson(MachineHttpConflictError)(this, { status: 409 });
  }
}

export class MachineCloudEndpointUnavailableError extends Schema.TaggedError<MachineCloudEndpointUnavailableError>()(
  "MachineCloudEndpointUnavailableError",
  {
    message: Schema.String,
    endpointRuntimeStatus: Schema.Unknown,
  },
  { httpApiStatus: 503 },
) {
  [HttpServerRespondable.symbol]() {
    return HttpServerResponse.schemaJson(MachineCloudEndpointUnavailableError)(this, {
      status: 503,
    });
  }
}
const MachineSessionCreationErrors = [MachineAuthInvalidError, MachineInternalError] as const;
const MachineTokenExchangeErrors = [
  MachineRequestInvalidError,
  MachineAuthInvalidError,
  MachineInternalError,
] as const;
const MachineScopedOperationErrors = [MachineScopeRequiredError, MachineInternalError] as const;
const MachinePairingCredentialErrors = [
  MachineRequestInvalidError,
  ...MachineScopedOperationErrors,
] as const;
const MachineSessionRevokeErrors = [
  MachineScopeRequiredError,
  MachineOperationForbiddenError,
  MachineInternalError,
] as const;
const MachineProjectSnapshotErrors = [MachineScopeRequiredError, MachineInternalError] as const;
const MachineOrchestrationSnapshotErrors = [
  MachineScopeRequiredError,
  MachineInternalError,
] as const;
const MachineOrchestrationThreadSnapshotErrors = [
  MachineScopeRequiredError,
  MachineResourceNotFoundError,
  MachineInternalError,
] as const;
const MachineProjectMutationErrors = [
  MachineRequestInvalidError,
  MachineScopeRequiredError,
  MachineInternalError,
] as const;

export interface MachineSessionPrincipalShape {
  readonly sessionId: AuthSessionId;
  readonly subject: string;
  readonly method: ServerAuthSessionMethod;
  readonly scopes: ReadonlySet<AuthMachineScope>;
  readonly proofKeyThumbprint?: string;
  readonly expiresAt?: DateTime.DateTime;
}

export class MachineAuthenticatedPrincipal extends Context.Service<
  MachineAuthenticatedPrincipal,
  MachineSessionPrincipalShape
>()("@vekrel/contracts/machineHttp/MachineAuthenticatedPrincipal") {}

export class MachineAuthenticatedAuth extends HttpApiMiddleware.Service<
  MachineAuthenticatedAuth,
  { provides: MachineAuthenticatedPrincipal }
>()("MachineAuthenticatedAuth", {
  error: MachineAuthenticationErrors,
}) {}

const MachineHttpCloudErrors = [
  MachineHttpBadRequestError,
  MachineHttpUnauthorizedError,
  MachineHttpForbiddenError,
  MachineHttpConflictError,
  MachineHttpInternalServerError,
  MachineScopeRequiredError,
] as const;

export const MachineCloudRelayConfigResult = Schema.Struct({
  ok: Schema.Boolean,
  endpointRuntimeStatus: Schema.Unknown,
});
export type MachineCloudRelayConfigResult = typeof MachineCloudRelayConfigResult.Type;

export const MachineConnectReachabilityCause = Schema.Literals([
  "not_linked",
  "connector_not_running",
  "connector_not_ready",
  "edge_unreachable",
  "no_connector",
  "origin_unreachable",
  "dns_failed",
  "timeout",
  "unexpected_response",
]);
export type MachineConnectReachabilityCause = typeof MachineConnectReachabilityCause.Type;

/** Whether other devices can reach this machine through its T3 Connect tunnel. */
export const MachineConnectReachability = Schema.Union([
  Schema.Struct({
    status: Schema.Literal("reachable"),
    checkedAt: Schema.String,
    /** Null when cloudflared did not report a count. */
    readyConnections: Schema.NullOr(Schema.Int),
    protocol: Schema.NullOr(Schema.String),
  }),
  Schema.Struct({
    status: Schema.Literal("unreachable"),
    checkedAt: Schema.String,
    cause: MachineConnectReachabilityCause,
    reason: Schema.String,
    hint: Schema.NullOr(Schema.String),
    fix: Schema.String,
  }),
]);
export type MachineConnectReachability = typeof MachineConnectReachability.Type;

/** "Tunnel connected · 4 connections · QUIC" for a healthy check. */
export function formatConnectReachable(
  result: Extract<MachineConnectReachability, { status: "reachable" }>,
): string {
  const count = result.readyConnections;
  return [
    "Tunnel connected",
    ...(count === null ? [] : [`${count} connection${count === 1 ? "" : "s"}`]),
    ...(result.protocol ? [result.protocol.toUpperCase()] : []),
  ].join(" · ");
}

/**
 * Sections of docs/troubleshooting.md. Each is the GitHub anchor of a
 * heading there; the test fails when a heading is renamed without updating this list.
 */
export const VEKREL_CONNECT_TROUBLESHOOTING_SECTIONS = [
  "the-hosts-tunnel-is-offline",
  "port-7844-is-blocked-on-the-host",
  "the-tunnel-works-but-vekrel-does-not-answer",
  "the-vekrel-connect-address-does-not-resolve-or-answer",
  "this-device-cant-reach-vekrel-connect",
  "sign-in-account-or-link-problems",
  "the-clock-is-off",
  "the-host-runs-an-older-vekrel",
  "vekrel-connect-returned-an-error",
] as const;
export type T3ConnectTroubleshootingSection =
  (typeof VEKREL_CONNECT_TROUBLESHOOTING_SECTIONS)[number];

/** Clients, the CLI, and server logs link here as "More info". */
export function t3ConnectTroubleshootingUrl(section: T3ConnectTroubleshootingSection): string {
  return `https://github.com/vekrel/engine/blob/main/docs/troubleshooting.md#${section}`;
}

const REACHABILITY_SECTION = {
  not_linked: "sign-in-account-or-link-problems",
  connector_not_running: "the-hosts-tunnel-is-offline",
  connector_not_ready: "the-hosts-tunnel-is-offline",
  no_connector: "the-hosts-tunnel-is-offline",
  edge_unreachable: "port-7844-is-blocked-on-the-host",
  origin_unreachable: "the-tunnel-works-but-vekrel-does-not-answer",
  unexpected_response: "the-tunnel-works-but-vekrel-does-not-answer",
  dns_failed: "the-vekrel-connect-address-does-not-resolve-or-answer",
  timeout: "the-vekrel-connect-address-does-not-resolve-or-answer",
} satisfies Record<MachineConnectReachabilityCause, T3ConnectTroubleshootingSection>;

export function connectReachabilityHelpUrl(cause: MachineConnectReachabilityCause): string {
  return t3ConnectTroubleshootingUrl(REACHABILITY_SECTION[cause]);
}

export const MachineCloudLinkStateResult = Schema.Struct({
  linked: Schema.Boolean,
  cloudUserId: Schema.NullOr(Schema.String),
  relayUrl: Schema.NullOr(Schema.String),
  relayIssuer: Schema.NullOr(Schema.String),
  managedTunnelActive: Schema.optional(Schema.Boolean),
  publishAgentActivity: Schema.Boolean,
  holdWebhooksWhileOffline: Schema.optional(Schema.Boolean),
  linkRevoked: Schema.optional(Schema.Boolean),
  /** The latest check the server ran on its own, absent until one finishes. */
  reachability: Schema.optional(MachineConnectReachability),
});
export type MachineCloudLinkStateResult = typeof MachineCloudLinkStateResult.Type;

export const MachineCloudPreferencesRequest = Schema.Struct({
  publishAgentActivity: Schema.Boolean,
  holdWebhooksWhileOffline: Schema.optional(Schema.Boolean),
});
export type MachineCloudPreferencesRequest = typeof MachineCloudPreferencesRequest.Type;

export const AuthPairingLinkRevokeResult = Schema.Struct({
  revoked: Schema.Boolean,
});
export type AuthPairingLinkRevokeResult = typeof AuthPairingLinkRevokeResult.Type;

export const AuthClientSessionRevokeResult = Schema.Struct({
  revoked: Schema.Boolean,
});
export type AuthClientSessionRevokeResult = typeof AuthClientSessionRevokeResult.Type;

export const AuthOtherClientSessionsRevokeResult = Schema.Struct({
  revokedCount: Schema.Number,
});
export type AuthOtherClientSessionsRevokeResult = typeof AuthOtherClientSessionsRevokeResult.Type;

class MachineMetadataHttpApi extends HttpApiGroup.make("metadata").add(
  HttpApiEndpoint.get("descriptor", "/.well-known/vekrel/machine", {
    success: ExecutionMachineDescriptor,
  }),
) {}

class MachineAuthHttpApi extends HttpApiGroup.make("auth")
  .add(
    HttpApiEndpoint.get("session", "/api/auth/session", {
      headers: OptionalBearerHeaders,
      success: AuthSessionState,
      error: [MachineInternalError],
    }),
  )
  .add(
    HttpApiEndpoint.post("browserSession", "/api/auth/browser-session", {
      payload: AuthBrowserSessionRequest,
      success: AuthBrowserSessionResult,
      error: MachineSessionCreationErrors,
    }),
  )
  .add(
    HttpApiEndpoint.post("token", "/oauth/token", {
      headers: OptionalDpopProofHeaders,
      payload: AuthTokenExchangeRequest,
      success: AuthAccessTokenResult,
      error: MachineTokenExchangeErrors,
    }),
  )
  .add(
    HttpApiEndpoint.post("webSocketTicket", "/api/auth/websocket-ticket", {
      headers: OptionalBearerHeaders,
      success: AuthWebSocketTicketResult,
      error: [MachineInternalError],
    }).middleware(MachineAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("pairingCredential", "/api/auth/pairing-token", {
      headers: OptionalBearerHeaders,
      payload: AuthCreatePairingCredentialInput,
      success: AuthPairingCredentialResult,
      error: MachinePairingCredentialErrors,
    }).middleware(MachineAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get("pairingLinks", "/api/auth/pairing-links", {
      headers: OptionalBearerHeaders,
      success: Schema.Array(AuthPairingLink),
      error: MachineScopedOperationErrors,
    }).middleware(MachineAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("revokePairingLink", "/api/auth/pairing-links/revoke", {
      headers: OptionalBearerHeaders,
      payload: AuthRevokePairingLinkInput,
      success: AuthPairingLinkRevokeResult,
      error: MachineScopedOperationErrors,
    }).middleware(MachineAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get("clients", "/api/auth/clients", {
      headers: OptionalBearerHeaders,
      success: Schema.Array(AuthClientSession),
      error: MachineScopedOperationErrors,
    }).middleware(MachineAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("revokeClient", "/api/auth/clients/revoke", {
      headers: OptionalBearerHeaders,
      payload: AuthRevokeClientSessionInput,
      success: AuthClientSessionRevokeResult,
      error: MachineSessionRevokeErrors,
    }).middleware(MachineAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("revokeOtherClients", "/api/auth/clients/revoke-others", {
      headers: OptionalBearerHeaders,
      success: AuthOtherClientSessionsRevokeResult,
      error: MachineScopedOperationErrors,
    }).middleware(MachineAuthenticatedAuth),
  ) {}

class MachineMcpOAuthHttpApi extends HttpApiGroup.make("mcpOAuth")
  .add(
    HttpApiEndpoint.get("protectedResource", "/.well-known/oauth-protected-resource", {
      success: AuthMcpProtectedResourceMetadata,
    }),
  )
  .add(
    HttpApiEndpoint.get("mcpProtectedResource", "/.well-known/oauth-protected-resource/mcp", {
      success: AuthMcpProtectedResourceMetadata,
    }),
  )
  .add(
    HttpApiEndpoint.get("authorizationServer", "/.well-known/oauth-authorization-server", {
      success: AuthMcpAuthorizationServerMetadata,
    }),
  )
  .add(
    HttpApiEndpoint.post("register", "/oauth/mcp/register", {
      payload: AuthMcpClientRegistration,
      success: AuthMcpRegisteredClient,
      error: AuthMcpRegistrationError,
    }),
  )
  .add(HttpApiEndpoint.get("authorize", "/oauth/mcp/authorize"))
  .add(
    HttpApiEndpoint.post("approval", "/oauth/mcp/approval", {
      payload: AuthMcpAuthorizationRequest,
      success: [AuthMcpApprovalDetails, AuthMcpApprovalRedirect],
      error: AuthMcpApprovalError,
    }),
  )
  .add(
    HttpApiEndpoint.post("decision", "/oauth/mcp/decision", {
      payload: AuthMcpApprovalDecisionRequest,
      success: AuthMcpApprovalRedirect,
      error: AuthMcpApprovalError,
    }),
  )
  .add(
    HttpApiEndpoint.post("token", "/oauth/mcp/token", {
      payload: AuthMcpTokenRequest,
      success: AuthMcpTokenResult,
      error: AuthMcpTokenError,
    }),
  ) {}

const MachineOrchestrationThreadSnapshotParams = Schema.Struct({
  threadId: ThreadId,
});

const MachineOrchestrationThreadBoundedSnapshotQuery = Schema.Struct({
  compactTurnItems: Schema.optionalKey(Schema.String),
});

const MachineOrchestrationThreadHistoryQuery = Schema.Struct({
  cursor: TrimmedNonEmptyString,
  throughEntryId: Schema.optional(TrimmedNonEmptyString),
  view: Schema.optional(Schema.Literals(["conversation", "activity"])),
});

const MachineOrchestrationThreadHistoryErrors = [
  MachineRequestInvalidError,
  MachineScopeRequiredError,
  MachineResourceNotFoundError,
  MachineInternalError,
] as const;

class MachineOrchestrationHttpApi extends HttpApiGroup.make("orchestration")
  .add(
    HttpApiEndpoint.get("shellSnapshot", "/api/orchestration/shell", {
      headers: OrchestrationProtocolHeaders,
      success: OrchestrationV2ShellSnapshot,
      error: MachineOrchestrationSnapshotErrors,
    }).middleware(MachineAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get("threadSnapshot", "/api/orchestration/threads/:threadId", {
      headers: OrchestrationProtocolHeaders,
      params: MachineOrchestrationThreadSnapshotParams,
      success: OrchestrationV2ThreadDetailSnapshot,
      error: MachineOrchestrationThreadSnapshotErrors,
    }).middleware(MachineAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get("threadTranscript", "/api/orchestration/threads/:threadId/transcript", {
      headers: OrchestrationProtocolHeaders,
      params: MachineOrchestrationThreadSnapshotParams,
      success: OrchestrationV2ThreadTranscript,
      error: [MachineRequestInvalidError, ...MachineOrchestrationThreadSnapshotErrors],
    }).middleware(MachineAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get("threadBoundedSnapshot", "/api/orchestration/threads/:threadId/bounded", {
      headers: OrchestrationProtocolHeaders,
      params: MachineOrchestrationThreadSnapshotParams,
      query: MachineOrchestrationThreadBoundedSnapshotQuery,
      success: OrchestrationV2ThreadBoundedSnapshot,
      error: MachineOrchestrationThreadSnapshotErrors,
    }).middleware(MachineAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get("threadHistoryPage", "/api/orchestration/threads/:threadId/history", {
      headers: OrchestrationProtocolHeaders,
      params: MachineOrchestrationThreadSnapshotParams,
      query: MachineOrchestrationThreadHistoryQuery,
      success: OrchestrationV2ThreadHistoryPage,
      error: MachineOrchestrationThreadHistoryErrors,
    }).middleware(MachineAuthenticatedAuth),
  ) {}

class MachineProjectsHttpApi extends HttpApiGroup.make("projects")
  .add(
    HttpApiEndpoint.get("snapshot", "/api/projects", {
      headers: OptionalBearerHeaders,
      success: ProjectSnapshot,
      error: MachineProjectSnapshotErrors,
    }).middleware(MachineAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("mutate", "/api/projects/mutate", {
      headers: OptionalBearerHeaders,
      payload: ProjectMutation,
      success: Project,
      error: MachineProjectMutationErrors,
    }).middleware(MachineAuthenticatedAuth),
  ) {}

class MachinePullRequestsHttpApi extends HttpApiGroup.make("pullRequests").add(
  HttpApiEndpoint.post("diff", "/api/pull-requests/diff", {
    headers: OptionalBearerHeaders,
    payload: PullRequestDiffInput,
    success: PullRequestDiffResult,
    error: [
      PullRequestUnavailableError,
      PullRequestOperationError,
      MachineAuthInvalidError,
      MachineScopeRequiredError,
      MachineInternalError,
    ],
  }).middleware(MachineAuthenticatedAuth),
) {}

class MachineConnectHttpApi extends HttpApiGroup.make("connect")
  .add(
    HttpApiEndpoint.post("linkProof", "/api/connect/link-proof", {
      headers: OptionalBearerHeaders,
      payload: RelayLinkProofRequest,
      success: RelayMachineLinkProof,
      error: MachineHttpCloudErrors,
    }).middleware(MachineAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("relayConfig", "/api/connect/relay-config", {
      headers: OptionalBearerHeaders,
      payload: RelayMachineConfigRequest,
      success: MachineCloudRelayConfigResult,
      error: [...MachineHttpCloudErrors, MachineCloudEndpointUnavailableError],
    }).middleware(MachineAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.get("linkState", "/api/connect/link-state", {
      headers: OptionalBearerHeaders,
      success: MachineCloudLinkStateResult,
      error: MachineHttpCloudErrors,
    }).middleware(MachineAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("checkReachability", "/api/connect/reachability", {
      headers: OptionalBearerHeaders,
      success: MachineConnectReachability,
      error: MachineHttpCloudErrors,
    }).middleware(MachineAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("unlink", "/api/connect/unlink", {
      headers: OptionalBearerHeaders,
      success: MachineCloudRelayConfigResult,
      error: MachineHttpCloudErrors,
    }).middleware(MachineAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("preferences", "/api/connect/preferences", {
      headers: OptionalBearerHeaders,
      payload: MachineCloudPreferencesRequest,
      success: MachineCloudLinkStateResult,
      error: MachineHttpCloudErrors,
    }).middleware(MachineAuthenticatedAuth),
  )
  .add(
    HttpApiEndpoint.post("health", "/api/vekrel-connect/health", {
      payload: RelayCloudMachineHealthRequest,
      success: RelayMachineHealthResponse,
      error: MachineHttpCloudErrors,
    }),
  )
  .add(
    HttpApiEndpoint.post("mintCredential", "/api/connect/mint-credential", {
      payload: RelayCloudMintCredentialRequest,
      success: RelayMachineMintResponse,
      error: MachineHttpCloudErrors,
    }),
  )
  .add(
    HttpApiEndpoint.post("t3MintCredential", "/api/vekrel-connect/mint-credential", {
      payload: RelayCloudMintCredentialRequest,
      success: RelayMachineMintResponse,
      error: MachineHttpCloudErrors,
    }),
  ) {}

/**
 * Public entry point for webhook tasks. Unauthenticated by design: the token
 * in the path, and an optional body signature, are the credential. The handler
 * reads the raw body itself so a signature is checked over the exact bytes.
 */
const WebhookParams = Schema.Struct({
  hookId: TrimmedNonEmptyString,
  token: TrimmedNonEmptyString,
});
const WebhookAccepted = Schema.Struct({ deliveryId: TrimmedNonEmptyString }).pipe(
  HttpApiSchema.status(202),
);
const webhookEndpoint = {
  params: WebhookParams,
  success: WebhookAccepted,
} as const;
const WEBHOOK_PATH = "/api/hooks/:hookId/:token";

class MachineWebhooksHttpApi extends HttpApiGroup.make("webhooks")
  .add(HttpApiEndpoint.post("webhookPost", WEBHOOK_PATH, webhookEndpoint))
  .add(HttpApiEndpoint.put("webhookPut", WEBHOOK_PATH, webhookEndpoint))
  .add(HttpApiEndpoint.patch("webhookPatch", WEBHOOK_PATH, webhookEndpoint))
  .add(HttpApiEndpoint.get("webhookGet", WEBHOOK_PATH, webhookEndpoint)) {}

export class MachineHttpApi extends HttpApi.make("machine")
  .add(MachineMetadataHttpApi)
  .add(MachineAuthHttpApi)
  .add(MachineMcpOAuthHttpApi)
  .add(MachineOrchestrationHttpApi)
  .add(MachinePullRequestsHttpApi)
  .add(MachineProjectsHttpApi)
  .add(MachineConnectHttpApi)
  .add(MachineWebhooksHttpApi) {}
