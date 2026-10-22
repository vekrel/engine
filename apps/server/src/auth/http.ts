import {
  authScopeRequiredResponse,
  AuthAccessReadScope,
  AuthAccessWriteScope,
  AuthStandardClientScopes,
  AuthGrantScope,
  MachineAuthInvalidError,
  type MachineAuthInvalidReason,
  MachineHttpApi,
  MachineInternalError,
  type MachineInternalErrorReason,
  MachineOperationForbiddenError,
  MachineRequestInvalidError,
  type MachineRequestInvalidReason,
  MachineResourceNotFoundError,
  type MachineResourceNotFoundReason,
  MachineScopeRequiredError,
  MachineAuthenticatedAuth,
  MachineAuthenticatedPrincipal,
} from "@vekrel/contracts";
import type { AuthMachineScope, DpopFailureReason } from "@vekrel/contracts";
import { parseOAuthScope } from "@vekrel/shared/oauthScope";
import { causeErrorTag } from "@vekrel/shared/observability";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { identity } from "effect/Function";
import * as Layer from "effect/Layer";
import * as Cookies from "effect/http/Cookies";
import * as HttpEffect from "effect/http/HttpEffect";
import { HttpServerRequest, HttpServerResponse } from "effect/http";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";

import * as MachineAuth from "./MachineAuth.ts";
import * as SessionStore from "./SessionStore.ts";
import { traceAuthenticatedRelayRequest, traceRelayRequest } from "../cloud/traceRelayRequest.ts";
import { deriveAuthClientMetadata } from "./utils.ts";
import { verifyRequestDpopProof } from "./dpop.ts";

const CREDENTIAL_RESPONSE_HEADERS = {
  "cache-control": "no-store",
  pragma: "no-cache",
} as const;

const appendCredentialResponseHeaders = HttpEffect.appendPreResponseHandler((_request, response) =>
  Effect.succeed(HttpServerResponse.setHeaders(response, CREDENTIAL_RESPONSE_HEADERS)),
);

const appendDpopChallengeHeader = HttpEffect.appendPreResponseHandler((_request, response) =>
  Effect.succeed(HttpServerResponse.setHeader(response, "www-authenticate", "DPoP")),
);

const appendDpopChallengeOnUnauthorized =
  /* Request instrumentation already owns the span; keep this helper in that context. */
  Effect.fnUntraced(function* (error: MachineAuthInvalidError) {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const usesDpop =
      (request.originalUrl.startsWith("/oauth/token") && request.headers.dpop !== undefined) ||
      request.headers.authorization?.startsWith("DPoP ") === true;
    if (usesDpop) {
      yield* appendDpopChallengeHeader;
    }
    return yield* error;
  });

const currentMachineTraceId = Effect.currentParentSpan.pipe(
  Effect.map((span) => span.traceId),
  Effect.orElseSucceed(() => "unavailable"),
);

export const annotateMachineRequest =
  /* Request instrumentation already owns the span; keep this helper in that context. */
  Effect.fnUntraced(function* (endpoint: string) {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const url = HttpServerRequest.toURL(request);
    const traceId = yield* currentMachineTraceId;

    yield* Effect.addFinalizer((exit) =>
      exit._tag === "Failure"
        ? Effect.logWarning("machine api request failed", {
            endpoint,
            traceId,
            errorTag: causeErrorTag(exit.cause),
            cause: exit.cause,
          })
        : Effect.void,
    );
    yield* Effect.annotateLogsScoped({ "machine.endpoint": endpoint, traceId });
    yield* Effect.annotateCurrentSpan({
      "machine.endpoint": endpoint,
      "http.request.method": request.method,
      "url.path": url._tag === "Some" ? url.value.pathname : "unknown",
    });
  });

export function failMachineAuthInvalid(
  reason: MachineAuthInvalidReason,
  dpopFailureReason?: DpopFailureReason,
) {
  return currentMachineTraceId.pipe(
    Effect.flatMap((traceId) =>
      Effect.fail(
        new MachineAuthInvalidError({
          code: "auth_invalid",
          reason,
          ...(dpopFailureReason === undefined ? {} : { dpopFailureReason }),
          traceId,
        }),
      ),
    ),
  );
}

export function failMachineInvalidRequest(reason: MachineRequestInvalidReason) {
  return currentMachineTraceId.pipe(
    Effect.flatMap((traceId) =>
      Effect.fail(new MachineRequestInvalidError({ code: "invalid_request", reason, traceId })),
    ),
  );
}

export function failMachineScopeRequired(requiredScope: AuthMachineScope) {
  return currentMachineTraceId.pipe(
    Effect.flatMap((traceId) =>
      Effect.fail(
        new MachineScopeRequiredError({
          code: "insufficient_scope",
          ...authScopeRequiredResponse(requiredScope),
          traceId,
        }),
      ),
    ),
  );
}

function failMachineOperationForbidden(reason: "current_session_revoke_not_allowed") {
  return currentMachineTraceId.pipe(
    Effect.flatMap((traceId) =>
      Effect.fail(
        new MachineOperationForbiddenError({
          code: "operation_forbidden",
          reason,
          traceId,
        }),
      ),
    ),
  );
}

export function failMachineNotFound(reason: MachineResourceNotFoundReason) {
  return currentMachineTraceId.pipe(
    Effect.flatMap((traceId) =>
      Effect.fail(new MachineResourceNotFoundError({ code: "not_found", reason, traceId })),
    ),
  );
}

/**
 * `<img>` and WebSocket cannot set headers, so media routes (device hub, preview stream)
 * authenticate the way the `/ws` upgrade does: a cookie for browser sessions, or a short-lived
 * `wsTicket` minted over authenticated HTTP for bearer and DPoP clients.
 */
export const authenticateMediaRequest =
  /* Request instrumentation already owns the span; keep this helper in that context. */
  Effect.fnUntraced(function* (requiredScope: AuthMachineScope) {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const serverAuth = yield* MachineAuth.MachineAuth;
    const session = yield* serverAuth.authenticateWebSocketUpgrade(request).pipe(
      Effect.catch((error) =>
        Effect.gen(function* () {
          if (MachineAuth.isServerAuthCredentialError(error)) {
            return yield* failMachineAuthInvalid(
              MachineAuth.serverAuthCredentialReason(error),
              MachineAuth.serverAuthDpopFailureReason(error),
            );
          }
          return yield* failMachineInternal("internal_error", error);
        }),
      ),
    );
    if (!session.scopes.includes(requiredScope)) {
      return yield* failMachineScopeRequired(requiredScope);
    }
    return session;
  });

export const failMachineInternal =
  /* Request instrumentation already owns the span; keep this helper in that context. */
  Effect.fnUntraced(function* (reason: MachineInternalErrorReason, error?: unknown) {
    const traceId = yield* currentMachineTraceId;
    if (error !== undefined) {
      yield* Effect.logError("machine api operation failed", {
        reason,
        traceId,
        cause: error,
      });
    }
    return yield* new MachineInternalError({ code: "internal_error", reason, traceId });
  });

const appendSessionCookie = (cookieName: string, token: string, expiresAt: DateTime.DateTime) =>
  Effect.fromResult(
    Cookies.set(Cookies.empty, cookieName, token, {
      expires: DateTime.toDate(expiresAt),
      httpOnly: true,
      path: "/",
      sameSite: "lax",
    }),
  ).pipe(
    Effect.catch(() => failMachineInternal("browser_session_cookie_failed")),
    Effect.flatMap((cookies) =>
      HttpEffect.appendPreResponseHandler((_request, response) =>
        Effect.succeed(HttpServerResponse.mergeCookies(response, cookies)),
      ),
    ),
  );

export const requireMachineScope = Effect.fn("machine.auth.requireScope")(function* (
  scope: AuthMachineScope,
) {
  const session = yield* MachineAuthenticatedPrincipal;
  if (!session.scopes.has(scope)) {
    return yield* failMachineScopeRequired(scope);
  }
  return session;
});

export const layerAuthenticatedAuth = Layer.effect(
  MachineAuthenticatedAuth,
  Effect.gen(function* () {
    const serverAuth = yield* MachineAuth.MachineAuth;
    return (httpEffect) =>
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const startTime = yield* Clock.currentTimeNanos;
        const session = yield* serverAuth.authenticateHttpRequest(request).pipe(
          Effect.catchIf(MachineAuth.isServerAuthCredentialError, (error) =>
            failMachineAuthInvalid(
              MachineAuth.serverAuthCredentialReason(error),
              MachineAuth.serverAuthDpopFailureReason(error),
            ),
          ),
          Effect.catchIf(MachineAuth.isServerAuthInternalError, (error) =>
            failMachineInternal("internal_error", error),
          ),
        );
        const endTime = yield* Clock.currentTimeNanos;
        const handler = httpEffect.pipe(
          Effect.provideService(MachineAuthenticatedPrincipal, {
            ...session,
            scopes: new Set(session.scopes),
          }),
        );
        return yield* session.subject === "cloud-connect"
          ? traceAuthenticatedRelayRequest(handler, { startTime, endTime })
          : handler;
      }).pipe(Effect.catchTags({ MachineAuthInvalidError: appendDpopChallengeOnUnauthorized }));
  }),
);

export const layer = HttpApiBuilder.group(
  MachineHttpApi,
  "auth",
  Effect.fnUntraced(function* (handlers) {
    const serverAuth = yield* MachineAuth.MachineAuth;
    const sessions = yield* SessionStore.SessionStore;

    return handlers
      .handle(
        "session",
        Effect.fn("machine.auth.session")(
          function* (args) {
            yield* annotateMachineRequest(args.endpoint.name);
            const request = yield* HttpServerRequest.HttpServerRequest;
            const result = yield* serverAuth.getSessionState(request);
            const credential = MachineAuth.selectRequestCredential(
              request,
              sessions.cookieName,
              sessions.legacyCookieName,
            );
            if (
              credential?.source === "legacy-cookie" &&
              result.authenticated &&
              result.sessionMethod === "browser-session-cookie" &&
              result.expiresAt
            ) {
              yield* appendSessionCookie(sessions.cookieName, credential.token, result.expiresAt);
              yield* appendCredentialResponseHeaders;
            }
            return result;
          },
          Effect.catchIf(MachineAuth.isServerAuthInternalError, (error) =>
            failMachineInternal("internal_error", error),
          ),
        ),
      )
      .handle(
        "browserSession",
        Effect.fn("machine.auth.browserSession")(
          function* (args) {
            yield* annotateMachineRequest(args.endpoint.name);
            const request = yield* HttpServerRequest.HttpServerRequest;
            const previousCredential = MachineAuth.selectRequestCredential(
              request,
              sessions.cookieName,
              sessions.legacyCookieName,
            );
            const result = yield* serverAuth.createBrowserSession(
              args.payload.credential,
              deriveAuthClientMetadata({ request }),
              previousCredential &&
                MachineAuth.isBrowserCookieCredential(previousCredential.source)
                ? previousCredential.token
                : undefined,
            );
            const cookieName = result.cookieName ?? sessions.cookieName;
            const selectedCookie = yield* Effect.fromResult(
              Cookies.set(Cookies.empty, cookieName, result.sessionToken, {
                expires: DateTime.toDate(result.response.expiresAt),
                httpOnly: true,
                path: "/",
                sameSite: "lax",
              }),
            ).pipe(Effect.catch(() => failMachineInternal("browser_session_cookie_failed")));
            const sessionCookies = result.expireNormalCookie
              ? yield* Effect.fromResult(
                  Cookies.expireCookie(selectedCookie, sessions.cookieName, {
                    httpOnly: true,
                    path: "/",
                    sameSite: "lax",
                  }),
                ).pipe(Effect.catch(() => failMachineInternal("browser_session_cookie_failed")))
              : selectedCookie;

            yield* HttpEffect.appendPreResponseHandler((_request, response) =>
              Effect.succeed(HttpServerResponse.mergeCookies(response, sessionCookies)),
            );
            yield* appendCredentialResponseHeaders;
            return result.response;
          },
          Effect.catchIf(MachineAuth.isServerAuthCredentialError, (error) =>
            failMachineAuthInvalid(
              MachineAuth.serverAuthCredentialReason(error),
              MachineAuth.serverAuthDpopFailureReason(error),
            ),
          ),
          Effect.catchIf(MachineAuth.isServerAuthInternalError, (error) =>
            failMachineInternal("browser_session_issuance_failed", error),
          ),
        ),
      )
      .handle(
        "token",
        Effect.fn("machine.auth.token")(
          function* (args) {
            yield* annotateMachineRequest(args.endpoint.name);
            const request = yield* HttpServerRequest.HttpServerRequest;
            const requestedScopes =
              args.payload.scope === undefined
                ? undefined
                : (parseOAuthScope(args.payload.scope)?.filter(Schema.is(AuthGrantScope)) ?? null);
            if (requestedScopes === null || requestedScopes?.length === 0) {
              return yield* failMachineInvalidRequest("invalid_scope");
            }
            const proofKeyThumbprint = args.headers.dpop
              ? yield* verifyRequestDpopProof({ request }).pipe(
                  Effect.catchIf(MachineAuth.isServerAuthCredentialError, (error) =>
                    appendDpopChallengeHeader.pipe(
                      Effect.andThen(
                        failMachineAuthInvalid(
                          "invalid_credential",
                          MachineAuth.serverAuthDpopFailureReason(error),
                        ),
                      ),
                    ),
                  ),
                  Effect.catchIf(MachineAuth.isServerAuthInternalError, (error) =>
                    failMachineInternal("access_token_issuance_failed", error),
                  ),
                )
              : undefined;
            yield* appendCredentialResponseHeaders;
            return yield* serverAuth.exchangeBootstrapCredentialForAccessToken(
              args.payload.subject_token,
              requestedScopes,
              deriveAuthClientMetadata({
                request,
                presented: {
                  ...(args.payload.client_label ? { label: args.payload.client_label } : {}),
                  ...(args.payload.client_device_type
                    ? { deviceType: args.payload.client_device_type }
                    : {}),
                  ...(args.payload.client_os ? { os: args.payload.client_os } : {}),
                },
              }),
              proofKeyThumbprint ? { proofKeyThumbprint } : undefined,
            );
          },
          traceRelayRequest,
          Effect.catchIf(MachineAuth.isServerAuthCredentialError, (error) =>
            failMachineAuthInvalid(
              MachineAuth.serverAuthCredentialReason(error),
              MachineAuth.serverAuthDpopFailureReason(error),
            ),
          ),
          Effect.catchIf(MachineAuth.isServerAuthInvalidRequestError, (error) =>
            failMachineInvalidRequest(MachineAuth.serverAuthInvalidRequestReason(error)),
          ),
          Effect.catchIf(MachineAuth.isServerAuthInternalError, (error) =>
            failMachineInternal("access_token_issuance_failed", error),
          ),
        ),
      )
      .handle(
        "webSocketTicket",
        Effect.fn("machine.auth.webSocketTicket")(
          function* (args) {
            yield* annotateMachineRequest(args.endpoint.name);
            const session = yield* MachineAuthenticatedPrincipal;
            yield* appendCredentialResponseHeaders;
            return yield* serverAuth.issueWebSocketTicket(session);
          },
          Effect.catchIf(MachineAuth.isServerAuthInternalError, (error) =>
            failMachineInternal("websocket_ticket_issuance_failed", error),
          ),
        ),
      )
      .handle(
        "pairingCredential",
        Effect.fn("machine.auth.pairingCredential")(
          function* (args) {
            yield* annotateMachineRequest(args.endpoint.name);
            const session = yield* requireMachineScope(AuthAccessWriteScope);
            const delegatedScopes = args.payload.scopes ?? AuthStandardClientScopes;
            if (
              delegatedScopes.length === 0 ||
              new Set<AuthMachineScope>(delegatedScopes).size !== delegatedScopes.length
            ) {
              return yield* failMachineInvalidRequest("invalid_scope");
            }
            for (const delegatedScope of delegatedScopes) {
              if (!session.scopes.has(delegatedScope)) {
                return yield* failMachineScopeRequired(delegatedScope);
              }
            }
            return yield* serverAuth.issuePairingCredential(args.payload);
          },
          Effect.catchIf(MachineAuth.isServerAuthInternalError, (error) =>
            failMachineInternal("pairing_credential_issuance_failed", error),
          ),
        ),
      )
      .handle(
        "pairingLinks",
        Effect.fn("machine.auth.pairingLinks")(
          function* (args) {
            yield* annotateMachineRequest(args.endpoint.name);
            yield* requireMachineScope(AuthAccessReadScope);
            return yield* serverAuth.listPairingLinks();
          },
          Effect.catchIf(MachineAuth.isServerAuthInternalError, (error) =>
            failMachineInternal("pairing_links_load_failed", error),
          ),
        ),
      )
      .handle(
        "revokePairingLink",
        Effect.fn("machine.auth.revokePairingLink")(
          function* (args) {
            yield* annotateMachineRequest(args.endpoint.name);
            yield* requireMachineScope(AuthAccessWriteScope);
            const revoked = yield* serverAuth.revokePairingLink(args.payload.id);
            return { revoked };
          },
          Effect.catchIf(MachineAuth.isServerAuthInternalError, (error) =>
            failMachineInternal("pairing_link_revoke_failed", error),
          ),
        ),
      )
      .handle(
        "clients",
        Effect.fn("machine.auth.clients")(
          function* (args) {
            yield* annotateMachineRequest(args.endpoint.name);
            const session = yield* requireMachineScope(AuthAccessReadScope);
            return yield* serverAuth.listClientSessions(session.sessionId);
          },
          Effect.catchIf(MachineAuth.isServerAuthInternalError, (error) =>
            failMachineInternal("client_sessions_load_failed", error),
          ),
        ),
      )
      .handle(
        "revokeClient",
        Effect.fn("machine.auth.revokeClient")(
          function* (args) {
            yield* annotateMachineRequest(args.endpoint.name);
            const session = yield* requireMachineScope(AuthAccessWriteScope);
            const revoked = yield* serverAuth.revokeClientSession(
              session.sessionId,
              args.payload.sessionId,
            );
            return { revoked };
          },
          Effect.catchTags({
            ServerAuthForbiddenOperationError: () =>
              failMachineOperationForbidden("current_session_revoke_not_allowed"),
          }),
          Effect.catchIf(MachineAuth.isServerAuthInternalError, (error) =>
            failMachineInternal("client_session_revoke_failed", error),
          ),
        ),
      )
      .handle(
        "revokeOtherClients",
        Effect.fn("machine.auth.revokeOtherClients")(
          function* (args) {
            yield* annotateMachineRequest(args.endpoint.name);
            const session = yield* requireMachineScope(AuthAccessWriteScope);
            const revokedCount = yield* serverAuth.revokeOtherClientSessions(session.sessionId);
            return { revokedCount };
          },
          Effect.catchIf(MachineAuth.isServerAuthInternalError, (error) =>
            failMachineInternal("client_session_revoke_failed", error),
          ),
        ),
      );
  }),
);
