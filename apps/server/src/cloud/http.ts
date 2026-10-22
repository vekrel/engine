import {
  AuthRelayReadScope,
  AuthRelayWriteScope,
  MachineCloudEndpointUnavailableError,
  MachineHttpApi,
  MachineHttpBadRequestError,
  MachineHttpConflictError,
  MachineHttpForbiddenError,
  MachineHttpInternalServerError,
  MachineHttpUnauthorizedError,
} from "@vekrel/contracts";
import * as Effect from "effect/Effect";
import * as HttpEffect from "effect/http/HttpEffect";
import { HttpServerRequest, HttpServerResponse } from "effect/http";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";

import { requireMachineScope } from "../auth/http.ts";
import * as MachineAuth from "../auth/MachineAuth.ts";
import * as CloudLink from "./CloudLink.ts";
import * as ConnectReachability from "./ConnectReachability.ts";
import type { RelayRequestError } from "./relayResponse.ts";
import { traceRelayRequest } from "./traceRelayRequest.ts";

const CLOUD_CREDENTIAL_RESPONSE_HEADERS = {
  "cache-control": "no-store",
  pragma: "no-cache",
} as const;

const appendCloudCredentialResponseHeaders = HttpEffect.appendPreResponseHandler(
  (_request, response) =>
    Effect.succeed(HttpServerResponse.setHeaders(response, CLOUD_CREDENTIAL_RESPONSE_HEADERS)),
);

const internalServerError = (error: { readonly message: string }, cause: unknown) =>
  Effect.logError(error.message, { cause }).pipe(
    Effect.andThen(Effect.fail(new MachineHttpInternalServerError({ message: error.message }))),
  );

const relayFailure = (error: RelayRequestError) => {
  const message = error.message;
  switch (error.rejection) {
    case "unauthorized":
      return Effect.fail(new MachineHttpUnauthorizedError({ message }));
    case "forbidden":
      return Effect.fail(new MachineHttpForbiddenError({ message }));
    case "rejected":
      return Effect.fail(new MachineHttpBadRequestError({ message }));
    case "unavailable":
      return Effect.fail(new MachineHttpInternalServerError({ message }));
  }
};

const badRequest = (error: { readonly message: string }) =>
  Effect.fail(new MachineHttpBadRequestError({ message: error.message }));
const unauthorized = (error: { readonly message: string }) =>
  Effect.fail(new MachineHttpUnauthorizedError({ message: error.message }));
const conflict = (error: { readonly message: string }) =>
  Effect.fail(new MachineHttpConflictError({ message: error.message }));

const connectErrorCases = {
  CloudLinkRelayConfigInvalidError: badRequest,
  CloudLinkOriginInvalidError: badRequest,
  CloudLinkNotLinkedError: badRequest,
  CloudLinkAccountMismatchError: conflict,
  CloudLinkAuthorizationMissingError: unauthorized,
  CloudLinkProofRejectedError: unauthorized,
  CloudLinkProofReplayedError: conflict,
  CloudLinkTunnelSupersededError: conflict,
  CloudLinkInternalError: (error: CloudLink.CloudLinkInternalError) =>
    internalServerError(error, error.cause),
  RelayRequestError: relayFailure,
} as const;

type ConnectFailure =
  | Exclude<CloudLink.CloudLinkError, CloudLink.CloudLinkEndpointUnavailableError>
  | MachineAuth.ServerAuthInternalError
  | RelayRequestError;

const toHttpError = <A, R>(effect: Effect.Effect<A, ConnectFailure, R>) =>
  effect.pipe(
    Effect.catchTags(connectErrorCases),
    Effect.catchIf(MachineAuth.isServerAuthInternalError, (error) =>
      internalServerError(error, error),
    ),
  );

const toHttpErrorOrUnavailable = <A, R>(
  effect: Effect.Effect<A, ConnectFailure | CloudLink.CloudLinkEndpointUnavailableError, R>,
) =>
  effect.pipe(
    Effect.catchTags({
      ...connectErrorCases,
      CloudLinkEndpointUnavailableError: (error) =>
        Effect.fail(
          new MachineCloudEndpointUnavailableError({
            message: error.message,
            endpointRuntimeStatus: error.endpointRuntimeStatus,
          }),
        ),
    }),
    Effect.catchIf(MachineAuth.isServerAuthInternalError, (error) =>
      internalServerError(error, error),
    ),
  );

export const layer = HttpApiBuilder.group(
  MachineHttpApi,
  "connect",
  Effect.fnUntraced(function* (handlers) {
    const cloudLink = yield* CloudLink.CloudLink;
    const reachability = yield* ConnectReachability.ConnectReachability;
    return handlers
      .handle("linkProof", ({ payload }) =>
        Effect.gen(function* () {
          yield* requireMachineScope(AuthRelayWriteScope);
          const request = yield* HttpServerRequest.HttpServerRequest;
          const proof = yield* toHttpError(cloudLink.linkProof(payload, request));
          yield* appendCloudCredentialResponseHeaders;
          return proof;
        }),
      )
      .handle("relayConfig", ({ payload }) =>
        requireMachineScope(AuthRelayWriteScope).pipe(
          Effect.andThen(toHttpErrorOrUnavailable(cloudLink.applyRelayConfig(payload))),
        ),
      )
      .handle("linkState", () =>
        requireMachineScope(AuthRelayReadScope).pipe(
          Effect.andThen(toHttpError(cloudLink.linkState())),
        ),
      )
      .handle("checkReachability", () =>
        requireMachineScope(AuthRelayReadScope).pipe(Effect.andThen(reachability.check)),
      )
      .handle("unlink", () =>
        requireMachineScope(AuthRelayWriteScope).pipe(
          Effect.andThen(toHttpError(cloudLink.unlink())),
        ),
      )
      .handle("preferences", ({ payload }) =>
        requireMachineScope(AuthRelayWriteScope).pipe(
          Effect.andThen(toHttpError(cloudLink.updatePreferences(payload))),
        ),
      )
      .handle("health", ({ payload }) =>
        toHttpError(cloudLink.answerHealthRequest(payload)).pipe(
          Effect.tap(() => appendCloudCredentialResponseHeaders),
        ),
      )
      .handle("mintCredential", ({ payload }) =>
        toHttpError(cloudLink.mintCredential(payload)).pipe(
          Effect.tap(() => appendCloudCredentialResponseHeaders),
        ),
      )
      .handle("t3MintCredential", ({ payload }) =>
        traceRelayRequest(
          toHttpError(cloudLink.mintCredential(payload)).pipe(
            Effect.tap(() => appendCloudCredentialResponseHeaders),
          ),
        ),
      );
  }),
);
