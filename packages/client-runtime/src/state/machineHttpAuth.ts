import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import {
  ORCHESTRATION_PROTOCOL_HEADER,
  ORCHESTRATION_PROTOCOL_VERSION_TEXT,
} from "@vekrel/contracts";
import * as Result from "effect/Result";
import { withRelayClientTracing } from "@vekrel/shared/relayTracing";
import { FetchHttpClient, type HttpMethod } from "effect/http";

import type { RemoteMachineAuthorization } from "../authorization/service.ts";
import type { PreparedConnection, PreparedHttpAuthorization } from "../connection/model.ts";
import type { ManagedRelayDpopSigner } from "../relay/managedRelay.ts";
import {
  executeMachineHttpRequest,
  makeMachineHttpApiGroupClient,
  makeMachineHttpApiUrlBuilder,
  RemoteMachineAuthFetchError,
  RemoteMachineAuthTimeoutError,
  type RemoteMachineRequestError,
} from "../rpc/http.ts";

export interface MachineHttpAuthHeaders {
  readonly authorization?: string;
  readonly dpop?: string;
}

export function withOrchestrationProtocolHeader(
  headers: MachineHttpAuthHeaders,
): MachineHttpAuthHeaders & {
  readonly [ORCHESTRATION_PROTOCOL_HEADER]: typeof ORCHESTRATION_PROTOCOL_VERSION_TEXT;
} {
  return {
    ...headers,
    [ORCHESTRATION_PROTOCOL_HEADER]: ORCHESTRATION_PROTOCOL_VERSION_TEXT,
  };
}

/**
 * Primary/local machines with no bearer or DPoP credential authenticate the
 * browser via a session cookie. A cross-origin `fetch` does not send cookies by
 * default, so those requests must opt into credentialed mode; bearer/DPoP
 * connections carry their credential in a header and need no cookies. Applied
 * per-request via `FetchHttpClient.RequestInit`, which the fetch client reads
 * from the fiber context at request time.
 */
const withMachineCredentials = <A, E, R>(
  authorization: PreparedHttpAuthorization | null,
  request: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> =>
  authorization === null
    ? request.pipe(Effect.provideService(FetchHttpClient.RequestInit, { credentials: "include" }))
    : request;

const buildMachineAuthHeaders = Effect.fn("machineHttpAuth.buildMachineAuthHeaders")(
  function* (
    authorization: PreparedHttpAuthorization | null,
    method: HttpMethod.HttpMethod,
    url: string,
    signer: Option.Option<ManagedRelayDpopSigner["Service"]>,
  ): Effect.fn.Return<MachineHttpAuthHeaders, RemoteMachineAuthFetchError> {
    if (authorization === null) {
      return {};
    }
    if (authorization._tag === "Bearer") {
      return { authorization: `Bearer ${authorization.token}` };
    }
    if (Option.isNone(signer)) {
      return yield* new RemoteMachineAuthFetchError({
        message: "No DPoP signer is available to authorize the machine request.",
        cause: authorization._tag,
      });
    }
    const proof = yield* signer.value
      .createProof({ method, url, accessToken: authorization.accessToken })
      .pipe(
        Effect.mapError(
          (cause) =>
            new RemoteMachineAuthFetchError({
              message: "Could not create the machine request authorization proof.",
              cause,
            }),
        ),
      );
    return { authorization: `DPoP ${authorization.accessToken}`, dpop: proof };
  },
);

/**
 * A DPoP request is T3 Connect work, so its span starts an exported trace that
 * the machine continues; its local caller's span would leave that trace
 * without a root.
 */
export const executeAuthenticatedMachineHttpRequest = <
  Group extends Parameters<typeof makeMachineHttpApiGroupClient>[1],
  A,
  E,
  R,
>(
  input: Parameters<typeof executeMachineRequest<Group, A, E, R>>[0],
) =>
  input.prepared.httpAuthorization?._tag === "Dpop"
    ? executeMachineRequest(input).pipe(
        Effect.withSpan(MACHINE_REQUEST_SPAN, { root: true }),
        withRelayClientTracing,
      )
    : executeMachineRequest(input).pipe(Effect.withSpan(MACHINE_REQUEST_SPAN));

const MACHINE_REQUEST_SPAN = "clientRuntime.state.executeAuthenticatedMachineHttpRequest";

const executeMachineRequest = Effect.fnUntraced(function* <
  Group extends Parameters<typeof makeMachineHttpApiGroupClient>[1],
  A,
  E,
  R,
>(input: {
  readonly prepared: PreparedConnection;
  readonly signer: Option.Option<ManagedRelayDpopSigner["Service"]>;
  readonly remoteAuthorization?: Option.Option<RemoteMachineAuthorization["Service"]>;
  readonly method: HttpMethod.HttpMethod;
  /**
   * The URL to sign, from the group's contract URL builder. It encodes path
   * params exactly like the request client, so the DPoP proof matches the
   * URL actually sent.
   */
  readonly url: (urls: ReturnType<typeof makeMachineHttpApiUrlBuilder>[Group]) => string;
  readonly timeoutMs: number;
  readonly group: Group;
  readonly request: (input: {
    readonly client: Effect.Success<ReturnType<typeof makeMachineHttpApiGroupClient<Group>>>;
    readonly headers: MachineHttpAuthHeaders;
  }) => Effect.Effect<A, E, R>;
  /** Some endpoints report rejected credentials in a successful response. */
  readonly isUnauthorizedResponse?: (response: NoInfer<A>) => boolean;
}): Effect.fn.Return<
  A,
  RemoteMachineRequestError,
  Effect.Services<ReturnType<typeof makeMachineHttpApiGroupClient<Group>>> | R
> {
  let httpBaseUrl = input.prepared.httpBaseUrl;
  const requestUrlFor = (baseUrl: string) =>
    input.url(makeMachineHttpApiUrlBuilder(baseUrl)[input.group]);
  return yield* Effect.gen(function* () {
    let rejectedAccessToken: string | undefined;
    for (;;) {
      let authorization = input.prepared.httpAuthorization;
      if (authorization?._tag === "Dpop") {
        const remote = input.remoteAuthorization;
        if (remote === undefined || Option.isNone(remote)) {
          return yield* new RemoteMachineAuthFetchError({
            message: "No relay authorization service is available for the machine request.",
            cause: input.prepared.target._tag,
          });
        }
        const current = yield* remote.value
          .authorizeDpopHttp({
            expectedMachineId: input.prepared.machineId,
            ...(rejectedAccessToken === undefined ? {} : { rejectedAccessToken }),
          })
          .pipe(
            Effect.mapError(
              (cause) =>
                new RemoteMachineAuthFetchError({
                  message: "Could not authorize the machine request.",
                  cause,
                }),
            ),
          );
        // A learned direct route keeps its own origin; only the token renews.
        if (input.prepared.target._tag === "RelayConnectionTarget") {
          httpBaseUrl = current.httpBaseUrl;
        }
        authorization = current.httpAuthorization;
      }

      const requestUrl = requestUrlFor(httpBaseUrl);
      const client = yield* makeMachineHttpApiGroupClient(httpBaseUrl, input.group);
      const headers = yield* buildMachineAuthHeaders(
        authorization,
        input.method,
        requestUrl,
        input.signer,
      );
      const result = yield* executeMachineHttpRequest(
        requestUrl,
        input.timeoutMs,
        withMachineCredentials(authorization, input.request({ client, headers })),
      ).pipe(Effect.result);

      if (Result.isFailure(result)) {
        if (
          authorization?._tag === "Dpop" &&
          rejectedAccessToken === undefined &&
          result.failure._tag === "MachineAuthInvalidError" &&
          result.failure.reason === "invalid_credential"
        ) {
          rejectedAccessToken = authorization.accessToken;
          continue;
        }
        return yield* result.failure;
      }

      if (
        authorization?._tag === "Dpop" &&
        input.isUnauthorizedResponse?.(result.success) === true
      ) {
        if (rejectedAccessToken === undefined) {
          rejectedAccessToken = authorization.accessToken;
          continue;
        }
        return yield* new RemoteMachineAuthFetchError({
          message: "The machine rejected the renewed session authorization.",
          cause: result.success,
        });
      }
      return result.success;
    }
  }).pipe(
    Effect.timeoutOrElse({
      duration: input.timeoutMs,
      orElse: () =>
        Effect.fail(
          new RemoteMachineAuthTimeoutError(requestUrlFor(httpBaseUrl), input.timeoutMs),
        ),
    }),
  );
});
