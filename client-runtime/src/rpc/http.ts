import {
  MachineHttpApi,
  MachineHttpCommonError,
  type MachineAuthInvalidError,
  type MachineInternalError,
  type MachineOperationForbiddenError,
  type MachineRequestInvalidError,
  type MachineResourceNotFoundError,
  type MachineScopeRequiredError,
} from "@vekrel/contracts";
import * as HttpObservability from "@vekrel/shared/httpObservability";
import * as Data from "effect/Data";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { FetchHttpClient, HttpClient, HttpClientError } from "effect/http";
import * as HttpApiClient from "effect/http-api/HttpApiClient";

const isMachineHttpCommonError = Schema.is(MachineHttpCommonError);

export class RemoteMachineAuthFetchError extends Data.TaggedError(
  "RemoteMachineAuthFetchError",
)<{
  readonly message: string;
  readonly cause: unknown;
}> {}

class RemoteMachineAuthInvalidJsonError extends Data.TaggedError(
  "RemoteMachineAuthInvalidJsonError",
)<{
  readonly message: string;
  readonly cause: unknown;
}> {}

export class RemoteMachineAuthUndeclaredStatusError extends Data.TaggedError(
  "RemoteMachineAuthUndeclaredStatusError",
)<{
  readonly message: string;
  readonly status: number;
  readonly requestUrl: string;
}> {
  constructor(requestUrl: string, status: number) {
    super({
      message: `Remote machine endpoint ${requestUrl} returned undeclared status ${status}.`,
      requestUrl,
      status,
    });
  }
}

export class RemoteMachineAuthTimeoutError extends Data.TaggedError(
  "RemoteMachineAuthTimeoutError",
)<{
  readonly message: string;
  readonly requestUrl: string;
  readonly timeoutMs: number;
}> {
  constructor(requestUrl: string, timeoutMs: number) {
    super({
      message: `Remote machine endpoint ${requestUrl} timed out after ${timeoutMs}ms.`,
      requestUrl,
      timeoutMs,
    });
  }
}

export type RemoteMachineRequestError =
  | MachineRequestInvalidError
  | MachineAuthInvalidError
  | MachineScopeRequiredError
  | MachineOperationForbiddenError
  | MachineResourceNotFoundError
  | MachineInternalError
  | RemoteMachineAuthFetchError
  | RemoteMachineAuthInvalidJsonError
  | RemoteMachineAuthUndeclaredStatusError
  | RemoteMachineAuthTimeoutError;

export const layerRemoteHttpClient = (
  fetchFn: typeof globalThis.fetch,
): Layer.Layer<HttpClient.HttpClient> =>
  Layer.merge(
    FetchHttpClient.layer.pipe(Layer.provide(Layer.succeed(FetchHttpClient.Fetch, fetchFn))),
    HttpObservability.layer,
  );

const remoteApiBaseUrl = (httpBaseUrl: string): string => {
  const url = new URL(httpBaseUrl);
  url.pathname = "/";
  url.search = "";
  url.hash = "";
  return url.toString();
};

export const makeMachineHttpApiClient = (httpBaseUrl: string) =>
  HttpApiClient.make(MachineHttpApi, {
    baseUrl: remoteApiBaseUrl(httpBaseUrl),
  });

export const makeMachineHttpApiGroupClient = <
  Group extends keyof typeof MachineHttpApi.groups,
>(
  httpBaseUrl: string,
  group: Group,
) =>
  Effect.flatMap(HttpClient.HttpClient, (httpClient) =>
    HttpApiClient.group(MachineHttpApi, {
      httpClient,
      group,
      baseUrl: remoteApiBaseUrl(httpBaseUrl),
    }),
  );

export const makeMachineHttpApiUrlBuilder = (httpBaseUrl: string) =>
  HttpApiClient.urlBuilder(MachineHttpApi, {
    baseUrl: remoteApiBaseUrl(httpBaseUrl),
  });

const failRemoteRequest = (
  requestUrl: string,
  cause: unknown,
): Effect.Effect<never, RemoteMachineRequestError> => {
  if (cause instanceof RemoteMachineAuthTimeoutError) {
    return Effect.fail(cause);
  }
  if (isMachineHttpCommonError(cause)) {
    return Effect.fail(cause);
  }
  if (Schema.isSchemaError(cause)) {
    return Effect.fail(
      new RemoteMachineAuthInvalidJsonError({
        message: `Remote machine endpoint returned an invalid response from ${requestUrl}.`,
        cause,
      }),
    );
  }
  if (HttpClientError.isHttpClientError(cause) && cause.response !== undefined) {
    const response = cause.response;
    if (response.status < 200 || response.status >= 300) {
      return Effect.fail(
        new RemoteMachineAuthUndeclaredStatusError(requestUrl, response.status),
      );
    }
    return Effect.fail(
      new RemoteMachineAuthInvalidJsonError({
        message: `Remote machine endpoint returned an invalid response from ${requestUrl}.`,
        cause,
      }),
    );
  }
  return Effect.fail(
    new RemoteMachineAuthFetchError({
      message: `Failed to fetch remote machine endpoint ${requestUrl} (${String(cause)}).`,
      cause,
    }),
  );
};

export const executeMachineHttpRequest = <A, E, R>(
  requestUrl: string,
  timeoutMs: number,
  request: Effect.Effect<A, E, R>,
): Effect.Effect<A, RemoteMachineRequestError, R> =>
  request.pipe(
    Effect.timeoutOption(Duration.millis(timeoutMs)),
    Effect.flatMap(
      Option.match({
        onNone: () => Effect.fail(new RemoteMachineAuthTimeoutError(requestUrl, timeoutMs)),
        onSome: Effect.succeed,
      }),
    ),
    Effect.catch((cause) => failRemoteRequest(requestUrl, cause)),
  );
