import { RelayApi } from "@vekrel/contracts/relay";
import * as Effect from "effect/Effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpApiClient from "effect/http-api/HttpApiClient";

export const makeRelayMachineClient = (connection: {
  readonly url: string;
  readonly machineCredential: string;
}) =>
  HttpApiClient.make(RelayApi, {
    baseUrl: connection.url,
    transformClient: HttpClient.mapRequest(
      HttpClientRequest.setHeader("authorization", `Bearer ${connection.machineCredential}`),
    ),
  }).pipe(Effect.provide(FetchHttpClient.layer));
