import {
  MachineAuthInvalidError,
  type PullRequestDiffInput,
  type PullRequestDiffResult,
} from "@vekrel/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { HttpClient } from "effect/http";

import * as RemoteMachineAuthorization from "../authorization/service.ts";
import type { PreparedConnection } from "../connection/model.ts";
import * as ManagedRelay from "../relay/managedRelay.ts";
import { type RemoteMachineRequestError } from "../rpc/http.ts";
import { executeAuthenticatedMachineHttpRequest } from "./machineHttpAuth.ts";

const DEFAULT_PULL_REQUEST_DIFF_TIMEOUT_MS = 60_000;

export class PullRequestDiffCredentialRejectedError extends Schema.TaggedError<PullRequestDiffCredentialRejectedError>()(
  "PullRequestDiffCredentialRejectedError",
  {
    repository: Schema.String,
    number: Schema.Number,
    traceId: Schema.String,
    cause: MachineAuthInvalidError,
  },
) {
  override get message(): string {
    return "This machine session is no longer valid (invalid_credential). Refresh the page or quit and reopen T3 Code.";
  }
}

export type PullRequestDiffLoadError =
  | RemoteMachineRequestError
  | PullRequestDiffCredentialRejectedError;

export const fetchMachinePullRequestDiff = Effect.fn(
  "clientRuntime.state.fetchMachinePullRequestDiff",
)(function* (input: {
  readonly prepared: PreparedConnection;
  readonly diff: PullRequestDiffInput;
  readonly signer: Option.Option<ManagedRelay.ManagedRelayDpopSigner["Service"]>;
  readonly remoteAuthorization?: Option.Option<
    RemoteMachineAuthorization.RemoteMachineAuthorization["Service"]
  >;
  readonly timeoutMs?: number;
}) {
  return yield* executeAuthenticatedMachineHttpRequest({
    ...input,
    group: "pullRequests",
    method: "POST",
    url: (urls) => urls.diff(),
    timeoutMs: input.timeoutMs ?? DEFAULT_PULL_REQUEST_DIFF_TIMEOUT_MS,
    request: ({ client, headers }) => client.diff({ payload: input.diff, headers }),
  }).pipe(
    Effect.mapError((error) =>
      error._tag === "MachineAuthInvalidError" && error.reason === "invalid_credential"
        ? new PullRequestDiffCredentialRejectedError({
            repository: input.diff.repository,
            number: input.diff.number,
            traceId: error.traceId,
            cause: error,
          })
        : error,
    ),
  );
});

export class PullRequestDiffLoader extends Context.Service<
  PullRequestDiffLoader,
  {
    readonly load: (
      prepared: PreparedConnection,
      input: PullRequestDiffInput,
    ) => Effect.Effect<PullRequestDiffResult, PullRequestDiffLoadError>;
  }
>()("@vekrel/client-runtime/state/pullRequestDiffHttp/PullRequestDiffLoader") {}

export const layer: Layer.Layer<PullRequestDiffLoader, never, HttpClient.HttpClient> = Layer.effect(
  PullRequestDiffLoader,
  Effect.gen(function* () {
    const httpClient = yield* HttpClient.HttpClient;
    const signer = yield* Effect.serviceOption(ManagedRelay.ManagedRelayDpopSigner);
    const remoteAuthorization = yield* Effect.serviceOption(
      RemoteMachineAuthorization.RemoteMachineAuthorization,
    );
    return PullRequestDiffLoader.of({
      load: (prepared, input) =>
        fetchMachinePullRequestDiff({
          prepared,
          diff: input,
          signer,
          remoteAuthorization,
        }).pipe(Effect.provideService(HttpClient.HttpClient, httpClient)),
    });
  }),
);
