import type { OrchestrationV2ThreadDetailSnapshot, ThreadId } from "@vekrel/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import * as RemoteMachineAuthorization from "../authorization/service.ts";
import type { PreparedConnection } from "../connection/model.ts";
import * as ManagedRelay from "../relay/managedRelay.ts";
import type { RemoteMachineRequestError } from "../rpc/http.ts";
import {
  executeAuthenticatedMachineHttpRequest,
  withOrchestrationProtocolHeader,
} from "./machineHttpAuth.ts";

// Long enough for a slow but alive server to finish. On a cold open a timeout makes the socket
// ask the same server for the same snapshot again, and older turn pages have no fallback, so a
// short deadline only drops work. The socket fallback is for setups where /api fails but /ws
// works, such as a proxy that blocks /api.
const DEFAULT_THREAD_SNAPSHOT_TIMEOUT_MS = 20_000;

export type ThreadSnapshotHistoryMeta = {
  readonly historyCursor: string | null;
  readonly hasMoreHistory: boolean;
  readonly latestLocalTurnOrdinal?: number | null;
};

export type ThreadSnapshotLoadResult =
  | {
      readonly _tag: "present";
      readonly snapshot: OrchestrationV2ThreadDetailSnapshot;
      readonly history?: ThreadSnapshotHistoryMeta;
    }
  | { readonly _tag: "missing" }
  | { readonly _tag: "unavailable" };

export const fetchMachineThreadSnapshot = Effect.fn(
  "clientRuntime.state.fetchMachineThreadSnapshot",
)(function* (input: {
  readonly prepared: PreparedConnection;
  readonly threadId: ThreadId;
  readonly signer: Option.Option<ManagedRelay.ManagedRelayDpopSigner["Service"]>;
  readonly remoteAuthorization?: Option.Option<
    RemoteMachineAuthorization.RemoteMachineAuthorization["Service"]
  >;
  readonly timeoutMs?: number;
}) {
  return yield* executeAuthenticatedMachineHttpRequest({
    ...input,
    group: "orchestration",
    method: "GET",
    url: (urls) => urls.threadSnapshot({ params: { threadId: input.threadId } }),
    timeoutMs: input.timeoutMs ?? DEFAULT_THREAD_SNAPSHOT_TIMEOUT_MS,
    request: ({ client, headers }) =>
      client.threadSnapshot({
        params: { threadId: input.threadId },
        headers: withOrchestrationProtocolHeader(headers),
      }),
  });
});

export type FetchMachineThreadSnapshotError = RemoteMachineRequestError;

export class ThreadSnapshotLoader extends Context.Service<
  ThreadSnapshotLoader,
  {
    readonly load: (
      prepared: PreparedConnection,
      threadId: ThreadId,
    ) => Effect.Effect<ThreadSnapshotLoadResult>;
  }
>()("@vekrel/client-runtime/state/threadSnapshotHttp/ThreadSnapshotLoader") {}
