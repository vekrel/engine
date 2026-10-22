import type { ThreadId } from "@vekrel/contracts";
import * as Effect from "effect/Effect";
import type * as Option from "effect/Option";

import type { RemoteMachineAuthorization } from "../authorization/service.ts";
import type { PreparedConnection } from "../connection/model.ts";
import type { ManagedRelayDpopSigner } from "../relay/managedRelay.ts";
import {
  executeAuthenticatedMachineHttpRequest,
  withOrchestrationProtocolHeader,
} from "./machineHttpAuth.ts";

export const fetchMachineThreadTranscript = Effect.fn(
  "clientRuntime.state.fetchMachineThreadTranscript",
)(function* (input: {
  readonly prepared: PreparedConnection;
  readonly threadId: ThreadId;
  readonly signer: Option.Option<ManagedRelayDpopSigner["Service"]>;
  readonly remoteAuthorization?: Option.Option<RemoteMachineAuthorization["Service"]>;
}) {
  return yield* executeAuthenticatedMachineHttpRequest({
    ...input,
    group: "orchestration",
    method: "GET",
    url: (urls) => urls.threadTranscript({ params: { threadId: input.threadId } }),
    timeoutMs: 60_000,
    request: ({ client, headers }) =>
      client.threadTranscript({
        params: { threadId: input.threadId },
        headers: withOrchestrationProtocolHeader(headers),
      }),
  });
});
