import * as Effect from "effect/Effect";

import { machineEndpointUrl } from "./endpoint.ts";
import { executeMachineHttpRequest, makeMachineHttpApiGroupClient } from "../rpc/http.ts";

const DEFAULT_REMOTE_REQUEST_TIMEOUT_MS = 10_000;

export const fetchRemoteMachineDescriptor = Effect.fn(
  "clientRuntime.machine.fetchRemoteMachineDescriptor",
)(function* (input: { readonly httpBaseUrl: string; readonly timeoutMs?: number }) {
  const client = yield* makeMachineHttpApiGroupClient(input.httpBaseUrl, "metadata");
  return yield* executeMachineHttpRequest(
    machineEndpointUrl(input.httpBaseUrl, "/.well-known/vekrel/machine"),
    input.timeoutMs ?? DEFAULT_REMOTE_REQUEST_TIMEOUT_MS,
    client.descriptor(),
  );
});
