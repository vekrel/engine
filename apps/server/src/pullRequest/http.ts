import { AuthOrchestrationReadScope, MachineHttpApi } from "@vekrel/contracts";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";

import { annotateMachineRequest, requireMachineScope } from "../auth/http.ts";
import { traceLocalHandlerWork } from "../cloud/traceRelayRequest.ts";
import * as PullRequestService from "./PullRequestService.ts";

/** The patch is often the largest PR payload and benefits from HTTP compression and flow control. */
export const layer = HttpApiBuilder.group(
  MachineHttpApi,
  "pullRequests",
  Effect.fnUntraced(function* (handlers) {
    const pullRequests = yield* PullRequestService.PullRequestService;
    return handlers.handle(
      "diff",
      Effect.fn("machine.pullRequests.diff")(function* (args) {
        yield* annotateMachineRequest(args.endpoint.name);
        yield* requireMachineScope(AuthOrchestrationReadScope);
        return yield* pullRequests.diff(args.payload).pipe(traceLocalHandlerWork);
      }),
    );
  }),
);
