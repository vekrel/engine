import {
  AuthOrchestrationReadScope,
  MachineHttpApi,
  requiredScopesForProjectMutation,
} from "@vekrel/contracts";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";

import {
  annotateMachineRequest,
  failMachineInternal,
  failMachineInvalidRequest,
  requireMachineScope,
} from "../auth/http.ts";
import { traceLocalHandlerWork } from "../cloud/traceRelayRequest.ts";
import * as ServerRuntimeStartup from "../serverRuntimeStartup.ts";
import * as ProjectService from "./ProjectService.ts";
import { projectMutationOperation } from "./ProjectMutation.ts";

const failProjectMutation = Effect.fn("machine.projects.failMutation")(function* (
  cause: ProjectService.ProjectServiceError | ServerRuntimeStartup.ServerRuntimeStartupError,
) {
  if (
    cause._tag === "ProjectNotFoundError" ||
    cause._tag === "ProjectConflictError" ||
    cause._tag === "ProjectNotEmptyError"
  ) {
    return yield* failMachineInvalidRequest("invalid_command");
  }
  return yield* failMachineInternal("project_mutation_failed", cause);
});

export const layer = HttpApiBuilder.group(
  MachineHttpApi,
  "projects",
  Effect.fnUntraced(function* (handlers) {
    const projects = yield* ProjectService.ProjectService;
    const startup = yield* ServerRuntimeStartup.ServerRuntimeStartup;

    return handlers
      .handle(
        "snapshot",
        Effect.fn("machine.projects.snapshot")(function* (args) {
          yield* annotateMachineRequest(args.endpoint.name);
          yield* requireMachineScope(AuthOrchestrationReadScope);
          return yield* projects.snapshot.pipe(
            traceLocalHandlerWork,
            Effect.catch((cause) => failMachineInternal("project_snapshot_failed", cause)),
          );
        }),
      )
      .handle(
        "mutate",
        Effect.fn("machine.projects.mutate")(function* (args) {
          yield* annotateMachineRequest(args.endpoint.name);
          for (const scope of requiredScopesForProjectMutation(args.payload)) {
            yield* requireMachineScope(scope);
          }
          const operation = projectMutationOperation(projects, args.payload);
          return yield* startup
            .enqueueCommand(operation)
            .pipe(traceLocalHandlerWork, Effect.catch(failProjectMutation));
        }),
      );
  }),
);
