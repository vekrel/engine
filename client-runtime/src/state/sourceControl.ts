import { WS_METHODS } from "@vekrel/contracts";
import { Atom } from "effect/reactivity";

import {
  createAtomCommandScheduler,
  createMachineRpcCommand,
  createMachineRpcQueryAtomFamily,
  createMachineRpcSubscriptionAtomFamily,
} from "./runtime.ts";
import type { MachineRegistry } from "../connection/registry.ts";
import * as Persistence from "../platform/persistence.ts";
import { vcsCommandConcurrency, vcsCommandScheduler } from "./vcsCommandScheduler.ts";
import { invalidateCachedVcsRefs } from "./vcsRefInvalidation.ts";

export function createSourceControlMachineAtoms<R, E>(
  runtime: Atom.AtomRuntime<MachineRegistry | Persistence.MachineCacheStore | R, E>,
) {
  const commandScheduler = createAtomCommandScheduler();
  return {
    discovery: createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:server:source-control-discovery",
      tag: WS_METHODS.serverDiscoverSourceControl,
    }),
    repository: createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:source-control:repository",
      tag: WS_METHODS.sourceControlLookupRepository,
    }),
    cloneRepository: createMachineRpcCommand(runtime, {
      label: "machine-data:source-control:clone-repository",
      tag: WS_METHODS.sourceControlCloneRepository,
      scheduler: commandScheduler,
      concurrency: {
        mode: "serial",
        key: ({ machineId }) => machineId,
      },
    }),
    // Clone-backed project creation. The RPC returns once the project exists
    // and the clone runs in the background; `projectClones` carries progress.
    startProjectClone: createMachineRpcCommand(runtime, {
      label: "machine-data:source-control:project-clone-start",
      tag: WS_METHODS.projectCloneStart,
      scheduler: commandScheduler,
      concurrency: {
        mode: "serial",
        key: ({ machineId }) => machineId,
      },
    }),
    // Cancel and retry share the start queue so a double click cannot race
    // two actions against the same clone.
    cancelProjectClone: createMachineRpcCommand(runtime, {
      label: "machine-data:source-control:project-clone-cancel",
      tag: WS_METHODS.projectCloneCancel,
      scheduler: commandScheduler,
      concurrency: {
        mode: "serial",
        key: ({ machineId }) => machineId,
      },
    }),
    retryProjectClone: createMachineRpcCommand(runtime, {
      label: "machine-data:source-control:project-clone-retry",
      tag: WS_METHODS.projectCloneRetry,
      scheduler: commandScheduler,
      concurrency: {
        mode: "serial",
        key: ({ machineId }) => machineId,
      },
    }),
    projectClones: createMachineRpcSubscriptionAtomFamily(runtime, {
      label: "machine-data:source-control:project-clones",
      tag: WS_METHODS.subscribeProjectClones,
    }),
    publishRepository: createMachineRpcCommand(runtime, {
      label: "machine-data:source-control:publish-repository",
      tag: WS_METHODS.sourceControlPublishRepository,
      scheduler: vcsCommandScheduler,
      concurrency: vcsCommandConcurrency,
      onSettled: (target, registry) =>
        invalidateCachedVcsRefs(registry, {
          machineId: target.machineId,
          cwd: target.input.cwd,
        }),
    }),
  };
}
