import { WS_METHODS } from "@vekrel/contracts";
import { Atom } from "effect/reactivity";

import { createMachineRpcCommand, createMachineRpcQueryAtomFamily } from "./runtime.ts";
import type { MachineRegistry } from "../connection/registry.ts";
import { vcsCommandConcurrency, vcsCommandScheduler } from "./vcsCommandScheduler.ts";

export function createGitMachineAtoms<R, E>(
  runtime: Atom.AtomRuntime<MachineRegistry | R, E>,
) {
  return {
    pullRequestResolution: createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:git:resolve-pull-request",
      tag: WS_METHODS.gitResolvePullRequest,
    }),
    preparePullRequestThread: createMachineRpcCommand(runtime, {
      label: "machine-data:git:prepare-pull-request-thread",
      tag: WS_METHODS.gitPreparePullRequestThread,
      scheduler: vcsCommandScheduler,
      concurrency: vcsCommandConcurrency,
    }),
  };
}
