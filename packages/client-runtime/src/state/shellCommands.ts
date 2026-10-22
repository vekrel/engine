import { WS_METHODS } from "@vekrel/contracts";
import { Atom } from "effect/reactivity";

import { createMachineRpcCommand } from "./runtime.ts";
import type { MachineRegistry } from "../connection/registry.ts";

export function createShellMachineAtoms<R, E>(
  runtime: Atom.AtomRuntime<MachineRegistry | R, E>,
) {
  return {
    openInEditor: createMachineRpcCommand(runtime, {
      label: "machine-data:shell:open-in-editor",
      tag: WS_METHODS.shellOpenInEditor,
    }),
  };
}
