import { WS_METHODS } from "@vekrel/contracts";
import type { Atom } from "effect/reactivity";

import type { MachineRegistry } from "../connection/registry.ts";
import { createMachineRpcCommand } from "./runtime.ts";

export function createMcpAppMachineAtoms<R, E>(
  runtime: Atom.AtomRuntime<MachineRegistry | R, E>,
) {
  return {
    callTool: createMachineRpcCommand(runtime, {
      label: "machine-command:mcp-apps:call-tool",
      tag: WS_METHODS.mcpAppsCallTool,
    }),
    toolInfo: createMachineRpcCommand(runtime, {
      label: "machine-command:mcp-apps:tool-info",
      tag: WS_METHODS.mcpAppsToolInfo,
    }),
    readResource: createMachineRpcCommand(runtime, {
      label: "machine-command:mcp-apps:read-resource",
      tag: WS_METHODS.mcpAppsReadResource,
    }),
    updateModelContext: createMachineRpcCommand(runtime, {
      label: "machine-command:mcp-apps:update-model-context",
      tag: WS_METHODS.mcpAppsUpdateModelContext,
    }),
  };
}
