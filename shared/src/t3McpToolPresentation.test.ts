import { describe, expect, it } from "vite-plus/test";

import { VEKREL_MCP_TOOL_NAMES, resolveT3McpToolPresentation } from "./t3McpToolPresentation.ts";

describe("resolveT3McpToolPresentation", () => {
  it("recognizes every T3 tool across provider prefixes and completion suffixes", () => {
    for (const tool of VEKREL_MCP_TOOL_NAMES) {
      const presentation = resolveT3McpToolPresentation(tool);
      for (const prefix of [
        "mcp__vekrel__",
        "Vekrel.",
        "vekrel/",
        "vekrel:",
        "mcp_vekrel_",
        "Vekrel ",
        "vekrel · ",
      ]) {
        expect(resolveT3McpToolPresentation(`${prefix}${tool} completed`), tool).toEqual(
          presentation,
        );
      }
      expect(resolveT3McpToolPresentation(`mcp__another-server__${tool}`), tool).toBeNull();
    }
  });

  it("matches OpenCode 2's per-thread server names, whose thread ids hold underscores", () => {
    expect(
      resolveT3McpToolPresentation("vekrel-thread_opencode2-adapter_delegate_task")?.displayName,
    ).toBe("Delegate a child task");
    expect(resolveT3McpToolPresentation("vekrel-thread_opencode2-adapter_not_a_tool")).toBeNull();
  });
});
