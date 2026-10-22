import {
  USAGE_CONTRACT_VERSION,
  UsageDay,
  type UsageProviderKind,
  type UsageSummary,
} from "@vekrel/contracts";
import { describe, expect, it } from "vite-plus/test";

import { usageMachineProgress, usageLoadingState, usageProgress } from "./usageProgress.ts";

function summary(refreshing: readonly UsageProviderKind[] = []): UsageSummary {
  return {
    contractVersion: USAGE_CONTRACT_VERSION,
    readAt: "2026-09-05T12:00:00Z",
    timeZone: "UTC",
    sinceDay: UsageDay.make("2026-09-05"),
    untilDay: UsageDay.make("2026-09-05"),
    buckets: [],
    sources: (["codex", ...refreshing] as const).map((provider) => ({
      fingerprint: { hostId: "host", provider, resolvedHomePath: "/home", volumeId: "1:1" },
      status: "ok" as const,
      scannedFiles: 1,
      skippedFiles: 0,
      malformedRecords: 0,
      distinctSessions: 1,
      message: null,
      ...(provider === "codex" ? {} : { refreshing: true as const }),
    })),
    pricing: { status: "fresh", source: "test", fetchedAt: null, knownModels: 1 },
    scanDurationMs: 1,
  };
}

function machine(
  label: string,
  overrides: Partial<{
    isConnected: boolean;
    isPending: boolean;
    error: string | null;
    summary: UsageSummary | null;
  }> = {},
) {
  return {
    label,
    isConnected: true,
    isPending: false,
    error: null as string | null,
    summary: summary(),
    ...overrides,
  };
}

const options = { providerLabel: (provider: UsageProviderKind) => `${provider} label` };

describe("usageProgress", () => {
  it("undims once one device answers and names the slow one", () => {
    const waiting = [machine("a", { isPending: true }), machine("b", { isPending: true })];
    expect(usageProgress(waiting, options)).toEqual({
      dimmed: true,
      label: "Updating 2 machines…",
    });
    expect(
      usageProgress([machine("a"), ...waiting.slice(1), machine("c")], options),
    ).toEqual({ dimmed: false, label: "Updating b…" });
    expect(
      usageProgress(
        [machine("a"), machine("b", { summary: null, isPending: true })],
        options,
      ),
    ).toEqual({ dimmed: false, label: "Updating b…" });
  });

  it("names a slow source without dimming once its machine answers from cache", () => {
    const partway = machine("a", { isPending: true, summary: summary(["cursor"]) });
    expect(usageMachineProgress(partway)).toEqual({ phase: "partway", providers: ["cursor"] });
    expect(usageProgress([partway, machine("b")], options)).toEqual({
      dimmed: false,
      label: "Updating cursor label…",
    });
    expect(
      usageProgress(
        [partway, machine("b", { isPending: true, summary: summary(["cursor", "grok"]) })],
        options,
      ),
    ).toEqual({ dimmed: false, label: "Updating 2 providers…" });
    expect(usageProgress([{ ...partway, isPending: false }], options)).toEqual({
      dimmed: false,
      label: null,
    });
  });

  it("ignores reconnecting and failed machines", () => {
    const ignored = [
      machine("reconnecting", { isPending: true, isConnected: false }),
      machine("failed", { error: "Offline", summary: null }),
    ];
    expect(usageProgress([machine("a"), ...ignored], options)).toEqual({
      dimmed: false,
      label: null,
    });
    expect(
      usageProgress(
        [machine("a"), ...ignored, machine("slow", { summary: null, isPending: true })],
        options,
      ),
    ).toEqual({ dimmed: false, label: "Updating slow…" });
    expect(usageMachineProgress(ignored[0]!)).toEqual({ phase: "inactive" });
  });
});

describe("usageLoadingState", () => {
  it("ignores machines that cannot answer and settles once all have", () => {
    const state = usageLoadingState([
      machine("a"),
      machine("b", { summary: null, isConnected: false }),
      machine("c", { summary: null, error: "Offline" }),
    ]);
    expect(state).toEqual({ partial: false, everyProvider: false, providers: new Set() });
  });
});
