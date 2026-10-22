import {
  MachineId,
  ProviderDriverKind,
  ProviderInstanceId,
  UsageDay,
  USAGE_CONTRACT_VERSION,
  type ServerProvider,
  type UsageSummary,
} from "@vekrel/contracts";
import * as Effect from "effect/Effect";
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import type { MachinePresentation } from "../connection/presentation.ts";
import {
  cursorKeychainAccessMachines,
  needsCursorKeychainAccess,
  refreshUsage,
  refreshUsageLimits,
} from "./usage.ts";

const input = {
  sinceDay: UsageDay.make("2026-09-05"),
  untilDay: UsageDay.make("2026-09-05"),
  timeZone: "UTC",
};
const pricing = { status: "fresh" as const, source: "test", fetchedAt: null, knownModels: 1 };
const summary: UsageSummary = {
  ...input,
  contractVersion: USAGE_CONTRACT_VERSION,
  readAt: "2026-09-05T12:00:00Z",
  buckets: [],
  sources: [],
  pricing,
  scanDurationMs: 1,
};
const registries: AtomRegistry.AtomRegistry[] = [];
afterEach(() => {
  for (const registry of registries.splice(0)) registry.dispose();
});

function harness(ids = ["a"]) {
  const registry = AtomRegistry.make();
  registries.push(registry);
  const machines = ids.map((id) => {
    const machineId = MachineId.make(id);
    const rates = Promise.withResolvers<
      AsyncResult.Success<typeof pricing> | AsyncResult.Failure<never, unknown>
    >();
    const scan = Promise.withResolvers<UsageSummary>();
    const scanStarted = Promise.withResolvers<void>();
    const presentation = Atom.make({
      connection: { phase: "connected" },
    } as MachinePresentation | null);
    const query = Atom.make(
      Effect.promise(() => {
        scanStarted.resolve();
        return scan.promise;
      }),
    );
    return { machineId, rates, scan, scanStarted, presentation, query };
  });
  function get(machineId: MachineId) {
    const machine = machines.find((entry) => entry.machineId === machineId);
    if (!machine) throw new Error(`Unknown machine: ${machineId}`);
    return machine;
  }
  const options = {
    registry,
    machineIds: machines.map((entry) => entry.machineId),
    input,
    server: {
      usageSummary: ({ machineId }: { machineId: MachineId }) =>
        get(machineId).query,
      refreshUsageRates: {
        label: "test:rates",
        requiredScopes: () => [],
        permissionAtom: () => Atom.make(true),
        run: (
          _registry: AtomRegistry.AtomRegistry,
          { machineId }: { machineId: MachineId },
        ) => get(machineId).rates.promise,
      },
    },
    presentations: {
      presentationAtom: (machineId: MachineId) => get(machineId).presentation,
    },
  } satisfies Parameters<typeof refreshUsage>[0];
  return { registry, machines, refresh: () => refreshUsage(options) };
}

describe("manual usage refresh", () => {
  it.each(["success", "failure"])("waits for the rescan after a pricing %s", async (result) => {
    const {
      machines: [machine],
      refresh,
    } = harness();
    const entry = machine!;
    let finished = false;
    const refreshing = refresh().then(() => {
      finished = true;
    });
    expect(finished).toBe(false);
    entry.rates.resolve(
      result === "success"
        ? AsyncResult.success(pricing)
        : AsyncResult.fail(new Error("Pricing offline")),
    );
    await entry.scanStarted.promise;
    expect(finished).toBe(false);
    entry.scan.resolve(summary);
    await refreshing;
    expect(finished).toBe(true);
  });

  it("settles when a machine disconnects during the rescan", async () => {
    const {
      registry,
      machines: [machine],
      refresh,
    } = harness();
    const entry = machine!;
    const refreshing = refresh();
    entry.rates.resolve(AsyncResult.success(pricing));
    await entry.scanStarted.promise;
    registry.set(entry.presentation, null);
    await refreshing;
  });

  it("waits for healthy machines without waiting for a recovering machine", async () => {
    const { registry, machines, refresh } = harness(["healthy", "recovering"]);
    const [healthy, recovering] = machines;
    registry.set(recovering!.presentation, null);
    let finished = false;
    const refreshing = refresh().then(() => {
      finished = true;
    });
    for (const entry of machines) entry.rates.resolve(AsyncResult.success(pricing));
    await healthy!.scanStarted.promise;
    expect(finished).toBe(false);
    healthy!.scan.resolve(summary);
    await refreshing;
    expect(finished).toBe(true);
  });

  it("replaces a scan that started before pricing was refreshed", async () => {
    const {
      registry,
      machines: [machine],
      refresh,
    } = harness();
    const entry = machine!;
    let reads = 0;
    const rescanned = Promise.withResolvers<void>();
    const query = Atom.make(
      Effect.promise(() => {
        reads += 1;
        if (reads > 1) {
          rescanned.resolve();
          return Promise.resolve(summary);
        }
        return new Promise<UsageSummary>(() => {});
      }),
    );
    entry.query = query;
    const unmount = registry.mount(query);
    expect(reads).toBe(1);
    const refreshing = refresh();
    entry.rates.resolve(AsyncResult.success(pricing));
    await rescanned.promise;
    await refreshing;
    expect(reads).toBe(2);
    unmount();
  });
});

describe("limits refresh cooldown", () => {
  it("runs a fresh check after an in-flight check when settings change", async () => {
    const id = MachineId.make("limits-after-enable");
    const oldCheck = Promise.withResolvers<string>();
    const first = refreshUsageLimits(id, () => oldCheck.promise, true);
    const newCheck = vi.fn(async () => "new limits");
    const afterEnable = refreshUsageLimits(id, newCheck, false, true);
    expect(newCheck).not.toHaveBeenCalled();
    oldCheck.resolve("old limits");
    expect(await first).toBe("old limits");
    expect(await afterEnable).toBe("new limits");
    expect(newCheck).toHaveBeenCalledTimes(1);
  });
});

describe("needsCursorKeychainAccess", () => {
  const cursorPrompt: UsageSummary = {
    ...summary,
    sources: [
      {
        fingerprint: {
          hostId: "host",
          provider: "cursor",
          resolvedHomePath: "/Users/me/.cursor/auth.json",
          volumeId: "volume",
        },
        status: "ok",
        scannedFiles: 0,
        skippedFiles: 0,
        malformedRecords: 0,
        distinctSessions: 0,
        message: "Cursor account usage is off on this machine.",
        action: "enableCursorKeychain",
      },
    ],
  };
  const cursor = (status: ServerProvider["status"]): ServerProvider => ({
    instanceId: ProviderInstanceId.make("cursor"),
    driver: ProviderDriverKind.make("cursor"),
    enabled: status !== "disabled",
    installed: status === "ready",
    version: null,
    status,
    auth: { status: "unknown" },
    checkedAt: "2026-09-05T12:00:00.000Z",
    models: [],
    slashCommands: [],
    skills: [],
  });

  it("offers access only when Cursor is ready on that machine", () => {
    expect(needsCursorKeychainAccess(cursorPrompt, [cursor("ready")])).toBe(true);
    expect(needsCursorKeychainAccess(cursorPrompt, [cursor("error")])).toBe(false);
    expect(needsCursorKeychainAccess(cursorPrompt, [cursor("disabled")])).toBe(false);
    expect(needsCursorKeychainAccess(cursorPrompt, [])).toBe(false);
    expect(needsCursorKeychainAccess(cursorPrompt, null)).toBe(false);
    expect(needsCursorKeychainAccess(summary, [cursor("ready")])).toBe(false);
  });

  it("stops offering access once any machine reads the Cursor account", () => {
    const off = { summary: cursorPrompt, needsCursorKeychainAccess: true };
    const account: UsageSummary = {
      ...summary,
      sources: [
        {
          fingerprint: {
            hostId: "cursor.com",
            provider: "cursor",
            resolvedHomePath: "cursor-account:abc",
            volumeId: "abc",
          },
          status: "ok",
          scannedFiles: 1,
          skippedFiles: 0,
          malformedRecords: 0,
          distinctSessions: 1,
          message: null,
        },
      ],
    };
    expect(cursorKeychainAccessMachines([off, off])).toEqual([off, off]);
    expect(
      cursorKeychainAccessMachines([
        off,
        { summary: account, needsCursorKeychainAccess: false },
      ]),
    ).toEqual([]);
  });
});
