import type {
  MachineId,
  ServerProvider,
  UsageSummary,
  UsageSummaryInput,
} from "@vekrel/contracts";
import * as Schema from "effect/Schema";
import type { AtomRegistry } from "effect/reactivity";

import { MachineRpcUnavailableError } from "../rpc/client.ts";
import type { createMachinePresentationAtoms } from "./presentation.ts";
import { executeAtomQuery, runAtomCommand, squashAtomCommandFailure } from "./runtime.ts";
import type { createServerMachineAtoms } from "./server.ts";

const isMachineRpcUnavailable = Schema.is(MachineRpcUnavailableError);

export function needsCursorKeychainAccess(
  summary: UsageSummary | null,
  providers: readonly ServerProvider[] | null,
): boolean {
  return (
    summary?.sources.some((source) => source.action === "enableCursorKeychain") === true &&
    providers?.some((provider) => provider.driver === "cursor" && provider.status === "ready") ===
      true
  );
}

/**
 * Any machine reading a Cursor account already reports that account's
 * history from every machine, so the prompt only adds duplicates there.
 */
export function cursorKeychainAccessMachines<
  E extends { readonly summary: UsageSummary | null; readonly needsCursorKeychainAccess: boolean },
>(machines: readonly E[]): readonly E[] {
  const hasCursorAccount = machines.some((machine) =>
    machine.summary?.sources.some(
      (source) =>
        source.fingerprint.provider === "cursor" && source.fingerprint.hostId === "cursor.com",
    ),
  );
  return hasCursorAccount
    ? []
    : machines.filter((machine) => machine.needsCursorKeychainAccess);
}

const limitsRefreshAfter = new Map<MachineId, number>();
const limitsRefreshes = new Map<MachineId, Promise<unknown>>();

export async function refreshUsageLimits<A>(
  machineId: MachineId,
  refresh: () => Promise<A>,
  automatic = false,
  afterPending = false,
): Promise<A | undefined> {
  const pending = limitsRefreshes.get(machineId);
  if (pending !== undefined) {
    if (afterPending) {
      try {
        await pending;
      } catch {}
      return refreshUsageLimits(machineId, refresh, false, true);
    }
    return automatic ? undefined : ((await pending) as A);
  }
  const refreshAfter = limitsRefreshAfter.get(machineId) ?? 0;
  // @effect-diagnostics-next-line globalDate:off
  if (automatic && Date.now() < refreshAfter) return;
  const current = Promise.resolve()
    .then(refresh)
    .finally(() => {
      limitsRefreshes.delete(machineId);
      // @effect-diagnostics-next-line globalDate:off
      limitsRefreshAfter.set(machineId, Date.now() + 5 * 60_000);
    });
  limitsRefreshes.set(machineId, current);
  return await current;
}

export async function refreshUsage({
  registry,
  server,
  presentations,
  machineIds,
  input,
}: {
  registry: AtomRegistry.AtomRegistry;
  server: Pick<
    ReturnType<typeof createServerMachineAtoms>,
    "usageSummary" | "refreshUsageRates"
  >;
  presentations: Pick<ReturnType<typeof createMachinePresentationAtoms>, "presentationAtom">;
  machineIds: readonly MachineId[];
  input: UsageSummaryInput;
}): Promise<void> {
  await Promise.all(
    machineIds.map(async (machineId) => {
      const query = server.usageSummary({ machineId, input });
      const presentation = presentations.presentationAtom(machineId);
      const controller = new AbortController();
      const abortWhenDisconnected = () => {
        if (registry.get(presentation)?.connection.phase !== "connected") controller.abort();
      };
      const unsubscribe = registry.subscribe(presentation, abortWhenDisconnected);
      abortWhenDisconnected();
      try {
        const ratesResult = await runAtomCommand(
          registry,
          server.refreshUsageRates,
          { machineId, input: {} },
          { reportFailure: false },
        );
        const sessionUnavailable =
          ratesResult._tag === "Failure" &&
          isMachineRpcUnavailable(squashAtomCommandFailure(ratesResult));
        registry.refresh(query);
        if (sessionUnavailable || controller.signal.aborted) return;
        await executeAtomQuery(registry, query, {
          reportFailure: false,
          signal: controller.signal,
        });
      } finally {
        unsubscribe();
      }
    }),
  );
}
