import type { MachineId } from "@vekrel/contracts";

import { createAtomCommandScheduler, type AtomCommandConcurrency } from "./runtime.ts";

export const vcsCommandScheduler = createAtomCommandScheduler();

export const vcsCommandConcurrency: AtomCommandConcurrency<{
  readonly machineId: MachineId;
  readonly input: { readonly cwd: string };
}> = {
  mode: "serial",
  key: ({ machineId, input }) => JSON.stringify([machineId, input.cwd]),
};
