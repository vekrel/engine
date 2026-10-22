import type { MachineId, VcsRef as ContractVcsRef } from "@vekrel/contracts";

export interface VcsRefTarget {
  readonly machineId: MachineId | null;
  readonly cwd: string | null;
  readonly query?: string | null;
}

export type VcsRef = ContractVcsRef;
