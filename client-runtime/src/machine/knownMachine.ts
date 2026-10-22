import type { MachineId } from "@vekrel/contracts";

export interface KnownMachineConnectionTarget {
  readonly httpBaseUrl: string;
  readonly wsBaseUrl: string;
}

export type KnownMachineSource = "configured" | "desktop-managed" | "manual" | "window-origin";

export interface KnownMachine {
  readonly id: string;
  readonly label: string;
  readonly source: KnownMachineSource;
  readonly machineId?: MachineId;
  readonly target: KnownMachineConnectionTarget;
}
