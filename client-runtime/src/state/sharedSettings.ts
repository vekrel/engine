import type {
  MachineId,
  ExecutionMachineCapabilities,
  ServerSettings,
  ServerSettingsPatch,
} from "@vekrel/contracts";
import { isModelSelectionProviderEnabled } from "@vekrel/shared/serverSettings";
import * as Equal from "effect/Equal";
import * as Struct from "effect/Struct";

import type { MachineConnectionPhase } from "../connection/presentation.ts";

const SHARED_SERVER_SETTING_KEYS = [
  "continueThreadsAfterServerUpdate",
  "sidebarAutoSettleAfterDays",
  "sidebarAutoSettleOnMerge",
  "autoResumeLimitedThreads",
  "snoozeLimitedThreads",
  "newWorktreesStartFromOrigin",
  "sourceControlWritingStyle",
  "textGenerationModelSelection",
] as const satisfies ReadonlyArray<keyof ServerSettings & keyof ServerSettingsPatch>;

export type SharedServerSettingKey = (typeof SHARED_SERVER_SETTING_KEYS)[number];

const SHARED_KEY_SET = new Set<string>(SHARED_SERVER_SETTING_KEYS);

export function splitSharedServerPatch(patch: ServerSettingsPatch): {
  sharedPatch: ServerSettingsPatch;
  localPatch: ServerSettingsPatch;
} {
  const sharedPatch: Record<string, unknown> = {};
  const localPatch: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(patch)) {
    if (SHARED_KEY_SET.has(key)) {
      sharedPatch[key] = value;
    } else {
      localPatch[key] = value;
    }
  }
  return {
    sharedPatch: sharedPatch as ServerSettingsPatch,
    localPatch: localPatch as ServerSettingsPatch,
  };
}

export function filterSharedServerPatch(
  patch: ServerSettingsPatch,
  capabilities: Pick<ExecutionMachineCapabilities, "threadRestartContinuation"> | undefined,
  settings?: ServerSettings,
  sourceSettings = settings,
  targetIsSource = false,
): ServerSettingsPatch {
  const instanceId =
    patch.textGenerationModelSelection?.instanceId ??
    sourceSettings?.textGenerationModelSelection.instanceId;
  if (
    !targetIsSource &&
    patch.textGenerationModelSelection &&
    (!settings ||
      (instanceId !== undefined &&
        (sourceSettings?.providerInstances[instanceId]?.driver ?? instanceId) !==
          (settings.providerInstances[instanceId]?.driver ?? instanceId)) ||
      !isModelSelectionProviderEnabled(settings, {
        ...settings.textGenerationModelSelection,
        ...patch.textGenerationModelSelection,
      }))
  ) {
    patch = Struct.omit(patch, ["textGenerationModelSelection"]);
  }
  return capabilities?.threadRestartContinuation === true
    ? patch
    : Struct.omit(patch, ["continueThreadsAfterServerUpdate"]);
}

export function pickSharedServerSettings(
  settings: ServerSettings,
  capabilities?: Pick<ExecutionMachineCapabilities, "threadRestartContinuation">,
): ServerSettingsPatch {
  return filterSharedServerPatch(
    Struct.pick(settings, SHARED_SERVER_SETTING_KEYS),
    capabilities,
    settings,
  );
}

export function supportsSharedSettingsSync(machine: {
  readonly connection: { readonly phase: MachineConnectionPhase };
  readonly serverConfig: {
    readonly machine: {
      readonly capabilities: Pick<ExecutionMachineCapabilities, "threadAutoSettlement">;
    };
  } | null;
}): boolean {
  return (
    machine.connection.phase === "connected" &&
    machine.serverConfig?.machine.capabilities.threadAutoSettlement === true
  );
}

export interface SharedSettingsMachine {
  readonly machineId: MachineId;
  readonly label: string;
  readonly syncEligible: boolean;
  readonly settings: ServerSettings | null;
  readonly capabilities?:
    | Pick<ExecutionMachineCapabilities, "threadRestartContinuation">
    | undefined;
}

/**
 * Callers must pass the real loaded settings, never a default fallback, or
 * "apply to all" would push defaults over real values.
 */
export function findSharedSettingsMismatches(input: {
  readonly primaryMachineId: MachineId | null;
  readonly primarySettings: ServerSettings | null;
  readonly primaryCapabilities?:
    | Pick<ExecutionMachineCapabilities, "threadRestartContinuation">
    | undefined;
  readonly machines: ReadonlyArray<SharedSettingsMachine>;
}): ReadonlyArray<{ readonly machineId: MachineId; readonly label: string }> {
  if (input.primaryMachineId === null || input.primarySettings === null) {
    return [];
  }
  const primarySettings = pickSharedServerSettings(
    input.primarySettings,
    input.primaryCapabilities,
  );
  return input.machines.flatMap((machine) => {
    if (
      machine.machineId === input.primaryMachineId ||
      !machine.syncEligible ||
      machine.settings === null
    ) {
      return [];
    }
    const expected = filterSharedServerPatch(
      primarySettings,
      machine.capabilities,
      machine.settings,
      input.primarySettings ?? undefined,
    );
    let actual = filterSharedServerPatch(
      pickSharedServerSettings(machine.settings, machine.capabilities),
      input.primaryCapabilities,
      machine.settings,
    );
    if (!expected.textGenerationModelSelection) {
      actual = Struct.omit(actual, ["textGenerationModelSelection"]);
    }
    return Equal.equals(actual, expected)
      ? []
      : [{ machineId: machine.machineId, label: machine.label }];
  });
}
