import {
  DEFAULT_SERVER_SETTINGS,
  MachineId,
  ProviderDriverKind,
  ProviderInstanceId,
} from "@vekrel/contracts";
import { describe, expect, it } from "@effect/vitest";

import {
  filterSharedServerPatch,
  findSharedSettingsMismatches,
  pickSharedServerSettings,
} from "./sharedSettings.ts";

const primaryId = MachineId.make("env-primary");
const laptopId = MachineId.make("env-laptop");
const boxId = MachineId.make("env-box");
const restartCapabilities = { threadRestartContinuation: true };

describe("filterSharedServerPatch", () => {
  it.each([true, false])(
    "resets a disabled default provider only on the originating machine (%s)",
    (targetIsSource) => {
      const settings = {
        ...DEFAULT_SERVER_SETTINGS,
        providerInstances: {
          codex: { driver: ProviderDriverKind.make("codex"), enabled: false, config: {} },
          claudeAgent: {
            driver: ProviderDriverKind.make("claudeAgent"),
            enabled: true,
            config: {},
          },
        },
        textGenerationModelSelection: {
          instanceId: ProviderInstanceId.make("claudeAgent"),
          model: "claude-opus-4-6",
        },
      };
      const patch = {
        textGenerationModelSelection: DEFAULT_SERVER_SETTINGS.textGenerationModelSelection,
        continueThreadsAfterServerUpdate: true,
        sidebarAutoSettleAfterDays: 7,
      };
      expect(filterSharedServerPatch(patch, undefined, settings, settings, targetIsSource)).toEqual(
        {
          ...(targetIsSource
            ? { textGenerationModelSelection: DEFAULT_SERVER_SETTINGS.textGenerationModelSelection }
            : {}),
          sidebarAutoSettleAfterDays: 7,
        },
      );
    },
  );

  it.each(["missing", "disabled", "different-driver", "enabled"] as const)(
    "shares a custom model only when its target provider is enabled (%s)",
    (availability) => {
      const instanceId = ProviderInstanceId.make("codex_personal");
      const selection = {
        instanceId,
        model: "gpt-5.6-luna",
        options: [{ id: "reasoningEffort", value: "low" }],
      };
      const instance = {
        driver: ProviderDriverKind.make(
          availability === "different-driver" ? "claudeAgent" : "codex",
        ),
        enabled: availability !== "disabled",
        config: {},
      };
      const settings = {
        ...DEFAULT_SERVER_SETTINGS,
        providerInstances: availability === "missing" ? {} : { [instanceId]: instance },
      };
      const patch = { sidebarAutoSettleAfterDays: 7, textGenerationModelSelection: selection };
      const sourceSettings = {
        ...settings,
        providerInstances: {
          [instanceId]: { ...instance, driver: ProviderDriverKind.make("codex"), enabled: true },
        },
      };
      expect(filterSharedServerPatch(patch, restartCapabilities, settings, sourceSettings)).toEqual(
        availability === "enabled" ? patch : { sidebarAutoSettleAfterDays: 7 },
      );
      const primarySettings = {
        ...sourceSettings,
        textGenerationModelSelection: selection,
      };
      expect(
        findSharedSettingsMismatches({
          primaryMachineId: primaryId,
          primarySettings,
          machines: [
            { machineId: boxId, label: "Remote Box", syncEligible: true, settings },
          ],
        }),
      ).toEqual(availability === "enabled" ? [{ machineId: boxId, label: "Remote Box" }] : []);
    },
  );

  it.each([undefined, {}, { threadRestartContinuation: false }])(
    "omits only the unsupported restart preference with capabilities %j",
    (capabilities) => {
      expect(
        filterSharedServerPatch(
          { continueThreadsAfterServerUpdate: true, sidebarAutoSettleAfterDays: 7 },
          capabilities,
        ),
      ).toEqual({ sidebarAutoSettleAfterDays: 7 });
      expect(pickSharedServerSettings(DEFAULT_SERVER_SETTINGS, capabilities)).not.toHaveProperty(
        "continueThreadsAfterServerUpdate",
      );
    },
  );
});

describe("findSharedSettingsMismatches", () => {
  const primarySettings = { ...DEFAULT_SERVER_SETTINGS, sidebarAutoSettleAfterDays: 7 };

  it.each([true, false])(
    "detects remote restart continuation drift when the preference is %s",
    (enabled) => {
      const settings = { ...primarySettings, continueThreadsAfterServerUpdate: enabled };
      const remoteSettings = { ...settings, continueThreadsAfterServerUpdate: !enabled };
      const machine = {
        machineId: boxId,
        label: "Remote Box",
        syncEligible: true,
        settings: remoteSettings,
        capabilities: restartCapabilities,
      };
      expect(
        findSharedSettingsMismatches({
          primaryMachineId: primaryId,
          primarySettings: settings,
          primaryCapabilities: restartCapabilities,
          machines: [machine],
        }),
      ).toEqual([{ machineId: boxId, label: "Remote Box" }]);
      expect(
        findSharedSettingsMismatches({
          primaryMachineId: primaryId,
          primarySettings: settings,
          primaryCapabilities: restartCapabilities,
          machines: [
            {
              ...machine,
              settings: Object.assign(
                {},
                remoteSettings,
                pickSharedServerSettings(settings, restartCapabilities),
              ),
            },
          ],
        }),
      ).toEqual([]);
    },
  );

  it.each([
    [undefined, restartCapabilities],
    [restartCapabilities, undefined],
    [undefined, undefined],
  ])(
    "ignores restart drift unless both servers support it (%j, %j)",
    (primaryCapabilities, capabilities) => {
      const machine = {
        machineId: boxId,
        label: "Remote Box",
        syncEligible: true,
        capabilities,
        settings: { ...primarySettings, continueThreadsAfterServerUpdate: true },
      };
      const input = {
        primaryMachineId: primaryId,
        primarySettings,
        primaryCapabilities,
        machines: [machine],
      };
      expect(findSharedSettingsMismatches(input)).toEqual([]);
      expect(
        findSharedSettingsMismatches({
          ...input,
          machines: [
            {
              ...machine,
              settings: { ...machine.settings, sidebarAutoSettleAfterDays: 14 },
            },
          ],
        }),
      ).toEqual([{ machineId: boxId, label: "Remote Box" }]);
    },
  );

  it("lists sync-eligible machines whose shared settings differ", () => {
    const mismatches = findSharedSettingsMismatches({
      primaryMachineId: primaryId,
      primarySettings,
      machines: [
        {
          machineId: primaryId,
          label: "Desktop",
          syncEligible: true,
          settings: primarySettings,
        },
        {
          machineId: laptopId,
          label: "Laptop",
          syncEligible: true,
          settings: primarySettings,
        },
        {
          machineId: boxId,
          label: "Remote Box",
          syncEligible: true,
          settings: DEFAULT_SERVER_SETTINGS,
        },
      ],
    });
    expect(mismatches).toEqual([{ machineId: boxId, label: "Remote Box" }]);
  });

  it("ignores machine-only differences", () => {
    const mismatches = findSharedSettingsMismatches({
      primaryMachineId: primaryId,
      primarySettings,
      machines: [
        {
          machineId: boxId,
          label: "Remote Box",
          syncEligible: true,
          settings: {
            ...primarySettings,
            enableAgentBrowserAccess: false,
            defaultThreadEnvMode:
              primarySettings.defaultThreadEnvMode === "local" ? "worktree" : "local",
          },
        },
      ],
    });
    expect(mismatches).toEqual([]);
  });

  it("skips ineligible machines and machines without a loaded config", () => {
    const mismatches = findSharedSettingsMismatches({
      primaryMachineId: primaryId,
      primarySettings,
      machines: [
        {
          machineId: laptopId,
          label: "Laptop",
          syncEligible: false,
          settings: DEFAULT_SERVER_SETTINGS,
        },
        { machineId: boxId, label: "Remote Box", syncEligible: true, settings: null },
      ],
    });
    expect(mismatches).toEqual([]);
  });
});
