import {
  BackgroundActivityProfile,
  BackgroundActivityProfileSelection,
  ExecutionMachineDescriptor,
  OrchestratorMcpFailure,
  ServerSettings,
  ServerSettingsPatch,
} from "@vekrel/contracts";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/ai";
import * as ServerMachine from "../../../machine/ServerMachine.ts";
import * as ThreadCommandExecutor from "../../../orchestration-v2/ThreadCommandExecutor.ts";
import * as ThreadManagementService from "../../../orchestration-v2/ThreadManagementService.ts";
import * as Settings from "../../../serverSettings.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";

const PreferenceFields = {
  defaultThreadEnvMode: ServerSettings.fields.defaultThreadEnvMode,
  newWorktreesStartFromOrigin: ServerSettings.fields.newWorktreesStartFromOrigin,
  enableProviderUpdateChecks: ServerSettings.fields.enableProviderUpdateChecks,
  backgroundActivity: Schema.Struct({ profile: BackgroundActivityProfileSelection }),
  sourceControlWritingStyle: Schema.Struct({
    mode: Schema.String,
    followChangeRequestTemplates: Schema.Boolean,
    customInstructions: Schema.String,
    truncated: Schema.Boolean,
  }),
};
const shared = {
  failure: OrchestratorMcpFailure,
  failureMode: "return" as const,
  dependencies: [
    McpInvocationContext.McpInvocationContext,
    ThreadManagementService.ThreadManagementService,
    ServerMachine.ServerMachine,
    Settings.ServerSettingsService,
    ThreadCommandExecutor.ThreadCommandExecutor,
  ],
};
const MachineReadTool = Tool.make("vekrel_machine_read", {
  ...shared,
  description:
    "Read this server's identity and selected machine preferences. Provider/model availability is exposed by orchestrator_capabilities. Writing instructions are limited to 4,000 characters.",
  success: Schema.Struct({
    machineId: ExecutionMachineDescriptor.fields.machineId,
    label: Schema.String,
    serverVersion: Schema.String,
    platform: ExecutionMachineDescriptor.fields.platform,
    preferences: Schema.Struct(PreferenceFields),
  }),
})
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false);
const MachinePreferencesTool = Tool.make("vekrel_machine_preferences_update", {
  ...shared,
  description:
    "Update selected machine-wide preferences through normal settings persistence and notifications. Requires a live full-access/default calling thread. Omitted fields are preserved; empty customInstructions clears them.",
  parameters: Schema.Struct({
    defaultThreadEnvMode: ServerSettingsPatch.fields.defaultThreadEnvMode,
    newWorktreesStartFromOrigin: ServerSettingsPatch.fields.newWorktreesStartFromOrigin,
    enableProviderUpdateChecks: ServerSettingsPatch.fields.enableProviderUpdateChecks,
    backgroundActivity: Schema.optionalKey(Schema.Struct({ profile: BackgroundActivityProfile })),
    sourceControlWritingStyle: ServerSettingsPatch.fields.sourceControlWritingStyle,
  }),
  success: Schema.Struct(PreferenceFields),
}).annotate(Tool.Destructive, true);
export const MachineToolkit = Toolkit.make(MachineReadTool, MachinePreferencesTool);
