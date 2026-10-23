import {
  MachineId,
  ORCHESTRATION_PROTOCOL_VERSION,
  PROVIDER_SEND_TURN_MAX_FILE_BYTES,
  type ExecutionMachineDescriptor,
} from "@vekrel/contracts";
import * as HostProcess from "@vekrel/shared/HostProcess";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import packageJson from "../../package.json" with { type: "json" };
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import { resolveServerInstallation } from "../cli/invocation.ts";
import { readAgentActivityPublishingActive } from "../cloud/config.ts";
import { resolveServerSelfUpdateCapability } from "../cloud/selfUpdate.ts";
import { resolveServiceLauncherMode } from "../cloud/serviceLauncherClient.ts";
import * as ServerConfig from "../config.ts";
import * as ProcessRunner from "../processRunner.ts";
import { resolveServerMachineLabel } from "./ServerMachineLabel.ts";
import { detectServerMachineKind } from "./ServerMachineKind.ts";

export class ServerMachineIdPersistenceError extends Schema.TaggedError<ServerMachineIdPersistenceError>()(
  "ServerMachineIdPersistenceError",
  {
    operation: Schema.Literals(["check", "read", "write", "initialize"]),
    machineIdPath: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    if (this.operation === "initialize") {
      return `Server machine ID file is missing or empty after initialization at '${this.machineIdPath}'.`;
    }
    return `Server machine ID ${this.operation} failed at '${this.machineIdPath}'.`;
  }
}

export class ServerMachine extends Context.Service<
  ServerMachine,
  {
    readonly getMachineId: Effect.Effect<MachineId>;
    readonly getDescriptor: Effect.Effect<ExecutionMachineDescriptor>;
  }
>()("vekrel-engine/machine/ServerMachine") {}

export class ServerMachineIdentity extends Context.Service<
  ServerMachineIdentity,
  {
    readonly getMachineId: Effect.Effect<MachineId>;
  }
>()("vekrel-engine/machine/ServerMachine/ServerMachineIdentity") {}

function platformOs(platform: NodeJS.Platform): ExecutionMachineDescriptor["platform"]["os"] {
  switch (platform) {
    case "darwin":
      return "darwin";
    case "linux":
      return "linux";
    case "win32":
      return "windows";
    default:
      return "unknown";
  }
}

function platformArch(
  architecture: NodeJS.Architecture,
): ExecutionMachineDescriptor["platform"]["arch"] {
  switch (architecture) {
    case "arm64":
      return "arm64";
    case "x64":
      return "x64";
    default:
      return "other";
  }
}

const makeIdentity = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const serverConfig = yield* ServerConfig.ServerConfig;
  const crypto = yield* Crypto.Crypto;

  const readPersistedMachineId = Effect.gen(function* () {
    const exists = yield* fileSystem.exists(serverConfig.machineIdPath).pipe(
      Effect.mapError(
        (cause) =>
          new ServerMachineIdPersistenceError({
            operation: "check",
            machineIdPath: serverConfig.machineIdPath,
            cause,
          }),
      ),
    );
    if (!exists) {
      return null;
    }

    const raw = yield* fileSystem.readFileString(serverConfig.machineIdPath).pipe(
      Effect.map((value) => value.trim()),
      Effect.mapError(
        (cause) =>
          new ServerMachineIdPersistenceError({
            operation: "read",
            machineIdPath: serverConfig.machineIdPath,
            cause,
          }),
      ),
    );

    return raw.length > 0 ? raw : null;
  });

  const persistMachineId = Effect.fn("ServerMachineIdentity.persistMachineId")(
    function* (value: string, mode: "create" | "recover") {
      const destinationPath =
        mode === "recover" ? `${serverConfig.machineIdPath}.recovery` : serverConfig.machineIdPath;
      const tempPath = yield* fileSystem.makeTempFileScoped({
        directory: serverConfig.stateDir,
        prefix: ".machine-id-",
      });
      yield* fileSystem.writeFileString(tempPath, `${value}\n`);
      // Publish the completed file without replacing an ID created by another process.
      yield* fileSystem.link(tempPath, destinationPath).pipe(
        Effect.catchIf(
          (cause) => cause.reason._tag === "AlreadyExists",
          () => Effect.void,
        ),
      );
      if (mode === "recover") {
        // Keep the recovery ID so delayed initializers also publish the same winner.
        yield* fileSystem.remove(tempPath);
        yield* fileSystem.copyFile(destinationPath, tempPath);
        yield* fileSystem.rename(tempPath, serverConfig.machineIdPath);
      }
    },
    Effect.scoped,
    Effect.mapError(
      (cause) =>
        new ServerMachineIdPersistenceError({
          operation: "write",
          machineIdPath: serverConfig.machineIdPath,
          cause,
        }),
    ),
  );

  const machineIdRaw = yield* Effect.gen(function* () {
    const persisted = yield* readPersistedMachineId;
    if (persisted) {
      return persisted;
    }

    const generated = yield* crypto.randomUUIDv4();
    yield* persistMachineId(generated, "create");
    let winner = yield* readPersistedMachineId;
    if (winner === null) {
      yield* persistMachineId(generated, "recover");
      winner = yield* readPersistedMachineId;
    }
    if (winner === null) {
      return yield* new ServerMachineIdPersistenceError({
        operation: "initialize",
        machineIdPath: serverConfig.machineIdPath,
      });
    }
    return winner;
  });

  const machineId = MachineId.make(machineIdRaw);
  return ServerMachineIdentity.of({
    getMachineId: Effect.succeed(machineId),
  });
});

/** @public */
export const make = Effect.gen(function* () {
  const path = yield* Path.Path;
  const serverConfig = yield* ServerConfig.ServerConfig;
  const secrets = yield* ServerSecretStore.ServerSecretStore;
  const identity = yield* ServerMachineIdentity;
  const hostPlatform = yield* HostProcess.Platform;
  const hostArchitecture = yield* HostProcess.Architecture;
  const machineId = yield* identity.getMachineId;
  const cwdBaseName = path.basename(serverConfig.cwd).trim();
  const label = yield* resolveServerMachineLabel({ cwdBaseName });
  const machine = yield* detectServerMachineKind();
  const launcher = yield* resolveServiceLauncherMode();
  const serverSelfUpdate = resolveServerSelfUpdateCapability({
    desktopManaged: serverConfig.mode === "desktop",
    launcherManaged: launcher.managed,
  });
  const serverInstallation = serverSelfUpdate === null ? yield* resolveServerInstallation : null;
  // Static is correct: the control fd is known at bootstrap, and the desktop
  // app and its bundled server ship in one artifact, so a present fd means the
  // app speaks the requestDesktopUpdate protocol.

  const desktopAppUpdate =
    serverSelfUpdate === "desktop-managed" && serverConfig.desktopTelemetryControlFd !== undefined;

  const descriptor: ExecutionMachineDescriptor = {
    machineId,
    label,
    platform: {
      os: platformOs(hostPlatform),
      arch: platformArch(hostArchitecture),
      ...(machine === null ? {} : { machine }),
    },
    serverVersion: packageJson.version,
    orchestrationProtocolVersion: ORCHESTRATION_PROTOCOL_VERSION,
    capabilities: {
      repositoryIdentity: true,
      connectionProbe: true,
      attachmentUploads: true,
      questionAttachments: true,
      fileAttachments: { maxUploadBytes: PROVIDER_SEND_TURN_MAX_FILE_BYTES },
      pullRequests: true,
      pullRequestChecks: true,
      inlineMessageContext: true,
      requiredWorktreeBootstrap: true,
      threadSettlement: true,
      threadAutoSettlement: true,
      storageCleanup: true,
      storageCleanupRun: true,
      databaseStorage: true,
      otlpEndpointCheck: true,
      projectWorktreeCleanup: true,
      settledWorktreeCleanup: true,
      worktreesDirectory: true,
      threadAutoSettlementScope: true,
      threadRestartContinuation: true,
      projectSettingsOverrides: true,
      projectProviderInstanceOverrides: true,
      threadSnooze: true,
      machineThemes: true,
      usageLimitSources: true,
      usagePriceOverrides: true,
      usageModelAliases: true,
      threadPinning: true,
      threadPinReorder: true,
      threadActiveReorder: true,
      threadAutoSettleOptOut: true,
      threadTitleRegeneration: true,
      commandOutputStreaming: true,
      threadVisitedTracking: true,
      threadPullRequests: true,
      threadPullRequestWatch: true,
      pullRequestStackActions: true,
      threadPullRequestLinking: true,
      serverResolvedCommandContext: true,
      threadStop: true,
      machineIcon: true,
      projectCloneTracking: true,
      ignoredFileListing: true,
      ...(serverSelfUpdate === null ? {} : { serverSelfUpdate }),
      ...(serverInstallation === null ? {} : { serverInstallation }),
      ...(serverSelfUpdate === "boot-service" || desktopAppUpdate
        ? { serverSelfUpdateProgress: true }
        : {}),
      ...(desktopAppUpdate ? { desktopAppUpdate: true } : {}),
      serverBrowser: true,
    },
  };

  return ServerMachine.of({
    getMachineId: Effect.succeed(machineId),
    // The publish opt-in and relay link change at runtime (`vekrel connect
    // publish`, the client settings toggle), so the capability is read per
    // descriptor request rather than baked in at startup.
    getDescriptor: readAgentActivityPublishingActive(secrets).pipe(
      Effect.map((agentActivityPublishing) => ({
        ...descriptor,
        capabilities: { ...descriptor.capabilities, agentActivityPublishing },
      })),
    ),
  });
});

export const layerIdentity = Layer.effect(ServerMachineIdentity, makeIdentity);

export const layer = Layer.effect(ServerMachine, make).pipe(
  Layer.provideMerge(layerIdentity),
  Layer.provide(ProcessRunner.layer),
);
