import * as NodeServices from "@effect/platform-node/NodeServices";
import { ORCHESTRATION_PROTOCOL_VERSION } from "@vekrel/contracts";
import { expect, it } from "@effect/vitest";
import * as Crypto from "effect/Crypto";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";

import { DEFAULT_SIGNAL_EXPORT } from "@vekrel/shared/observability";
import * as HostProcess from "@vekrel/shared/HostProcess";
import * as OtelEnvironment from "@vekrel/shared/otelEnvironment";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import {
  PUBLISH_AGENT_ACTIVITY_SECRET,
  RELAY_MACHINE_CREDENTIAL_SECRET,
  RELAY_URL_SECRET,
} from "../cloud/config.ts";
import * as ServerConfig from "../config.ts";
import * as ServerMachine from "./ServerMachine.ts";

const layerServerMachine = (baseDir: string) =>
  ServerMachine.layer.pipe(
    Layer.provide(ServerSecretStore.layer),
    Layer.provide(ServerConfig.layerTest(process.cwd(), baseDir)),
  );

const layerEmptySecretStore = Layer.succeed(
  ServerSecretStore.ServerSecretStore,
  ServerSecretStore.ServerSecretStore.of({
    get: () => Effect.succeedNone,
    set: () => Effect.void,
    create: () => Effect.void,
    getOrCreateRandom: () => Effect.succeed(new Uint8Array()),
    remove: () => Effect.void,
  }),
);

const makeServerConfig = Effect.fn(function* (baseDir: string) {
  const derivedPaths = yield* ServerConfig.deriveServerPaths(baseDir, undefined);

  return {
    ...derivedPaths,
    logLevel: "Error",
    traceMinLevel: "Info",
    traceTimingEnabled: true,
    traceBatchWindowMs: 200,
    traceMaxBytes: 10 * 1024 * 1024,
    traceMaxFiles: 10,
    otlpTracesUrl: undefined,
    otlpMetricsUrl: undefined,
    otlpLogsUrl: undefined,
    otlpTracesExport: DEFAULT_SIGNAL_EXPORT,
    otlpMetricsExport: DEFAULT_SIGNAL_EXPORT,
    otlpLogsExport: DEFAULT_SIGNAL_EXPORT,
    otelEnvironment: OtelEnvironment.none,
    cwd: process.cwd(),
    baseDir,
    mode: "web",
    autoBootstrapProjectFromCwd: false,
    logWebSocketEvents: false,
    tailscaleServeEnabled: false,
    tailscaleServePort: 443,
    port: 0,
    host: undefined,
    desktopBootstrapToken: undefined,
    staticDir: undefined,
    devUrl: undefined,
    devAllowedOrigins: [],
    noBrowser: false,
    startupPresentation: "browser",
  } satisfies ServerConfig.ServerConfig["Service"];
});

it.layer(NodeServices.layer)("ServerMachineLive", (it) => {
  it.effect("publishes proven install ownership only for manually updated servers", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const baseDir = yield* fs.makeTempDirectoryScoped();
      const prefix = `${baseDir}/node`;
      const entry = `${prefix}/lib/node_modules/t3/dist/bin.mjs`;
      yield* fs.makeDirectory(`${prefix}/lib/node_modules/t3/dist`, { recursive: true });
      yield* fs.makeDirectory(`${prefix}/bin`, { recursive: true });
      yield* fs.writeFileString(entry, "");
      yield* fs.writeFileString(
        `${prefix}/lib/node_modules/t3/package.json`,
        '{"name":"t3","version":"0.0.45","bin":{"t3":"./dist/bin.mjs"}}',
      );
      yield* fs.symlink(entry, `${prefix}/bin/t3`);
      const config = yield* makeServerConfig(baseDir);
      yield* fs.makeDirectory(config.stateDir, { recursive: true });
      for (const mode of ["web", "desktop"] as const) {
        const descriptor = yield* Effect.gen(function* () {
          const machine = yield* ServerMachine.ServerMachine;
          return yield* machine.getDescriptor;
        }).pipe(
          Effect.provide(
            ServerMachine.layer.pipe(
              Layer.provide(layerEmptySecretStore),
              Layer.provide(ServerConfig.layer({ ...config, mode })),
            ),
          ),
          Effect.provideService(HostProcess.Arguments, ["node", entry]),
          Effect.provideService(HostProcess.IsExecutable, false),
          Effect.provideService(HostProcess.Platform, "linux"),
          Effect.provideService(HostProcess.Environment, {}),
        );
        expect(descriptor.capabilities.serverInstallation).toEqual(
          mode === "web" ? { kind: "npm-global", prefix } : undefined,
        );
        expect(descriptor.capabilities.serverSelfUpdate).toBe(
          mode === "web" ? undefined : "desktop-managed",
        );
        expect(descriptor.capabilities).toMatchObject({
          settledWorktreeCleanup: true,
          storageCleanupRun: true,
          projectWorktreeCleanup: true,
          worktreesDirectory: true,
          threadAutoSettlementScope: true,
        });
      }
    }),
  );
  it.effect.each([
    { name: "missing", content: undefined },
    { name: "empty", content: "" },
    { name: "whitespace-only", content: " \t\n" },
  ])("concurrent initializers recover a $name machine id file", ({ content }) =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const crypto = yield* Crypto.Crypto;
      const baseDir = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-server-machine-concurrent-test-",
      });
      const serverConfig = yield* makeServerConfig(baseDir);
      yield* fileSystem.makeDirectory(serverConfig.stateDir, { recursive: true });
      if (content !== undefined) {
        yield* fileSystem.writeFileString(serverConfig.machineIdPath, content);
      }
      const bothGenerated = yield* Deferred.make<void>();
      const bothReadEmpty = yield* Deferred.make<void>();
      const firstInitialized = yield* Deferred.make<void>();
      let remaining = 2;
      let emptyReads = 0;
      const readIdentity = Effect.gen(function* () {
        const identity = yield* ServerMachine.ServerMachineIdentity;
        return yield* identity.getMachineId;
      }).pipe(
        Effect.tap(() => Deferred.succeed(firstInitialized, undefined)),
        Effect.provide(Layer.fresh(ServerMachine.layerIdentity)),
        Effect.provideService(ServerConfig.ServerConfig, serverConfig),
        Effect.provideService(FileSystem.FileSystem, {
          ...fileSystem,
          readFileString: (path) =>
            fileSystem.readFileString(path).pipe(
              Effect.tap(
                Effect.fn(function* (value) {
                  if (path !== serverConfig.machineIdPath || remaining > 0 || value.trim()) {
                    return;
                  }
                  if (++emptyReads === 2) {
                    yield* Deferred.succeed(bothReadEmpty, undefined);
                    yield* Deferred.await(firstInitialized);
                  } else {
                    yield* Deferred.await(bothReadEmpty);
                  }
                }),
              ),
            ),
        }),
        Effect.provideService(Crypto.Crypto, {
          ...crypto,
          randomUUIDv4: (() =>
            Effect.gen(function* () {
              const id = yield* crypto.randomUUIDv4();
              if (--remaining === 0) {
                yield* Deferred.succeed(bothGenerated, undefined);
              }
              yield* Deferred.await(bothGenerated);
              return id;
            })) as Crypto.Crypto["randomUUIDv4"],
        }),
      );

      const [first, second] = yield* Effect.all([readIdentity, readIdentity], {
        concurrency: "unbounded",
      });
      const persisted = yield* fileSystem.readFileString(serverConfig.machineIdPath);

      expect(first).toBe(second);
      expect(persisted.trim()).toBe(first);
    }),
  );

  it.effect("persists the machine id across service restarts", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const baseDir = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-server-machine-test-",
      });

      const first = yield* Effect.gen(function* () {
        const serverMachine = yield* ServerMachine.ServerMachine;
        return yield* serverMachine.getDescriptor;
      }).pipe(Effect.provide(layerServerMachine(baseDir)));
      const second = yield* Effect.gen(function* () {
        const serverMachine = yield* ServerMachine.ServerMachine;
        return yield* serverMachine.getDescriptor;
      }).pipe(Effect.provide(layerServerMachine(baseDir)));

      expect(first.machineId).toBe(second.machineId);
      expect(first.orchestrationProtocolVersion).toBe(ORCHESTRATION_PROTOCOL_VERSION);
      expect(second.capabilities.repositoryIdentity).toBe(true);
      expect(second.capabilities.connectionProbe).toBe(true);
      expect(second.capabilities.attachmentUploads).toBe(true);
      expect(second.capabilities.fileAttachments).toEqual({ maxUploadBytes: 50 * 1024 * 1024 });
      expect(second.capabilities.pullRequests).toBe(true);
      expect(second.capabilities.requiredWorktreeBootstrap).toBe(true);
      expect(second.capabilities.usagePriceOverrides).toBe(true);
      expect(second.capabilities.threadActiveReorder).toBe(true);
      expect(second.capabilities.threadTitleRegeneration).toBe(true);
      expect(second.capabilities.threadPullRequests).toBe(true);
      expect(second.capabilities.threadPullRequestLinking).toBe(true);
      expect(second.capabilities.serverResolvedCommandContext).toBe(true);
      expect(second.capabilities.projectProviderInstanceOverrides).toBe(true);
      expect(second.capabilities.agentActivityPublishing).toBe(false);
    }),
  );

  it.effect("reports agent activity publishing from the current secret state", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const baseDir = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-server-machine-publish-test-",
      });
      const layerTest = Layer.mergeAll(
        ServerMachine.layer.pipe(Layer.provide(ServerSecretStore.layer)),
        ServerSecretStore.layer,
      ).pipe(Layer.provide(ServerConfig.layerTest(process.cwd(), baseDir)));

      yield* Effect.gen(function* () {
        const secrets = yield* ServerSecretStore.ServerSecretStore;
        const serverMachine = yield* ServerMachine.ServerMachine;
        const encode = (value: string) => new TextEncoder().encode(value);

        const unlinked = yield* serverMachine.getDescriptor;
        expect(unlinked.capabilities.agentActivityPublishing).toBe(false);

        yield* secrets.set(PUBLISH_AGENT_ACTIVITY_SECRET, encode("true"));
        const withoutLink = yield* serverMachine.getDescriptor;
        expect(withoutLink.capabilities.agentActivityPublishing).toBe(false);

        yield* secrets.set(RELAY_URL_SECRET, encode(""));
        yield* secrets.set(RELAY_MACHINE_CREDENTIAL_SECRET, encode("credential"));
        const emptyUrl = yield* serverMachine.getDescriptor;
        expect(emptyUrl.capabilities.agentActivityPublishing).toBe(false);

        yield* secrets.set(RELAY_URL_SECRET, encode("https://relay.example"));
        const linked = yield* serverMachine.getDescriptor;
        expect(linked.capabilities.agentActivityPublishing).toBe(true);

        yield* secrets.set(PUBLISH_AGENT_ACTIVITY_SECRET, encode("false"));
        const disabled = yield* serverMachine.getDescriptor;
        expect(disabled.capabilities.agentActivityPublishing).toBe(false);
      }).pipe(Effect.provide(layerTest));
    }),
  );
});
