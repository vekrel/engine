import { AuthStandardClientScopes } from "@vekrel/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as ServerConfig from "../config.ts";
import * as ServerMachine from "../machine/ServerMachine.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as MachineAuth from "./MachineAuth.ts";
import * as ServerSecretStore from "./ServerSecretStore.ts";
import * as SessionStore from "./SessionStore.ts";

const layerServerConfig = (
  overrides?: Partial<Pick<ServerConfig.ServerConfig["Service"], "desktopBootstrapToken">>,
) =>
  Layer.effect(
    ServerConfig.ServerConfig,
    Effect.gen(function* () {
      const config = yield* ServerConfig.ServerConfig;
      return {
        ...config,
        ...overrides,
      } satisfies ServerConfig.ServerConfig["Service"];
    }),
  ).pipe(
    Layer.provide(
      ServerConfig.layerTest(process.cwd(), {
        prefix: "t3-auth-control-plane-test-",
      }),
    ),
  );

const layerMachineAuth = (
  overrides?: Partial<Pick<ServerConfig.ServerConfig["Service"], "desktopBootstrapToken">>,
) =>
  MachineAuth.layer.pipe(
    Layer.provideMerge(ServerSecretStore.layer),
    Layer.provideMerge(SqlitePersistence.layerMemory),
    Layer.provide(ServerMachine.layerIdentity),
    Layer.provide(layerServerConfig(overrides)),
  );

it.layer(NodeServices.layer)("MachineAuth administrative operations", (it) => {
  it.effect("creates, lists, and revokes client pairing links", () =>
    Effect.gen(function* () {
      const machineAuth = yield* MachineAuth.MachineAuth;

      const created = yield* machineAuth.createPairingLink({
        scopes: ["orchestration:read"],
        subject: "one-time-token",
        label: "CI phone",
      });
      const listedBeforeRevoke = yield* machineAuth.listPairingLinks();
      const revoked = yield* machineAuth.revokePairingLink(created.id);
      const listedAfterRevoke = yield* machineAuth.listPairingLinks();

      expect(created.scopes).toEqual(["orchestration:read"]);
      expect(created.credential.length).toBeGreaterThan(0);
      expect(listedBeforeRevoke).toHaveLength(1);
      expect(listedBeforeRevoke[0]?.id).toBe(created.id);
      expect(listedBeforeRevoke[0]?.label).toBe("CI phone");
      expect(listedBeforeRevoke[0]).not.toHaveProperty("credential");
      expect(revoked).toBe(true);
      expect(listedAfterRevoke).toHaveLength(0);
    }).pipe(Effect.provide(layerMachineAuth())),
  );

  it.effect("issues bearer access token sessions without exposing raw tokens", () =>
    Effect.gen(function* () {
      const machineAuth = yield* MachineAuth.MachineAuth;
      const sessionCredentials = yield* SessionStore.SessionStore;

      const issued = yield* machineAuth.issueSession({
        label: "deploy-bot",
      });
      const verified = yield* sessionCredentials.verify(issued.token);
      const listedBeforeRevoke = yield* machineAuth.listSessions();
      const revoked = yield* machineAuth.revokeSession(issued.sessionId);
      const listedAfterRevoke = yield* machineAuth.listSessions();

      expect(issued.method).toBe("bearer-access-token");
      expect(issued.scopes).toEqual(AuthStandardClientScopes);
      expect(issued.client.deviceType).toBe("bot");
      expect(issued.client.label).toBe("deploy-bot");
      expect(verified.sessionId).toBe(issued.sessionId);
      expect(verified.scopes).toEqual(AuthStandardClientScopes);
      expect(verified.method).toBe("bearer-access-token");
      expect(listedBeforeRevoke).toHaveLength(1);
      expect(listedBeforeRevoke[0]?.sessionId).toBe(issued.sessionId);
      expect("token" in (listedBeforeRevoke[0] ?? {})).toBe(false);
      expect(revoked).toBe(true);
      expect(listedAfterRevoke).toHaveLength(0);
    }).pipe(Effect.provide(layerMachineAuth())),
  );
});
