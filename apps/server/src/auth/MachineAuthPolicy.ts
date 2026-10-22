import { AuthMachineMaintainScope, type ServerAuthDescriptor } from "@vekrel/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as ServerConfig from "../config.ts";
import * as ServerMachine from "../machine/ServerMachine.ts";
import { isRemoteReachableHost, resolveSessionCookieName } from "./utils.ts";

export class MachineAuthPolicy extends Context.Service<
  MachineAuthPolicy,
  {
    readonly getDescriptor: () => Effect.Effect<ServerAuthDescriptor>;
  }
>()("vekrel-engine/auth/MachineAuthPolicy") {}

/** @public Service construction is part of the canonical Effect module API. */
export const make = Effect.gen(function* () {
  const config = yield* ServerConfig.ServerConfig;
  const serverMachine = yield* ServerMachine.ServerMachineIdentity;
  const isRemoteReachable = isRemoteReachableHost(config.host);

  const policy =
    config.mode === "desktop"
      ? isRemoteReachable
        ? "remote-reachable"
        : "desktop-managed-local"
      : isRemoteReachable
        ? "remote-reachable"
        : "loopback-browser";

  const bootstrapMethods: ServerAuthDescriptor["bootstrapMethods"] =
    policy === "desktop-managed-local"
      ? ["desktop-bootstrap"]
      : config.mode === "desktop" && policy === "remote-reachable"
        ? ["desktop-bootstrap", "one-time-token"]
        : ["one-time-token"];

  const descriptor: ServerAuthDescriptor = {
    policy,
    bootstrapMethods,
    sessionMethods: ["browser-session-cookie", "bearer-access-token", "dpop-access-token"],
    serverUpdateScope: AuthMachineMaintainScope,
    sessionCookieName: resolveSessionCookieName({
      mode: config.mode,
      port: config.port,
      host: config.host,
      instanceKey: config.stateDir,
      machineId: yield* serverMachine.getMachineId,
      development: config.devUrl !== undefined,
    }),
  };

  return MachineAuthPolicy.of({
    getDescriptor: () =>
      Effect.succeed(descriptor).pipe(Effect.withSpan("MachineAuthPolicy.getDescriptor")),
  });
});

export const layer = Layer.effect(MachineAuthPolicy, make);
