import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as ServerConfig from "../config.ts";
import * as ServerMachine from "../machine/ServerMachine.ts";
import * as MachineAuthPolicy from "./MachineAuthPolicy.ts";

const layerMachineAuthPolicy = (overrides?: Partial<ServerConfig.ServerConfig["Service"]>) =>
  MachineAuthPolicy.layer.pipe(
    Layer.provide(ServerMachine.layerIdentity),
    Layer.provide(
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
        Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "t3-auth-policy-test-" })),
      ),
    ),
  );

it.layer(NodeServices.layer)("MachineAuthPolicy.layer", (it) => {
  it.effect("keeps desktop cookies port-scoped on the port a second instance lands on", () =>
    Effect.gen(function* () {
      const policy = yield* MachineAuthPolicy.MachineAuthPolicy;
      const descriptor = yield* policy.getDescriptor();

      expect(descriptor.sessionCookieName).toBe("vekrel_session_3774");
    }).pipe(
      Effect.provide(
        layerMachineAuthPolicy({
          mode: "desktop",
          port: 3774,
        }),
      ),
    ),
  );

  it.effect("uses remote-reachable policy for desktop mode when bound beyond loopback", () =>
    Effect.gen(function* () {
      const policy = yield* MachineAuthPolicy.MachineAuthPolicy;
      const descriptor = yield* policy.getDescriptor();

      expect(descriptor.policy).toBe("remote-reachable");
      expect(descriptor.bootstrapMethods).toEqual(["desktop-bootstrap", "one-time-token"]);
    }).pipe(
      Effect.provide(
        layerMachineAuthPolicy({
          mode: "desktop",
          host: "0.0.0.0",
        }),
      ),
    ),
  );

  it.effect("uses loopback-browser policy for loopback web hosts", () =>
    Effect.gen(function* () {
      const policy = yield* MachineAuthPolicy.MachineAuthPolicy;
      const descriptor = yield* policy.getDescriptor();

      expect(descriptor.policy).toBe("loopback-browser");
      expect(descriptor.bootstrapMethods).toEqual(["one-time-token"]);
      expect(descriptor.serverUpdateScope).toBe("machine:maintain");
      expect(descriptor.sessionCookieName).toMatch(/^vekrel_session_3773_[a-f0-9]{12}$/);
    }).pipe(
      Effect.provide(
        layerMachineAuthPolicy({
          mode: "web",
          host: "127.0.0.1",
          port: 3773,
        }),
      ),
    ),
  );
});
