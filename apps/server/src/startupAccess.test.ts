import { describe, expect, it } from "@effect/vitest";
import * as ConfigProvider from "effect/ConfigProvider";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import { HttpServer } from "effect/http";
import * as NetAddress from "effect/net/NetAddress";

import * as MachineAuth from "./auth/MachineAuth.ts";
import * as ServerConfig from "./config.ts";
import { resolveStartupBrowserTarget } from "./serverRuntimeStartup.ts";
import { makePersistedServerRuntimeState } from "./serverRuntimeState.ts";
import {
  advertisedOriginConfig,
  buildPairingUrl,
  formatHeadlessServeOutput,
  issueHeadlessServeAccessInfo,
  parseAdvertisedOrigin,
  renderTerminalQrCode,
  resolveAdvertisedOrigin,
  resolveHeadlessConnectionString,
} from "./startupAccess.ts";

it("resolves wildcard hosts to a concrete external interface when one is available", () => {
  const connectionString = resolveHeadlessConnectionString("0.0.0.0", 3773, {
    en0: [
      {
        address: "192.168.1.42",
        netmask: "255.255.255.0",
        family: "IPv4",
        mac: "00:00:00:00:00:00",
        internal: false,
        cidr: "192.168.1.42/24",
      },
    ],
    lo0: [
      {
        address: "127.0.0.1",
        netmask: "255.0.0.0",
        family: "IPv4",
        mac: "00:00:00:00:00:00",
        internal: true,
        cidr: "127.0.0.1/8",
      },
    ],
  });

  expect(connectionString).toBe("http://192.168.1.42:3773");
});

describe("VEKREL_ADVERTISED_ORIGIN", () => {
  const token = "PAIRTOKEN7Q4K9";
  const enginePort = 3783;

  const withStartup = <A, E, R>(
    program: Effect.Effect<A, E, R>,
    env: Record<string, string>,
    warnings: Array<unknown> = [],
  ) =>
    program.pipe(
      Effect.provide(
        Layer.mergeAll(
          Layer.mock(ServerConfig.ServerConfig)({
            host: "127.0.0.1",
            port: enginePort,
            mode: "web",
            devUrl: undefined,
          } as unknown as ServerConfig.ServerConfig["Service"]),
          Layer.mock(HttpServer.HttpServer)({
            address: NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", enginePort),
          }),
          Layer.mock(MachineAuth.MachineAuth)({
            issueStartupPairingUrl: (baseUrl) => Effect.succeed(buildPairingUrl(baseUrl, token)),
            issueStartupPairingCredential: () =>
              Effect.succeed({
                id: "startup",
                credential: token,
                expiresAt: DateTime.makeUnsafe(0),
              }),
          }),
          ConfigProvider.layer(ConfigProvider.fromEnv({ env })),
          Logger.layer(
            [
              Logger.make(({ logLevel, message }) => {
                if (logLevel === "Warn") warnings.push(message);
              }),
            ],
            { mergeWithExisting: false },
          ),
        ),
      ),
    );

  const headlessOutput = (env: Record<string, string>, warnings: Array<unknown> = []) =>
    withStartup(
      Effect.gen(function* () {
        const advertisedOrigin = yield* resolveAdvertisedOrigin;
        return formatHeadlessServeOutput(yield* issueHeadlessServeAccessInfo(advertisedOrigin));
      }),
      env,
      warnings,
    );

  const browserTarget = (env: Record<string, string>) =>
    withStartup(
      Effect.flatMap(resolveAdvertisedOrigin, (origin) => resolveStartupBrowserTarget(origin)),
      env,
    );

  it.effect("uses the advertised origin for the non-headless pairingUrl, else its own", () =>
    Effect.gen(function* () {
      expect(yield* browserTarget({ VEKREL_ADVERTISED_ORIGIN: "http://127.0.0.1:3773" })).toBe(
        `http://127.0.0.1:3773/pair#token=${token}`,
      );
      expect(yield* browserTarget({})).toBe(`http://127.0.0.1:${enginePort}/pair#token=${token}`);
    }),
  );

  it.effect("records the advertised origin next to the unchanged runtime origin", () =>
    withStartup(
      Effect.gen(function* () {
        const state = yield* makePersistedServerRuntimeState({
          config: { host: "127.0.0.1", devUrl: undefined },
          port: enginePort,
          advertisedOrigin: yield* advertisedOriginConfig,
        });
        expect(state.origin).toBe(`http://127.0.0.1:${enginePort}`);
        expect(state.advertisedOrigin).toBe("http://127.0.0.1:3773");
      }),
      { VEKREL_ADVERTISED_ORIGIN: "http://127.0.0.1:3773/" },
    ),
  );

  it.effect("prints the advertised origin and a QR of its pairing URL", () =>
    Effect.gen(function* () {
      const advertised = "http://127.0.0.1:3773";
      const output = yield* headlessOutput({ VEKREL_ADVERTISED_ORIGIN: advertised });
      const pairingUrl = `${advertised}/pair#token=${token}`;
      expect(output).toContain(`Connection string: ${advertised}\n`);
      expect(output).toContain(`Pairing URL: ${pairingUrl}\n`);
      expect(output).toContain(renderTerminalQrCode(pairingUrl));
      expect(output).not.toContain(String(enginePort));
    }),
  );

  it.effect("prints the engine's own origin when unset", () =>
    Effect.gen(function* () {
      const output = yield* headlessOutput({});
      const pairingUrl = `http://127.0.0.1:${enginePort}/pair#token=${token}`;
      expect(output).toContain(`Connection string: http://127.0.0.1:${enginePort}\n`);
      expect(output).toContain(`Pairing URL: ${pairingUrl}\n`);
      expect(output).toContain(renderTerminalQrCode(pairingUrl));
    }),
  );

  it.effect("ignores an invalid value with one warning", () =>
    Effect.gen(function* () {
      for (const invalid of ["ftp://127.0.0.1:3773", "http://127.0.0.1:3773/x", "not a url"]) {
        const warnings: Array<unknown> = [];
        const output = yield* headlessOutput({ VEKREL_ADVERTISED_ORIGIN: invalid }, warnings);
        expect(output).toContain(`Connection string: http://127.0.0.1:${enginePort}\n`);
        expect(warnings).toHaveLength(1);
      }
    }),
  );

  it("accepts http(s) origins with or without a trailing slash", () => {
    expect(parseAdvertisedOrigin("https://example.com/")).toBe("https://example.com");
    expect(parseAdvertisedOrigin("http://[::1]:3773")).toBe("http://[::1]:3773");
    expect(parseAdvertisedOrigin("http://127.0.0.1:3773?x=1")).toBeUndefined();
    expect(parseAdvertisedOrigin("")).toBeUndefined();
  });
});
