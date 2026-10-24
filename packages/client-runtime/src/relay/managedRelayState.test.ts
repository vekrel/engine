import { MachineId } from "@vekrel/contracts";
import {
  type RelayClientDeviceRecord,
  type RelayClientMachineRecord,
} from "@vekrel/contracts/relay";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import { AtomRegistry } from "effect/reactivity";
import { afterEach, vi } from "vite-plus/test";

import * as ManagedRelay from "./managedRelay.ts";
import {
  createManagedRelaySession,
  deregisterManagedRelayMachine,
  ManagedRelaySessionError,
  managedRelaySessionAtom,
  setManagedRelaySession,
} from "./managedRelayState.ts";

let registry = AtomRegistry.make();

const machine = {
  machineId: MachineId.make("machine-1"),
  label: "Main machine",
  endpoint: {
    httpBaseUrl: "https://machine.example.test",
    wsBaseUrl: "wss://machine.example.test",
    providerKind: "cloudflare_tunnel",
  },
  linkedAt: "2026-06-01T00:00:00.000Z",
} satisfies RelayClientMachineRecord;

const device = {
  deviceId: "device-1",
  label: "Julius iPhone",
  platform: "ios",
  iosMajorVersion: 18,
  appVersion: null,
  notifications: {
    enabled: true,
    notifyOnApproval: true,
    notifyOnInput: true,
    notifyOnCompletion: true,
    notifyOnFailure: true,
  },
  liveActivities: {
    enabled: true,
  },
  updatedAt: "2026-06-01T00:00:00.000Z",
} satisfies RelayClientDeviceRecord;

function resetRegistry() {
  registry.dispose();
  registry = AtomRegistry.make();
}

function createClient(overrides?: Partial<ManagedRelay.ManagedRelayClient["Service"]>) {
  return ManagedRelay.ManagedRelayClient.of({
    relayUrl: "https://relay.example.test",
    listMachines: () => Effect.succeed([machine]),
    listDevices: () => Effect.succeed([device]),
    createMachineLinkChallenge: () => Effect.die("unused"),
    linkMachine: () => Effect.die("unused"),
    unlinkMachine: () => Effect.die("unused"),
    getMachineStatus: () =>
      Effect.succeed({
        machineId: machine.machineId,
        endpoint: machine.endpoint,
        status: "online",
        checkedAt: "2026-06-01T00:00:00.000Z",
      }),
    connectMachine: () => Effect.die("unused"),
    registerDevice: () => Effect.die("unused"),
    unregisterDevice: () => Effect.die("unused"),
    registerLiveActivity: () => Effect.die("unused"),
    getAgentActivitySnapshot: () => Effect.die("unused"),
    resetTokenCache: Effect.void,
    ...overrides,
  });
}

function setSession() {
  setManagedRelaySession(registry, {
    accountId: "account-1",
    readToken: () => Promise.resolve("account-token"),
  });
}

function accountToken(expiresAtSeconds: number): string {
  const encode = (value: unknown) =>
    btoa(JSON.stringify(value)).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/u, "");
  return `${encode({ alg: "none" })}.${encode({ exp: expiresAtSeconds })}.signature`;
}

describe("createManagedRelayQueryManager", () => {
  afterEach(resetRegistry);

  it.effect("rejects deregistration after the account changes", () =>
    Effect.gen(function* () {
      const unlinkMachine = vi.fn(() => Effect.succeed({ ok: true }));
      setSession();

      const error = yield* deregisterManagedRelayMachine(registry, {
        accountId: "previous-account",
        machineId: machine.machineId,
      }).pipe(
        Effect.provideService(ManagedRelay.ManagedRelayClient, createClient({ unlinkMachine })),
        Effect.flip,
      );

      expect(error).toBeInstanceOf(ManagedRelaySessionError);
      expect(unlinkMachine).not.toHaveBeenCalled();
    }),
  );

  it.effect("deduplicates concurrent account token reads and reuses the token until JWT expiry", () =>
    Effect.gen(function* () {
      const token = accountToken(4_102_444_800);
      let resolveToken!: (value: string) => void;
      const readToken = vi.fn(
        () =>
          new Promise<string>((resolve) => {
            resolveToken = resolve;
          }),
      );
      const session = createManagedRelaySession({
        accountId: "account-1",
        readToken,
      });

      const readsFiber = yield* Effect.all([session.readToken(), session.readToken()], {
        concurrency: "unbounded",
      }).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      expect(readToken).toHaveBeenCalledTimes(1);

      resolveToken(token);
      expect(yield* Fiber.join(readsFiber)).toEqual([token, token]);
      expect(yield* session.readToken()).toBe(token);
      expect(readToken).toHaveBeenCalledTimes(1);
    }),
  );

  it.effect("updates the token provider without replacing a same-account session", () =>
    Effect.gen(function* () {
      const firstRead = vi.fn(() => Promise.resolve<string | null>(null));
      setManagedRelaySession(registry, {
        accountId: "account-1",
        readToken: firstRead,
      });
      const firstSession = registry.get(managedRelaySessionAtom);
      expect(firstSession).not.toBeNull();
      expect(yield* firstSession!.readToken()).toBeNull();

      const secondRead = vi.fn(() => Promise.resolve<string | null>("refreshed-token"));
      setManagedRelaySession(registry, {
        accountId: "account-1",
        readToken: secondRead,
      });

      expect(registry.get(managedRelaySessionAtom)).toBe(firstSession);
      expect(yield* firstSession!.readToken()).toBe("refreshed-token");
      expect(firstRead).toHaveBeenCalledTimes(1);
      expect(secondRead).toHaveBeenCalledTimes(1);
    }),
  );

  it.effect("does not pin a refreshed session to an older pending token read", () =>
    Effect.gen(function* () {
      let resolveFirst!: (token: string) => void;
      setManagedRelaySession(registry, {
        accountId: "account-1",
        readToken: () =>
          new Promise<string>((resolve) => {
            resolveFirst = resolve;
          }),
      });
      const session = registry.get(managedRelaySessionAtom);
      const firstRead = yield* session!.readToken().pipe(Effect.forkChild);
      yield* Effect.yieldNow;

      setManagedRelaySession(registry, {
        accountId: "account-1",
        readToken: () => Promise.resolve("refreshed-token"),
      });

      expect(yield* session!.readToken()).toBe("refreshed-token");
      resolveFirst("older-token");
      expect(yield* Fiber.join(firstRead)).toBe("older-token");
    }),
  );
});
