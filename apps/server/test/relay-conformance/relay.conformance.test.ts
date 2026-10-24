/* oxlint-disable t3code/no-manual-effect-runtime-in-tests -- a live gate against an external relay process: each machine's CloudLink runtime is shared with the node:http callback the relay calls into, which it.effect layers cannot reach. */
// @effect-diagnostics globalDate:off globalRandom:off globalTimers:off globalConsole:off -- a live gate against another process's real clock.
// Plan 6A Task 13 conformance gate. Test-only; skipped unless RELAY_CONFORMANCE_URL is set.
//
// Drives the real Engine `cloud/CloudLink.ts` and the real client-runtime `managedRelay.ts`
// against a running Vekrel relay, decodes every relay answer with Shared's contract schemas in
// strict mode (excess keys are errors, bytes must equal the schema encoding), verifies every
// relay-signed JWS with Shared's `relayJwt.ts`, and replays the HTTP cases of T3's
// `infra/relay/src/http/Api.test.ts` that a live relay can answer.
//
// Local run (see the Task 13 report): the relay serves http on 127.0.0.1 with
//   RELAY_ORIGIN=https://localhost:<port>, CLOUDFLARE_API_BASE=http://127.0.0.1:<cf>/client/v4,
//   RELAY_DEV_MACHINE_ENDPOINT=http://127.0.0.1:<machine>, MAGIC_LINK_TRANSPORT=log, SIGNUP_MODE=open
// and this file runs with
//   RELAY_CONFORMANCE_URL=https://localhost:<port> RELAY_CONFORMANCE_TRANSPORT=http://127.0.0.1:<port>
//   RELAY_CONFORMANCE_CLOUDFLARE_PORT=<cf> RELAY_CONFORMANCE_MACHINE_PORT=<machine>
//   RELAY_CONFORMANCE_TUNNEL_ZONE=<RELAY_TUNNEL_ZONE_NAME> RELAY_CONFORMANCE_CLOUDFLARE_TOKEN=<token>
// @effect-diagnostics-next-line nodeBuiltinImport:off -- reads the secret files CloudLink writes.
import * as NodeFS from "node:fs";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- test-only random ids.
import * as NodeCrypto from "node:crypto";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- the type of the test servers.
import type * as NodeHttp from "node:http";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { afterAll, beforeAll, describe, expect, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import { HttpServer } from "effect/http";
import * as NetAddress from "effect/net/NetAddress";

import { MachineId, type ThreadId } from "@vekrel/contracts";
import {
  RelayCloudMachineHealthRequest,
  RelayCloudMintCredentialRequest,
  RelayInternalError,
  RelayMachineConnectScope,
  RelayMachineStatusScope,
  type RelayManagedEndpointRuntimeConfig,
} from "@vekrel/contracts/relay";
import {
  normalizeRelayIssuer,
  RELAY_ACTIVITY_PUBLISH_TYP,
  RELAY_HEALTH_REQUEST_TYP,
  RELAY_MANAGED_TUNNEL_RECOVERY_TYP,
  RELAY_MINT_REQUEST_TYP,
  signRelayJwt,
  verifyRelayJwt,
} from "@vekrel/shared/relayJwt";

import * as ManagedRelay from "../../../../packages/client-runtime/src/relay/managedRelay.ts";
import * as RpcHttp from "../../../../packages/client-runtime/src/rpc/http.ts";
import * as MachineAuth from "../../src/auth/MachineAuth.ts";
import * as ServerSecretStore from "../../src/auth/ServerSecretStore.ts";
import * as CliState from "../../src/cloud/CliState.ts";
import * as CliTokenManager from "../../src/cloud/CliTokenManager.ts";
import * as CloudLink from "../../src/cloud/CloudLink.ts";
import * as CloudConfig from "../../src/cloud/config.ts";
import * as ConnectReachability from "../../src/cloud/ConnectReachability.ts";
import * as ManagedEndpointRuntime from "../../src/cloud/ManagedEndpointRuntime.ts";
import { getOrCreateMachineKeyPairFromSecretStore } from "../../src/cloud/machineKeys.ts";
import * as ServerConfig from "../../src/config.ts";
import * as ServerMachine from "../../src/machine/ServerMachine.ts";
import * as AgentAwarenessRelay from "../../src/relay/AgentAwarenessRelay.ts";
import { makeRelayMachineClient } from "../../src/relay/relayMachineClient.ts";
import {
  CLOUDFLARE_PORT,
  CLOUDFLARE_TOKEN,
  checkExchanges,
  close,
  exchanges,
  FakeCloudflare,
  listen,
  MACHINE_PORT,
  makeDpopKey,
  RELAY_URL,
  relayFetch,
  signIn,
  TUNNEL_ZONE,
  type AccountTokens,
  type Exchange,
} from "./harness.ts";

const enabled = RELAY_URL !== "";
if (enabled) {
  // FetchHttpClient reads `globalThis.fetch` per request; the Engine's relay clients use it.
  globalThis.fetch = relayFetch;
  process.env.VEKREL_RELAY_URL = RELAY_URL;
}
const encodeInternalError = Schema.encodeSync(RelayInternalError);
const ISSUER = normalizeRelayIssuer(RELAY_URL || "https://unset.invalid");
// oxlint-disable-next-line t3code/no-wall-clock-in-tests -- JWT iat/exp are checked against the relay's real clock.
const nowSeconds = () => Math.floor(Date.now() / 1000);
const run = Math.random().toString(36).slice(2, 8);

interface Machine {
  readonly id: string;
  readonly origin: string;
  readonly runtime: ManagedRuntime.ManagedRuntime<
    CloudLink.CloudLink | ServerSecretStore.ServerSecretStore | ServerConfig.ServerConfig,
    unknown
  >;
  readonly issued: Array<string>;
  readonly runtimeConfigs: Array<RelayManagedEndpointRuntimeConfig | null>;
  token: AccountTokens;
}

function makeMachine(id: string, port: number, token: AccountTokens): Machine {
  const issued: Array<string> = [];
  const runtimeConfigs: Array<RelayManagedEndpointRuntimeConfig | null> = [];
  const machine: Machine = {
    id,
    origin: `http://127.0.0.1:${port}`,
    issued,
    runtimeConfigs,
    token,
    runtime: undefined as never,
  };
  const services = Layer.mergeAll(
    ServerSecretStore.layer,
    FetchHttpClient.layer,
    Layer.succeed(
      ConnectReachability.ConnectReachability,
      ConnectReachability.ConnectReachability.of({
        check: Effect.die("unused"),
        latest: Effect.succeedNone,
      }),
    ),
    Layer.succeed(
      HttpServer.HttpServer,
      HttpServer.HttpServer.of({
        address: NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", port),
        serve: (() => Effect.void) as HttpServer.HttpServer["Service"]["serve"],
      }),
    ),
    Layer.succeed(
      AgentAwarenessRelay.AgentAwarenessRelay,
      AgentAwarenessRelay.AgentAwarenessRelay.of({
        publishThread: () => Effect.void,
        drain: Effect.void,
        requestCatchUp: () => Effect.void,
        start: () => Effect.void,
      }),
    ),
    Layer.succeed(
      ServerMachine.ServerMachine,
      ServerMachine.ServerMachine.of({
        getMachineId: Effect.succeed(MachineId.make(id)),
        getDescriptor: Effect.succeed({
          machineId: MachineId.make(id),
          label: `Conformance ${id}`,
          platform: { os: "darwin", arch: "arm64" },
          serverVersion: "0.0.0-conformance",
          capabilities: { repositoryIdentity: true },
        }),
      }),
    ),
    // No cloudflared: the runtime reports "running" for whatever config CloudLink applies.
    Layer.succeed(
      ManagedEndpointRuntime.CloudManagedEndpointRuntime,
      ManagedEndpointRuntime.CloudManagedEndpointRuntime.of({
        applyConfig: (config) =>
          Effect.sync(() => {
            runtimeConfigs.push(config);
            return config === null
              ? ({ status: "disabled" } as const)
              : ({ status: "running", providerKind: "cloudflare_tunnel", pid: 1 } as const);
          }),
        recoveryRequests: Stream.empty,
        requestRecovery: () => Effect.void,
        tunnelConnected: Stream.empty,
        inspectConnector: Effect.succeed(null),
        readinessChanged: Stream.empty,
        withLinkStateLock: (effect) => effect,
      }),
    ),
    Layer.succeed(
      MachineAuth.MachineAuth,
      MachineAuth.MachineAuth.of({
        createPairingLink: (input?: {
          readonly scopes?: ReadonlyArray<string>;
          readonly subject?: string;
        }) =>
          Effect.gen(function* () {
            const now = yield* DateTime.now;
            const credential = `pairing-${NodeCrypto.randomUUID()}`;
            issued.push(credential);
            return {
              id: NodeCrypto.randomUUID(),
              credential,
              scopes: input?.scopes ?? [],
              subject: input?.subject ?? "cloud-connect",
              createdAt: now,
              // CloudLink asks for 2 minutes (`mintCredential`).
              expiresAt: DateTime.add(now, { minutes: 2 }),
            };
          }),
      } as unknown as MachineAuth.MachineAuth["Service"]),
    ),
    Layer.succeed(
      CliTokenManager.CloudCliTokenManager,
      CliTokenManager.CloudCliTokenManager.of({
        get: Effect.die("unused"),
        getExisting: Effect.sync(() =>
          Option.some({
            accessToken: machine.token.access_token,
            refreshToken: machine.token.refresh_token,
            expiresAtEpochMs: Number.MAX_SAFE_INTEGER,
          }),
        ),
        hasCredential: Effect.die("unused"),
        store: () => Effect.die("unused"),
        clear: Effect.die("unused"),
      } as unknown as CliTokenManager.CloudCliTokenManager["Service"]),
    ),
  );
  const layer = CloudLink.layer.pipe(
    Layer.provideMerge(services),
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "vekrel-relay-t13-" })),
    Layer.provideMerge(NodeServices.layer),
  );
  return Object.assign(machine, { runtime: ManagedRuntime.make(layer) });
}

const withLink = <A, E>(
  m: Machine,
  f: (link: CloudLink.CloudLink["Service"]) => Effect.Effect<A, E, never>,
) => m.runtime.runPromise(Effect.flatMap(CloudLink.CloudLink, f));

const secret = (m: Machine, name: string) =>
  m.runtime.runPromise(
    Effect.gen(function* () {
      const secrets = yield* ServerSecretStore.ServerSecretStore;
      const value = yield* secrets.get(name);
      return Option.isSome(value) ? new TextDecoder().decode(value.value) : null;
    }),
  );

const secretFiles = (m: Machine) =>
  m.runtime.runPromise(
    Effect.map(ServerConfig.ServerConfig, (c) =>
      NodeFS.existsSync(c.secretsDir) ? NodeFS.readdirSync(c.secretsDir).sort() : [],
    ),
  );

const machineKey = (m: Machine) =>
  m.runtime.runPromise(
    Effect.flatMap(ServerSecretStore.ServerSecretStore, (s) =>
      getOrCreateMachineKeyPairFromSecretStore(s),
    ),
  );

const signAs = async (m: Machine, typ: string, payload: Record<string, unknown>) =>
  Effect.runPromise(signRelayJwt({ privateKey: (await machineKey(m)).privateKey, typ, payload }));

const recoveryClaims = (m: Machine, cloudUserId: string) => {
  const iat = nowSeconds();
  return {
    iss: `vekrel-machine:${m.id}`,
    aud: ISSUER,
    sub: m.id,
    jti: NodeCrypto.randomUUID(),
    iat,
    exp: iat + 60,
    machineId: m.id,
    cloudUserId,
  };
};

const postJson = (path: string, bearer: string, body: unknown) =>
  relayFetch(`${RELAY_URL}${path}`, {
    method: "POST",
    headers: { authorization: `Bearer ${bearer}`, "content-type": "application/json" },
    body: JSON.stringify(body),
  });

const lastExchange = (method: string, suffix: string): Exchange => {
  const e = exchanges.findLast((x) => x.method === method && x.path.endsWith(suffix));
  if (e === undefined) throw new Error(`no ${method} ...${suffix} exchange`);
  return e;
};

const jwsVerified: Array<string> = [];
const verifyRelaySigned = async (input: {
  token: string;
  typ: string;
  audience: string;
  publicKey: string;
  maxTokenAge?: string;
}) => {
  const claims = await Effect.runPromise(
    verifyRelayJwt({
      publicKey: input.publicKey,
      token: input.token,
      typ: input.typ,
      issuer: ISSUER,
      audience: input.audience,
      nowEpochSeconds: nowSeconds(),
      ...(input.maxTokenAge ? { maxTokenAge: input.maxTokenAge } : {}),
    }),
  );
  jwsVerified.push(input.typ);
  return claims;
};

const machineRequests: Array<{ path: string; proof: string }> = [];
let currentMachine: Machine | undefined;
let mintPublicKey = "";

async function machineEndpoint(method: string, url: URL, raw: string) {
  const m = currentMachine;
  if (m === undefined || method !== "POST") return { status: 404 };
  const json = JSON.parse(raw) as unknown;
  const mint = url.pathname === "/api/vekrel-connect/mint-credential";
  const health = url.pathname === "/api/vekrel-connect/health";
  if (!mint && !health) return { status: 404 };
  const request = Schema.decodeUnknownSync(
    mint ? RelayCloudMintCredentialRequest : RelayCloudMachineHealthRequest,
  )(json, { onExcessProperty: "error" });
  machineRequests.push({ path: url.pathname, proof: request.proof });
  await verifyRelaySigned({
    token: request.proof,
    typ: mint ? RELAY_MINT_REQUEST_TYP : RELAY_HEALTH_REQUEST_TYP,
    audience: `vekrel-machine:${m.id}`,
    publicKey: mintPublicKey,
  });
  const answer = await withLink(m, (link) =>
    Effect.result<unknown, unknown, never>(
      mint ? link.mintCredential(request) : link.answerHealthRequest(request),
    ),
  );
  if (answer._tag === "Success") {
    return { status: 200, body: answer.success, headers: { "cache-control": "no-store" } };
  }
  // cloud/http.ts maps rejected proofs to 401 and replays to 409.
  const tag = (answer.failure as { _tag?: string })._tag;
  return {
    status:
      tag === "CloudLinkProofReplayedError"
        ? 409
        : tag === "CloudLinkProofRejectedError"
          ? 401
          : 500,
  };
}

describe.skipIf(!enabled)("relay conformance (plan 6A Task 13)", () => {
  const cloudflare = new FakeCloudflare();
  let cfServer: NodeHttp.Server | undefined;
  let machineServer: NodeHttp.Server | undefined;
  let alice: AccountTokens;
  let bob: AccountTokens;
  let a: Machine;
  let b: Machine;
  let cloudUserA = "";

  const mobile = (token: AccountTokens) => {
    const key = makeDpopKey();
    const signer = Layer.succeed(
      ManagedRelay.ManagedRelayDpopSigner,
      ManagedRelay.ManagedRelayDpopSigner.of({
        thumbprint: Effect.succeed(key.thumbprint),
        createProof: (input) =>
          Effect.sync(() =>
            key.proof({
              method: input.method,
              url: input.url,
              ...(input.accessToken ? { accessToken: input.accessToken } : {}),
            }),
          ),
      }),
    );
    const layer = ManagedRelay.layer({ relayUrl: RELAY_URL, clientId: "vekrel-mobile" }).pipe(
      Layer.provide(signer),
      Layer.provide(RpcHttp.layerRemoteHttpClient(relayFetch)),
    );
    const call = <A, E>(
      f: (c: ManagedRelay.ManagedRelayClient["Service"]) => Effect.Effect<A, E>,
    ) =>
      Effect.runPromise(
        Effect.flatMap(ManagedRelay.ManagedRelayClient, f).pipe(Effect.provide(layer)),
      );
    return { key, call, clerkToken: token.access_token };
  };

  beforeAll(async () => {
    cfServer = await cloudflare.start(CLOUDFLARE_PORT);
    machineServer = await listen(MACHINE_PORT, machineEndpoint);
    alice = await signIn(`t13-alice-${run}@example.test`);
    bob = await signIn(`t13-bob-${run}@example.test`);
    a = makeMachine(`t13-a-${run}`, 18_641, alice);
    b = makeMachine(`t13-b-${run}`, 18_642, bob);
  });

  afterAll(async () => {
    await a?.runtime.dispose();
    await b?.runtime.dispose();
    await close(machineServer);
    await close(cfServer);
  });

  it("health and the protected-resource metadata decode strictly", async () => {
    for (const path of [
      "/health",
      "/.well-known/oauth-authorization-server",
      "/.well-known/oauth-protected-resource",
    ]) {
      const r = await relayFetch(`${RELAY_URL}${path}`);
      expect(r.status, path).toBe(200);
    }
  });

  it("CloudLink links a managed machine: challenge, proof, link, secret files", async () => {
    currentMachine = a;
    const mode = await withLink(a, (link) => link.reconcileDesiredLink(a.origin));
    expect(mode).toBe("managed");

    const linkBody = JSON.parse(lastExchange("POST", "/v1/client/machine-links").body);
    mintPublicKey = linkBody.cloudMintPublicKey;
    cloudUserA = linkBody.cloudUserId;
    expect(linkBody.relayIssuer).toBe(ISSUER);
    const challenge = JSON.parse(lastExchange("POST", "/machine-link-challenges").body).challenge;
    await verifyRelaySigned({
      token: challenge,
      typ: "vekrel-link-challenge+jwt",
      audience: ISSUER,
      publicKey: mintPublicKey,
    });

    // The link is stored where the Engine reads it back (`secrets/<name>.bin`).
    const files = await secretFiles(a);
    for (const name of [
      CloudConfig.RELAY_URL_SECRET,
      CloudConfig.RELAY_ISSUER_SECRET,
      CloudConfig.CLOUD_LINKED_USER_ID,
      CloudConfig.RELAY_MACHINE_CREDENTIAL_SECRET,
      CloudConfig.CLOUD_MINT_PUBLIC_KEY,
      CloudConfig.CLOUD_ENDPOINT_RUNTIME_CONFIG,
      CloudConfig.CLOUD_ENDPOINT_CONFIRMED_ORIGIN,
    ]) {
      expect(files, name).toContain(`${name}.bin`);
    }
    expect(await secret(a, CloudConfig.RELAY_MACHINE_CREDENTIAL_SECRET)).toBe(
      linkBody.machineCredential,
    );
    const runtime = JSON.parse((await secret(a, CloudConfig.CLOUD_ENDPOINT_RUNTIME_CONFIG))!);
    expect(runtime.providerKind).toBe("cloudflare_tunnel");
    // The relay provisioned one tunnel and one proxied CNAME, with the configured token only.
    expect(cloudflare.tunnels.map((t) => t.id)).toEqual([runtime.tunnelId]);
    expect(cloudflare.records).toHaveLength(1);
    expect(cloudflare.records[0]!.name.endsWith(`.${TUNNEL_ZONE}`)).toBe(true);
    expect(cloudflare.records[0]!.content).toBe(`${runtime.tunnelId}.cfargotunnel.com`);
    expect([...cloudflare.authHeaders]).toEqual([`Bearer ${CLOUDFLARE_TOKEN}`]);
    // The relay's answer never carries the Cloudflare API token.
    expect(lastExchange("POST", "/v1/client/machine-links").body).not.toContain(CLOUDFLARE_TOKEN);
  });

  it("tunnel recovery: register and recover through CloudLink (T3 Api.test recovery cases)", async () => {
    const registered = await withLink(a, (l) => l.registerManagedTunnelRecovery(a.origin));
    expect(registered).toMatchObject({ status: "ready" });
    expect(JSON.parse(lastExchange("POST", "/tunnel/recovery").body)).toEqual({ status: "ready" });

    const tunnelBefore = cloudflare.tunnels[0]!.id;
    expect(await withLink(a, (l) => l.recoverManagedTunnel(a.origin))).toBe(true);
    const recovered = JSON.parse(lastExchange("POST", "/tunnel").body);
    expect(recovered.endpointRuntime.tunnelId).toBe(tunnelBefore);
  });

  it("tunnel recovery refusals: changed tunnel, other owner, other origin, wrong action", async () => {
    const credential = (await secret(a, CloudConfig.RELAY_MACHINE_CREDENTIAL_SECRET))!;
    const origin = { localHttpHost: "127.0.0.1", localHttpPort: 18_641 };
    const tunnelId = cloudflare.tunnels[0]!.id;
    const register = async (over: { body?: object; claims?: object }) => {
      const proof = await signAs(a, RELAY_MANAGED_TUNNEL_RECOVERY_TYP, {
        ...recoveryClaims(a, cloudUserA),
        action: "register",
        tunnelId,
        origin,
        ...over.claims,
      });
      return postJson(`/v1/machines/${a.id}/tunnel/recovery`, credential, {
        cloudUserId: cloudUserA,
        tunnelId,
        origin,
        proof,
        ...over.body,
      });
    };
    expect((await register({})).status).toBe(200);
    // A tunnel id the relay did not record: T3's live path answers recovery_required from
    // `reconcileOrigin` (ManagedEndpointProvider.ts:742-748) before `enableRecovery` runs; the
    // Unauthorized of "rejects recovery registration when the recorded tunnel changed" needs a
    // provision racing between the two, which only its mock produces.
    const changed = "00000000-0000-4000-8000-0000000000ff";
    const stale = await register({ body: { tunnelId: changed }, claims: { tunnelId: changed } });
    expect(stale.status).toBe(200);
    expect(await stale.json()).toEqual({ status: "recovery_required" });
    // "binds recovery requests to the host, cloud user, and T3 service origin"
    expect((await register({ body: { cloudUserId: "usr_other" } })).status).toBe(401);
    expect((await register({ body: { origin: { ...origin, localHttpPort: 5432 } } })).status).toBe(
      401,
    );
    // a recover proof is not a registration proof
    expect((await register({ claims: { action: "recover", tunnelId: undefined } })).status).toBe(
      401,
    );
    for (const e of exchanges.slice(-3)) {
      expect(JSON.parse(e.body)).toMatchObject({
        _tag: "RelayAuthInvalidError",
        code: "auth_invalid",
      });
    }
  });

  it("a tunnel deleted at Cloudflare is replaced under the same hostname", async () => {
    const host = cloudflare.records[0]!.name;
    cloudflare.tunnels = [];
    const registered = await withLink(a, (l) => l.registerManagedTunnelRecovery(a.origin));
    // "requests recovery without enabling a stale tunnel"
    expect(registered).toMatchObject({ status: "recovery_required" });
    expect(await withLink(a, (l) => l.recoverManagedTunnel(a.origin))).toBe(true);
    expect(cloudflare.tunnels).toHaveLength(1);
    expect(cloudflare.records.map((r) => r.name)).toEqual([host]);
    expect(cloudflare.records[0]!.content).toBe(`${cloudflare.tunnels[0]!.id}.cfargotunnel.com`);
  });

  it("link preferences go through the Engine's relay machine client", async () => {
    const state = await withLink(a, (l) =>
      l.updatePreferences({ publishAgentActivity: true, holdWebhooksWhileOffline: true }),
    );
    expect(state.holdWebhooksWhileOffline).toBe(true);
    expect(JSON.parse(lastExchange("POST", "/link-preferences").body)).toEqual({
      holdWebhooksWhileOffline: true,
    });
  });

  it("managedRelay: token exchange, list machines, connect (mint) and status (health)", async () => {
    const phone = mobile(alice);
    const listed = await phone.call((c) => c.listMachines({ clerkToken: phone.clerkToken }));
    expect(listed.map((m) => m.machineId)).toEqual([a.id]);

    const before = machineRequests.length;
    const connected = await phone.call((c) =>
      c.connectMachine({
        clerkToken: phone.clerkToken,
        scopes: [RelayMachineConnectScope],
        machineId: MachineId.make(a.id),
        deviceId: "t13-phone",
      }),
    );
    expect(connected.credential).toBe(a.issued.at(-1));
    expect(connected.endpoint.httpBaseUrl).toBe(`https://${cloudflare.records[0]!.name}/`);
    const status = await phone.call((c) =>
      c.getMachineStatus({
        clerkToken: phone.clerkToken,
        scopes: [RelayMachineStatusScope],
        machineId: MachineId.make(a.id),
      }),
    );
    expect(status).toMatchObject({ machineId: a.id, status: "online" });
    expect(machineRequests.slice(before).map((r) => r.path)).toEqual([
      "/api/vekrel-connect/mint-credential",
      "/api/vekrel-connect/health",
    ]);

    // The relay DPoP access tokens it handed out are relay-signed JWS bound to this phone's key.
    for (const e of exchanges.filter(
      (x) => x.path === "/v1/client/dpop-token" && x.status === 200,
    )) {
      const claims = await verifyRelaySigned({
        token: JSON.parse(e.body).access_token,
        typ: "vekrel-relay-dpop-access+jwt",
        audience: ISSUER,
        publicKey: mintPublicKey,
        maxTokenAge: "30 minutes",
      });
      expect((claims.cnf as { jkt?: string }).jkt).toBe(phone.key.thumbprint);
    }
  });

  it("devices: v1 lists iOS only, v2 every platform, both no-store (T3 device listing)", async () => {
    const phone = mobile(alice);
    const preferences = {
      notificationsEnabled: true,
      notifyOnApproval: true,
      notifyOnInput: true,
      notifyOnCompletion: true,
      notifyOnFailure: true,
      liveActivitiesEnabled: true,
    };
    for (const payload of [
      {
        deviceId: "iphone",
        label: "iPhone",
        platform: "ios",
        iosMajorVersion: 18,
        appVersion: "1.0.0",
      },
      {
        deviceId: "android",
        label: "Android",
        platform: "android",
        androidApiLevel: 36,
        appVersion: "1.0.0",
      },
    ] as const) {
      await phone.call((c) =>
        c.registerDevice({
          clerkToken: phone.clerkToken,
          payload: { ...payload, preferences } as never,
        }),
      );
    }
    // The phone app reads v2; installed v1 clients decode iOS only.
    const v2 = await phone.call((c) => c.listDevices({ clerkToken: phone.clerkToken }));
    expect(v2.map((d) => d.deviceId)).toEqual(["iphone", "android"]);
    const v1 = await relayFetch(`${RELAY_URL}/v1/client/devices`, {
      headers: { authorization: `Bearer ${alice.access_token}` },
    });
    expect(v1.headers.get("cache-control")).toBe("no-store");
    expect(lastExchange("GET", "/v2/client/devices").headers.get("cache-control")).toBe("no-store");
    const v1Body = (await v1.json()) as { devices: Array<{ deviceId: string }> };
    expect(v1Body.devices.map((d) => d.deviceId)).toEqual(["iphone"]);
  });

  it("agent activity publishes for escaped thread ids longer than 100 characters (T3 routing fallback)", async () => {
    const credential = (await secret(a, CloudConfig.RELAY_MACHINE_CREDENTIAL_SECRET))!;
    const threadIds = [
      "b7c8c522-d244-43dc-875f-7224fce79912",
      "t".repeat(512),
      "thread:delegated-task:command%3Amcp%3Ab7c8c522-d244-43dc-875f-7224fce79912%3Adelegate-task%3Agreet-subagent-20260813",
    ];
    for (const threadId of threadIds) {
      const iat = nowSeconds();
      const proof = await signAs(a, RELAY_ACTIVITY_PUBLISH_TYP, {
        iss: `vekrel-machine:${a.id}`,
        aud: ISSUER,
        sub: a.id,
        jti: NodeCrypto.randomUUID(),
        iat,
        exp: iat + 300,
        machineId: a.id,
        threadId,
        state: null,
      });
      const response = await Effect.runPromise(
        Effect.flatMap(
          makeRelayMachineClient({ url: RELAY_URL, machineCredential: credential }),
          (c) =>
            c.server.publishAgentActivity({
              params: { machineId: MachineId.make(a.id), threadId: threadId as ThreadId },
              payload: { state: null, proof },
            }),
        ),
      );
      expect(response.ok, threadId).toBe(true);
    }
  });

  it("another account sees nothing of this machine and cannot use it", async () => {
    // Bob links a publish-only machine of his own.
    currentMachine = b;
    await b.runtime.runPromise(
      Effect.flatMap(ServerSecretStore.ServerSecretStore, (s) =>
        CliState.setCliDesiredCloudLink(true, "publish_only").pipe(
          Effect.provideService(ServerSecretStore.ServerSecretStore, s),
        ),
      ),
    );
    expect(await withLink(b, (l) => l.reconcileDesiredLink(b.origin))).toBe("publish_only");
    expect(cloudflare.tunnels).toHaveLength(1);

    const phone = mobile(bob);
    const listed = await phone.call((c) => c.listMachines({ clerkToken: phone.clerkToken }));
    expect(listed.map((m) => m.machineId)).toEqual([b.id]);
    const refused = await phone.call((c) =>
      c
        .connectMachine({
          clerkToken: phone.clerkToken,
          scopes: [RelayMachineConnectScope],
          machineId: MachineId.make(a.id),
        })
        .pipe(Effect.flip),
    );
    expect(refused._tag).toBe("ManagedRelayRequestFailedError");
    expect(lastExchange("POST", `/v1/machines/${a.id}/connect`).status).toBeGreaterThanOrEqual(400);

    // "rejects a credential from a different environment owner"
    const bobCredential = (await secret(b, CloudConfig.RELAY_MACHINE_CREDENTIAL_SECRET))!;
    const origin = { localHttpHost: "127.0.0.1", localHttpPort: 18_641 };
    const proof = await signAs(b, RELAY_MANAGED_TUNNEL_RECOVERY_TYP, {
      ...recoveryClaims(b, cloudUserA),
      action: "recover",
      origin,
    });
    const crossOwner = await postJson(`/v1/machines/${a.id}/tunnel`, bobCredential, {
      cloudUserId: cloudUserA,
      origin,
      proof,
    });
    expect(crossOwner.status).toBe(401);
    // "does not recover a publish-only environment"
    expect(await withLink(b, (l) => l.recoverManagedTunnel(b.origin))).toBe(false);
    const bobUser = JSON.parse(lastExchange("POST", "/v1/client/machine-links").body).cloudUserId;
    const ownProof = await signAs(b, RELAY_MANAGED_TUNNEL_RECOVERY_TYP, {
      ...recoveryClaims(b, bobUser),
      action: "recover",
      origin: { localHttpHost: "127.0.0.1", localHttpPort: 18_642 },
    });
    const publishOnly = await postJson(`/v1/machines/${b.id}/tunnel`, bobCredential, {
      cloudUserId: bobUser,
      origin: { localHttpHost: "127.0.0.1", localHttpPort: 18_642 },
      proof: ownProof,
    });
    expect(publishOnly.status).toBe(401);
  });

  it("release on shutdown deletes the tunnel and keeps the hostname", async () => {
    currentMachine = a;
    await a.runtime.runPromise(
      Effect.flatMap(ServerSecretStore.ServerSecretStore, (s) =>
        CliState.setCliDesiredCloudLink(true, "managed").pipe(
          Effect.provideService(ServerSecretStore.ServerSecretStore, s),
        ),
      ),
    );
    const host = cloudflare.records[0]!.name;
    expect(await withLink(a, (l) => l.releaseManagedTunnelOnShutdown())).toBe(true);
    expect(cloudflare.tunnels).toHaveLength(0);
    expect(cloudflare.records.map((r) => r.name)).toEqual([host]);
    expect(await secret(a, CloudConfig.CLOUD_ENDPOINT_RUNTIME_CONFIG)).toBeNull();
    // The next start relinks onto a fresh tunnel under the same hostname.
    expect(await withLink(a, (l) => l.reconcileDesiredLink(a.origin))).toBe("managed");
    expect(cloudflare.tunnels).toHaveLength(1);
    expect(cloudflare.records.map((r) => r.name)).toEqual([host]);
  });

  it("unlink: the relay removes tunnel and CNAME, the old credential is refused", async () => {
    const phone = mobile(alice);
    const ok = await phone.call((c) =>
      c.unlinkMachine({ clerkToken: phone.clerkToken, machineId: MachineId.make(a.id) }),
    );
    expect(ok).toEqual({ ok: true });
    expect(cloudflare.tunnels).toHaveLength(0);
    expect(cloudflare.records).toHaveLength(0);
    // The Engine sees 401 on its next recovery registration and marks the link revoked.
    expect(await withLink(a, (l) => l.registerManagedTunnelRecovery(a.origin))).toMatchObject({
      status: "revoked",
    });
    expect((await withLink(a, (l) => l.linkState())).linkRevoked).toBe(true);
    await withLink(a, (l) => l.unlink());
    expect((await withLink(a, (l) => l.linkState())).linked).toBe(false);
    const phoneList = await phone.call((c) => c.listMachines({ clerkToken: phone.clerkToken }));
    expect(phoneList).toEqual([]);
  });

  it("provisioning failures: failed stage without secrets, Cloudflare pause sends retry-after (T3)", async () => {
    currentMachine = a;
    cloudflare.failLists = 5;
    const failed = await withLink(a, (l) => l.reconcileDesiredLink(a.origin).pipe(Effect.flip));
    expect(failed).toBeDefined();
    const e = lastExchange("POST", "/v1/client/machine-links");
    expect(e.status).toBe(503);
    expect(JSON.parse(e.body)).toMatchObject({
      _tag: "RelayMachineLinkUnavailableError",
      code: "machine_link_unavailable",
      reason: "managed_endpoint_provisioning_failed",
      provisioningStage: "ensure-tunnel",
    });
    expect(e.body).not.toContain(CLOUDFLARE_TOKEN);
    expect(e.headers.get("retry-after")).toBeNull();
    cloudflare.failLists = 0;

    cloudflare.rateLimit = { count: 1, retryAfter: 3 };
    await withLink(a, (l) => l.reconcileDesiredLink(a.origin).pipe(Effect.flip));
    const paused = lastExchange("POST", "/v1/client/machine-links");
    expect(paused.status).toBe(503);
    const retryAfter = Number(paused.headers.get("retry-after"));
    expect(retryAfter).toBeGreaterThanOrEqual(1);
    expect(retryAfter).toBeLessThanOrEqual(3);
    cloudflare.rateLimit = null;
    // After the pause the same link goes through.
    // oxlint-disable-next-line t3code/no-real-time-wait-in-tests -- the pause is the relay's real clock in another process.
    await new Promise((resolve) => setTimeout(resolve, (retryAfter + 1) * 1000));
    expect(await withLink(a, (l) => l.reconcileDesiredLink(a.origin))).toBe("managed");
    await phoneUnlink(alice, a.id);
  });

  it("RelayInternalError: _tag first, schema order, 32-hex traceId (Task 2 carry-over)", async () => {
    const traceId = await Effect.runPromise(
      Effect.currentSpan.pipe(
        Effect.map((s) => s.traceId),
        Effect.withSpan("t13"),
      ),
    );
    expect(traceId).toMatch(/^[0-9a-f]{32}$/u);
    const encoded = JSON.stringify(
      encodeInternalError(
        new RelayInternalError({ code: "internal_error", reason: "persistence_failed", traceId }),
      ),
    );
    // The same literal is pinned in Vekrel-Relay `src/http/errors.rs`.
    expect(encoded).toBe(
      `{"_tag":"RelayInternalError","code":"internal_error","reason":"persistence_failed","traceId":"${traceId}"}`,
    );
  });

  it("every relay answer decodes strictly with Shared's schemas; every relay JWS verified", async () => {
    for (const e of exchanges.filter((x) => x.status === 200)) {
      if (e.path === "/v1/client/machine-link-challenges") {
        await verifyRelaySigned({
          token: JSON.parse(e.body).challenge,
          typ: "vekrel-link-challenge+jwt",
          audience: ISSUER,
          publicKey: mintPublicKey,
        });
      } else if (e.path === "/v1/client/dpop-token") {
        await verifyRelaySigned({
          token: JSON.parse(e.body).access_token,
          typ: "vekrel-relay-dpop-access+jwt",
          audience: ISSUER,
          publicKey: mintPublicKey,
          maxTokenAge: "30 minutes",
        });
      }
    }
    const checked = checkExchanges(exchanges);
    console.info(
      `relay conformance: ${exchanges.length} exchanges, ${checked.decoded} success bodies and ${checked.errorBodies} error bodies decoded strictly, ${checked.emptyErrors} empty error bodies, ${checked.accountRoutes} account-route calls; ${jwsVerified.length} relay JWS verified (${[...new Set(jwsVerified)].join(", ")})`,
    );
    expect(checked.violations).toEqual([]);
    // The checker itself: an excess key and a reordered body are both violations.
    const ok = exchanges.find((x) => x.path === "/health")!;
    const probe = (body: string) => checkExchanges([{ ...ok, body }]).violations.length;
    expect(probe(ok.body)).toBe(0);
    expect(probe(ok.body.replace(/\}$/u, ',"extra":1}'))).toBe(1);
    expect(
      probe(JSON.stringify(Object.fromEntries(Object.entries(JSON.parse(ok.body)).toReversed()))),
    ).toBe(1);
    expect(new Set(jwsVerified)).toEqual(
      new Set([
        "vekrel-link-challenge+jwt",
        "vekrel-relay-dpop-access+jwt",
        RELAY_MINT_REQUEST_TYP,
        RELAY_HEALTH_REQUEST_TYP,
      ]),
    );
  });

  const phoneUnlink = async (token: AccountTokens, machineId: string) => {
    const phone = mobile(token);
    await phone.call((c) =>
      c.unlinkMachine({ clerkToken: phone.clerkToken, machineId: MachineId.make(machineId) }),
    );
  };
});
