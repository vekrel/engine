// @effect-diagnostics globalFetch:off globalDate:off -- test-only fetch recorder and fakes around a live relay.
// Test-only plumbing for relay.conformance.test.ts: a recording fetch, strict decoding of every
// relay answer with Shared's contract schemas, a fake Cloudflare API, a fake machine endpoint that
// hands relay requests to the real CloudLink, the account sign-in flow and a DPoP signer.
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Effect's Crypto has no P-256 signing.
import * as NodeCrypto from "node:crypto";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- test servers on fixed loopback ports.
import * as NodeHttp from "node:http";

import * as Schema from "effect/Schema";

import {
  RelayAgentActivitySnapshotResponse,
  RelayAuthorizationServerMetadata,
  RelayDpopAccessTokenResponse,
  RelayHealthResponse,
  RelayListDevicesResponse,
  RelayListDevicesResponseV1,
  RelayListMachinesResponse,
  RelayMachineConnectResponse,
  RelayMachineLinkChallengeResponse,
  RelayMachineLinkPreferencesRequest,
  RelayMachineLinkResponse,
  RelayMachineStatusResponse,
  RelayManagedEndpointRecoveryRegistrationResponse,
  RelayManagedEndpointRecoveryResponse,
  RelayOkResponse,
  RelayProtectedError,
  RelayProtectedResourceMetadata,
  RelayPublishResponse,
  RelayWakeHeldHooksResponse,
} from "@vekrel/contracts/relay";
import { computeDpopAccessTokenHash, computeDpopJwkThumbprint } from "@vekrel/shared/dpop";

const env = process.env;
/** The relay's public origin (`RELAY_ORIGIN`); clients require https. */
export const RELAY_URL = env.RELAY_CONFORMANCE_URL?.trim().replace(/\/+$/u, "") ?? "";
/** Local runs only: where the relay really listens when `RELAY_URL` is `https://localhost:<port>`. */
const TRANSPORT = env.RELAY_CONFORMANCE_TRANSPORT?.trim().replace(/\/+$/u, "") ?? "";
export const CLOUDFLARE_PORT = Number(env.RELAY_CONFORMANCE_CLOUDFLARE_PORT ?? 0);
export const MACHINE_PORT = Number(env.RELAY_CONFORMANCE_MACHINE_PORT ?? 0);
export const TUNNEL_ZONE = env.RELAY_CONFORMANCE_TUNNEL_ZONE ?? "";
export const CLOUDFLARE_TOKEN = env.RELAY_CONFORMANCE_CLOUDFLARE_TOKEN ?? "";

export interface Exchange {
  readonly method: string;
  readonly path: string;
  readonly status: number;
  readonly headers: Headers;
  readonly body: string;
}
export const exchanges: Array<Exchange> = [];
const realFetch = globalThis.fetch;
const NULL_BODY = new Set([101, 204, 205, 304]);

/** Sends relay requests to `TRANSPORT` (same bytes, other socket) and records every answer. */
export const relayFetch: typeof globalThis.fetch = async (input, init) => {
  const request = new Request(input, init);
  const isRelay = RELAY_URL !== "" && request.url.startsWith(`${RELAY_URL}/`);
  const target =
    isRelay && TRANSPORT !== "" ? TRANSPORT + request.url.slice(RELAY_URL.length) : request.url;
  const body =
    request.method === "GET" || request.method === "HEAD" ? undefined : await request.arrayBuffer();
  const response = await realFetch(target, {
    method: request.method,
    headers: request.headers,
    redirect: request.redirect,
    signal: request.signal,
    ...(body === undefined ? {} : { body }),
  });
  if (!isRelay) return response;
  const text = await response.text();
  exchanges.push({
    method: request.method,
    path: new URL(request.url).pathname,
    status: response.status,
    headers: response.headers,
    body: text,
  });
  return new Response(NULL_BODY.has(response.status) ? null : text, {
    status: response.status,
    statusText: response.statusText,
    headers: response.headers,
  });
};

const STRICT = { onExcessProperty: "error" } as const;
// Account routes are Vekrel's own; every other route is T3's contract.
const ACCOUNT_ROUTE =
  /^\/(oauth|auth|device|\.well-known\/(jwks\.json|openid-configuration))(\/|$)/u;
const SEG = "[^/]+";
const ROUTES: ReadonlyArray<readonly [string, RegExp, Schema.Codec<unknown, unknown>]> = [
  ["GET", /^\/health$/u, RelayHealthResponse],
  ["GET", /^\/\.well-known\/oauth-authorization-server$/u, RelayAuthorizationServerMetadata],
  ["GET", /^\/\.well-known\/oauth-protected-resource$/u, RelayProtectedResourceMetadata],
  ["GET", /^\/v1\/machines$/u, RelayListMachinesResponse],
  ["GET", /^\/v1\/client\/devices$/u, RelayListDevicesResponseV1],
  ["GET", /^\/v2\/client\/devices$/u, RelayListDevicesResponse],
  ["POST", /^\/v1\/client\/machine-link-challenges$/u, RelayMachineLinkChallengeResponse],
  ["POST", /^\/v1\/client\/machine-links$/u, RelayMachineLinkResponse],
  ["DELETE", new RegExp(`^/v1/client/machine-links/${SEG}(/tunnel)?$`, "u"), RelayOkResponse],
  ["POST", /^\/v1\/client\/dpop-token$/u, RelayDpopAccessTokenResponse],
  ["POST", new RegExp(`^/v1/machines/${SEG}/connect$`, "u"), RelayMachineConnectResponse],
  ["POST", new RegExp(`^/v1/machines/${SEG}/status$`, "u"), RelayMachineStatusResponse],
  [
    "POST",
    new RegExp(`^/v1/machines/${SEG}/tunnel/recovery$`, "u"),
    RelayManagedEndpointRecoveryRegistrationResponse,
  ],
  ["POST", new RegExp(`^/v1/machines/${SEG}/tunnel$`, "u"), RelayManagedEndpointRecoveryResponse],
  [
    "POST",
    new RegExp(`^/v1/machines/${SEG}/link-preferences$`, "u"),
    RelayMachineLinkPreferencesRequest,
  ],
  ["POST", new RegExp(`^/v1/machines/${SEG}/hooks/wake$`, "u"), RelayWakeHeldHooksResponse],
  [
    "POST",
    new RegExp(`^/v1/machines/${SEG}/threads/${SEG}/agent-activity$`, "u"),
    RelayPublishResponse,
  ],
  ["POST", /^\/v1\/mobile\/(devices|live-activities)$/u, RelayOkResponse],
  ["DELETE", new RegExp(`^/v1/mobile/devices/${SEG}$`, "u"), RelayOkResponse],
  ["GET", /^\/v1\/mobile\/agent-activity$/u, RelayAgentActivitySnapshotResponse],
] as const;

/** Decodes strictly, then re-encodes: the bytes must be what Effect writes (keys in schema order). */
function strictRoundTrip(schema: Schema.Codec<unknown, unknown>, body: string): string | null {
  let json: unknown;
  try {
    json = JSON.parse(body);
  } catch {
    return "body is not JSON";
  }
  try {
    const decoded = Schema.decodeUnknownSync(schema)(json, STRICT);
    const encoded = JSON.stringify(Schema.encodeSync(schema)(decoded));
    return encoded === body ? null : `bytes differ from the schema encoding: ${encoded}`;
  } catch (cause) {
    return `strict decode failed: ${String(cause)}`;
  }
}

export interface Checked {
  readonly decoded: number;
  readonly errorBodies: number;
  readonly emptyErrors: number;
  readonly accountRoutes: number;
  readonly violations: ReadonlyArray<string>;
}

export function checkExchanges(list: ReadonlyArray<Exchange>): Checked {
  let decoded = 0;
  let errorBodies = 0;
  let emptyErrors = 0;
  let accountRoutes = 0;
  const violations: Array<string> = [];
  for (const e of list) {
    const where = `${e.method} ${e.path} ${e.status}`;
    if (ACCOUNT_ROUTE.test(e.path)) {
      accountRoutes += 1;
      continue;
    }
    if (e.status < 400) {
      const route = ROUTES.find(([m, p]) => m === e.method && p.test(e.path));
      if (route === undefined) {
        violations.push(`${where}: no contract schema for this route`);
        continue;
      }
      const problem = strictRoundTrip(route[2], e.body);
      if (problem === null) decoded += 1;
      else violations.push(`${where}: ${problem}: ${e.body}`);
      continue;
    }
    if (e.body === "") {
      emptyErrors += 1;
      continue;
    }
    const problem = strictRoundTrip(RelayProtectedError as Schema.Codec<unknown, unknown>, e.body);
    if (problem !== null) {
      violations.push(`${where}: error body: ${problem}: ${e.body}`);
      continue;
    }
    const traceId = (JSON.parse(e.body) as { traceId?: unknown }).traceId;
    if (typeof traceId !== "string" || !/^[0-9a-f]{32}$/u.test(traceId)) {
      violations.push(`${where}: traceId is not 32 lowercase hex: ${String(traceId)}`);
      continue;
    }
    errorBodies += 1;
  }
  return { decoded, errorBodies, emptyErrors, accountRoutes, violations };
}

export interface Served {
  readonly status: number;
  readonly body?: unknown;
  readonly headers?: Record<string, string>;
}

export async function listen(
  port: number,
  handle: (method: string, url: URL, body: string) => Promise<Served>,
): Promise<NodeHttp.Server> {
  const server = NodeHttp.createServer((req, res) => {
    const chunks: Array<Buffer> = [];
    req.on("data", (c: Buffer) => chunks.push(c));
    req.on("end", () => {
      const url = new URL(req.url ?? "/", "http://127.0.0.1");
      handle(req.method ?? "GET", url, Buffer.concat(chunks).toString("utf8"))
        .catch((cause: unknown): Served => ({ status: 500, body: { error: String(cause) } }))
        .then((r) => {
          const text = r.body === undefined ? "" : JSON.stringify(r.body);
          res.writeHead(r.status, { "content-type": "application/json", ...r.headers });
          res.end(text);
        });
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, "127.0.0.1", resolve);
  });
  return server;
}

export const close = (server: NodeHttp.Server | undefined) =>
  new Promise<void>((resolve) => (server ? server.close(() => resolve()) : resolve()));

export interface FakeTunnel {
  readonly id: string;
  readonly name: string;
  status: string;
  readonly created_at: string;
}

export class FakeCloudflare {
  tunnels: Array<FakeTunnel> = [];
  records: Array<{ id: string; name: string; content: string }> = [];
  calls: Array<string> = [];
  authHeaders = new Set<string>();
  /** The next N tunnel-list calls answer 500. */
  failLists = 0;
  /** The next N calls answer 429 with this Retry-After. */
  rateLimit: { count: number; retryAfter: number } | null = null;
  private next = 0;

  handle = async (method: string, url: URL, raw: string, auth: string): Promise<Served> => {
    this.calls.push(`${method} ${url.pathname}`);
    this.authHeaders.add(auth);
    const ok = (result: unknown): Served => ({
      status: 200,
      body: { success: true, errors: [], messages: [], result },
    });
    const notFound: Served = {
      status: 404,
      body: { success: false, errors: [{ code: 1000, message: "not found" }], result: null },
    };
    if (this.rateLimit !== null && this.rateLimit.count > 0) {
      this.rateLimit.count -= 1;
      return {
        status: 429,
        headers: { "retry-after": String(this.rateLimit.retryAfter) },
        body: { success: false, errors: [{ code: 10000, message: "Rate limited" }] },
      };
    }
    const body = raw === "" ? {} : (JSON.parse(raw) as Record<string, unknown>);
    const segs = url.pathname
      .split("/")
      .filter((s) => s !== "")
      .slice(4);
    const q = url.searchParams;
    const key = `${method} ${segs.map((s, i) => (i === 0 ? s : i === 2 ? s : "*")).join("/")}`;
    const tunnel = (id: string | undefined) => this.tunnels.find((t) => t.id === id);
    switch (key) {
      case "GET cfd_tunnel": {
        if (this.failLists > 0) {
          this.failLists -= 1;
          return { status: 500, body: { success: false, errors: [{ code: 1000, message: "x" }] } };
        }
        const all = this.tunnels.filter(
          (t) =>
            (q.get("name") === null || t.name === q.get("name")) &&
            (q.get("include_prefix") === null || t.name.startsWith(q.get("include_prefix")!)),
        );
        return {
          status: 200,
          body: {
            success: true,
            errors: [],
            result: all.map((t) => ({ ...t, conns_inactive_at: null, deleted_at: null })),
            result_info: { page: 1, per_page: 1000, total_count: all.length },
          },
        };
      }
      case "POST cfd_tunnel": {
        this.next += 1;
        const t = {
          id: `00000000-0000-4000-8000-${String(this.next).padStart(12, "0")}`,
          name: String(body.name),
          status: "inactive",
          created_at: new Date().toISOString(),
        };
        this.tunnels.push(t);
        return ok({ ...t, conns_inactive_at: null, deleted_at: null });
      }
      case "GET cfd_tunnel/*": {
        const t = tunnel(segs[1]);
        return t ? ok({ ...t, conns_inactive_at: null, deleted_at: null }) : notFound;
      }
      case "GET cfd_tunnel/*/token": {
        const t = tunnel(segs[1]);
        return t ? ok(`fake-connector-token-${t.id}`) : notFound;
      }
      case "PUT cfd_tunnel/*/configurations":
        return tunnel(segs[1]) ? ok({ tunnel_id: segs[1], version: 1 }) : notFound;
      case "DELETE cfd_tunnel/*": {
        const before = this.tunnels.length;
        this.tunnels = this.tunnels.filter((t) => t.id !== segs[1]);
        return before === this.tunnels.length ? notFound : ok({ id: segs[1] });
      }
      case "GET dns_records": {
        const search = q.get("search") ?? "";
        return ok(
          this.records.filter((r) => r.name.includes(search)).map((r) => ({ ...r, type: "CNAME" })),
        );
      }
      case "POST dns_records": {
        this.next += 1;
        const r = {
          id: `dns-${this.next}`,
          name: String(body.name),
          content: String(body.content),
        };
        this.records.push(r);
        return ok({ id: r.id, name: r.name });
      }
      case "PUT dns_records/*": {
        const r = this.records.find((x) => x.id === segs[1]);
        if (!r) return notFound;
        r.content = String(body.content);
        return ok({ id: r.id });
      }
      case "DELETE dns_records/*": {
        const before = this.records.length;
        this.records = this.records.filter((r) => r.id !== segs[1]);
        return before === this.records.length ? notFound : ok({ id: segs[1] });
      }
      default:
        return notFound;
    }
  };

  async start(port: number): Promise<NodeHttp.Server> {
    const server = NodeHttp.createServer((req, res) => {
      const chunks: Array<Buffer> = [];
      req.on("data", (c: Buffer) => chunks.push(c));
      req.on("end", () => {
        const url = new URL(req.url ?? "/", "http://127.0.0.1");
        void this.handle(
          req.method ?? "GET",
          url,
          Buffer.concat(chunks).toString("utf8"),
          String(req.headers.authorization ?? ""),
        ).then((r) => {
          res.writeHead(r.status, { "content-type": "application/json", ...r.headers });
          res.end(JSON.stringify(r.body));
        });
      });
    });
    await new Promise<void>((resolve) => server.listen(port, "127.0.0.1", resolve));
    return server;
  }
}

const b64url = (v: Buffer) => v.toString("base64url");
const CLI_REDIRECT = "http://127.0.0.1:34338/callback";

export interface AccountTokens {
  readonly access_token: string;
  readonly refresh_token: string;
  readonly expires_in: number;
}

/** Signs `email` in as the `vekrel-cli` client and returns the token response. */
export async function signIn(email: string): Promise<AccountTokens> {
  const verifier = b64url(NodeCrypto.randomBytes(32));
  const challenge = b64url(NodeCrypto.createHash("sha256").update(verifier).digest());
  const state = b64url(NodeCrypto.randomBytes(8));
  const authorize = new URL(`${RELAY_URL}/oauth/authorize`);
  for (const [k, v] of Object.entries({
    response_type: "code",
    client_id: "vekrel-cli",
    redirect_uri: CLI_REDIRECT,
    code_challenge: challenge,
    code_challenge_method: "S256",
    state,
  })) {
    authorize.searchParams.set(k, v);
  }
  const page = await relayFetch(authorize, { redirect: "manual" });
  const cookie = (page.headers.get("set-cookie") ?? "").split(";")[0] ?? "";
  if (!cookie.startsWith("vekrel_auth_request=")) throw new Error(`no auth cookie: ${page.status}`);
  const sent = await relayFetch(`${RELAY_URL}/auth/email`, {
    method: "POST",
    headers: { cookie, "content-type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ email }).toString(),
  });
  const html = await sent.text();
  const escaped = RELAY_URL.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
  const link = new RegExp(`${escaped}/auth/email/verify\\?t=[A-Za-z0-9_-]+`, "u").exec(html)?.[0];
  if (link === undefined) throw new Error(`no magic link on the page (${sent.status})`);
  const verified = await relayFetch(link, { headers: { cookie }, redirect: "manual" });
  const location = new URL(verified.headers.get("location") ?? "", RELAY_URL);
  if (`${location.origin}${location.pathname}` !== CLI_REDIRECT) {
    throw new Error(`unexpected redirect ${verified.status} ${location.origin}`);
  }
  if (location.searchParams.get("state") !== state) throw new Error("state not echoed");
  const token = await relayFetch(`${RELAY_URL}/oauth/token`, {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "authorization_code",
      code: location.searchParams.get("code") ?? "",
      redirect_uri: CLI_REDIRECT,
      client_id: "vekrel-cli",
      code_verifier: verifier,
    }).toString(),
  });
  if (token.status !== 200) throw new Error(`token endpoint ${token.status}`);
  return (await token.json()) as AccountTokens;
}

export function makeDpopKey() {
  const { privateKey, publicKey } = NodeCrypto.generateKeyPairSync("ec", { namedCurve: "P-256" });
  const { kty, crv, x, y } = publicKey.export({ format: "jwk" });
  const jwk = { kty: kty!, crv: crv!, x: x!, y: y! };
  const thumbprint = computeDpopJwkThumbprint(
    jwk as Parameters<typeof computeDpopJwkThumbprint>[0],
  );
  const proof = (input: { method: string; url: string; accessToken?: string }) => {
    const header = b64url(Buffer.from(JSON.stringify({ typ: "dpop+jwt", alg: "ES256", jwk })));
    const payload = b64url(
      Buffer.from(
        JSON.stringify({
          htm: input.method,
          htu: input.url,
          jti: NodeCrypto.randomUUID(),
          iat: Math.floor(Date.now() / 1000),
          ...(input.accessToken ? { ath: computeDpopAccessTokenHash(input.accessToken) } : {}),
        }),
      ),
    );
    const signature = NodeCrypto.sign("sha256", Buffer.from(`${header}.${payload}`), {
      key: privateKey,
      dsaEncoding: "ieee-p1363",
    });
    return `${header}.${payload}.${b64url(signature)}`;
  };
  return { thumbprint, proof };
}
