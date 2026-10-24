// Test-only. Writes the relay conformance vectors (Vekrel-Relay `tests/fixtures/relay/`, ArchBoss
// `docs/superpowers/plans/fixtures/relay/`) from Shared's own `relayJwt.ts` and `dpop.ts`:
//   node apps/server/test/relay-conformance/gen-vectors.ts <out-dir>
// Every verdict below is what Shared returns, never a hand-written expectation.
//
// Keys are TEST-ONLY: each Ed25519 key's seed is sha256(label) so the Rust tests can rebuild the
// private key without any private key in the files; DPoP keys are throwaway, made per run.
// Tokens are stored split (`parts`), joined with "." by the reader, so secret scanners do not
// mistake these test tokens for leaked credentials.
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Effect's Crypto has no ed25519 or P-256 signing.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

import * as Effect from "effect/Effect";

import {
  computeDpopAccessTokenHash,
  computeDpopJwkThumbprint,
  type DpopPublicJwk,
  verifyDpopProof,
} from "../../../../packages/shared/src/dpop.ts";
import {
  RELAY_ACTIVITY_PUBLISH_TYP,
  RELAY_HEALTH_REQUEST_TYP,
  RELAY_HOOK_DELIVERY_TYP,
  RELAY_LINK_PROOF_TYP,
  RELAY_MINT_REQUEST_TYP,
  RelayJwtError,
  signRelayJwt,
  verifyRelayJwt,
} from "../../../../packages/shared/src/relayJwt.ts";

const outDir = process.argv[2];
if (!outDir) throw new Error("usage: gen-vectors.ts <out-dir>");

// Relay-internal typs (the relay signs and verifies them itself), renamed for Vekrel.
const LINK_CHALLENGE_TYP = "vekrel-link-challenge+jwt";
const ACCESS_TOKEN_TYP = "vekrel-relay-dpop-access+jwt";

const NOW = 1_800_000_000;
const RELAY = "https://relay.example.test";
const MACHINE_ID = "machine-fixture-1";
const MACHINE = `vekrel-machine:${MACHINE_ID}`;
const USER = "usr_00000000-0000-4000-8000-000000000001";

const b64 = (v: string | Uint8Array) => Buffer.from(v).toString("base64url");
const parts = (token: string) => token.split(".");

function ed25519(name: string) {
  const label = `vekrel-relay TEST-ONLY fixture key: ${name}`;
  const seed = NodeCrypto.createHash("sha256").update(label).digest();
  const der = Buffer.concat([Buffer.from("302e020100300506032b657004220420", "hex"), seed]);
  const privateKey = NodeCrypto.createPrivateKey({ key: der, format: "der", type: "pkcs8" });
  const publicKey = NodeCrypto.createPublicKey(privateKey);
  return {
    label,
    privateKey,
    privatePem: privateKey.export({ format: "pem", type: "pkcs8" }).toString(),
    publicPem: publicKey.export({ format: "pem", type: "spki" }).toString(),
  };
}

const keys = {
  relayMint: ed25519("relay-mint"),
  machine: ed25519("machine"),
  stranger: ed25519("stranger"),
};
type KeyName = keyof typeof keys;

// Raw header/payload JSON text, so cases can carry what `SignJWT` would never produce.
function signRaw(key: KeyName, header: string, payload: string, mutateSig?: (s: Buffer) => Buffer) {
  const hp = `${b64(header)}.${b64(payload)}`;
  let sig = NodeCrypto.sign(null, Buffer.from(hp), keys[key].privateKey);
  if (mutateSig) sig = mutateSig(sig);
  return `${hp}.${b64(sig)}`;
}

async function verdict(input: {
  key: KeyName;
  token: string;
  typ: string;
  issuer: string;
  audience: string;
  now: number;
  maxTokenAgeSeconds?: number;
}) {
  return Effect.runPromise(
    verifyRelayJwt({
      publicKey: keys[input.key].publicPem,
      token: input.token,
      typ: input.typ,
      issuer: input.issuer,
      audience: input.audience,
      nowEpochSeconds: input.now,
      ...(input.maxTokenAgeSeconds === undefined ? {} : { maxTokenAge: input.maxTokenAgeSeconds }),
    }).pipe(
      Effect.match({
        onSuccess: (payload) => ({ ok: true, payload }),
        onFailure: (error) => {
          const cause = error.cause as { claim?: string; reason?: string };
          return {
            ok: false,
            code: RelayJwtError.diagnosticCode(error),
            ...(cause.claim === undefined ? {} : { claim: cause.claim }),
            ...(cause.reason === undefined ? {} : { reason: cause.reason }),
          };
        },
      }),
    ),
  );
}

// jws.json: verify vectors (a machine-signed link proof unless noted)

const proofClaims = (over: Record<string, unknown> = {}) => ({
  iss: MACHINE,
  aud: RELAY,
  sub: MACHINE_ID,
  jti: "jti-1",
  iat: NOW,
  exp: NOW + 300,
  ...over,
});
const omit = (key: string) => {
  const c: Record<string, unknown> = proofClaims();
  delete c[key];
  return c;
};
const hdr = (over: Record<string, unknown> = {}) =>
  JSON.stringify({ alg: "EdDSA", typ: RELAY_LINK_PROOF_TYP, ...over });
const tok = (claims: Record<string, unknown>, header = hdr(), key: KeyName = "machine") =>
  signRaw(key, header, JSON.stringify(claims));
const L = 2n ** 252n + 27742317777372353535851937790883648493n;
const sPlusL = (sig: Buffer) => {
  const s = BigInt(`0x${Buffer.from(sig.subarray(32).toReversed()).toString("hex")}`) + L;
  const le = Buffer.from(Buffer.from(s.toString(16).padStart(64, "0"), "hex").toReversed());
  return Buffer.concat([sig.subarray(0, 32), le]);
};
const valid = tok(proofClaims());

type VerifyCase = {
  token: string;
  key?: KeyName;
  typ?: string;
  issuer?: string;
  audience?: string;
  now?: number;
  maxTokenAgeSeconds?: number;
};
const verifyCases: Array<[string, VerifyCase]> = [
  ["valid", { token: valid }],
  [
    "valid_typ_case_insensitive",
    { token: tok(proofClaims(), hdr({ typ: RELAY_LINK_PROOF_TYP.toUpperCase() })) },
  ],
  [
    "valid_typ_application_prefix",
    { token: tok(proofClaims(), hdr({ typ: `application/${RELAY_LINK_PROOF_TYP}` })) },
  ],
  [
    "valid_extra_header_and_claims",
    { token: tok(proofClaims({ extra: [1, { a: null }] }), hdr({ kid: "k1" })) },
  ],
  ["valid_aud_array", { token: tok(proofClaims({ aud: ["https://other.example", RELAY] })) }],
  ["valid_no_exp", { token: tok(omit("exp")) }],
  ["valid_exp_within_tolerance", { token: tok(proofClaims({ iat: NOW - 100, exp: NOW - 59 })) }],
  ["expired_exp_at_tolerance", { token: tok(proofClaims({ iat: NOW - 100, exp: NOW - 60 })) }],
  ["valid_iat_age_360", { token: tok(proofClaims({ iat: NOW - 360 })) }],
  ["too_old_iat_361", { token: tok(proofClaims({ iat: NOW - 361 })) }],
  ["valid_future_iat_60", { token: tok(proofClaims({ iat: NOW + 60 })) }],
  ["future_iat_61", { token: tok(proofClaims({ iat: NOW + 61 })) }],
  ["valid_iat_fraction", { token: tok(proofClaims({ iat: NOW + 0.5 })) }],
  ["valid_nbf_within_tolerance", { token: tok(proofClaims({ nbf: NOW + 60 })) }],
  ["nbf_future_61", { token: tok(proofClaims({ nbf: NOW + 61 })) }],
  ["nbf_string", { token: tok(proofClaims({ nbf: "1" })) }],
  ["exp_string", { token: tok(proofClaims({ exp: String(NOW + 300) })) }],
  ["exp_null", { token: tok(proofClaims({ exp: null })) }],
  ["iat_string", { token: tok(proofClaims({ iat: String(NOW) })) }],
  ["missing_iat", { token: tok(omit("iat")) }],
  ["missing_iss", { token: tok(omit("iss")) }],
  ["missing_aud", { token: tok(omit("aud")) }],
  ["missing_all_registered", { token: tok({ jti: "j" }) }],
  ["wrong_typ", { token: tok(proofClaims(), hdr({ typ: RELAY_ACTIVITY_PUBLISH_TYP })) }],
  ["missing_typ", { token: tok(proofClaims(), JSON.stringify({ alg: "EdDSA" })) }],
  ["typ_not_string", { token: tok(proofClaims(), hdr({ typ: 1 })) }],
  ["wrong_aud", { token: tok(proofClaims({ aud: "https://other.example" })) }],
  ["aud_array_without_relay", { token: tok(proofClaims({ aud: ["https://other.example"] })) }],
  ["wrong_iss", { token: tok(proofClaims({ iss: "vekrel-machine:other" })) }],
  ["t3_prefix_iss_refused", { token: tok(proofClaims({ iss: `t3-env:${MACHINE_ID}` })) }],
  ["iss_not_string", { token: tok(proofClaims({ iss: 1 })) }],
  ["bad_signature", { token: tok(proofClaims(), hdr(), "stranger") }],
  [
    "bad_signature_before_expiry",
    { token: tok(proofClaims({ exp: NOW - 1000 }), hdr(), "stranger") },
  ],
  [
    "signature_s_plus_l",
    { token: signRaw("machine", hdr(), JSON.stringify(proofClaims()), sPlusL) },
  ],
  [
    "signature_63_bytes",
    { token: signRaw("machine", hdr(), JSON.stringify(proofClaims()), (s) => s.subarray(0, 63)) },
  ],
  [
    "signature_not_base64url",
    { token: `${parts(valid).slice(0, 2).join(".")}.*${parts(valid)[2]}` },
  ],
  ["signature_padded", { token: `${valid}==` }],
  ["empty_signature", { token: `${parts(valid).slice(0, 2).join(".")}.` }],
  ["alg_hs256", { token: tok(proofClaims(), hdr({ alg: "HS256" })) }],
  ["alg_missing", { token: tok(proofClaims(), JSON.stringify({ typ: RELAY_LINK_PROOF_TYP })) }],
  ["alg_empty", { token: tok(proofClaims(), hdr({ alg: "" })) }],
  ["crit_unknown", { token: tok(proofClaims(), hdr({ crit: ["exp"], exp: 1 })) }],
  ["crit_empty", { token: tok(proofClaims(), hdr({ crit: [] })) }],
  ["crit_b64_missing_param", { token: tok(proofClaims(), hdr({ crit: ["b64"] })) }],
  ["valid_crit_b64_true", { token: tok(proofClaims(), hdr({ crit: ["b64"], b64: true })) }],
  ["crit_b64_not_boolean", { token: tok(proofClaims(), hdr({ crit: ["b64"], b64: "no" })) }],
  ["b64_false_without_crit_is_ignored", { token: tok(proofClaims(), hdr({ b64: false })) }],
  ["empty_header_segment", { token: `.${parts(valid).slice(1).join(".")}` }],
  ["header_not_json", { token: signRaw("machine", "{", JSON.stringify(proofClaims())) }],
  ["header_array", { token: signRaw("machine", "[]", JSON.stringify(proofClaims())) }],
  ["payload_not_json", { token: signRaw("machine", hdr(), "{") }],
  ["payload_array", { token: signRaw("machine", hdr(), "[]") }],
  ["payload_null", { token: signRaw("machine", hdr(), "null") }],
  ["valid_payload_bom", { token: signRaw("machine", hdr(), `﻿${JSON.stringify(proofClaims())}`) }],
  ["two_parts", { token: parts(valid).slice(0, 2).join(".") }],
  ["four_parts", { token: `${valid}.x` }],
  ["empty_token", { token: "" }],
  // Relay DPoP access tokens: relay key, 30-minute max age (`RelayTokens.ts:29`, its test's numbers).
  ...(() => {
    const access = (iat: number, exp: number) =>
      tok(
        {
          iss: RELAY,
          aud: RELAY,
          sub: USER,
          jti: "access-token-1",
          iat,
          exp,
          client_id: "vekrel-mobile",
          scope: "machine:connect",
          cnf: { jkt: "proof-key-thumbprint" },
        },
        JSON.stringify({ alg: "EdDSA", typ: ACCESS_TOKEN_TYP }),
        "relayMint",
      );
    const base = {
      key: "relayMint" as const,
      typ: ACCESS_TOKEN_TYP,
      issuer: RELAY,
      audience: RELAY,
      maxTokenAgeSeconds: 1800,
    };
    return [
      ["access_valid_at_600s", { ...base, token: access(NOW, NOW + 1800), now: NOW + 600 }],
      ["access_expired_after_ttl", { ...base, token: access(NOW, NOW + 1800), now: NOW + 1861 }],
      ["access_iat_older_than_30min", { ...base, token: access(NOW - 1861, NOW + 60) }],
      [
        "access_default_max_age_refuses_it",
        { ...base, maxTokenAgeSeconds: undefined, token: access(NOW, NOW + 1800), now: NOW + 600 },
      ],
    ] as Array<[string, VerifyCase]>;
  })(),
];

const verify = [];
for (const [name, c] of verifyCases) {
  const input = {
    key: c.key ?? "machine",
    typ: c.typ ?? RELAY_LINK_PROOF_TYP,
    issuer: c.issuer ?? MACHINE,
    audience: c.audience ?? RELAY,
    now: c.now ?? NOW,
    ...(c.maxTokenAgeSeconds === undefined ? {} : { maxTokenAgeSeconds: c.maxTokenAgeSeconds }),
  };
  verify.push({
    name,
    parts: parts(c.token),
    ...input,
    shared: await verdict({ ...input, token: c.token }),
  });
}

// jws.json: relay-signed shapes, signed with `signRelayJwt` (jose `SignJWT`)

const signCases: Array<[string, string, string, Record<string, unknown>]> = [
  // RelayTokens.ts:121-137 (kind renamed: "environment" is "machine").
  [
    "link_challenge",
    LINK_CHALLENGE_TYP,
    RELAY,
    {
      kind: "machine_link_challenge",
      iss: RELAY,
      aud: RELAY,
      sub: USER,
      jti: "challenge-1",
      iat: NOW,
      exp: NOW + 300,
      notificationsEnabled: true,
      liveActivitiesEnabled: false,
      managedTunnelsEnabled: true,
    },
  ],
  // RelayTokens.ts:167-185.
  [
    "dpop_access_mobile",
    ACCESS_TOKEN_TYP,
    RELAY,
    {
      iss: RELAY,
      aud: RELAY,
      sub: USER,
      jti: "access-token-1",
      iat: NOW,
      exp: NOW + 1800,
      client_id: "vekrel-mobile",
      scope: "machine:connect machine:status mobile:registration",
      cnf: { jkt: "proof-key-thumbprint" },
    },
  ],
  [
    "dpop_access_web",
    ACCESS_TOKEN_TYP,
    RELAY,
    {
      iss: RELAY,
      aud: RELAY,
      sub: USER,
      jti: "web-access-token-1",
      iat: NOW,
      exp: NOW + 1800,
      client_id: "vekrel-web",
      scope: "machine:connect machine:status",
      cnf: { jkt: "web-proof-key-thumbprint" },
    },
  ],
  // EnvironmentConnector.ts:625-641 with contracts `RelayCloudMintCredentialProofPayload`.
  [
    "mint_request",
    RELAY_MINT_REQUEST_TYP,
    MACHINE,
    {
      iss: RELAY,
      aud: MACHINE,
      sub: USER,
      jti: "mint-1",
      iat: NOW,
      exp: NOW + 120,
      machineId: MACHINE_ID,
      clientProofKeyThumbprint: "client-jkt",
      cnf: { jkt: "client-jkt" },
      deviceId: "device-1",
      nonce: "nonce-1",
      scope: ["machine:connect"],
    },
  ],
  [
    "mint_request_no_device",
    RELAY_MINT_REQUEST_TYP,
    MACHINE,
    {
      iss: RELAY,
      aud: MACHINE,
      sub: USER,
      jti: "mint-2",
      iat: NOW,
      exp: NOW + 120,
      machineId: MACHINE_ID,
      clientProofKeyThumbprint: "client-jkt",
      cnf: { jkt: "client-jkt" },
      nonce: "nonce-2",
      scope: ["machine:connect"],
    },
  ],
  // EnvironmentConnector.ts:460-476 with `RelayCloudMachineHealthProofPayload`.
  [
    "health_request",
    RELAY_HEALTH_REQUEST_TYP,
    MACHINE,
    {
      iss: RELAY,
      aud: MACHINE,
      sub: USER,
      jti: "health-1",
      iat: NOW,
      exp: NOW + 120,
      machineId: MACHINE_ID,
      nonce: "nonce-3",
      scope: ["machine:status"],
    },
  ],
  // HookForwarder.ts:248-262 with `RelayHookDeliveryProofPayload`.
  [
    "hook_delivery",
    RELAY_HOOK_DELIVERY_TYP,
    MACHINE,
    {
      iss: RELAY,
      aud: MACHINE,
      sub: MACHINE_ID,
      jti: "delivery-jti-1",
      iat: NOW,
      exp: NOW + 300,
      machineId: MACHINE_ID,
      deliveryId: "delivery-1",
      receivedAt: "2027-01-15T08:00:00.000Z",
      hookId: "hook-1",
    },
  ],
];

const sign = [];
for (const [name, typ, audience, payload] of signCases) {
  const token = await Effect.runPromise(
    signRelayJwt({ privateKey: keys.relayMint.privatePem, typ, payload }),
  );
  sign.push({
    name,
    typ,
    payload,
    parts: parts(token),
    // What the receiver (Engine for machine audiences, the relay itself otherwise) decides.
    shared: await verdict({ key: "relayMint", token, typ, issuer: RELAY, audience, now: NOW }),
  });
}

const jws = {
  _note:
    "TEST-ONLY vectors from Vekrel-Engine apps/server/test/relay-conformance/gen-vectors.ts. Not secrets.",
  keys: Object.fromEntries(
    Object.entries(keys).map(([k, v]) => [
      k,
      { seedIsSha256Of: v.label, publicKeySpkiPem: v.publicPem },
    ]),
  ),
  verify,
  sign,
};

// dpop.json: verifyDpopProof (cases follow dpop.test.ts and DpopProofs.verifyAndConsume.test.ts)

const ec = NodeCrypto.generateKeyPairSync("ec", { namedCurve: "P-256" });
const otherEc = NodeCrypto.generateKeyPairSync("ec", { namedCurve: "P-256" }).privateKey;
const jwk = ec.publicKey.export({ format: "jwk" }) as DpopPublicJwk;
const privJwk = ec.privateKey.export({ format: "jwk" });
const thumb = computeDpopJwkThumbprint(jwk);
const URL1 = `${RELAY}/v1/machines/${MACHINE_ID}/connect`;
const TOKEN = "relay-access-token-1";

function dpopRaw(
  header: string,
  payload: string,
  o: { key?: NodeCrypto.KeyObject; mutateSig?: (s: Buffer) => Buffer } = {},
) {
  const hp = `${b64(header)}.${b64(payload)}`;
  let s = NodeCrypto.sign("sha256", Buffer.from(hp), {
    key: o.key ?? ec.privateKey,
    dsaEncoding: "ieee-p1363",
  });
  if (o.mutateSig) s = o.mutateSig(s);
  return `${hp}.${b64(s)}`;
}
const dh = (over: Record<string, unknown> = {}) =>
  JSON.stringify({ typ: "dpop+jwt", alg: "ES256", jwk, ...over });
const dp = (over: Record<string, unknown> = {}) =>
  JSON.stringify({ htm: "POST", htu: URL1, jti: "proof-1", iat: NOW, ...over });
const dproof = (h = {}, p = {}, o = {}) => dpopRaw(dh(h), dp(p), o);
const N = BigInt("0xffffffff00000000ffffffffffffffffbce6faada7179e84f3b9cac2fc632551");
const highS = (s: Buffer) => {
  const v = BigInt(`0x${Buffer.from(s.subarray(32)).toString("hex")}`);
  const hi = v > N / 2n ? v : N - v;
  return Buffer.concat([s.subarray(0, 32), Buffer.from(hi.toString(16).padStart(64, "0"), "hex")]);
};
const v1 = dproof();

type DpopCase = {
  proof: string;
  method?: string;
  url?: string;
  now?: number;
  expectedThumbprint?: string | null;
  expectedAccessToken?: string;
};
const dpopCases: Array<[string, DpopCase]> = [
  ["valid", { proof: v1 }],
  ["valid_lowercase_htm", { proof: dproof({}, { htm: "post" }) }],
  ["valid_request_url_query_fragment", { proof: v1, url: `${URL1}?a=1#f` }],
  [
    "valid_extra_fields_and_kid",
    { proof: dproof({ jwk: { ...jwk, kid: "k" }, x5: 1 }, { extra: true }) },
  ],
  [
    "valid_iat_float_integral",
    { proof: dpopRaw(dh(), dp().replace(`"iat":${NOW}`, `"iat":${NOW}.0`)) },
  ],
  ["valid_high_s", { proof: dproof({}, {}, { mutateSig: highS }) }],
  [
    "valid_with_ath",
    { proof: dproof({}, { ath: computeDpopAccessTokenHash(TOKEN) }), expectedAccessToken: TOKEN },
  ],
  ["valid_without_expected_thumbprint", { proof: v1, expectedThumbprint: null }],
  ["valid_skew_minus_300", { proof: v1, now: NOW + 300 }],
  ["valid_skew_plus_300", { proof: v1, now: NOW - 300 }],
  [
    "valid_padded_segments",
    {
      proof: v1
        .split(".")
        .map((p) => p + "=".repeat((4 - (p.length % 4)) % 4))
        .join("."),
    },
  ],
  [
    "valid_future_iat_300_for_replay_window",
    { proof: dproof({}, { jti: "proof-future", iat: NOW + 300 }) },
  ],
  ["time_window_plus_301", { proof: v1, now: NOW + 301 }],
  ["time_window_minus_301", { proof: v1, now: NOW - 301 }],
  [
    "bad_signature_before_time_window",
    { proof: dproof({}, {}, { key: otherEc }), now: NOW + 1000 },
  ],
  ["method_mismatch", { proof: v1, method: "GET" }],
  ["url_mismatch", { proof: v1, url: `${RELAY}/v1/machines/other/connect` }],
  ["key_mismatch", { proof: v1, expectedThumbprint: "other-thumbprint" }],
  [
    "key_mismatch_before_bad_signature",
    { proof: dproof({}, {}, { key: otherEc }), expectedThumbprint: "x" },
  ],
  [
    "method_mismatch_before_bad_signature",
    { proof: dproof({}, {}, { key: otherEc }), method: "GET" },
  ],
  ["ath_missing", { proof: v1, expectedAccessToken: TOKEN }],
  [
    "ath_wrong",
    { proof: dproof({}, { ath: computeDpopAccessTokenHash("other") }), expectedAccessToken: TOKEN },
  ],
  [
    "ath_before_bad_signature",
    { proof: dproof({}, {}, { key: otherEc }), expectedAccessToken: TOKEN },
  ],
  ["private_jwk_d", { proof: dproof({ jwk: privJwk }) }],
  ["malformed_header_json", { proof: `${b64("{")}.${parts(v1)[1]}.${parts(v1)[2]}` }],
  ["malformed_payload_json", { proof: `${parts(v1)[0]}.${b64("{")}.${parts(v1)[2]}` }],
  ["header_not_base64url", { proof: `*${v1}` }],
  ["missing_proof", { proof: "" }],
  ["blank_proof", { proof: " \t" }],
  ["two_parts", { proof: parts(v1).slice(0, 2).join(".") }],
  ["empty_signature_part", { proof: `${parts(v1).slice(0, 2).join(".")}.` }],
  ["typ_wrong", { proof: dproof({ typ: "JWT" }) }],
  ["alg_es384", { proof: dproof({ alg: "ES384" }) }],
  ["kty_rsa", { proof: dproof({ jwk: { ...jwk, kty: "RSA" } }) }],
  ["empty_jti", { proof: dproof({}, { jti: "" }) }],
  ["empty_htm", { proof: dproof({}, { htm: "" }) }],
  ["iat_fraction", { proof: dproof({}, { iat: NOW + 0.5 }) }],
  ["iat_string", { proof: dproof({}, { iat: String(NOW) }) }],
  ["ath_not_string", { proof: dproof({}, { ath: 1 }) }],
  [
    "x_31_bytes",
    { proof: dproof({ jwk: { ...jwk, x: b64(Buffer.alloc(31, 1)) } }), expectedThumbprint: null },
  ],
  [
    "point_off_curve",
    { proof: dproof({ jwk: { ...jwk, y: b64(Buffer.alloc(32, 1)) } }), expectedThumbprint: null },
  ],
  ["signature_63_bytes", { proof: dproof({}, {}, { mutateSig: (s) => s.subarray(0, 63) }) }],
  [
    "signature_zero_s",
    {
      proof: dproof(
        {},
        {},
        { mutateSig: (s) => Buffer.concat([s.subarray(0, 32), Buffer.alloc(32)]) },
      ),
    },
  ],
];

const dpopVectors = dpopCases.map(([name, c]) => {
  const input = {
    method: c.method ?? "POST",
    url: c.url ?? URL1,
    now: c.now ?? NOW,
    expectedThumbprint: c.expectedThumbprint === undefined ? thumb : c.expectedThumbprint,
    expectedAccessToken: c.expectedAccessToken ?? null,
  };
  const r = verifyDpopProof({
    proof: c.proof,
    method: input.method,
    url: input.url,
    nowEpochSeconds: input.now,
    ...(input.expectedThumbprint ? { expectedThumbprint: input.expectedThumbprint } : {}),
    ...(input.expectedAccessToken ? { expectedAccessToken: input.expectedAccessToken } : {}),
  });
  return { name, parts: c.proof.split("."), ...input, shared: r };
});

const dpop = {
  _note:
    'TEST-ONLY vectors from Vekrel-Engine apps/server/test/relay-conformance/gen-vectors.ts; throwaway keys, not secrets. Proofs are stored split (`parts`), join with ".".',
  thumbprint: thumb,
  vectors: dpopVectors,
};

NodeFS.mkdirSync(outDir, { recursive: true });
NodeFS.writeFileSync(NodePath.join(outDir, "jws.json"), `${JSON.stringify(jws, null, 1)}\n`);
NodeFS.writeFileSync(NodePath.join(outDir, "dpop.json"), `${JSON.stringify(dpop, null, 1)}\n`);
