import { sha256 } from "@noble/hashes/sha2";
import * as Base64Url from "effect/encoding/Base64Url";
import { describe, expect, it } from "vite-plus/test";

import {
  buildRelayAuthorizeUrl,
  connectLoopbackRedirectUri,
  createOAuthState,
  createPkcePair,
} from "./connectAuth.ts";

describe("connectAuth", () => {
  it("makes an RFC 7636 S256 verifier and challenge pair", () => {
    const pair = createPkcePair();

    expect(pair.verifier).toMatch(/^[A-Za-z0-9_-]{43}$/u);
    expect(pair.challenge).toMatch(/^[A-Za-z0-9_-]{43}$/u);
    expect(pair.challenge).toBe(Base64Url.encode(sha256(new TextEncoder().encode(pair.verifier))));
    expect(createPkcePair().verifier).not.toBe(pair.verifier);
    expect(createOAuthState()).toMatch(/^[A-Za-z0-9_-]{43}$/u);
    expect(createOAuthState()).not.toBe(createOAuthState());
  });

  it("matches the RFC 7636 appendix B example", () => {
    const verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
    expect(Base64Url.encode(sha256(new TextEncoder().encode(verifier)))).toBe(
      "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM",
    );
  });

  it("builds the relay authorize URL with PKCE S256 and state", () => {
    const url = new URL(
      buildRelayAuthorizeUrl({
        relayUrl: "https://relay.example.test/",
        clientId: "vekrel-cli",
        redirectUri: connectLoopbackRedirectUri(34338),
        state: "state-1",
        challenge: "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM",
      }),
    );

    expect(url.origin + url.pathname).toBe("https://relay.example.test/oauth/authorize");
    expect(Object.fromEntries(url.searchParams)).toEqual({
      response_type: "code",
      client_id: "vekrel-cli",
      redirect_uri: "http://127.0.0.1:34338/callback",
      state: "state-1",
      code_challenge: "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM",
      code_challenge_method: "S256",
    });
  });
});
