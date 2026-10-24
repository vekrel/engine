import { sha256 } from "@noble/hashes/sha2";
import * as Base64Url from "effect/encoding/Base64Url";

const CONNECT_LOOPBACK_CALLBACK_PATH = "/callback";

export const DEFAULT_HOSTED_APP_URL = "https://app.t3.codes";

export const CONNECT_OAUTH_SCOPES = ["openid", "profile", "email", "offline_access"] as const;

export interface PkcePair {
  readonly verifier: string;
  readonly challenge: string;
}

function randomBase64Url(byteLength: number): string {
  return Base64Url.encode(globalThis.crypto.getRandomValues(new Uint8Array(byteLength)));
}

/** RFC 7636 S256 pair: 32 random bytes give a 43-char verifier, the relay's minimum. */
export function createPkcePair(): PkcePair {
  const verifier = randomBase64Url(32);
  return {
    verifier,
    challenge: Base64Url.encode(sha256(new TextEncoder().encode(verifier))),
  };
}

export function createOAuthState(): string {
  return randomBase64Url(32);
}

/**
 * Redirect URI for the CLI's local callback listener. Must stay equal to the
 * `vekrel-cli` redirect URI in the relay (`src/accounts/clients.rs`).
 */
export function connectLoopbackRedirectUri(port: number): string {
  return `http://127.0.0.1:${port}${CONNECT_LOOPBACK_CALLBACK_PATH}`;
}

/** The relay's `GET /oauth/authorize`: PKCE S256 only, exact redirect URI per client. */
export function buildRelayAuthorizeUrl(input: {
  readonly relayUrl: string;
  readonly clientId: string;
  readonly redirectUri: string;
  readonly state: string;
  readonly challenge: string;
}): string {
  // Joined, not resolved: a relay mounted under a path prefix keeps it.
  const url = new URL(`${input.relayUrl.replace(/\/+$/u, "")}/oauth/authorize`);
  url.searchParams.set("response_type", "code");
  url.searchParams.set("client_id", input.clientId);
  url.searchParams.set("redirect_uri", input.redirectUri);
  url.searchParams.set("state", input.state);
  url.searchParams.set("code_challenge", input.challenge);
  url.searchParams.set("code_challenge_method", "S256");
  return url.toString();
}
