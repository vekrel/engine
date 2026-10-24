// @effect-diagnostics globalFetch:off globalDate:off globalTimers:off - Promise API for app auth providers (React, Electron, Expo), outside an Effect runtime.
/**
 * Vekrel account sign-in against the relay's OAuth endpoints (`src/accounts/oauth.rs` in
 * Vekrel-Relay): authorization code + PKCE S256, rotating refresh tokens, RFC 7009 revoke.
 *
 * The relay rotates the refresh token on every use and revokes the whole family when a rotated
 * token is presented again, so a refresh must never run twice at once: one in-flight request
 * per session, and across tabs or windows of one origin a Web Lock.
 *
 * The web client never sees its refresh token: the relay keeps it in an HttpOnly cookie, sent
 * with `credentials: "include"`. Desktop, mobile and CLI get it in the body and keep it in a
 * `RefreshTokenStore`.
 */
import {
  buildRelayAuthorizeUrl,
  createOAuthState,
  createPkcePair,
} from "@vekrel/shared/connectAuth";
import * as Data from "effect/Data";

export const AccountClientId = {
  web: "vekrel-web",
  desktop: "vekrel-desktop",
  mobile: "vekrel-mobile",
  cli: "vekrel-cli",
} as const;
export type AccountClientId = (typeof AccountClientId)[keyof typeof AccountClientId];

/** Refresh the access token this long before it expires. */
const ACCESS_TOKEN_EXPIRY_SKEW_MS = 30_000;
/** A hung request must not hold the refresh lock (and every other tab) forever. */
const REQUEST_TIMEOUT_MS = 15_000;
const LOOPBACK_HOSTS = new Set(["localhost", "127.0.0.1", "[::1]"]);

export class AccountAuthError extends Data.TaggedError("AccountAuthError")<{
  readonly reason:
    | "state-mismatch"
    | "authorization-denied"
    | "missing-code"
    | "token-rejected"
    | "token-request-failed"
    | "invalid-token-response"
    | "invalid-relay-url";
  readonly message: string;
  readonly status?: number;
  readonly cause?: unknown;
}> {}

/** Kept by the client between opening the authorize URL and receiving the callback. */
export interface PendingSignIn {
  readonly state: string;
  readonly verifier: string;
  readonly redirectUri: string;
}

/**
 * The relay base URL without a trailing slash: https, or http on loopback for local dev; no
 * credentials, query or fragment; a path prefix is kept. Not `normalizeSecureRelayUrl`: that one
 * is https-origin only (Engine CloudLink relies on it) and would refuse a local relay.
 */
export function normalizeAccountRelayUrl(value: string): string {
  let url: URL | null = null;
  try {
    url = new URL(value.trim());
  } catch {}
  const secure =
    url?.protocol === "https:" || (url?.protocol === "http:" && LOOPBACK_HOSTS.has(url.hostname));
  if (!url || !secure || url.username || url.password || url.search || url.hash) {
    throw new AccountAuthError({
      reason: "invalid-relay-url",
      message: "The relay URL must be https (or http on localhost) without credentials or query.",
    });
  }
  return `${url.origin}${url.pathname.replace(/\/+$/u, "")}`;
}

export function beginSignIn(input: {
  readonly relayUrl: string;
  readonly clientId: AccountClientId;
  readonly redirectUri: string;
}): { readonly url: string; readonly pending: PendingSignIn } {
  const { verifier, challenge } = createPkcePair();
  const state = createOAuthState();
  return {
    url: buildRelayAuthorizeUrl({
      ...input,
      relayUrl: normalizeAccountRelayUrl(input.relayUrl),
      state,
      challenge,
    }),
    pending: { state, verifier, redirectUri: input.redirectUri },
  };
}

/** The authorization code from a callback URL; throws unless `state` is the one we sent. */
export function readSignInCallback(callbackUrl: string | URL, pending: PendingSignIn): string {
  const params = new URL(callbackUrl).searchParams;
  // State first: an error or code under a foreign state is not ours to act on.
  if (params.get("state") !== pending.state) {
    throw new AccountAuthError({
      reason: "state-mismatch",
      message: "The sign-in response does not belong to this sign-in.",
    });
  }
  const error = params.get("error");
  if (error) {
    throw new AccountAuthError({
      reason: "authorization-denied",
      message: params.get("error_description") || error,
    });
  }
  const code = params.get("code");
  if (!code) {
    throw new AccountAuthError({
      reason: "missing-code",
      message: "The sign-in response has no authorization code.",
    });
  }
  return code;
}

export interface RefreshTokenStore {
  readonly read: () => Promise<string | null>;
  readonly write: (refreshToken: string) => Promise<void>;
  readonly clear: () => Promise<void>;
}

type Fetch = (input: string, init: RequestInit) => Promise<Response>;

interface LockManagerLike {
  readonly request: <T>(name: string, callback: () => Promise<T>) => Promise<T>;
}

export interface AccountSessionOptions {
  readonly relayUrl: string;
  readonly clientId: AccountClientId;
  /** Required for every client except `vekrel-web`, whose refresh token is a relay cookie. */
  readonly store?: RefreshTokenStore;
  readonly fetch?: Fetch;
  /**
   * Called when a signed-in session ends because the relay refused its refresh token (revoked,
   * reused, disabled account). Not called for an explicit `signOut`.
   */
  readonly onSignedOut?: () => void;
}

export interface AccountSession {
  readonly completeSignIn: (callbackUrl: string | URL, pending: PendingSignIn) => Promise<void>;
  /** A valid access token, refreshing if needed; `null` when signed out (no request then). */
  readonly getAccessToken: () => Promise<string | null>;
  readonly refresh: () => Promise<string | null>;
  readonly signOut: () => Promise<void>;
}

interface TokenBody {
  readonly access_token: string;
  readonly expires_in: number;
  readonly refresh_token?: string;
}

function isTokenBody(value: unknown): value is TokenBody {
  const body = value as Partial<TokenBody> | null;
  return (
    typeof body?.access_token === "string" &&
    typeof body.expires_in === "number" &&
    (body.refresh_token === undefined || typeof body.refresh_token === "string")
  );
}

/**
 * Web Locks serialise refreshes across tabs and Electron windows of one origin.
 * shortcut: where Web Locks are missing (React Native, old browsers) only this session is
 * serialised; add a BroadcastChannel lock if a multi-context client without Web Locks appears.
 */
function crossContextLock(name: string): <T>(callback: () => Promise<T>) => Promise<T> {
  const locks = (globalThis.navigator as { readonly locks?: LockManagerLike } | undefined)?.locks;
  return locks ? (callback) => locks.request(name, callback) : (callback) => callback();
}

export function createAccountSession(options: AccountSessionOptions): AccountSession {
  const web = options.clientId === AccountClientId.web;
  const store = options.store;
  if (!web && !store) {
    throw new Error(`${options.clientId} needs a refresh token store.`);
  }
  const relayUrl = normalizeAccountRelayUrl(options.relayUrl);
  const fetchImpl: Fetch = options.fetch ?? ((input, init) => globalThis.fetch(input, init));
  const withLock = crossContextLock(`vekrel-account-refresh:${options.clientId}`);
  let access: { readonly token: string; readonly expiresAtMillis: number } | null = null;
  let inFlight: Promise<string | null> | null = null;
  // "unknown" until the relay answers once; "out" stops all token requests until a sign-in.
  let status: "unknown" | "in" | "out" = "unknown";
  // Bumped by sign-in and sign-out, so a request started before either cannot change the
  // session afterwards (a late rejection cannot clear a new sign-in).
  let generation = 0;
  // Every store mutation runs here, one at a time, with its generation checked inside, so a
  // superseded write cannot land after a sign-out's clear or over a newer sign-in's token.
  let storeTail: Promise<unknown> = Promise.resolve();
  const serialized = <T>(callback: () => Promise<T>): Promise<T> => {
    const run = storeTail.then(callback);
    storeTail = run.catch(() => undefined);
    return run;
  };

  /** POST a form; the timeout also covers reading the body. */
  const post = async (
    path: string,
    fields: Record<string, string>,
  ): Promise<{ readonly response: Response; readonly body: unknown }> => {
    const controller = new AbortController();
    const timer = setTimeout(
      () => controller.abort(new Error("The relay did not answer in time.")),
      REQUEST_TIMEOUT_MS,
    );
    try {
      const response = await fetchImpl(`${relayUrl}${path}`, {
        method: "POST",
        headers: { "content-type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams(fields).toString(),
        signal: controller.signal,
        ...(web ? { credentials: "include" } : {}),
      });
      const body: unknown = await response.json().catch(() => null);
      return { response, body };
    } catch (cause) {
      throw new AccountAuthError({
        reason: "token-request-failed",
        message: "Could not reach the Vekrel relay.",
        cause,
      });
    } finally {
      clearTimeout(timer);
    }
  };

  const requestTokens = async (fields: Record<string, string>): Promise<TokenBody> => {
    const { response, body } = await post("/oauth/token", {
      ...fields,
      client_id: options.clientId,
    });
    if (!response.ok) {
      const oauthError = (body as { readonly error?: unknown } | null)?.error;
      throw new AccountAuthError({
        // The relay answers every OAuth refusal (invalid_grant, invalid_request, invalid_client)
        // with 400. A 401 can come from a proxy; it, 429 and 5xx keep the session.
        reason: response.status === 400 ? "token-rejected" : "token-request-failed",
        message: `The relay refused the token request (${typeof oauthError === "string" ? oauthError : response.status}).`,
        status: response.status,
      });
    }
    if (!isTokenBody(body) || (!web && body.refresh_token === undefined)) {
      throw new AccountAuthError({
        reason: "invalid-token-response",
        message: "The relay sent an unexpected token response.",
      });
    }
    return body;
  };

  const revoke = (token: string | null) =>
    post("/oauth/revoke", {
      ...(token ? { token } : {}),
      client_id: options.clientId,
    }).catch(() => undefined);

  const accept = async (body: TokenBody, issuedAtGeneration: number): Promise<string | null> => {
    const accepted = await serialized(async () => {
      if (issuedAtGeneration !== generation) return false;
      if (store && body.refresh_token !== undefined) {
        await store.write(body.refresh_token);
        if (issuedAtGeneration !== generation) {
          // Superseded during the write; nothing newer can have written yet (we hold the queue).
          await store.clear();
          return false;
        }
      }
      status = "in";
      access = { token: body.access_token, expiresAtMillis: Date.now() + body.expires_in * 1_000 };
      return true;
    });
    if (accepted) return body.access_token;
    // Superseded by a sign-in or sign-out: kill the token's family (best effort, outside the
    // queue so a newer sign-in is not held up) and report whatever the session holds now.
    if (body.refresh_token !== undefined) await revoke(body.refresh_token);
    return access?.token ?? null;
  };

  const refreshOnce = async (): Promise<string | null> => {
    if (status === "out") {
      return null;
    }
    const startedAt = generation;
    const refreshToken = store ? await store.read() : null;
    if (store && !refreshToken) {
      if (startedAt === generation) status = "out";
      return access?.token ?? null;
    }
    // If the response to a committed rotation is lost (timeout, network), the token we hold is
    // already rotated; the relay has no grace window, so the next refresh is reuse and signs out.
    try {
      const body = await requestTokens({
        grant_type: "refresh_token",
        ...(refreshToken ? { refresh_token: refreshToken } : {}),
      });
      return await accept(body, startedAt);
    } catch (error) {
      if (!(error instanceof AccountAuthError && error.reason === "token-rejected")) {
        throw error;
      }
      const signedOut = await serialized(async () => {
        if (startedAt !== generation) return null;
        // A stored token counts as signed in: it is the previous launch's session.
        const wasSignedIn = status === "in" || refreshToken !== null;
        generation += 1;
        status = "out";
        access = null;
        await store?.clear();
        return wasSignedIn;
      });
      if (signedOut === null) return access?.token ?? null;
      if (signedOut) options.onSignedOut?.();
      return null;
    }
  };

  const refresh = (): Promise<string | null> => {
    inFlight ??= withLock(refreshOnce).finally(() => {
      inFlight = null;
    });
    return inFlight;
  };

  return {
    completeSignIn: async (callbackUrl, pending) => {
      const code = readSignInCallback(callbackUrl, pending);
      generation += 1;
      const startedAt = generation;
      const body = await requestTokens({
        grant_type: "authorization_code",
        code,
        redirect_uri: pending.redirectUri,
        code_verifier: pending.verifier,
      });
      await accept(body, startedAt);
    },
    getAccessToken: async () => {
      if (access && access.expiresAtMillis - ACCESS_TOKEN_EXPIRY_SKEW_MS > Date.now()) {
        return access.token;
      }
      return refresh();
    },
    refresh,
    signOut: async () => {
      generation += 1;
      status = "out";
      access = null;
      await serialized(async () => {
        const refreshToken = store ? await store.read() : null;
        // Revoke first, so a store that fails to clear still leaves a dead family behind.
        // Best effort: the local session is gone either way; the family expires in 30 days.
        if (web || refreshToken) await revoke(refreshToken);
        await store?.clear();
      });
    },
  };
}
