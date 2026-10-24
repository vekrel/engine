import * as Base64Url from "effect/encoding/Base64Url";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import {
  AccountAuthError,
  AccountClientId,
  beginSignIn,
  createAccountSession,
  readSignInCallback,
  type RefreshTokenStore,
} from "./accountAuth.ts";

const RELAY = "https://relay.example.test";
const APP_REDIRECT = "vekrel://auth/callback";

const challengeOf = async (verifier: string) =>
  Base64Url.encode(
    new Uint8Array(
      await globalThis.crypto.subtle.digest("SHA-256", new TextEncoder().encode(verifier)),
    ),
  );

/**
 * A fake `/oauth/token` + `/oauth/revoke` that rotates like the relay: every refresh
 * rotates, presenting a rotated token revokes the whole family, and the web client's
 * refresh token lives only in a cookie jar sent with `credentials: "include"`.
 */
function fakeRelay() {
  let serial = 0;
  const tokens = new Map<string, { family: number; rotated: boolean }>();
  const revokedFamilies = new Set<number>();
  const codes = new Map<string, { challenge: string; redirectUri: string }>();
  const jar = { refresh: null as string | null };
  const requests: Array<Record<string, string>> = [];
  let refreshHold: Promise<void> | null = null;

  const json = (status: number, body: unknown) =>
    new Response(JSON.stringify(body), {
      status,
      headers: { "content-type": "application/json" },
    });
  const issue = (family: number, web: boolean) => {
    serial += 1;
    const refresh = `refresh-${serial}`;
    tokens.set(refresh, { family, rotated: false });
    if (web) jar.refresh = refresh;
    return json(200, {
      access_token: `access-${serial}`,
      token_type: "Bearer",
      expires_in: 600,
      ...(web ? {} : { refresh_token: refresh }),
      id_token: "id",
    });
  };

  const fetch = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
    const url = new URL(String(input));
    const form = Object.fromEntries(new URLSearchParams(String(init?.body ?? "")));
    requests.push({ path: url.pathname, ...form });
    const web = form.client_id === AccountClientId.web;
    const cookie = init?.credentials === "include" ? jar.refresh : null;

    if (url.pathname.endsWith("/oauth/revoke")) {
      const presented = form.token ?? cookie;
      const row = presented ? tokens.get(presented) : undefined;
      if (row) revokedFamilies.add(row.family);
      if (init?.credentials === "include") jar.refresh = null;
      return new Response(null, { status: 200 });
    }

    if (form.grant_type === "authorization_code") {
      const pending = codes.get(form.code ?? "");
      codes.delete(form.code ?? "");
      if (
        !pending ||
        pending.redirectUri !== form.redirect_uri ||
        pending.challenge !== (await challengeOf(form.code_verifier ?? ""))
      ) {
        return json(400, { error: "invalid_grant", error_description: "bad code" });
      }
      return issue(serial + 1000, web);
    }

    if (refreshHold) await refreshHold;
    const presented = form.refresh_token ?? cookie;
    if (!presented) {
      return json(400, {
        error: "invalid_request",
        error_description: "refresh_token is required",
      });
    }
    const row = tokens.get(presented);
    if (!row || revokedFamilies.has(row.family)) {
      return json(400, { error: "invalid_grant", error_description: "revoked" });
    }
    if (row.rotated) {
      revokedFamilies.add(row.family);
      return json(400, { error: "invalid_grant", error_description: "reused" });
    }
    row.rotated = true;
    // The rotation is committed; the response arrives later, as over a network.
    await Promise.resolve();
    return issue(row.family, web);
  });

  return {
    fetch,
    jar,
    requests,
    revokedFamilies,
    tokenRequests: () => requests.filter((request) => request.path?.endsWith("/oauth/token")),
    /** Delays every refresh until the returned release is called. */
    holdRefreshes: () => {
      let release!: () => void;
      refreshHold = new Promise((resolve) => {
        release = resolve;
      });
      return () => {
        refreshHold = null;
        release();
      };
    },
    addCode: (code: string, challenge: string, redirectUri: string) =>
      codes.set(code, { challenge, redirectUri }),
    /** A live refresh family for a client that keeps its token in a store. */
    revokeAll: () => {
      for (const row of tokens.values()) revokedFamilies.add(row.family);
    },
    seedRefresh: () => {
      tokens.set("refresh-seed", { family: 1, rotated: false });
      return "refresh-seed";
    },
  };
}

function memoryStore(initial: string | null): RefreshTokenStore & { value: string | null } {
  const store = {
    value: initial,
    read: async () => store.value,
    write: async (token: string) => {
      store.value = token;
    },
    clear: async () => {
      store.value = null;
    },
  };
  return store;
}

/** A store whose next `write` waits until `release` is called; `started` resolves when it begins. */
function slowWriteStore(initial: string | null) {
  const store = memoryStore(initial);
  let release!: () => void;
  let started!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const writeStarted = new Promise<void>((resolve) => {
    started = resolve;
  });
  let held = true;
  const write = async (token: string) => {
    if (held) {
      held = false;
      started();
      await gate;
    }
    store.value = token;
  };
  return { store: Object.assign(store, { write }), release, writeStarted };
}

/** A Web Locks `LockManager` shared by every "tab" of one origin. */
function sharedWebLocks() {
  let tail: Promise<unknown> = Promise.resolve();
  return {
    request: <T>(_name: string, callback: () => Promise<T>): Promise<T> => {
      const run = tail.then(callback);
      tail = run.catch(() => undefined);
      return run;
    },
  };
}

/** A Web Locks `LockManager` that grants nothing until `grant` is called. */
function heldWebLocks() {
  let grant!: () => void;
  const granted = new Promise<void>((resolve) => {
    grant = resolve;
  });
  return {
    locks: {
      request: async <T>(_name: string, callback: () => Promise<T>): Promise<T> => {
        await granted;
        return callback();
      },
    },
    grant,
  };
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("beginSignIn", () => {
  it("sends the S256 challenge of the pending verifier and a fresh state", async () => {
    const first = beginSignIn({
      relayUrl: RELAY,
      clientId: AccountClientId.mobile,
      redirectUri: APP_REDIRECT,
    });
    const second = beginSignIn({
      relayUrl: RELAY,
      clientId: AccountClientId.mobile,
      redirectUri: APP_REDIRECT,
    });
    const url = new URL(first.url);

    expect(url.origin + url.pathname).toBe(`${RELAY}/oauth/authorize`);
    expect(url.searchParams.get("client_id")).toBe("vekrel-mobile");
    expect(url.searchParams.get("redirect_uri")).toBe(APP_REDIRECT);
    expect(url.searchParams.get("code_challenge_method")).toBe("S256");
    expect(url.searchParams.get("code_challenge")).toBe(await challengeOf(first.pending.verifier));
    expect(url.searchParams.get("state")).toBe(first.pending.state);
    expect(first.pending.verifier).not.toBe(second.pending.verifier);
    expect(first.pending.state).not.toBe(second.pending.state);
  });
});

describe("readSignInCallback", () => {
  const { pending } = beginSignIn({
    relayUrl: RELAY,
    clientId: AccountClientId.desktop,
    redirectUri: APP_REDIRECT,
  });
  const reason = (url: string) => {
    try {
      readSignInCallback(url, pending);
    } catch (error) {
      expect(error).toBeInstanceOf(AccountAuthError);
      return (error as AccountAuthError).reason;
    }
    return "accepted";
  };

  it("returns the code when the state matches", () => {
    expect(readSignInCallback(`${APP_REDIRECT}?code=c1&state=${pending.state}`, pending)).toBe(
      "c1",
    );
  });

  it("refuses a state mismatch, a missing state, and an error from a foreign state", () => {
    expect(reason(`${APP_REDIRECT}?code=c1&state=other`)).toBe("state-mismatch");
    expect(reason(`${APP_REDIRECT}?code=c1`)).toBe("state-mismatch");
    expect(reason(`${APP_REDIRECT}?error=access_denied&state=other`)).toBe("state-mismatch");
  });

  it("reports a relay error and a missing code for the right state", () => {
    expect(reason(`${APP_REDIRECT}?error=access_denied&state=${pending.state}`)).toBe(
      "authorization-denied",
    );
    expect(reason(`${APP_REDIRECT}?state=${pending.state}`)).toBe("missing-code");
  });
});

describe("createAccountSession", () => {
  it("exchanges the code with the verifier and stores the refresh token", async () => {
    const relay = fakeRelay();
    const store = memoryStore(null);
    const session = createAccountSession({
      relayUrl: RELAY,
      clientId: AccountClientId.mobile,
      store,
      fetch: relay.fetch,
    });
    const { pending } = beginSignIn({
      relayUrl: RELAY,
      clientId: AccountClientId.mobile,
      redirectUri: APP_REDIRECT,
    });
    relay.addCode("code-1", await challengeOf(pending.verifier), APP_REDIRECT);

    await session.completeSignIn(`${APP_REDIRECT}?code=code-1&state=${pending.state}`, pending);

    expect(relay.requests[0]).toMatchObject({
      grant_type: "authorization_code",
      client_id: "vekrel-mobile",
      code: "code-1",
      redirect_uri: APP_REDIRECT,
      code_verifier: pending.verifier,
    });
    expect(store.value).toMatch(/^refresh-/u);
    expect(await session.getAccessToken()).toMatch(/^access-/u);
    expect(relay.tokenRequests()).toHaveLength(1);
  });

  it("does not exchange a code whose state does not match", async () => {
    const relay = fakeRelay();
    const session = createAccountSession({
      relayUrl: RELAY,
      clientId: AccountClientId.mobile,
      store: memoryStore(null),
      fetch: relay.fetch,
    });
    const { pending } = beginSignIn({
      relayUrl: RELAY,
      clientId: AccountClientId.mobile,
      redirectUri: APP_REDIRECT,
    });

    await expect(
      session.completeSignIn(`${APP_REDIRECT}?code=code-1&state=forged`, pending),
    ).rejects.toMatchObject({ reason: "state-mismatch" });
    expect(relay.fetch).not.toHaveBeenCalled();
  });

  it("makes one refresh request for concurrent callers", async () => {
    const relay = fakeRelay();
    const store = memoryStore(relay.seedRefresh());
    const session = createAccountSession({
      relayUrl: RELAY,
      clientId: AccountClientId.desktop,
      store,
      fetch: relay.fetch,
    });

    const tokens = await Promise.all([
      session.getAccessToken(),
      session.getAccessToken(),
      session.refresh(),
      session.getAccessToken(),
    ]);

    expect(relay.tokenRequests()).toHaveLength(1);
    expect(new Set(tokens).size).toBe(1);
    expect(tokens[0]).toMatch(/^access-/u);
    expect(relay.revokedFamilies.size).toBe(0);
    expect(await session.getAccessToken()).toBe(tokens[0]);
    expect(relay.tokenRequests()).toHaveLength(1);
  });

  it("serialises refreshes across tabs with Web Locks so the cookie is never reused", async () => {
    const relay = fakeRelay();
    relay.jar.refresh = relay.seedRefresh();
    vi.stubGlobal("navigator", { locks: sharedWebLocks() });
    const signedOut = vi.fn();
    const tab = () =>
      createAccountSession({
        relayUrl: RELAY,
        clientId: AccountClientId.web,
        fetch: relay.fetch,
        onSignedOut: signedOut,
      });

    const [a, b] = await Promise.all([tab().getAccessToken(), tab().getAccessToken()]);

    expect(a).toMatch(/^access-/u);
    expect(b).toMatch(/^access-/u);
    expect(relay.revokedFamilies.size).toBe(0);
    expect(signedOut).not.toHaveBeenCalled();
    for (const request of relay.tokenRequests()) {
      expect(request).toEqual({
        path: "/oauth/token",
        grant_type: "refresh_token",
        client_id: "vekrel-web",
      });
    }
  });

  it("without Web Locks two tabs present one cookie twice and the relay revokes the family", async () => {
    // Why the lock matters: this is the race the previous test is protected from.
    const relay = fakeRelay();
    relay.jar.refresh = relay.seedRefresh();
    vi.stubGlobal("navigator", {});
    const tab = () =>
      createAccountSession({ relayUrl: RELAY, clientId: AccountClientId.web, fetch: relay.fetch });

    const results = await Promise.all([tab().getAccessToken(), tab().getAccessToken()]);

    expect(results).toContain(null);
    expect(relay.revokedFamilies.size).toBe(1);
  });

  it("signs out when the relay refuses the refresh token", async () => {
    const relay = fakeRelay();
    const store = memoryStore("refresh-unknown");
    const onSignedOut = vi.fn();
    const session = createAccountSession({
      relayUrl: RELAY,
      clientId: AccountClientId.mobile,
      store,
      fetch: relay.fetch,
      onSignedOut,
    });

    expect(await session.getAccessToken()).toBeNull();
    expect(store.value).toBeNull();
    expect(onSignedOut).toHaveBeenCalledTimes(1);
    expect(await session.getAccessToken()).toBeNull();
    expect(relay.tokenRequests()).toHaveLength(1);
  });

  it("keeps the session when the relay cannot be reached", async () => {
    const store = memoryStore("refresh-1");
    const onSignedOut = vi.fn();
    const session = createAccountSession({
      relayUrl: RELAY,
      clientId: AccountClientId.mobile,
      store,
      fetch: async () => {
        throw new TypeError("network down");
      },
      onSignedOut,
    });

    await expect(session.getAccessToken()).rejects.toMatchObject({
      reason: "token-request-failed",
    });
    expect(store.value).toBe("refresh-1");
    expect(onSignedOut).not.toHaveBeenCalled();
  });

  it("signs out by revoking the family: body token for apps, cookie for web", async () => {
    const relay = fakeRelay();
    const store = memoryStore(relay.seedRefresh());
    const app = createAccountSession({
      relayUrl: RELAY,
      clientId: AccountClientId.desktop,
      store,
      fetch: relay.fetch,
    });
    await app.signOut();
    expect(relay.requests.at(-1)).toEqual({
      path: "/oauth/revoke",
      token: "refresh-seed",
      client_id: "vekrel-desktop",
    });
    expect(store.value).toBeNull();
    expect(await app.getAccessToken()).toBeNull();

    relay.jar.refresh = "refresh-seed";
    const web = createAccountSession({
      relayUrl: RELAY,
      clientId: AccountClientId.web,
      fetch: relay.fetch,
    });
    await web.signOut();
    expect(relay.requests.at(-1)).toEqual({ path: "/oauth/revoke", client_id: "vekrel-web" });
    expect(relay.jar.refresh).toBeNull();
  });

  it("a refresh rejected after a new sign-in does not clear the new session", async () => {
    const relay = fakeRelay();
    const store = memoryStore("refresh-unknown");
    const onSignedOut = vi.fn();
    const session = createAccountSession({
      relayUrl: RELAY,
      clientId: AccountClientId.mobile,
      store,
      fetch: relay.fetch,
      onSignedOut,
    });
    const release = relay.holdRefreshes();
    const staleRefresh = session.getAccessToken();
    await Promise.resolve();

    const { pending } = beginSignIn({
      relayUrl: RELAY,
      clientId: AccountClientId.mobile,
      redirectUri: APP_REDIRECT,
    });
    relay.addCode("code-1", await challengeOf(pending.verifier), APP_REDIRECT);
    await session.completeSignIn(`${APP_REDIRECT}?code=code-1&state=${pending.state}`, pending);
    const signedInRefresh = store.value;
    release();
    await staleRefresh;

    expect(store.value).toBe(signedInRefresh);
    expect(store.value).toMatch(/^refresh-\d+$/u);
    expect(onSignedOut).not.toHaveBeenCalled();
    expect(await session.getAccessToken()).toMatch(/^access-/u);
  });

  it("a refresh queued on the lock before a web sign-in does not sign the new session out", async () => {
    const relay = fakeRelay();
    const lock = heldWebLocks();
    vi.stubGlobal("navigator", { locks: lock.locks });
    let openExchange!: () => void;
    const exchangeGate = new Promise<void>((resolve) => {
      openExchange = resolve;
    });
    const onSignedOut = vi.fn();
    const session = createAccountSession({
      relayUrl: RELAY,
      clientId: AccountClientId.web,
      fetch: async (input, init) => {
        if (String(init.body).includes("authorization_code")) await exchangeGate;
        return relay.fetch(input, init);
      },
      onSignedOut,
    });
    const redirectUri = "https://app.example.test/auth/callback";
    const { pending } = beginSignIn({
      relayUrl: RELAY,
      clientId: AccountClientId.web,
      redirectUri,
    });
    relay.addCode("code-1", await challengeOf(pending.verifier), redirectUri);

    // The refresh waits on the lock; the sign-in starts meanwhile, and the refresh then
    // reaches the relay before the sign-in's cookie exists.
    const staleRefresh = session.refresh();
    const signingIn = session.completeSignIn(
      `${redirectUri}?code=code-1&state=${pending.state}`,
      pending,
    );
    lock.grant();
    await staleRefresh;
    openExchange();
    await signingIn;

    expect(onSignedOut).not.toHaveBeenCalled();
    expect(await session.getAccessToken()).toMatch(/^access-/u);
  });

  it("a refresh queued on the lock before a sign-in does not rotate the new session's token", async () => {
    const relay = fakeRelay();
    const lock = heldWebLocks();
    vi.stubGlobal("navigator", { locks: lock.locks });
    const store = memoryStore(null);
    const onSignedOut = vi.fn();
    const session = createAccountSession({
      relayUrl: RELAY,
      clientId: AccountClientId.desktop,
      store,
      fetch: relay.fetch,
      onSignedOut,
    });
    const { pending } = beginSignIn({
      relayUrl: RELAY,
      clientId: AccountClientId.desktop,
      redirectUri: APP_REDIRECT,
    });
    relay.addCode("code-1", await challengeOf(pending.verifier), APP_REDIRECT);

    const staleRefresh = session.refresh();
    await session.completeSignIn(`${APP_REDIRECT}?code=code-1&state=${pending.state}`, pending);
    const signedIn = store.value;
    lock.grant();
    await staleRefresh;

    expect(relay.tokenRequests()).toHaveLength(1);
    expect(relay.revokedFamilies.size).toBe(0);
    expect(store.value).toBe(signedIn);
    expect(onSignedOut).not.toHaveBeenCalled();
    expect(await session.getAccessToken()).toMatch(/^access-/u);
  });

  it("stays signed out without asking the relay again, and reports only a real sign-out", async () => {
    const relay = fakeRelay();
    const onSignedOut = vi.fn();
    const session = createAccountSession({
      relayUrl: RELAY,
      clientId: AccountClientId.web,
      fetch: relay.fetch,
      onSignedOut,
    });

    expect(await session.getAccessToken()).toBeNull();
    for (let i = 0; i < 5; i += 1) expect(await session.getAccessToken()).toBeNull();
    expect(await Promise.all([session.refresh(), session.getAccessToken()])).toEqual([null, null]);
    expect(relay.tokenRequests()).toHaveLength(1);
    expect(onSignedOut).not.toHaveBeenCalled();

    const { pending } = beginSignIn({
      relayUrl: RELAY,
      clientId: AccountClientId.web,
      redirectUri: "https://app.example.test/auth/callback",
    });
    relay.addCode("code-1", await challengeOf(pending.verifier), pending.redirectUri);
    await session.completeSignIn(
      `${pending.redirectUri}?code=code-1&state=${pending.state}`,
      pending,
    );
    relay.revokeAll();
    expect(await session.refresh()).toBeNull();
    expect(onSignedOut).toHaveBeenCalledTimes(1);
    const requestsAfterSignOut = relay.tokenRequests().length;
    for (let i = 0; i < 3; i += 1) expect(await session.getAccessToken()).toBeNull();
    expect(relay.tokenRequests()).toHaveLength(requestsAfterSignOut);
    expect(onSignedOut).toHaveBeenCalledTimes(1);
  });

  it("aborts a hung token request after 15 s and releases the refresh", async () => {
    vi.useFakeTimers();
    try {
      const store = memoryStore("refresh-1");
      const hung = vi.fn(
        (_input: string, init: RequestInit) =>
          new Promise<Response>((_resolve, reject) => {
            init.signal?.addEventListener("abort", () => reject(init.signal?.reason));
          }),
      );
      const session = createAccountSession({
        relayUrl: RELAY,
        clientId: AccountClientId.mobile,
        store,
        fetch: hung,
      });

      const first = session.getAccessToken();
      const failed = expect(first).rejects.toMatchObject({ reason: "token-request-failed" });
      await vi.advanceTimersByTimeAsync(14_999);
      expect(hung.mock.calls[0]?.[1].signal?.aborted).toBe(false);
      await vi.advanceTimersByTimeAsync(1);
      await failed;
      expect(store.value).toBe("refresh-1");

      const second = session.getAccessToken();
      const failedAgain = expect(second).rejects.toMatchObject({ reason: "token-request-failed" });
      await vi.advanceTimersByTimeAsync(15_000);
      await failedAgain;
      expect(hung).toHaveBeenCalledTimes(2);
    } finally {
      vi.useRealTimers();
    }
  });

  it("revokes before clearing the store, and still revokes when clearing throws", async () => {
    const relay = fakeRelay();
    const order: string[] = [];
    const store = memoryStore(relay.seedRefresh());
    const session = createAccountSession({
      relayUrl: RELAY,
      clientId: AccountClientId.desktop,
      store: {
        ...store,
        read: store.read,
        clear: async () => {
          order.push("clear");
          throw new Error("keychain locked");
        },
      },
      fetch: async (input, init) => {
        order.push("revoke");
        return relay.fetch(input, init);
      },
    });

    await expect(session.signOut()).rejects.toThrow("keychain locked");
    expect(order).toEqual(["revoke", "clear"]);
    expect(relay.revokedFamilies.size).toBe(1);
    expect(await session.getAccessToken()).toBeNull();
  });

  it("keeps the session on 401, 429 and 5xx from the token endpoint", async () => {
    for (const status of [401, 429, 503]) {
      const store = memoryStore("refresh-1");
      const onSignedOut = vi.fn();
      const fetch = vi.fn(async () => new Response("{}", { status }));
      const session = createAccountSession({
        relayUrl: RELAY,
        clientId: AccountClientId.mobile,
        store,
        fetch,
        onSignedOut,
      });

      await expect(session.getAccessToken()).rejects.toMatchObject({
        reason: "token-request-failed",
        status,
      });
      await expect(session.getAccessToken()).rejects.toMatchObject({ status });
      expect(fetch).toHaveBeenCalledTimes(2);
      expect(store.value).toBe("refresh-1");
      expect(onSignedOut).not.toHaveBeenCalled();
    }
  });

  it("signOut during a slow refresh write leaves the session signed out and revokes the new token", async () => {
    const relay = fakeRelay();
    const { store, release, writeStarted } = slowWriteStore(relay.seedRefresh());
    const session = createAccountSession({
      relayUrl: RELAY,
      clientId: AccountClientId.desktop,
      store,
      fetch: relay.fetch,
    });

    const staleRefresh = session.getAccessToken();
    await writeStarted;
    const signingOut = session.signOut();
    release();
    await Promise.all([staleRefresh, signingOut]);

    expect(store.value).toBeNull();
    expect(await session.getAccessToken()).toBeNull();
    expect(relay.tokenRequests()).toHaveLength(1);
    expect(relay.requests).toContainEqual({
      path: "/oauth/revoke",
      token: "refresh-1",
      client_id: "vekrel-desktop",
    });
  });

  it("completeSignIn during a slow refresh write keeps the sign-in's token in the store", async () => {
    const relay = fakeRelay();
    const { store, release, writeStarted } = slowWriteStore(relay.seedRefresh());
    let exchanged!: () => void;
    const codeExchanged = new Promise<void>((resolve) => {
      exchanged = resolve;
    });
    const session = createAccountSession({
      relayUrl: RELAY,
      clientId: AccountClientId.mobile,
      store,
      fetch: async (input, init) => {
        const response = await relay.fetch(input, init);
        if (String(init.body).includes("authorization_code")) exchanged();
        return response;
      },
    });

    const staleRefresh = session.getAccessToken();
    await writeStarted;
    const { pending } = beginSignIn({
      relayUrl: RELAY,
      clientId: AccountClientId.mobile,
      redirectUri: APP_REDIRECT,
    });
    relay.addCode("code-1", await challengeOf(pending.verifier), APP_REDIRECT);
    const signingIn = session.completeSignIn(
      `${APP_REDIRECT}?code=code-1&state=${pending.state}`,
      pending,
    );
    // From the code exchange on, the sign-in runs in microtasks only: let it go as far as it can.
    await codeExchanged;
    for (let tick = 0; tick < 100; tick += 1) await Promise.resolve();
    release();
    await Promise.all([staleRefresh, signingIn]);

    expect(store.value).toBe("refresh-2");
    expect(await session.getAccessToken()).toBe("access-2");
    expect(relay.requests).toContainEqual({
      path: "/oauth/revoke",
      token: "refresh-1",
      client_id: "vekrel-mobile",
    });
  });
});

describe("relay URL", () => {
  it("accepts https and loopback http, keeps a path prefix, refuses the rest", async () => {
    const relay = fakeRelay();
    const session = createAccountSession({
      relayUrl: "https://relay.example.test/relay/",
      clientId: AccountClientId.desktop,
      store: memoryStore(relay.seedRefresh()),
      fetch: relay.fetch,
    });
    await session.getAccessToken();
    expect(relay.fetch.mock.calls[0]?.[0]).toBe("https://relay.example.test/relay/oauth/token");
    expect(
      beginSignIn({
        relayUrl: "https://relay.example.test/relay",
        clientId: AccountClientId.desktop,
        redirectUri: APP_REDIRECT,
      }).url,
    ).toMatch(/^https:\/\/relay\.example\.test\/relay\/oauth\/authorize\?/u);

    for (const ok of ["http://localhost:8787", "http://127.0.0.1:8787", "http://[::1]:8787"]) {
      expect(() =>
        createAccountSession({ relayUrl: ok, clientId: AccountClientId.web }),
      ).not.toThrow();
    }
    for (const bad of [
      "http://relay.example.test",
      "https://user:pw@relay.example.test",
      "https://relay.example.test/?x=1",
      "https://relay.example.test/#x",
      "ftp://relay.example.test",
      "not a url",
    ]) {
      expect(() => createAccountSession({ relayUrl: bad, clientId: AccountClientId.web })).toThrow(
        AccountAuthError,
      );
      expect(() =>
        beginSignIn({ relayUrl: bad, clientId: AccountClientId.web, redirectUri: APP_REDIRECT }),
      ).toThrow(AccountAuthError);
    }
  });
});
