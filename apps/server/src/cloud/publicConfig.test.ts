import { assert, it } from "@effect/vitest";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";

import {
  makeCloudCliOAuthConfig,
  makeRelayUrlConfig,
  resolveRelayClientTracingConfig,
} from "./publicConfig.ts";

const provideEnv = (env: Readonly<Record<string, string>>) =>
  Effect.provide(ConfigProvider.layer(ConfigProvider.fromEnv({ env })));

it.effect("rejects an insecure runtime relay URL override", () =>
  makeRelayUrlConfig("https://embedded.example.test").pipe(
    provideEnv({ VEKREL_RELAY_URL: "http://runtime.example.test" }),
    Effect.flip,
  ),
);

it.effect("CLI sign-in accepts an https or http loopback relay origin and refuses the rest", () =>
  Effect.gen(function* () {
    for (const value of [
      "relay.example.test",
      "http://relay.example.test",
      "https://relay.example.test/nested",
      "https://relay.example.test?alias=true",
      "https://user:pass@relay.example.test",
    ]) {
      const result = yield* makeCloudCliOAuthConfig({ relayUrlFallback: "" }).pipe(
        provideEnv({ VEKREL_RELAY_URL: value }),
        Effect.result,
      );
      assert.isTrue(Result.isFailure(result), value);
    }
    const local = yield* makeCloudCliOAuthConfig({ relayUrlFallback: "" }).pipe(
      provideEnv({ VEKREL_RELAY_URL: "http://127.0.0.1:4000" }),
    );
    assert.equal(local.tokenEndpoint, "http://127.0.0.1:4000/oauth/token");
  }),
);

it.effect("derives the relay's OAuth endpoints and the vekrel-cli client", () =>
  Effect.gen(function* () {
    const config = yield* makeCloudCliOAuthConfig({
      relayUrlFallback: "https://relay.example.test",
    }).pipe(provideEnv({}));

    assert.deepEqual(config, {
      authorizationBaseUrl: "https://relay.example.test",
      tokenEndpoint: "https://relay.example.test/oauth/token",
      deviceAuthorizationEndpoint: "https://relay.example.test/oauth/device_authorization",
      revocationEndpoint: "https://relay.example.test/oauth/revoke",
      clientId: "vekrel-cli",
      loopbackPort: 34338,
      redirectUri: "http://127.0.0.1:34338/callback",
      scopes: ["openid", "profile", "email", "offline_access"],
    });
  }),
);

it("product opt-out disables relay tracing with either built-in or runtime credentials", () => {
  const fallback = {
    tracesUrl: "https://embedded.example.test/v1/traces",
    tracesDataset: "embedded-dataset",
    tracesToken: "embedded-token",
  };
  const env = {
    VEKREL_TELEMETRY_ENABLED: "false",
    VEKREL_OTEL_SDK_DISABLED: "false",
  };

  assert.equal(resolveRelayClientTracingConfig(env, fallback), null);
  assert.equal(
    resolveRelayClientTracingConfig(
      {
        ...env,
        VEKREL_RELAY_CLIENT_OTLP_TRACES_URL: "https://runtime.example.test/v1/traces",
        VEKREL_RELAY_CLIENT_OTLP_TRACES_DATASET: "runtime-dataset",
        VEKREL_RELAY_CLIENT_OTLP_TRACES_TOKEN: "runtime-token",
      },
      fallback,
    ),
    null,
  );
});
