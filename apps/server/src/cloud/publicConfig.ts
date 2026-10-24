import { connectLoopbackRedirectUri, CONNECT_OAUTH_SCOPES } from "@vekrel/shared/connectAuth";
import { isRelayTracingEnabled } from "@vekrel/shared/otelEnvironment";
import { normalizeSecureRelayUrl } from "@vekrel/shared/relayUrl";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SchemaIssue from "effect/SchemaIssue";

declare const __VEKREL_BUILD_RELAY_URL__: string | undefined;
declare const __VEKREL_BUILD_RELAY_CLIENT_OTLP_TRACES_URL__: string | undefined;
declare const __VEKREL_BUILD_RELAY_CLIENT_OTLP_TRACES_DATASET__: string | undefined;
declare const __VEKREL_BUILD_RELAY_CLIENT_OTLP_TRACES_TOKEN__: string | undefined;

const CLOUD_CLI_OAUTH_LOOPBACK_PORT = 34338;
// Must match the relay's public client list (`src/accounts/clients.rs`).
const CLOUD_CLI_OAUTH_CLIENT_ID = "vekrel-cli";
const CLOUD_CLI_OAUTH_SCOPES = CONNECT_OAUTH_SCOPES;

function validateRelayUrl(value: string) {
  const relayUrl = normalizeSecureRelayUrl(value);
  return relayUrl === null
    ? Effect.fail(
        new Config.ConfigError(
          new Schema.SchemaError(
            new SchemaIssue.InvalidValue({
              message: "Relay URL must be a secure absolute HTTPS origin.",
            }),
          ),
        ),
      )
    : Effect.succeed(relayUrl);
}

function readBuildTimeValue(value: string | undefined): string {
  return typeof value === "undefined" ? "" : value.trim();
}

function normalizeSecureUrl(value: string): string | null {
  try {
    const url = new URL(value);
    return url.protocol === "https:" ? url.toString() : null;
  } catch {
    return null;
  }
}

const buildTimeRelayUrl =
  typeof __VEKREL_BUILD_RELAY_URL__ === "undefined"
    ? ""
    : (normalizeSecureRelayUrl(__VEKREL_BUILD_RELAY_URL__) ?? "");
const buildTimeRelayClientTracing = {
  tracesUrl: readBuildTimeValue(
    typeof __VEKREL_BUILD_RELAY_CLIENT_OTLP_TRACES_URL__ === "undefined"
      ? undefined
      : __VEKREL_BUILD_RELAY_CLIENT_OTLP_TRACES_URL__,
  ),
  tracesDataset: readBuildTimeValue(
    typeof __VEKREL_BUILD_RELAY_CLIENT_OTLP_TRACES_DATASET__ === "undefined"
      ? undefined
      : __VEKREL_BUILD_RELAY_CLIENT_OTLP_TRACES_DATASET__,
  ),
  tracesToken: readBuildTimeValue(
    typeof __VEKREL_BUILD_RELAY_CLIENT_OTLP_TRACES_TOKEN__ === "undefined"
      ? undefined
      : __VEKREL_BUILD_RELAY_CLIENT_OTLP_TRACES_TOKEN__,
  ),
} as const;

export function resolveRelayClientTracingConfig(
  env: Readonly<Record<string, string | undefined>> = process.env,
  fallback = buildTimeRelayClientTracing,
) {
  if (!isRelayTracingEnabled(env)) {
    return null;
  }

  const tracesUrl = env.VEKREL_RELAY_CLIENT_OTLP_TRACES_URL?.trim() || fallback.tracesUrl;
  const tracesDataset =
    env.VEKREL_RELAY_CLIENT_OTLP_TRACES_DATASET?.trim() || fallback.tracesDataset;
  const tracesToken = env.VEKREL_RELAY_CLIENT_OTLP_TRACES_TOKEN?.trim() || fallback.tracesToken;
  const normalizedTracesUrl = normalizeSecureUrl(tracesUrl);
  return normalizedTracesUrl && tracesDataset && tracesToken
    ? { tracesUrl: normalizedTracesUrl, tracesDataset, tracesToken }
    : null;
}

export function makeRelayUrlConfig(fallback = buildTimeRelayUrl) {
  const runtimeConfig = Config.NonEmptyString("VEKREL_RELAY_URL");
  return (fallback ? runtimeConfig.pipe(Config.withDefault(fallback)) : runtimeConfig).pipe(
    Config.mapEffect(validateRelayUrl),
  );
}

export const relayUrlConfig = makeRelayUrlConfig();

/**
 * Account sign-in talks to the relay's own OAuth endpoints. Unlike machine
 * calls (`relayUrlConfig`, https only), an http loopback relay is accepted so
 * the CLI can sign in against a local relay.
 */
function normalizeAccountRelayOrigin(value: string): string | null {
  try {
    const url = new URL(value.trim());
    const isLoopbackHttp =
      url.protocol === "http:" &&
      (url.hostname === "localhost" || url.hostname === "127.0.0.1" || url.hostname === "[::1]");
    if (
      (url.protocol !== "https:" && !isLoopbackHttp) ||
      url.username !== "" ||
      url.password !== "" ||
      url.pathname !== "/" ||
      url.search !== "" ||
      url.hash !== ""
    ) {
      return null;
    }
    return url.origin;
  } catch {
    return null;
  }
}

function validateAccountRelayUrl(value: string) {
  const origin = normalizeAccountRelayOrigin(value);
  return origin === null
    ? Effect.fail(
        new Config.ConfigError(
          new Schema.SchemaError(
            new SchemaIssue.InvalidValue({
              message: "Relay URL must be an absolute HTTPS origin (or HTTP loopback origin).",
            }),
          ),
        ),
      )
    : Effect.succeed(origin);
}

export interface CloudCliOAuthConfig {
  readonly authorizationBaseUrl: string;
  readonly tokenEndpoint: string;
  readonly deviceAuthorizationEndpoint: string;
  readonly revocationEndpoint: string;
  readonly clientId: string;
  readonly loopbackPort: number;
  readonly redirectUri: string;
  readonly scopes: typeof CLOUD_CLI_OAUTH_SCOPES;
}

export function makeCloudCliOAuthConfig({ relayUrlFallback = buildTimeRelayUrl } = {}) {
  const runtimeConfig = Config.NonEmptyString("VEKREL_RELAY_URL");
  return (
    relayUrlFallback ? runtimeConfig.pipe(Config.withDefault(relayUrlFallback)) : runtimeConfig
  ).pipe(
    Config.mapEffect(validateAccountRelayUrl),
    Config.map(
      (relayUrl) =>
        ({
          authorizationBaseUrl: relayUrl,
          tokenEndpoint: `${relayUrl}/oauth/token`,
          deviceAuthorizationEndpoint: `${relayUrl}/oauth/device_authorization`,
          revocationEndpoint: `${relayUrl}/oauth/revoke`,
          clientId: CLOUD_CLI_OAUTH_CLIENT_ID,
          loopbackPort: CLOUD_CLI_OAUTH_LOOPBACK_PORT,
          redirectUri: connectLoopbackRedirectUri(CLOUD_CLI_OAUTH_LOOPBACK_PORT),
          scopes: CLOUD_CLI_OAUTH_SCOPES,
        }) satisfies CloudCliOAuthConfig,
    ),
  );
}

export const cloudCliOAuthConfig = makeCloudCliOAuthConfig();

export const hasCloudPublicConfig = Boolean(
  normalizeAccountRelayOrigin(process.env.VEKREL_RELAY_URL ?? "") ?? buildTimeRelayUrl,
);
