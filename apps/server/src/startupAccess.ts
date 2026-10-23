import * as NodeOS from "node:os";

import { QrCode } from "@vekrel/shared/qrCode";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { HttpServer } from "effect/http";
import * as NetAddress from "effect/net/NetAddress";

import * as ServerConfig from "./config.ts";
import * as MachineAuth from "./auth/MachineAuth.ts";

export interface HeadlessServeAccessInfo {
  readonly connectionString: string;
  readonly token: string;
  readonly pairingUrl: string;
}

type NetworkInterfacesMap = ReturnType<typeof NodeOS.networkInterfaces>;

export const isLoopbackHost = (host: string | undefined): boolean => {
  if (!host || host.length === 0) {
    return true;
  }

  return (
    host === "localhost" ||
    host === "127.0.0.1" ||
    host === "::1" ||
    host === "[::1]" ||
    host.startsWith("127.")
  );
};

export const isWildcardHost = (host: string | undefined): boolean =>
  host === "0.0.0.0" || host === "::" || host === "[::]";

export const formatHostForUrl = (host: string): string =>
  host.includes(":") && !host.startsWith("[") ? `[${host}]` : host;

export const loopbackUrlHostForAddress = (address: NetAddress.InetAddress): string =>
  address._tag === "InetAddressV6" && NetAddress.isLoopback(address.address)
    ? "[::1]"
    : "127.0.0.1";

const normalizeHost = (host: string): string =>
  host.startsWith("[") && host.endsWith("]") ? host.slice(1, -1) : host;

const isIpv4Family = (family: string | number): boolean => family === "IPv4" || family === 4;

const isIpv6Family = (family: string | number): boolean => family === "IPv6" || family === 6;

const resolveHeadlessConnectionHost = (
  host: string | undefined,
  interfaces: NetworkInterfacesMap = NodeOS.networkInterfaces(),
): string => {
  if (!host) {
    return "localhost";
  }

  if (!isWildcardHost(host)) {
    return normalizeHost(host);
  }

  const interfaceEntries = Object.values(interfaces).flatMap((entries) => entries ?? []);
  const externalIpv4 = interfaceEntries.find(
    (entry) => !entry.internal && isIpv4Family(entry.family),
  );
  if (externalIpv4) {
    return externalIpv4.address;
  }

  const externalIpv6 = interfaceEntries.find(
    (entry) => !entry.internal && isIpv6Family(entry.family),
  );
  return externalIpv6 ? normalizeHost(externalIpv6.address) : "localhost";
};

export const resolveHeadlessConnectionString = (
  host: string | undefined,
  port: number,
  interfaces: NetworkInterfacesMap = NodeOS.networkInterfaces(),
): string => {
  const connectionHost = resolveHeadlessConnectionHost(host, interfaces);
  return `http://${formatHostForUrl(connectionHost)}:${port}`;
};

const resolveListeningPort = (address: unknown, fallbackPort: number): number => {
  if (
    typeof address === "object" &&
    address !== null &&
    "port" in address &&
    typeof address.port === "number"
  ) {
    return address.port;
  }
  return fallbackPort;
};

export const buildPairingUrl = (connectionString: string, token: string): string => {
  const url = new URL(connectionString);
  url.pathname = "/pair";
  url.searchParams.delete("token");
  url.hash = new URLSearchParams([["token", token]]).toString();
  return url.toString();
};

export const renderTerminalQrCode = (value: string, margin = 2): string => {
  const qrCode = QrCode.encodeText(value, QrCode.Ecc.MEDIUM);
  const rows: Array<string> = [];
  const isDark = (x: number, y: number): boolean =>
    x >= 0 && x < qrCode.size && y >= 0 && y < qrCode.size && qrCode.getModule(x, y);

  for (let y = -margin; y < qrCode.size + margin; y += 2) {
    let row = "";

    for (let x = -margin; x < qrCode.size + margin; x += 1) {
      const topDark = isDark(x, y);
      const bottomDark = isDark(x, y + 1);

      row += topDark ? (bottomDark ? "█" : "▀") : bottomDark ? "▄" : " ";
    }

    rows.push(row);
  }

  return rows.join("\n");
};

// Vekrel: the daemon fronts the engine on another port and sets this so printed URLs use its origin.
export const ADVERTISED_ORIGIN_ENV = "VEKREL_ADVERTISED_ORIGIN";

export const parseAdvertisedOrigin = (raw: string): string | undefined => {
  if (!URL.canParse(raw)) return undefined;
  const { origin, protocol } = new URL(raw);
  const isOrigin = raw === origin || raw === `${origin}/`;
  return isOrigin && (protocol === "http:" || protocol === "https:") ? origin : undefined;
};

const rawAdvertisedOrigin = Config.String(ADVERTISED_ORIGIN_ENV).pipe(
  Config.option,
  Config.map(Option.getOrUndefined),
);

// Silent, for the runtime state; resolveAdvertisedOrigin warns once at startup.
export const advertisedOriginConfig = rawAdvertisedOrigin.pipe(
  Config.map((raw) => (raw === undefined ? undefined : parseAdvertisedOrigin(raw))),
);

export const resolveAdvertisedOrigin = Effect.gen(function* () {
  const raw = yield* rawAdvertisedOrigin;
  const origin = raw === undefined ? undefined : parseAdvertisedOrigin(raw);
  if (raw !== undefined && origin === undefined) {
    yield* Effect.logWarning(`Ignoring ${ADVERTISED_ORIGIN_ENV}: not an http(s) origin.`);
  }
  return origin;
}).pipe(Effect.orElseSucceed(() => undefined));

export const formatHeadlessServeOutput = (accessInfo: HeadlessServeAccessInfo): string =>
  [
    "Vekrel server is ready.",
    `Connection string: ${accessInfo.connectionString}`,
    `Token: ${accessInfo.token}`,
    `Pairing URL: ${accessInfo.pairingUrl}`,
    "",
    renderTerminalQrCode(accessInfo.pairingUrl),
    "",
  ].join("\n");

export const issueHeadlessServeAccessInfo = Effect.fn("issueHeadlessServeAccessInfo")(function* (
  advertisedOrigin?: string,
) {
  const serverConfig = yield* ServerConfig.ServerConfig;
  const httpServer = yield* HttpServer.HttpServer;
  const serverAuth = yield* MachineAuth.MachineAuth;
  const connectionString =
    advertisedOrigin ??
    resolveHeadlessConnectionString(
      serverConfig.host,
      resolveListeningPort(httpServer.address, serverConfig.port),
    );
  const issued = yield* serverAuth.issueStartupPairingCredential();

  return {
    connectionString,
    token: issued.credential,
    pairingUrl: buildPairingUrl(connectionString, issued.credential),
  } satisfies HeadlessServeAccessInfo;
});
