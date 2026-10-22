import type { ScheduledTaskWebhookEndpoint } from "@vekrel/contracts";
import { isLocalLoopbackHost } from "@vekrel/shared/hostClassification";

export interface WebhookAddress {
  readonly address: string;
  readonly copyable: boolean;
  readonly note: string | null;
}

export function webhookAddress(
  endpoint: ScheduledTaskWebhookEndpoint,
  httpBaseUrl: string | null,
): WebhookAddress {
  if (endpoint.url !== null) {
    return { address: endpoint.url, copyable: true, note: null };
  }
  if (httpBaseUrl === null) {
    return {
      address: endpoint.path,
      copyable: false,
      note: "Link this machine to Vekrel Connect for a public URL.",
    };
  }
  const url = new URL(endpoint.path, httpBaseUrl);
  return {
    address: url.href,
    copyable: true,
    note: isLocalLoopbackHost(url.hostname)
      ? "Only this computer can call this address. Link Vekrel Connect for a public URL."
      : "Works wherever this machine's address is reachable, for example over Tailscale or your own proxy. Link Vekrel Connect for a public URL.",
  };
}
