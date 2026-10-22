import type { DpopFailureReason } from "@vekrel/contracts";
import type { RelayMachineStatusResponse, RelayProtectedError } from "@vekrel/contracts/relay";

export const DPOP_CLOCK_HINT =
  "Hint: Check that automatic date and time is enabled on both devices, then try again.";

/** Older servers omit the DPoP category, but newer servers can also omit it for
 * a credential failure that happens after proof verification. */
export const DPOP_UNKNOWN_HINT =
  "Hint: Try again. If it still fails, clock skew may be the cause; check that automatic date and time is enabled on both devices.";

export const DPOP_RETRY_HINT = "Hint: Try again. If the problem continues, copy the trace ID.";

function dpopFailureHint(reason: DpopFailureReason | undefined): string {
  if (reason === "time_window") return DPOP_CLOCK_HINT;
  if (reason === undefined) return DPOP_UNKNOWN_HINT;
  return DPOP_RETRY_HINT;
}

export function dpopFailureMessage(message: string, reason: DpopFailureReason | undefined): string {
  return `${message} ${dpopFailureHint(reason)}`;
}

export function relayProtectedErrorMessage(error: RelayProtectedError): string {
  switch (error._tag) {
    case "RelayAuthInvalidError":
      switch (error.reason) {
        case "missing_bearer":
        case "invalid_bearer":
          return "Relay rejected the cloud session token.";
        case "invalid_dpop":
          return dpopFailureMessage("Relay rejected the DPoP proof.", error.dpopFailureReason);
        case "not_authorized":
          return "Relay rejected the authenticated request.";
      }
    case "RelayMachineLinkProofExpiredError":
      return "Relay rejected an expired machine link proof.";
    case "RelayMachineLinkProofInvalidError":
      return `Relay rejected the machine link proof (${error.reason}).`;
    case "RelayMachineConnectNotAuthorizedError":
      // "Not authorized" covers non-auth causes too; surface the reason so a
      // missing link does not read as a credential problem.
      if (error.reason === "machine_link_not_found") {
        return "Relay has no active link for this machine. If it was deregistered from T3 Connect, link it again on that machine.";
      }
      if (error.reason === "managed_endpoint_allocation_not_ready") {
        return "The machine's T3 Connect tunnel is still being set up.";
      }
      if (error.reason === "endpoint_provider_not_managed") {
        return "This machine is linked only to publish agent activity, so it cannot accept connections. On that computer, open Settings > Connections and turn T3 Connect on.";
      }
      return error.reason
        ? `Relay rejected the machine connection request (${error.reason}).`
        : "Relay rejected the machine connection request.";
    case "RelayMachineEndpointUnavailableError":
      return `Relay could not reach the machine endpoint (${error.reason}).`;
    case "RelayMachineEndpointTimedOutError":
      return "Relay timed out while contacting the machine endpoint.";
    case "RelayMachineLinkFailedError":
      return `Relay could not link the machine (${error.reason}).`;
    case "RelayMachineLinkUnavailableError":
      return `Relay cannot provision the managed endpoint (${error.reason}).${
        error.provisioningStage ? ` Failed stage: ${error.provisioningStage}.` : ""
      }`;
    case "RelayMachineLinkLimitExceededError":
      return `Relay refused the link: this account already has its maximum of ${error.maxTunnels} managed tunnels. Unlink a machine to free one up.`;
    case "RelayAgentActivityPublishProofExpiredError":
      return "Relay rejected an expired agent activity publish proof.";
    case "RelayAgentActivityPublishProofInvalidError":
      return `Relay rejected the agent activity publish proof (${error.reason}).`;
    case "RelayInternalError":
      return `Relay encountered an internal error (${error.reason}).`;
  }
}

// The relay cuts off a request it cannot finish in time with a 504 that no
// endpoint declares, so the client sees only the status.
export const RELAY_GATEWAY_TIMEOUT_MESSAGE =
  "Relay timed out before it finished the request (HTTP 504). Try again in a few minutes. If it keeps failing, copy the trace ID when you report it.";

export function relayRequestFailureDetail(error: {
  readonly relayError?: RelayProtectedError | undefined;
  readonly status?: number | undefined;
}): string | null {
  if (error.relayError) return relayProtectedErrorMessage(error.relayError);
  if (error.status === 504) return RELAY_GATEWAY_TIMEOUT_MESSAGE;
  if (error.status !== undefined) return `Relay returned an unexpected HTTP ${error.status}.`;
  return null;
}

const RELAY_TUNNEL_RELEASED_MESSAGE =
  "Offline for a while, so its T3 Connect tunnel was removed. Start T3 Code on that computer and update it to the latest version to reconnect.";

export function relayOfflineReasonMessage(
  status: Pick<RelayMachineStatusResponse, "offlineReason">,
): string | null {
  switch (status.offlineReason) {
    case "tunnel_released":
      return RELAY_TUNNEL_RELEASED_MESSAGE;
    case undefined:
      return null;
  }
}
