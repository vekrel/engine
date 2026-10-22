import type { ClientConnectionMethod, MachineId } from "@vekrel/contracts";
import type { RelayProtectedError } from "@vekrel/contracts/relay";
import type { ManagedRelayClientError } from "../relay/managedRelay.ts";
import { dpopFailureMessage, relayProtectedErrorMessage } from "../relay/errorPresentation.ts";
import type { RemoteMachineAuthError } from "../authorization/remote.ts";
import { NETWORK_BLOCKING_HINT } from "../errors/network.ts";
import {
  ConnectionBlockedError,
  type ConnectionAttemptError,
  ConnectionTransientError,
} from "./model.ts";

export function profileMissingError(connectionId: string): ConnectionBlockedError {
  return new ConnectionBlockedError({
    reason: "configuration",
    detail: `Connection profile ${connectionId} is unavailable.`,
    hop: "device",
  });
}

export function credentialMissingError(connectionId: string): ConnectionBlockedError {
  return new ConnectionBlockedError({
    reason: "authentication",
    detail: `Connection credential ${connectionId} is unavailable.`,
    hop: "device",
  });
}

export function machineMismatchError(input: {
  readonly expected: MachineId;
  readonly actual: MachineId;
}): ConnectionBlockedError {
  return new ConnectionBlockedError({
    reason: "configuration",
    detail: `Connected machine ${input.actual} does not match ${input.expected}.`,
  });
}

function relayProtectedError(error: RelayProtectedError): ConnectionAttemptError {
  const detail = relayProtectedErrorMessage(error);
  const traceId = error.traceId;
  switch (error._tag) {
    case "RelayAuthInvalidError":
      return new ConnectionBlockedError({
        reason: "authentication",
        detail,
        traceId,
        hop: "relay",
        ...(error.dpopFailureReason === "time_window" ? { failureCause: "clock_skew" } : {}),
      });
    case "RelayMachineLinkProofExpiredError":
    case "RelayAgentActivityPublishProofExpiredError":
    case "RelayAgentActivityPublishProofInvalidError":
      return new ConnectionBlockedError({
        reason: "authentication",
        detail,
        traceId,
        hop: "relay",
      });
    case "RelayMachineConnectNotAuthorizedError":
      // A host swapping in a new tunnel leaves its allocation unready until
      // the tunnel is up, and nothing on the client changes when it is.
      if (error.reason === "managed_endpoint_allocation_not_ready") {
        return new ConnectionTransientError({
          reason: "endpoint-unavailable",
          detail,
          traceId,
          hop: "tunnel",
          failureCause: "tunnel_pending",
        });
      }
      if (error.reason === "endpoint_provider_not_managed") {
        return new ConnectionBlockedError({
          reason: "permission",
          detail,
          traceId,
          hop: "tunnel",
          failureCause: "tunnel_not_enabled",
        });
      }
      return new ConnectionBlockedError({
        reason: "permission",
        detail,
        traceId,
        hop: "relay",
        ...(error.reason === "machine_link_not_found" ? { failureCause: "not_linked" } : {}),
      });
    case "RelayMachineLinkProofInvalidError":
    case "RelayMachineLinkLimitExceededError":
      return new ConnectionBlockedError({ reason: "permission", detail, traceId, hop: "relay" });
    case "RelayMachineEndpointTimedOutError":
      return new ConnectionTransientError({
        reason: "timeout",
        detail,
        traceId,
        hop: "tunnel",
        failureCause: "timeout",
      });
    case "RelayMachineEndpointUnavailableError": {
      const endpointFailure =
        error.endpointFailure ??
        (error.reason === "endpoint_response_invalid" ? "invalid_response" : undefined);
      return new ConnectionTransientError({
        reason: "endpoint-unavailable",
        detail,
        traceId,
        // The tunnel carried the request when T3 Code is what failed to answer.
        hop:
          endpointFailure === "origin_unreachable" || endpointFailure === "invalid_response"
            ? "server"
            : "tunnel",
        via: "relay",
        ...(endpointFailure ? { failureCause: endpointFailure } : {}),
        ...(error.endpointStatus ? { httpStatus: error.endpointStatus } : {}),
      });
    }
    case "RelayMachineLinkUnavailableError":
      return new ConnectionTransientError({
        reason: "endpoint-unavailable",
        detail,
        traceId,
        hop: "tunnel",
      });
    case "RelayMachineLinkFailedError":
    case "RelayInternalError":
      return new ConnectionTransientError({
        reason: "relay-unavailable",
        detail,
        traceId,
        hop: "relay",
        failureCause: "relay_error",
      });
  }
}

export function mapManagedRelayError(error: ManagedRelayClientError): ConnectionAttemptError {
  switch (error._tag) {
    case "ManagedRelayRequestFailedError":
      if (error.relayError) {
        return relayProtectedError(error.relayError);
      }
      return new ConnectionTransientError({
        reason: "relay-unavailable",
        detail: error.message,
        hop: "relay",
        // A status means the relay answered, so this device did reach it.
        ...(error.status !== undefined
          ? { httpStatus: error.status, failureCause: "relay_error" }
          : {}),
        ...(error.traceId ? { traceId: error.traceId } : {}),
      });
    case "ManagedRelayRequestTimeoutError":
      return new ConnectionTransientError({
        reason: "timeout",
        detail: error.message,
        hop: "relay",
      });
    case "ManagedRelayUrlInvalidError":
      return new ConnectionBlockedError({
        reason: "configuration",
        detail: error.message,
        hop: "relay",
      });
    case "ManagedRelayAccessTokenScopesUnexpectedError":
      return new ConnectionBlockedError({
        reason: "permission",
        detail: error.message,
        hop: "relay",
      });
    case "ManagedRelayDpopKeyLoadError":
    case "ManagedRelayTokenProofCreationError":
    case "ManagedRelayRequestProofCreationError":
      return new ConnectionBlockedError({
        reason: "authentication",
        detail: error.message,
        hop: "device",
      });
  }
}

export function mapRemoteMachineError(
  error: RemoteMachineAuthError,
  connectionMethod: ClientConnectionMethod = "direct",
): ConnectionAttemptError {
  const networkHint = connectionMethod === "relay" ? ` ${NETWORK_BLOCKING_HINT}` : "";
  // Over T3 Connect the relay already reached the host, so a request that never
  // arrives was blocked between this device and the tunnel.
  const unreachableHop = connectionMethod === "relay" ? "tunnel" : "server";
  const via = connectionMethod === "relay" ? "relay" : "direct";
  switch (error._tag) {
    case "MachineAuthInvalidError":
      return new ConnectionBlockedError({
        reason: "authentication",
        detail: "The machine credential is invalid.",
        traceId: error.traceId,
        hop: "server",
        via,
      });
    case "MachineScopeRequiredError":
    case "MachineOperationForbiddenError":
      return new ConnectionBlockedError({
        reason: "permission",
        detail: "The machine credential does not grant the required access.",
        traceId: error.traceId,
        hop: "server",
        via,
      });
    case "MachineRequestInvalidError":
      return new ConnectionBlockedError({
        reason: "configuration",
        detail: "The machine rejected the authentication request.",
        traceId: error.traceId,
        hop: "server",
        via,
      });
    case "MachineResourceNotFoundError":
      return new ConnectionBlockedError({
        reason: "configuration",
        detail: "The machine endpoint could not be found.",
        traceId: error.traceId,
        hop: "server",
        via,
      });
    case "RemoteMachineAuthTimeoutError":
      return new ConnectionTransientError({
        reason: "timeout",
        detail: `${error.message}${networkHint}`,
        hop: unreachableHop,
        via,
      });
    case "RemoteMachineAuthFetchError":
      return new ConnectionTransientError({
        reason: "network",
        detail: `${error.message}${networkHint}`,
        hop: unreachableHop,
        via,
      });
    case "MachineInternalError":
      return new ConnectionTransientError({
        reason: "remote-unavailable",
        detail: "The machine could not authorize the connection.",
        traceId: error.traceId,
        hop: "server",
        via,
      });
    case "RemoteMachineAuthInvalidJsonError":
    case "RemoteMachineAuthUndeclaredStatusError":
      return new ConnectionTransientError({
        reason: "remote-unavailable",
        detail: error.message,
        hop: "server",
        via,
      });
  }
}

/**
 * An older machine server reports a DPoP clock failure as the same generic
 * invalid-credential response as other failures, so keep the compatibility hint
 * cautious when the server omits the category.
 */
export function mapRemoteDpopMachineError(
  error: RemoteMachineAuthError,
): ConnectionAttemptError {
  if (error._tag === "MachineAuthInvalidError" && error.reason === "invalid_credential") {
    return new ConnectionBlockedError({
      reason: "authentication",
      detail: dpopFailureMessage("The machine credential is invalid.", error.dpopFailureReason),
      traceId: error.traceId,
      hop: "server",
      via: "relay",
      ...(error.dpopFailureReason === "time_window" ? { failureCause: "clock_skew" } : {}),
    });
  }
  return mapRemoteMachineError(error, "relay");
}
