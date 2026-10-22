import {
  RelayAuthInvalidError,
  RelayMachineConnectNotAuthorizedError,
  RelayMachineEndpointTimedOutError,
} from "@vekrel/contracts/relay";
import { describe, expect, it } from "@effect/vitest";

import { mapManagedRelayError, mapRemoteDpopMachineError } from "./errors.ts";
import { DPOP_UNKNOWN_HINT } from "../relay/errorPresentation.ts";
import { ManagedRelayRequestFailedError } from "../relay/managedRelay.ts";
import { NETWORK_BLOCKING_HINT } from "../errors/network.ts";
import { RemoteMachineAuthFetchError, RemoteMachineAuthTimeoutError } from "../rpc/http.ts";

describe("mapManagedRelayError", () => {
  it("keeps a timeout reported by the relay distinct from a local network failure", () => {
    const relayError = new RelayMachineEndpointTimedOutError({
      code: "machine_endpoint_timed_out",
      traceId: "trace-server-timeout",
    });
    const mapped = mapManagedRelayError(
      new ManagedRelayRequestFailedError({
        action: "connect relay machine",
        cause: relayError,
        relayError,
      }),
    );
    expect(mapped).toMatchObject({
      reason: "timeout",
      detail: "Relay timed out while contacting the machine endpoint.",
      traceId: "trace-server-timeout",
    });
  });

  it("retries a tunnel that is still being set up, and blocks other refusals", () => {
    const mapConnectRefusal = (reason: RelayMachineConnectNotAuthorizedError["reason"]) => {
      const relayError = new RelayMachineConnectNotAuthorizedError({
        code: "machine_connect_not_authorized",
        reason,
        traceId: "trace-connect-refused",
      });
      return mapManagedRelayError(
        new ManagedRelayRequestFailedError({
          action: "connect relay machine",
          cause: relayError,
          relayError,
        }),
      );
    };

    expect(mapConnectRefusal("managed_endpoint_allocation_not_ready")).toMatchObject({
      _tag: "ConnectionTransientError",
      reason: "endpoint-unavailable",
      detail: "The machine's Vekrel Connect tunnel is still being set up.",
      traceId: "trace-connect-refused",
    });
    expect(mapConnectRefusal("endpoint_provider_not_managed")).toMatchObject({
      _tag: "ConnectionBlockedError",
      reason: "permission",
      detail: expect.stringContaining("turn Vekrel Connect on"),
    });
    expect(mapConnectRefusal(undefined)).toMatchObject({
      _tag: "ConnectionBlockedError",
      reason: "permission",
    });
  });

  it("presents clock skew as one possible cause for a generic DPoP error", () => {
    const mapped = mapManagedRelayError(
      new ManagedRelayRequestFailedError({
        action: "connect relay machine",
        cause: new Error("request failed"),
        relayError: new RelayAuthInvalidError({
          code: "auth_invalid",
          reason: "invalid_dpop",
          traceId: "trace-1",
        }),
        traceId: "trace-1",
      }),
    );

    expect(mapped).toMatchObject({
      _tag: "ConnectionBlockedError",
      reason: "authentication",
      detail: `Relay rejected the DPoP proof. ${DPOP_UNKNOWN_HINT}`,
      traceId: "trace-1",
    });
  });
});

describe("mapRemoteDpopMachineError", () => {
  it.each([
    new RemoteMachineAuthFetchError({
      message: "Failed to fetch remote machine endpoint.",
      cause: new TypeError("Failed to fetch"),
    }),
    new RemoteMachineAuthTimeoutError("https://machine.example.test", 10_000),
  ])("suggests another network when the relay endpoint cannot be reached: $_tag", (error) => {
    const mapped = mapRemoteDpopMachineError(error);
    expect(mapped).toMatchObject({
      _tag: "ConnectionTransientError",
      detail: `${error.message} ${NETWORK_BLOCKING_HINT}`,
    });
  });
});
