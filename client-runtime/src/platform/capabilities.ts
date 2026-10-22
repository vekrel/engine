import {
  type AuthClientPresentationMetadata,
  type DesktopSshMachineBootstrap,
  type DesktopSshMachineTarget,
  MachineId,
} from "@vekrel/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import type * as Option from "effect/Option";

import type { ConnectionAttemptError } from "../connection/model.ts";

export interface PreparedSshMachine {
  readonly bootstrap: DesktopSshMachineBootstrap;
  readonly bearerToken: string;
}

export interface ProvisionedSshMachine extends PreparedSshMachine {
  readonly machineId: MachineId;
  readonly label: string;
}

/** Stable for one signed-in session, including same-account token refreshes. */
export interface CloudSessionIdentity {
  readonly accountId: string;
}

export class CloudSession extends Context.Service<
  CloudSession,
  {
    readonly identity: Effect.Effect<Option.Option<CloudSessionIdentity>>;
    readonly clerkToken: Effect.Effect<string, ConnectionAttemptError>;
  }
>()("@vekrel/client-runtime/platform/capabilities/CloudSession") {}

export class RelayDeviceIdentity extends Context.Service<
  RelayDeviceIdentity,
  {
    readonly deviceId: Effect.Effect<Option.Option<string>, ConnectionAttemptError>;
  }
>()("@vekrel/client-runtime/platform/capabilities/RelayDeviceIdentity") {}

/**
 * A random id this client keeps across launches. Machines it connects to
 * record its hash, so two machines opened from one device share an owner
 * in analytics even when nobody is signed in.
 */
export class AnalyticsDevice extends Context.Reference<{
  readonly id: Effect.Effect<Option.Option<string>>;
}>("@vekrel/client-runtime/platform/capabilities/AnalyticsDevice", {
  defaultValue: () => ({ id: Effect.succeedNone }),
}) {}

export class ClientPresentation extends Context.Service<
  ClientPresentation,
  {
    readonly metadata: AuthClientPresentationMetadata;
  }
>()("@vekrel/client-runtime/platform/capabilities/ClientPresentation") {}

export class PrimaryMachineAuth extends Context.Service<
  PrimaryMachineAuth,
  {
    readonly bearerToken: Effect.Effect<Option.Option<string>, ConnectionAttemptError>;
  }
>()("@vekrel/client-runtime/platform/capabilities/PrimaryMachineAuth") {}

export class SshMachineGateway extends Context.Service<
  SshMachineGateway,
  {
    /**
     * With `expectedMachineId`, a server that is a different machine fails
     * before its one-time pairing credential is spent.
     */
    readonly provision: (
      target: DesktopSshMachineTarget,
      expectedMachineId?: MachineId,
    ) => Effect.Effect<ProvisionedSshMachine, ConnectionAttemptError>;
    /**
     * Starts or reuses the remote server for a saved connection. Returns
     * `bearerToken`, the token this client saved from an earlier pairing, while
     * the host still accepts it with the standard scopes, and pairs again only
     * when it does not, so a reconnect adds no session on the host.
     */
    readonly prepare: (input: {
      readonly connectionId: string;
      readonly expectedMachineId: MachineId;
      readonly target: DesktopSshMachineTarget;
      readonly bearerToken?: string;
    }) => Effect.Effect<PreparedSshMachine, ConnectionAttemptError>;
    readonly disconnect: (
      target: DesktopSshMachineTarget,
    ) => Effect.Effect<void, ConnectionAttemptError>;
  }
>()("@vekrel/client-runtime/platform/capabilities/SshMachineGateway") {}
