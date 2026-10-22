import {
  type MachineId,
  type OrchestrationV2ShellSnapshot,
  type OrchestrationV2ThreadDetailSnapshot,
  type ServerConfig,
  type ThreadId,
  type VcsListRefsResult,
} from "@vekrel/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import type { ConnectionRegistration } from "../connection/catalog.ts";
import type { DeferredShellSnapshot } from "../state/shellPullRequests.ts";
import type { ConnectionTarget, PersistedConnectionTarget } from "../connection/model.ts";

export class ConnectionPersistenceError extends Schema.TaggedError<ConnectionPersistenceError>()(
  "ConnectionPersistenceError",
  {
    operation: Schema.Literals([
      "list-targets",
      "list-disabled-targets",
      "register-connection",
      "set-connection-routes",
      "remove-connection",
      "set-connection-enabled",
      "load-shell",
      "save-shell",
      "load-thread",
      "save-thread",
      "remove-thread",
      "load-server-config",
      "save-server-config",
      "load-vcs-refs",
      "save-vcs-refs",
      "remove-vcs-refs",
      "clear-vcs-refs",
      "clear-machine",
    ]),
    message: Schema.String,
  },
) {}

export class ConnectionTargetStore extends Context.Service<
  ConnectionTargetStore,
  {
    readonly list: Effect.Effect<ReadonlyArray<ConnectionTarget>, ConnectionPersistenceError>;
    readonly listDisabled: Effect.Effect<ReadonlyArray<MachineId>, ConnectionPersistenceError>;
  }
>()("@vekrel/client-runtime/platform/persistence/ConnectionTargetStore") {}

export class ConnectionRegistrationStore extends Context.Service<
  ConnectionRegistrationStore,
  {
    /**
     * Records of routes missing from `routes` are dropped.
     */
    readonly register: (
      registration: ConnectionRegistration,
      routes: ReadonlyArray<PersistedConnectionTarget>,
    ) => Effect.Effect<void, ConnectionPersistenceError>;
    /**
     * `routes` must not be empty.
     */
    readonly setRoutes: (
      machineId: MachineId,
      routes: ReadonlyArray<PersistedConnectionTarget>,
    ) => Effect.Effect<void, ConnectionPersistenceError>;
    readonly remove: (
      machineId: MachineId,
    ) => Effect.Effect<void, ConnectionPersistenceError>;
    readonly setEnabled: (
      machineId: MachineId,
      enabled: boolean,
    ) => Effect.Effect<void, ConnectionPersistenceError>;
  }
>()("@vekrel/client-runtime/platform/persistence/ConnectionRegistrationStore") {}

export class MachineCacheStore extends Context.Service<
  MachineCacheStore,
  {
    readonly loadShell: (
      machineId: MachineId,
    ) => Effect.Effect<Option.Option<DeferredShellSnapshot>, ConnectionPersistenceError>;
    readonly saveShell: (
      machineId: MachineId,
      snapshot: OrchestrationV2ShellSnapshot,
    ) => Effect.Effect<void, ConnectionPersistenceError>;
    readonly loadThread: (
      machineId: MachineId,
      threadId: ThreadId,
    ) => Effect.Effect<
      Option.Option<OrchestrationV2ThreadDetailSnapshot>,
      ConnectionPersistenceError
    >;
    readonly saveThread: (
      machineId: MachineId,
      snapshot: OrchestrationV2ThreadDetailSnapshot,
    ) => Effect.Effect<void, ConnectionPersistenceError>;
    readonly removeThread: (
      machineId: MachineId,
      threadId: ThreadId,
    ) => Effect.Effect<void, ConnectionPersistenceError>;
    readonly loadServerConfig: (
      machineId: MachineId,
    ) => Effect.Effect<Option.Option<ServerConfig>, ConnectionPersistenceError>;
    readonly saveServerConfig: (
      machineId: MachineId,
      config: ServerConfig,
    ) => Effect.Effect<void, ConnectionPersistenceError>;
    /**
     * Query-specific lists are not cached because they are incomplete and unsafe
     * to present as a full picker.
     */
    readonly loadVcsRefs: (
      machineId: MachineId,
      cwd: string,
    ) => Effect.Effect<Option.Option<VcsListRefsResult>, ConnectionPersistenceError>;
    readonly saveVcsRefs: (
      machineId: MachineId,
      cwd: string,
      refs: VcsListRefsResult,
    ) => Effect.Effect<void, ConnectionPersistenceError>;
    readonly removeVcsRefs: (
      machineId: MachineId,
      cwd: string,
    ) => Effect.Effect<void, ConnectionPersistenceError>;
    /**
     * Git ref mutations are repository-wide, and linked worktrees may have
     * cached the same refs under different working-directory keys.
     */
    readonly clearVcsRefs: (
      machineId: MachineId,
    ) => Effect.Effect<void, ConnectionPersistenceError>;
    readonly clear: (
      machineId: MachineId,
    ) => Effect.Effect<void, ConnectionPersistenceError>;
  }
>()("@vekrel/client-runtime/platform/persistence/MachineCacheStore") {}

export class MachineOwnedDataCleanup extends Context.Reference<{
  readonly clear: (machineId: MachineId) => Effect.Effect<void>;
}>("@vekrel/client-runtime/platform/persistence/MachineOwnedDataCleanup", {
  defaultValue: () => ({
    clear: () => Effect.void,
  }),
}) {}
