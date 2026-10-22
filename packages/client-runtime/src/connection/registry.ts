import { MachineId } from "@vekrel/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";

import * as ClientCapabilities from "../platform/capabilities.ts";
import {
  type ConnectionCatalogEntry,
  type ConnectionCredential,
  type ConnectionProfile,
  type ConnectionRegistration,
  type ConnectionRoute,
  type PlatformConnectionRegistration,
  type PrimaryConnectionRegistration,
  SshConnectionProfile,
  connectionRegistrationCatalogEntry,
} from "./catalog.ts";
import * as ConnectionCredentialStore from "./credentialStore.ts";
import * as ConnectionProfileStore from "./profileStore.ts";
import * as Connectivity from "./connectivity.ts";
import type {
  ConnectionAttemptError,
  ConnectionTarget,
  NetworkStatus,
  PersistedConnectionTarget,
  SupervisorConnectionState,
} from "./model.ts";
import { ConnectionBlockedError } from "./model.ts";
import * as Persistence from "../platform/persistence.ts";
import * as MachineSupervisor from "./supervisor.ts";
import * as ConnectionDriver from "./driver.ts";
import * as ConnectionWakeups from "./wakeups.ts";
import {
  GitHubRoutingPermissions,
  gitHubRoutingConnectionKey,
} from "./githubRoutingPermissions.ts";
import {
  RELAY_ROUTE_ID,
  connectionRouteId,
  connectionRoutes,
  credentialConnectionId,
  entryWithRoutes,
  findRouteToSameAddress,
  hasSavedBearerRoute,
  mergeLearnedRoutes,
  routesAfterRemoving,
  upsertRoute,
  type ReportedEndpoint,
} from "./routes.ts";

const isSshConnectionProfile = Schema.is(SshConnectionProfile);

function unsupportedState(
  entry: ConnectionCatalogEntry,
): Pick<ConnectionCatalogEntry, "unsupportedReason" | "serverUpdateRequired"> {
  return {
    ...(entry.unsupportedReason === undefined
      ? {}
      : { unsupportedReason: entry.unsupportedReason }),
    ...(entry.serverUpdateRequired === true ? { serverUpdateRequired: true } : {}),
  };
}

export class MachineNotRegisteredError extends Schema.TaggedError<MachineNotRegisteredError>()(
  "MachineNotRegisteredError",
  {
    machineId: MachineId,
  },
) {
  override get message(): string {
    return `Machine ${this.machineId} is not registered.`;
  }
}

export class PlatformMachineRemovalError extends Schema.TaggedError<PlatformMachineRemovalError>()(
  "PlatformMachineRemovalError",
  {
    machineId: MachineId,
  },
) {
  override get message(): string {
    return `Platform-managed machine ${this.machineId} cannot be removed.`;
  }
}

export class MachineRegistry extends Context.Service<
  MachineRegistry,
  {
    readonly entries: SubscriptionRef.SubscriptionRef<
      ReadonlyMap<MachineId, ConnectionCatalogEntry>
    >;
    readonly networkStatus: SubscriptionRef.SubscriptionRef<NetworkStatus>;
    readonly start: Effect.Effect<void>;
    /**
     * Succeeds with `false` and changes nothing when the machine is
     * platform-managed, since the host owns that entry.
     */
    readonly register: (
      registration: ConnectionRegistration,
    ) => Effect.Effect<boolean, Persistence.ConnectionPersistenceError>;
    readonly registerPlatform: (registration: PrimaryConnectionRegistration) => Effect.Effect<void>;
    readonly reconcilePlatform: (
      registrations: ReadonlyArray<PlatformConnectionRegistration>,
    ) => Effect.Effect<void>;
    readonly remove: (
      machineId: MachineId,
    ) => Effect.Effect<
      void,
      | Persistence.ConnectionPersistenceError
      | ConnectionAttemptError
      | MachineNotRegisteredError
      | PlatformMachineRemovalError
    >;
    /**
     * Removing a machine's last route removes the machine, the same as
     * `remove`.
     */
    readonly removeRoute: (
      machineId: MachineId,
      routeId: string,
    ) => Effect.Effect<
      void,
      | Persistence.ConnectionPersistenceError
      | ConnectionAttemptError
      | MachineNotRegisteredError
      | PlatformMachineRemovalError
    >;
    readonly reorderRoutes: (
      machineId: MachineId,
      routeIds: ReadonlyArray<string>,
    ) => Effect.Effect<
      void,
      | Persistence.ConnectionPersistenceError
      | MachineNotRegisteredError
      | PlatformMachineRemovalError
      | ConnectionBlockedError
    >;
    readonly removeRelayMachines: () => Effect.Effect<
      void,
      | Persistence.ConnectionPersistenceError
      | ConnectionAttemptError
      | PlatformMachineRemovalError
    >;
    readonly retryNow: (machineId: MachineId) => Effect.Effect<void>;
    readonly setEnabled: (
      machineId: MachineId,
      enabled: boolean,
    ) => Effect.Effect<
      void,
      | MachineNotRegisteredError
      | Persistence.ConnectionPersistenceError
      | ConnectionBlockedError
    >;
    readonly setCompatibility: (
      machineId: MachineId,
      error: ConnectionBlockedError | null,
    ) => Effect.Effect<void, Persistence.ConnectionPersistenceError>;
    readonly state: (
      machineId: MachineId,
    ) => Effect.Effect<SupervisorConnectionState, MachineNotRegisteredError>;
    readonly stateChanges: (
      machineId: MachineId,
    ) => Stream.Stream<SupervisorConnectionState, MachineNotRegisteredError>;
    readonly run: <A, E, R>(
      machineId: MachineId,
      effect: Effect.Effect<A, E, R>,
    ) => Effect.Effect<
      A,
      E | MachineNotRegisteredError,
      Exclude<R, MachineSupervisor.MachineSupervisor>
    >;
    readonly runStream: <A, E, R>(
      machineId: MachineId,
      stream: Stream.Stream<A, E, R>,
    ) => Stream.Stream<
      A,
      E | MachineNotRegisteredError,
      Exclude<R, MachineSupervisor.MachineSupervisor>
    >;
    readonly followStream: <A, E, R>(
      machineId: MachineId,
      stream: Stream.Stream<A, E, R>,
    ) => Stream.Stream<A, E, Exclude<R, MachineSupervisor.MachineSupervisor>>;
  }
>()("@vekrel/client-runtime/connection/registry/MachineRegistry") {}

interface MachineServiceScope {
  readonly entry: ConnectionCatalogEntry;
  readonly supervisor: MachineSupervisor.MachineSupervisor["Service"];
  readonly scope: Scope.Closeable;
}

/** @public Service construction is part of the canonical Effect module API. */
export const make = Effect.gen(function* () {
  const registryScope = yield* Scope.Scope;
  const storage = yield* Persistence.ConnectionTargetStore;
  const registrations = yield* Persistence.ConnectionRegistrationStore;
  const cache = yield* Persistence.MachineCacheStore;
  const ownedDataCleanup = yield* Persistence.MachineOwnedDataCleanup;
  const profiles = yield* ConnectionProfileStore.ConnectionProfileStore;
  const credentials = yield* ConnectionCredentialStore.ConnectionCredentialStore;
  const githubRoutingPermissions = yield* GitHubRoutingPermissions;
  const connectivity = yield* Connectivity.Connectivity;
  const driver = yield* ConnectionDriver.ConnectionDriver;
  const wakeups = yield* ConnectionWakeups.ConnectionWakeups;
  const ssh = yield* ClientCapabilities.SshMachineGateway;
  const persistedTargets = yield* storage.list;
  const disabledMachineIds = new Set(yield* storage.listDisabled);
  const loadRoute = Effect.fn("MachineRegistry.loadRoute")(function* (
    target: ConnectionTarget,
  ) {
    const profile: Option.Option<ConnectionProfile> =
      target._tag === "BearerConnectionTarget" || target._tag === "SshConnectionTarget"
        ? yield* profiles.get(target.connectionId)
        : Option.none();
    return { target, profile } satisfies ConnectionRoute;
  });
  const persistedRoutesByMachine = new Map<MachineId, Array<ConnectionTarget>>();
  for (const target of persistedTargets) {
    const routes = persistedRoutesByMachine.get(target.machineId) ?? [];
    routes.push(target);
    persistedRoutesByMachine.set(target.machineId, routes);
  }
  const initialEntries = new Map(
    yield* Effect.forEach(
      persistedRoutesByMachine,
      Effect.fn("MachineRegistry.loadCatalogEntry")(function* ([machineId, targets]) {
        const loaded = yield* Effect.forEach(targets, loadRoute, { concurrency: "unbounded" });
        const seen = new Set<string>();
        const usable = loaded.filter((route) => {
          const id = connectionRouteId(route.target);
          if (seen.has(id)) return false;
          seen.add(id);
          return !(id.startsWith("learned:") && Option.isNone(route.profile));
        });
        const routes = usable.length > 0 ? usable : loaded.slice(0, 1);
        const first = routes[0]!;
        return [
          machineId,
          entryWithRoutes(
            {
              target: first.target,
              profile: first.profile,
              enabled: !disabledMachineIds.has(machineId),
            },
            routes,
          ),
        ] as const;
      }),
      { concurrency: "unbounded" },
    ),
  );
  const entries =
    yield* SubscriptionRef.make<ReadonlyMap<MachineId, ConnectionCatalogEntry>>(initialEntries);
  const networkStatus = yield* SubscriptionRef.make(yield* connectivity.status);
  const serviceScopes = yield* SubscriptionRef.make<
    ReadonlyMap<MachineId, MachineServiceScope>
  >(new Map());
  const platformMachineIds = yield* Ref.make<ReadonlySet<MachineId>>(new Set());
  const persistedMachineIds = yield* Ref.make<ReadonlySet<MachineId>>(
    new Set(persistedRoutesByMachine.keys()),
  );
  interface LeaseLock {
    readonly semaphore: Semaphore.Semaphore;
    readonly users: number;
  }

  const leaseLocks = yield* Ref.make<ReadonlyMap<MachineId, LeaseLock>>(new Map());
  const leaseLocksGuard = yield* Semaphore.make(1);
  const started = yield* Ref.make(false);

  const withLeaseLock = <A, E, R>(
    machineId: MachineId,
    effect: Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E, R> =>
    Effect.acquireUseRelease(
      leaseLocksGuard.withPermits(1)(
        Effect.gen(function* () {
          const current = yield* Ref.get(leaseLocks);
          const existing = current.get(machineId);
          if (existing !== undefined) {
            yield* Ref.set(
              leaseLocks,
              new Map(current).set(machineId, {
                semaphore: existing.semaphore,
                users: existing.users + 1,
              }),
            );
            return existing.semaphore;
          }
          const semaphore = yield* Semaphore.make(1);
          yield* Ref.set(leaseLocks, new Map(current).set(machineId, { semaphore, users: 1 }));
          return semaphore;
        }),
      ),
      (semaphore) => semaphore.withPermits(1)(effect),
      (semaphore) =>
        leaseLocksGuard.withPermits(1)(
          Ref.update(leaseLocks, (current) => {
            const existing = current.get(machineId);
            if (existing === undefined || existing.semaphore !== semaphore) {
              return current;
            }
            const next = new Map(current);
            if (existing.users === 1) {
              next.delete(machineId);
            } else {
              next.set(machineId, {
                semaphore,
                users: existing.users - 1,
              });
            }
            return next;
          }),
        ),
    ).pipe(Effect.withSpan("MachineRegistry.withLeaseLock"));

  const getEntry = Effect.fn("MachineRegistry.getEntry")(function* (
    machineId: MachineId,
  ) {
    const entry = (yield* SubscriptionRef.get(entries)).get(machineId);
    if (entry === undefined) {
      return yield* new MachineNotRegisteredError({
        machineId,
      });
    }
    return entry;
  });

  const closeServiceScope = Effect.fn("MachineRegistry.closeServiceScope")(function* (
    machineId: MachineId,
  ) {
    const current = yield* SubscriptionRef.get(serviceScopes);
    const lease = current.get(machineId);
    if (lease === undefined) {
      return;
    }
    const next = new Map(current);
    next.delete(machineId);
    yield* SubscriptionRef.set(serviceScopes, next);
    yield* Scope.close(lease.scope, Exit.void);
  });

  const createServiceScope = Effect.fn("MachineRegistry.createServiceScope")(
    (entry: ConnectionCatalogEntry) =>
      Effect.uninterruptible(
        Effect.gen(function* () {
          const machineId = entry.target.machineId;
          const scope = yield* Scope.fork(registryScope);
          const supervisor = yield* MachineSupervisor.make(entry, {
            // Seed the intent to avoid publishing a false switched-off state
            // before an enabled, compatible machine starts connecting.
            initiallyDesired: entry.enabled && entry.unsupportedReason === undefined,
            learnRoutes: (input) => learnRoutes({ machineId, ...input }),
          }).pipe(
            Effect.provideService(Connectivity.Connectivity, connectivity),
            Effect.provideService(ConnectionDriver.ConnectionDriver, driver),
            Effect.provideService(ConnectionWakeups.ConnectionWakeups, wakeups),
            Scope.provide(scope),
            Effect.onError(() => Scope.close(scope, Exit.void)),
          );
          yield* SubscriptionRef.update(serviceScopes, (current) => {
            const next = new Map(current);
            next.set(machineId, { entry, supervisor, scope });
            return next;
          });
          yield* SubscriptionRef.changes(supervisor.state).pipe(
            Stream.runForEach((state) =>
              state.phase === "blocked" && state.lastFailure?.reason === "unsupported"
                ? setCompatibility(machineId, state.lastFailure)
                : Effect.void,
            ),
            Effect.forkIn(scope),
          );
          return supervisor;
        }),
      ),
  );

  const acquireSupervisor = Effect.fn("MachineRegistry.acquireSupervisor")(function* (
    machineId: MachineId,
  ) {
    return yield* withLeaseLock(
      machineId,
      Effect.gen(function* () {
        const entry = yield* getEntry(machineId);
        const existing = (yield* SubscriptionRef.get(serviceScopes)).get(machineId);
        if (existing !== undefined) {
          if (Equal.equals(existing.entry, entry)) {
            return existing.supervisor;
          }
          yield* closeServiceScope(machineId);
        }
        return yield* createServiceScope(entry);
      }),
    );
  });

  const run: MachineRegistry["Service"]["run"] = Effect.fn("MachineRegistry.run")(
    function* <A, E, R>(machineId: MachineId, effect: Effect.Effect<A, E, R>) {
      const supervisor = yield* acquireSupervisor(machineId);
      return yield* Effect.provideService(
        effect,
        MachineSupervisor.MachineSupervisor,
        supervisor,
      );
    },
  );

  const runStream: MachineRegistry["Service"]["runStream"] = <A, E, R>(
    machineId: MachineId,
    stream: Stream.Stream<A, E, R>,
  ) =>
    Stream.unwrap(
      acquireSupervisor(machineId).pipe(
        Effect.map((supervisor) =>
          Stream.provideService(stream, MachineSupervisor.MachineSupervisor, supervisor),
        ),
      ),
    );

  const followStream: MachineRegistry["Service"]["followStream"] = <A, E, R>(
    machineId: MachineId,
    stream: Stream.Stream<A, E, R>,
  ) =>
    Stream.concat(
      Stream.fromEffect(SubscriptionRef.get(entries)),
      SubscriptionRef.changes(entries),
    ).pipe(
      Stream.map((current) => Option.fromUndefinedOr(current.get(machineId))),
      Stream.changesWith(
        (previous, current) => Option.getOrNull(previous) === Option.getOrNull(current),
      ),
      Stream.switchMap(
        Option.match({
          onNone: () => Stream.empty,
          onSome: () =>
            Stream.unwrap(
              acquireSupervisor(machineId).pipe(
                Effect.match({
                  onFailure: () => Stream.empty,
                  onSuccess: (supervisor) =>
                    Stream.provideService(
                      stream,
                      MachineSupervisor.MachineSupervisor,
                      supervisor,
                    ),
                }),
              ),
            ),
        }),
      ),
    );

  const start = Effect.gen(function* () {
    if (yield* Ref.getAndSet(started, true)) {
      return;
    }
    yield* Effect.forEach(
      persistedRoutesByMachine.keys(),
      (machineId) =>
        acquireSupervisor(machineId).pipe(
          Effect.catchTags({ MachineNotRegisteredError: () => Effect.void }),
        ),
      {
        concurrency: "unbounded",
        discard: true,
      },
    );
  }).pipe(Effect.withSpan("MachineRegistry.start"));

  const installEntryLocked = Effect.fn("MachineRegistry.installEntryLocked")(function* (
    entry: ConnectionCatalogEntry,
    options?: { readonly retainEquivalentRuntime?: boolean },
  ) {
    const target = entry.target;
    const previous = (yield* SubscriptionRef.get(entries)).get(target.machineId);
    const existingScope = (yield* SubscriptionRef.get(serviceScopes)).get(target.machineId);
    if (
      options?.retainEquivalentRuntime === true &&
      previous !== undefined &&
      Equal.equals(previous, entry) &&
      existingScope !== undefined &&
      Equal.equals(existingScope.entry, entry)
    ) {
      return;
    }

    yield* closeServiceScope(target.machineId);
    yield* SubscriptionRef.update(entries, (current) => {
      const next = new Map(current);
      next.set(target.machineId, entry);
      return next;
    });
    yield* createServiceScope(entry);
  });

  const forgetRoutingTrust = (
    machineId: MachineId,
    operation: "register-connection" | "set-connection-routes",
  ) =>
    githubRoutingPermissions.forget(machineId).pipe(
      Effect.mapError(
        (error) =>
          new Persistence.ConnectionPersistenceError({
            operation,
            message: error.message,
          }),
      ),
    );

  const persistedRoutes = (entry: ConnectionCatalogEntry) =>
    connectionRoutes(entry).map((route) => route.target as PersistedConnectionTarget);

  /**
   * GitHub trust and an unsupported verdict both belong to the saved addresses: a changed set may
   * reach a different server, so both are dropped and the next connection decides.
   */
  const withRoutes = (previous: ConnectionCatalogEntry, routes: ReadonlyArray<ConnectionRoute>) => {
    const next = entryWithRoutes(previous, routes);
    if (gitHubRoutingConnectionKey(previous) === gitHubRoutingConnectionKey(next)) {
      return { entry: next, addressesChanged: false };
    }
    const { unsupportedReason: _reason, serverUpdateRequired: _update, ...rest } = next;
    return { entry: rest, addressesChanged: true };
  };

  const register = Effect.fn("MachineRegistry.register")(function* (
    registration: ConnectionRegistration,
  ) {
    const registered = connectionRegistrationCatalogEntry(registration);
    const machineId = registered.target.machineId;
    return yield* withLeaseLock(
      machineId,
      Effect.gen(function* () {
        if ((yield* Ref.get(platformMachineIds)).has(machineId)) {
          return false;
        }
        const previous = (yield* SubscriptionRef.get(entries)).get(machineId);
        let entry = registered;
        if (previous !== undefined) {
          const route: ConnectionRoute = { target: registered.target, profile: registered.profile };
          const existing = connectionRoutes(previous);
          const sameAddress = findRouteToSameAddress(existing, route);
          const routes = existing.filter(
            (existing) =>
              existing !== sameAddress ||
              connectionRouteId(existing.target) === connectionRouteId(route.target),
          );
          const next = withRoutes(previous, upsertRoute(routes, route));
          if (next.addressesChanged) {
            yield* forgetRoutingTrust(machineId, "register-connection");
          }
          entry = next.entry;
        }
        yield* registrations.register(registration, persistedRoutes(entry));
        yield* Ref.update(persistedMachineIds, (current) =>
          new Set(current).add(machineId),
        );
        yield* installEntryLocked(entry);
        return true;
      }),
    );
  });

  const replaceRoutesLocked = Effect.fn("MachineRegistry.replaceRoutesLocked")(function* (
    previous: ConnectionCatalogEntry,
    routes: ReadonlyArray<ConnectionRoute>,
  ) {
    const machineId = previous.target.machineId;
    const next = withRoutes(previous, routes);
    if (next.addressesChanged) {
      yield* forgetRoutingTrust(machineId, "set-connection-routes");
    }
    yield* registrations.setRoutes(machineId, persistedRoutes(next.entry));
    yield* installEntryLocked(next.entry);
  });

  const userEntry = Effect.fn("MachineRegistry.userEntry")(function* (
    machineId: MachineId,
  ) {
    if ((yield* Ref.get(platformMachineIds)).has(machineId)) {
      return yield* new PlatformMachineRemovalError({ machineId });
    }
    return yield* getEntry(machineId);
  });

  const installPlatformRegistration = Effect.fn("MachineRegistry.installPlatformRegistration")(
    function* (registration: PlatformConnectionRegistration) {
      const registered = connectionRegistrationCatalogEntry(registration);
      const target = registered.target;
      yield* withLeaseLock(
        target.machineId,
        Effect.gen(function* () {
          const previous = (yield* SubscriptionRef.get(entries)).get(target.machineId);
          const entry: ConnectionCatalogEntry =
            previous?.unsupportedReason !== undefined &&
            gitHubRoutingConnectionKey(previous) === gitHubRoutingConnectionKey(registered)
              ? { ...registered, ...unsupportedState(previous) }
              : registered;
          const persisted = (yield* Ref.get(persistedMachineIds)).has(target.machineId);
          if (
            persisted ||
            (previous !== undefined &&
              gitHubRoutingConnectionKey(previous) !== gitHubRoutingConnectionKey(entry))
          ) {
            const revoked = yield* githubRoutingPermissions.forget(target.machineId).pipe(
              Effect.tapError((error) =>
                Effect.logWarning(
                  "Could not clear GitHub routing permission for a platform machine.",
                  {
                    machineId: target.machineId,
                    error,
                  },
                ),
              ),
              Effect.exit,
            );
            if (Exit.isFailure(revoked)) return;
          }
          yield* Ref.update(platformMachineIds, (current) => {
            const next = new Set(current);
            next.add(target.machineId);
            return next;
          });

          // Secondary desktop-local backends (e.g. a parallel WSL backend) live
          // on their own loopback origin, so they authenticate with a bearer
          // token instead of the primary's same-origin cookie. Stash it where
          // the resolver's bearer broker looks it up.
          let bearerReplaced = false;
          if (registration._tag === "BearerConnectionRegistration") {
            const stored = yield* credentials
              .get(registration.target.connectionId)
              .pipe(Effect.orElseSucceed(() => Option.none<ConnectionCredential>()));
            const changed =
              Option.isSome(stored) && !Equal.equals(stored.value, registration.credential);
            // Only a bearer that was actually stored can be retried with; a
            // failed write leaves the rejected one in place.
            bearerReplaced = yield* credentials
              .put(registration.target.connectionId, registration.credential)
              .pipe(
                Effect.as(changed),
                Effect.catch((error) =>
                  Effect.logWarning("Could not store the platform bearer credential.", {
                    machineId: target.machineId,
                    error,
                  }).pipe(Effect.as(false)),
                ),
              );
          }

          if (persisted) {
            yield* registrations.remove(target.machineId).pipe(
              Effect.tap(() =>
                Ref.update(persistedMachineIds, (current) => {
                  const next = new Set(current);
                  next.delete(target.machineId);
                  return next;
                }),
              ),
              Effect.catch((error) =>
                Effect.logWarning(
                  "Could not remove a persisted registration shadowed by a platform machine.",
                  {
                    machineId: target.machineId,
                    error,
                  },
                ),
              ),
            );
          }

          yield* installEntryLocked(entry, { retainEquivalentRuntime: true });

          if (bearerReplaced && registration._tag === "BearerConnectionRegistration") {
            const scope = (yield* SubscriptionRef.get(serviceScopes)).get(target.machineId);
            if (scope !== undefined) {
              const state = yield* SubscriptionRef.get(scope.supervisor.state);
              if (
                state.phase === "connecting" ||
                state.phase === "backoff" ||
                (state.phase === "blocked" &&
                  state.lastFailure?._tag === "ConnectionBlockedError" &&
                  state.lastFailure.reason === "authentication")
              ) {
                yield* scope.supervisor.retryNow;
              } else if (state.phase === "connected") {
                const token = registration.credential.token;
                yield* SubscriptionRef.updateSome(scope.supervisor.prepared, (current) =>
                  Option.isSome(current) &&
                  current.value.target._tag === "BearerConnectionTarget" &&
                  credentialConnectionId(current.value.target.connectionId) ===
                    registration.target.connectionId &&
                  current.value.httpAuthorization?._tag === "Bearer" &&
                  current.value.httpAuthorization.token !== token
                    ? Option.some(
                        Option.some({
                          ...current.value,
                          httpAuthorization: { _tag: "Bearer", token },
                        }),
                      )
                    : Option.none(),
                );
              }
            }
          }
        }),
      );
    },
  );

  const removePlatformMachine = Effect.fn("MachineRegistry.removePlatformMachine")(
    function* (machineId: MachineId) {
      yield* withLeaseLock(
        machineId,
        Effect.gen(function* () {
          const entry = (yield* SubscriptionRef.get(entries)).get(machineId);
          const revoked = yield* githubRoutingPermissions.forget(machineId).pipe(
            Effect.tapError((error) =>
              Effect.logWarning(
                "Could not clear GitHub routing permission after platform removal.",
                {
                  machineId,
                  error,
                },
              ),
            ),
            Effect.exit,
          );
          if (Exit.isFailure(revoked)) return;
          yield* Ref.update(platformMachineIds, (current) => {
            const next = new Set(current);
            next.delete(machineId);
            return next;
          });
          yield* closeServiceScope(machineId);
          yield* SubscriptionRef.update(entries, (current) => {
            const next = new Map(current);
            next.delete(machineId);
            return next;
          });
          if (entry !== undefined && entry.target._tag === "BearerConnectionTarget") {
            yield* credentials.remove(entry.target.connectionId).pipe(
              Effect.catch((error) =>
                Effect.logWarning("Could not clear the platform bearer credential.", {
                  machineId,
                  error,
                }),
              ),
            );
          }
          yield* Effect.all(
            [
              cache.clear(machineId).pipe(
                Effect.catch((error) =>
                  Effect.logWarning("Could not clear cached machine data after removal.", {
                    machineId,
                    error,
                  }),
                ),
              ),
              ownedDataCleanup.clear(machineId),
            ],
            { concurrency: "unbounded", discard: true },
          );
        }),
      );
    },
  );

  const registerPlatform = Effect.fn("MachineRegistry.registerPlatform")(function* (
    registration: PrimaryConnectionRegistration,
  ) {
    yield* installPlatformRegistration(registration);
  });

  const reconcilePlatform = Effect.fn("MachineRegistry.reconcilePlatform")(function* (
    platformRegistrations: ReadonlyArray<PlatformConnectionRegistration>,
  ) {
    const desiredIds = new Set(
      platformRegistrations.map((registration) => registration.target.machineId),
    );
    const currentPlatformIds = yield* Ref.get(platformMachineIds);
    yield* Effect.forEach(
      currentPlatformIds,
      (machineId) =>
        desiredIds.has(machineId) ? Effect.void : removePlatformMachine(machineId),
      { discard: true },
    );
    yield* Effect.forEach(platformRegistrations, installPlatformRegistration, { discard: true });
  });

  /** Callers hold its lease lock. */
  const removeLocked = Effect.fn("MachineRegistry.removeLocked")(function* (
    machineId: MachineId,
  ) {
    if ((yield* Ref.get(platformMachineIds)).has(machineId)) {
      return yield* new PlatformMachineRemovalError({
        machineId,
      });
    }
    const entry = yield* getEntry(machineId);

    yield* githubRoutingPermissions.forget(machineId);
    yield* registrations.remove(machineId);
    yield* Ref.update(persistedMachineIds, (current) => {
      const next = new Set(current);
      next.delete(machineId);
      return next;
    });
    yield* closeServiceScope(machineId);
    yield* SubscriptionRef.update(entries, (current) => {
      const next = new Map(current);
      next.delete(machineId);
      return next;
    });
    yield* Effect.all(
      [
        cache.clear(machineId).pipe(
          Effect.catch((error) =>
            Effect.logWarning("Could not clear cached machine data after removal.", {
              machineId,
              error,
            }),
          ),
        ),
        ownedDataCleanup.clear(machineId),
      ],
      { concurrency: "unbounded", discard: true },
    );

    for (const route of connectionRoutes(entry)) {
      const profile = Option.getOrNull(route.profile);
      if (profile !== null && isSshConnectionProfile(profile)) {
        yield* disconnectSsh(machineId, profile);
      }
    }
  });

  const remove = Effect.fn("MachineRegistry.remove")(function* (machineId: MachineId) {
    return yield* withLeaseLock(machineId, removeLocked(machineId));
  });

  const disconnectSsh = (machineId: MachineId, profile: SshConnectionProfile) =>
    ssh.disconnect(profile.target).pipe(
      Effect.tapError((error) =>
        Effect.logWarning("Could not disconnect the managed SSH machine.", {
          machineId,
          error,
        }),
      ),
      Effect.ignore,
    );

  // Plain HTTP routes are unusable from an HTTPS page, which blocks mixed content.
  const allowInsecureRoutes =
    typeof globalThis.location === "undefined" || globalThis.location.protocol !== "https:";

  /**
   * Learned routes reuse the active route's credential, so they do not revoke GitHub routing
   * trust the way a newly paired address does.
   */
  const learnRoutes = Effect.fn("MachineRegistry.learnRoutes")(function* (input: {
    readonly machineId: MachineId;
    readonly activeRoute: ConnectionRoute;
    readonly reported: ReadonlyArray<ReportedEndpoint>;
  }) {
    return yield* withLeaseLock(
      input.machineId,
      Effect.gen(function* () {
        if ((yield* Ref.get(platformMachineIds)).has(input.machineId)) {
          return Option.none<ConnectionCatalogEntry>();
        }
        const entry = (yield* SubscriptionRef.get(entries)).get(input.machineId);
        if (entry === undefined) return Option.none<ConnectionCatalogEntry>();
        const routes = mergeLearnedRoutes({
          entry,
          activeRoute: input.activeRoute,
          reported: input.reported,
          allowInsecure: allowInsecureRoutes,
        });
        if (routes === null) return Option.none<ConnectionCatalogEntry>();
        const next = entryWithRoutes(entry, routes);
        const previousProfiles = new Map(
          connectionRoutes(entry).map((route) => [
            connectionRouteId(route.target),
            Option.getOrNull(route.profile),
          ]),
        );
        for (const route of routes) {
          const profile = Option.getOrNull(route.profile);
          if (
            profile === null ||
            previousProfiles.get(connectionRouteId(route.target)) === profile
          ) {
            continue;
          }
          yield* profiles.put(profile);
        }
        yield* registrations.setRoutes(input.machineId, persistedRoutes(next));
        const lease = (yield* SubscriptionRef.get(serviceScopes)).get(input.machineId);
        if (lease !== undefined) {
          yield* SubscriptionRef.update(serviceScopes, (current) =>
            new Map(current).set(input.machineId, { ...lease, entry: next }),
          );
        }
        yield* SubscriptionRef.update(entries, (current) =>
          new Map(current).set(input.machineId, next),
        );
        return Option.some(next);
      }),
    ).pipe(
      Effect.catch((error) =>
        Effect.logWarning("Could not save routes learned from the machine.", {
          machineId: input.machineId,
          error,
        }).pipe(Effect.as(Option.none<ConnectionCatalogEntry>())),
      ),
    );
  });

  const removeRoute = Effect.fn("MachineRegistry.removeRoute")(function* (
    machineId: MachineId,
    routeId: string,
  ) {
    // One lock for the whole decision: a route registered between "this was
    // the last route" and the removal must not be deleted with it.
    yield* withLeaseLock(
      machineId,
      Effect.gen(function* () {
        const entry = yield* userEntry(machineId);
        const routes = connectionRoutes(entry);
        const route = routes.find((candidate) => connectionRouteId(candidate.target) === routeId);
        if (route === undefined) return;
        const remaining = routesAfterRemoving(routes, routeId);
        if (remaining.length === 0) return yield* removeLocked(machineId);
        yield* replaceRoutesLocked(entry, remaining);
        const profile = Option.getOrNull(route.profile);
        if (profile !== null && isSshConnectionProfile(profile)) {
          yield* disconnectSsh(machineId, profile);
        }
      }),
    );
  });

  const reorderRoutes = Effect.fn("MachineRegistry.reorderRoutes")(function* (
    machineId: MachineId,
    routeIds: ReadonlyArray<string>,
  ) {
    yield* withLeaseLock(
      machineId,
      Effect.gen(function* () {
        const entry = yield* userEntry(machineId);
        const routes = connectionRoutes(entry);
        const byId = new Map(routes.map((route) => [connectionRouteId(route.target), route]));
        const reordered = routeIds.flatMap((id) => byId.get(id) ?? []);
        if (reordered.length !== routes.length || new Set(routeIds).size !== routes.length) {
          return yield* new ConnectionBlockedError({
            reason: "configuration",
            detail: "The route order must list every saved route once.",
          });
        }
        if (reordered.every((route, index) => route === routes[index])) return;
        yield* replaceRoutesLocked(entry, reordered);
      }),
    );
  });

  const removeRelayMachines = Effect.fn("MachineRegistry.removeRelayMachines")(
    function* () {
      const relayMachineIds = [...(yield* SubscriptionRef.get(entries)).values()]
        .filter((entry) =>
          connectionRoutes(entry).some((route) => route.target._tag === "RelayConnectionTarget"),
        )
        .map((entry) => entry.target.machineId);

      yield* Effect.forEach(
        relayMachineIds,
        (machineId) =>
          removeRoute(machineId, RELAY_ROUTE_ID).pipe(
            Effect.catchTags({ MachineNotRegisteredError: () => Effect.void }),
          ),
        {
          concurrency: "unbounded",
          discard: true,
        },
      );
    },
  );

  const retryNow = (machineId: MachineId) =>
    acquireSupervisor(machineId).pipe(
      Effect.flatMap((supervisor) => supervisor.retryNow),
      Effect.catchTags({ MachineNotRegisteredError: () => Effect.void }),
      Effect.withSpan("MachineRegistry.retryNow"),
    );
  const setEnabled = Effect.fn("MachineRegistry.setEnabled")(function* (
    machineId: MachineId,
    enabled: boolean,
  ) {
    yield* withLeaseLock(
      machineId,
      Effect.gen(function* () {
        const entry = yield* getEntry(machineId);
        const retryUnsupported = enabled && entry.unsupportedReason !== undefined;
        if (retryUnsupported && !hasSavedBearerRoute(entry)) {
          return yield* new ConnectionBlockedError({
            reason: "unsupported",
            detail: entry.unsupportedReason,
          });
        }
        if (entry.enabled === enabled && !retryUnsupported) {
          return;
        }
        if (!(yield* Ref.get(platformMachineIds)).has(machineId)) {
          yield* registrations.setEnabled(machineId, enabled);
        }
        // A cached rejection can outlive a server update.
        const { unsupportedReason: _reason, serverUpdateRequired: _update, ...rest } = entry;
        const next: ConnectionCatalogEntry = { ...(retryUnsupported ? rest : entry), enabled };
        // Update the lease in place so the supervisor keeps its generation and
        // durable streams; `installEntryLocked` would tear it down instead.
        const lease = (yield* SubscriptionRef.get(serviceScopes)).get(machineId);
        if (lease !== undefined) {
          yield* SubscriptionRef.update(serviceScopes, (current) => {
            const nextScopes = new Map(current);
            nextScopes.set(machineId, { ...lease, entry: next });
            return nextScopes;
          });
        }
        yield* SubscriptionRef.update(entries, (current) => {
          const nextEntries = new Map(current);
          nextEntries.set(machineId, next);
          return nextEntries;
        });
        if (lease !== undefined) {
          yield* enabled ? lease.supervisor.connect : lease.supervisor.disconnect;
        } else if (enabled) {
          yield* createServiceScope(next);
        }
        // The supervisor only owns the RPC session. A managed SSH backend and
        // its tunnel outlive it, so switching off tears those down as well.
        if (!enabled) {
          for (const route of connectionRoutes(entry)) {
            const profile = Option.getOrNull(route.profile);
            if (profile !== null && isSshConnectionProfile(profile)) {
              yield* disconnectSsh(machineId, profile);
            }
          }
        }
      }),
    );
  });

  const state = Effect.fn("MachineRegistry.state")(function* (machineId: MachineId) {
    const supervisor = yield* acquireSupervisor(machineId);
    return yield* SubscriptionRef.get(supervisor.state);
  });
  const stateChanges = (machineId: MachineId) =>
    followStream(
      machineId,
      Stream.unwrap(
        MachineSupervisor.MachineSupervisor.pipe(
          Effect.map((supervisor) => SubscriptionRef.changes(supervisor.state)),
        ),
      ),
    );

  yield* Effect.addFinalizer(() =>
    SubscriptionRef.get(serviceScopes).pipe(
      Effect.flatMap((current) =>
        Effect.forEach(current.values(), (lease) => Scope.close(lease.scope, Exit.void), {
          concurrency: "unbounded",
          discard: true,
        }),
      ),
    ),
  );
  yield* connectivity.changes.pipe(
    Stream.runForEach((status) => SubscriptionRef.set(networkStatus, status)),
    Effect.forkScoped,
  );

  const setCompatibility = Effect.fn("MachineRegistry.setCompatibility")(function* (
    machineId: MachineId,
    error: ConnectionBlockedError | null,
  ) {
    yield* withLeaseLock(
      machineId,
      Effect.gen(function* () {
        const entry = (yield* SubscriptionRef.get(entries)).get(machineId);
        if (
          entry === undefined ||
          (entry.unsupportedReason === (error?.message ?? undefined) &&
            entry.serverUpdateRequired === (error?.serverUpdateRequired ?? undefined))
        )
          return;
        const {
          unsupportedReason: _previousReason,
          serverUpdateRequired: _previousUpdateRequired,
          ...rest
        } = entry;
        // Compatibility pauses connections without changing the user's saved on/off choice.
        const next: ConnectionCatalogEntry =
          error === null
            ? rest
            : {
                ...rest,
                unsupportedReason: error.message,
                ...(error.serverUpdateRequired === true ? { serverUpdateRequired: true } : {}),
              };
        const lease = (yield* SubscriptionRef.get(serviceScopes)).get(machineId);
        if (lease !== undefined) {
          yield* SubscriptionRef.update(serviceScopes, (current) =>
            new Map(current).set(machineId, { ...lease, entry: next }),
          );
          if (error !== null) yield* lease.supervisor.disconnect;
          else if (next.enabled) yield* lease.supervisor.connect;
        }
        yield* SubscriptionRef.update(entries, (current) =>
          new Map(current).set(machineId, next),
        );
      }),
    );
  });

  return MachineRegistry.of({
    entries,
    networkStatus,
    start,
    register,
    registerPlatform,
    reconcilePlatform,
    remove,
    removeRoute,
    reorderRoutes,
    removeRelayMachines,
    retryNow,
    setEnabled,
    setCompatibility,
    state,
    stateChanges,
    run,
    runStream,
    followStream,
  });
});

export const layer = Layer.effect(MachineRegistry, make);
