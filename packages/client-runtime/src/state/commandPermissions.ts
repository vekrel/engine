import {
  clientRpcRequiredScopes,
  sessionGrantsScope,
  authScopeRequiredResponse,
  MachineAuthorizationError,
  type MachineId,
  type AuthSessionState,
  type AuthMachineScope,
} from "@vekrel/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity";
import type { MachineRegistry } from "../connection/registry.ts";
import { createMachineSessionAtoms } from "./session.ts";

function makeCommandPermissions<R, E>(
  runtime: Atom.AtomRuntime<MachineRegistry | R, E>,
  method: string,
) {
  const sessions = createMachineSessionAtoms(runtime);
  const requiredScopes = (input?: unknown) => clientRpcRequiredScopes(method, input);
  const atomsByMachine = Atom.family((machineId: MachineId | null) =>
    Atom.family((scopeKey: string) =>
      Atom.make((get) => {
        if (machineId === null) return false;
        const scopes = scopeKey.split(",").filter(Boolean) as AuthMachineScope[];
        if (scopes.length === 0) return true;
        const result = get(sessions.sessionStateAtom(machineId));
        const session = Option.getOrNull(AsyncResult.value(result));
        return (
          result._tag !== "Failure" &&
          session !== null &&
          scopes.every((scope) => sessionGrantsScope(session, scope))
        );
      }),
    ),
  );
  const permissionAtom = (machineId: MachineId | null, input?: unknown) =>
    atomsByMachine(machineId)(requiredScopes(input).join(","));
  const authorize = (
    registry: AtomRegistry.AtomRegistry,
    machineId: MachineId,
    input?: unknown,
  ) =>
    Effect.suspend(() => {
      const scopes = requiredScopes(input);
      if (scopes.length === 0) return Effect.void;
      const atom = sessions.sessionStateAtom(machineId);
      return Effect.scoped(
        Effect.gen(function* () {
          yield* AtomRegistry.mount(registry, atom);
          const session = yield* AtomRegistry.getResult(registry, atom, {
            suspendOnWaiting: false,
          }).pipe(
            Effect.timeoutOption(6_000),
            Effect.catch(() => Effect.succeed(Option.none<AuthSessionState>())),
          );
          const missing = scopes.find(
            (scope) => Option.isNone(session) || !sessionGrantsScope(session.value, scope),
          );
          if (missing !== undefined)
            return yield* Effect.fail(
              new MachineAuthorizationError({
                ...authScopeRequiredResponse(missing),
                message: `This connection requires ${missing}.`,
              }),
            );
        }),
      );
    });
  return { requiredScopes, permissionAtom, authorize };
}

const permissionsByRuntime = new WeakMap<
  object,
  Map<string, ReturnType<typeof makeCommandPermissions>>
>();

export function createCommandPermissions<R, E>(
  runtime: Atom.AtomRuntime<MachineRegistry | R, E>,
  method: string,
) {
  let permissions = permissionsByRuntime.get(runtime);
  if (permissions === undefined) {
    permissions = new Map();
    permissionsByRuntime.set(runtime, permissions);
  }
  const existing = permissions.get(method);
  if (existing !== undefined) return existing;
  const created = makeCommandPermissions(runtime, method);
  permissions.set(method, created);
  return created;
}
