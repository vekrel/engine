import type { MachineId, ServerSelfUpdateInput } from "@vekrel/contracts";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import type * as HttpClient from "effect/http/HttpClient";
import type { Atom } from "effect/reactivity";
import type * as Socket from "effect/socket/Socket";

import { describeOutdatedHost, updateOutdatedHost } from "../connection/outdatedHostUpdate.ts";
import type * as ConnectionResolver from "../connection/resolver.ts";
import type * as MachineRegistry from "../connection/registry.ts";
import type * as RelayMachineDiscovery from "../relay/discovery.ts";
import { createAtomCommandScheduler, createRuntimeCommand } from "./runtime.ts";
import {
  serverUpdateFailureMessage,
  serverUpdateStateAtom,
  type ServerUpdateStage,
} from "./server.ts";

export interface OutdatedServerUpdateTarget {
  readonly machineId: MachineId;
  readonly input: ServerSelfUpdateInput;
  /** From the host descriptor when known; an outdated host never delivers a server config. */
  readonly fromVersion?: string;
}

export function createOutdatedServerDescriptionCommand<E>(
  runtime: Atom.AtomRuntime<
    MachineRegistry.MachineRegistry | ConnectionResolver.ConnectionResolver,
    E
  >,
) {
  return createRuntimeCommand(runtime, {
    label: "machine-data:server:describe-outdated-server",
    execute: (machineId: MachineId) =>
      describeOutdatedHost(machineId).pipe(Effect.timeout("20 seconds")),
  });
}

export function createOutdatedServerUpdateCommand<E>(
  runtime: Atom.AtomRuntime<
    | MachineRegistry.MachineRegistry
    | ConnectionResolver.ConnectionResolver
    | RelayMachineDiscovery.RelayMachineDiscovery
    | Socket.WebSocketConstructor
    | HttpClient.HttpClient,
    E
  >,
) {
  return createRuntimeCommand(runtime, {
    label: "machine-data:server:update-outdated-server",
    scheduler: createAtomCommandScheduler(),
    concurrency: {
      mode: "singleFlight",
      key: ({ machineId }: OutdatedServerUpdateTarget) => machineId,
    },
    execute: (target: OutdatedServerUpdateTarget, atomRegistry) => {
      const stateAtom = serverUpdateStateAtom(target.machineId);
      const targetVersion = target.input.targetVersion;
      const fromVersion = target.fromVersion ?? targetVersion;
      let currentStage: ServerUpdateStage = "downloading";
      const setStage = (stage: ServerUpdateStage) =>
        Effect.sync(() => {
          currentStage = stage;
          atomRegistry.set(stateAtom, { status: "running", stage, fromVersion, targetVersion });
        });
      return setStage(currentStage).pipe(
        Effect.andThen(updateOutdatedHost(target.machineId, target.input, setStage)),
        Effect.onExit((exit) =>
          Effect.sync(() => {
            if (Exit.isSuccess(exit) || Cause.hasInterruptsOnly(exit.cause)) {
              atomRegistry.set(stateAtom, { status: "idle" });
              return;
            }
            atomRegistry.set(stateAtom, {
              status: "failed",
              stage: currentStage,
              fromVersion,
              targetVersion,
              message: serverUpdateFailureMessage(Cause.squash(exit.cause)),
            });
          }),
        ),
      );
    },
  });
}
