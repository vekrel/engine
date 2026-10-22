import { type TerminalSummary, WS_METHODS } from "@vekrel/contracts";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { Atom } from "effect/reactivity";

import {
  createAtomCommandScheduler,
  createMachineRpcCommand,
  createMachineRpcSubscriptionAtomFamily,
  createMachineSubscriptionAtomFamily,
} from "./runtime.ts";
import type { MachineRegistry } from "../connection/registry.ts";
import { subscribe, subscribeDynamic, type MachineRpcInput } from "../rpc/client.ts";
import {
  applyTerminalAttachStreamEvent,
  applyTerminalMetadataStreamEvent,
  nextTerminalAttachSeedState,
} from "./terminalSession.ts";

export function createTerminalMachineAtoms<R, E>(
  runtime: Atom.AtomRuntime<MachineRegistry | R, E>,
) {
  const lifecycleScheduler = createAtomCommandScheduler();
  const resizeScheduler = createAtomCommandScheduler();
  const terminalThreadKey = ({
    machineId,
    input,
  }: {
    readonly machineId: string;
    readonly input: { readonly threadId: string; readonly terminalId?: string | undefined };
  }) => JSON.stringify([machineId, input.threadId]);
  const terminalSessionKey = ({
    machineId,
    input,
  }: {
    readonly machineId: string;
    readonly input: { readonly threadId: string; readonly terminalId?: string | undefined };
  }) => JSON.stringify([machineId, input.threadId, input.terminalId ?? null]);
  const lifecycleConcurrency = { mode: "serial" as const, key: terminalThreadKey };
  return {
    observe: createMachineSubscriptionAtomFamily(runtime, {
      label: "machine-data:terminal:observe",
      subscribe: (input: MachineRpcInput<typeof WS_METHODS.terminalObserve>) =>
        Stream.suspend(() =>
          subscribe(WS_METHODS.terminalObserve, input).pipe(
            Stream.scan(nextTerminalAttachSeedState, applyTerminalAttachStreamEvent),
          ),
        ),
    }),
    attach: createMachineSubscriptionAtomFamily(runtime, {
      label: "machine-data:terminal:attach",
      subscribe: (input: MachineRpcInput<typeof WS_METHODS.terminalAttach>) => {
        // Retry an unanswered first attach unchanged so it can still open the shell.
        let replied = false;
        return subscribeDynamic(WS_METHODS.terminalAttach, () =>
          Effect.sync(() => (replied ? { ...input, resume: true } : input)),
        ).pipe(
          Stream.onFirst(() =>
            Effect.sync(() => {
              replied = true;
            }),
          ),
          Stream.scan(nextTerminalAttachSeedState, applyTerminalAttachStreamEvent),
        );
      },
    }),
    events: createMachineRpcSubscriptionAtomFamily(runtime, {
      label: "machine-data:terminal:events",
      tag: WS_METHODS.subscribeTerminalEvents,
    }),
    metadata: createMachineSubscriptionAtomFamily(runtime, {
      label: "machine-data:terminal:metadata",
      subscribe: (_input: null) =>
        subscribe(WS_METHODS.subscribeTerminalMetadata, {}).pipe(
          Stream.scan((): ReadonlyArray<TerminalSummary> => [], applyTerminalMetadataStreamEvent),
          // The scan seed is not an authoritative empty server snapshot.
          Stream.drop(1),
        ),
    }),
    open: createMachineRpcCommand(runtime, {
      label: "machine-data:terminal:open",
      tag: WS_METHODS.terminalOpen,
      scheduler: lifecycleScheduler,
      concurrency: lifecycleConcurrency,
    }),
    write: createMachineRpcCommand(runtime, {
      label: "machine-data:terminal:write",
      tag: WS_METHODS.terminalWrite,
    }),
    resize: createMachineRpcCommand(runtime, {
      label: "machine-data:terminal:resize",
      tag: WS_METHODS.terminalResize,
      scheduler: resizeScheduler,
      concurrency: { mode: "latest", key: terminalSessionKey },
    }),
    clear: createMachineRpcCommand(runtime, {
      label: "machine-data:terminal:clear",
      tag: WS_METHODS.terminalClear,
      scheduler: lifecycleScheduler,
      concurrency: lifecycleConcurrency,
    }),
    restart: createMachineRpcCommand(runtime, {
      label: "machine-data:terminal:restart",
      tag: WS_METHODS.terminalRestart,
      scheduler: lifecycleScheduler,
      concurrency: lifecycleConcurrency,
    }),
    close: createMachineRpcCommand(runtime, {
      label: "machine-data:terminal:close",
      tag: WS_METHODS.terminalClose,
      scheduler: lifecycleScheduler,
      concurrency: lifecycleConcurrency,
    }),
  };
}

export * from "./terminalSession.ts";
