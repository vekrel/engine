import { ORCHESTRATION_V2_WS_METHODS, type ThreadId } from "@vekrel/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import type { HttpClient } from "effect/http";
import { Atom } from "effect/reactivity";

import { applyCommandOutputFrame, EMPTY_COMMAND_OUTPUT } from "./commandOutput.ts";

import {
  createMachineRpcCommand,
  createMachineRpcQueryAtomFamily,
  createMachineRpcSubscriptionAtomFamily,
  createMachineCommand,
} from "./runtime.ts";
import type { MachineRegistry } from "../connection/registry.ts";
import * as MachineSupervisor from "../connection/supervisor.ts";
import * as RemoteMachineAuthorization from "../authorization/service.ts";
import * as ManagedRelayDpopSigner from "../relay/managedRelay.ts";
import { MachineRpcUnavailableError } from "../rpc/client.ts";
import { fetchMachineThreadTranscript } from "./threadTranscriptHttp.ts";

export function createOrchestrationMachineAtoms<R, E>(
  runtime: Atom.AtomRuntime<MachineRegistry | HttpClient.HttpClient | R, E>,
) {
  return {
    threadTranscript: createMachineCommand(runtime, {
      label: "machine-data:orchestration:thread-transcript",
      execute: Effect.fn("orchestration.execute")(function* (input: {
        readonly threadId: ThreadId;
      }) {
        const supervisor = yield* MachineSupervisor.MachineSupervisor;
        const prepared = yield* SubscriptionRef.get(supervisor.prepared);
        if (Option.isNone(prepared)) {
          return yield* new MachineRpcUnavailableError({
            machineId: supervisor.target.machineId,
            message: "The source machine is not connected.",
          });
        }
        return yield* fetchMachineThreadTranscript({
          prepared: prepared.value,
          threadId: input.threadId,
          signer: yield* Effect.serviceOption(ManagedRelayDpopSigner.ManagedRelayDpopSigner),
          remoteAuthorization: yield* Effect.serviceOption(
            RemoteMachineAuthorization.RemoteMachineAuthorization,
          ),
        });
      }),
    }),
    v2: {
      dispatchCommand: createMachineRpcCommand(runtime, {
        label: "machine-data:orchestration-v2:dispatch-command",
        tag: ORCHESTRATION_V2_WS_METHODS.dispatchCommand,
      }),
      threadProjection: createMachineRpcQueryAtomFamily(runtime, {
        label: "machine-data:orchestration-v2:thread-projection",
        tag: ORCHESTRATION_V2_WS_METHODS.getThreadProjection,
        staleTimeMs: 0,
        idleTtlMs: 0,
      }),
      shell: createMachineRpcSubscriptionAtomFamily(runtime, {
        label: "machine-data:orchestration-v2:shell",
        tag: ORCHESTRATION_V2_WS_METHODS.subscribeShell,
      }),
      thread: createMachineRpcSubscriptionAtomFamily(runtime, {
        label: "machine-data:orchestration-v2:thread",
        tag: ORCHESTRATION_V2_WS_METHODS.subscribeThread,
        idleTtlMs: 0,
      }),
      // One expanded command row's output. Mounted only while the row is open,
      // and released as soon as it closes, so hidden rows receive nothing.
      commandOutput: createMachineRpcSubscriptionAtomFamily(runtime, {
        label: "machine-data:orchestration-v2:command-output",
        tag: ORCHESTRATION_V2_WS_METHODS.subscribeCommandOutput,
        idleTtlMs: 0,
        // `scan` emits its seed first; skipping it keeps the row "loading" until the first frame.
        transform: (frames) =>
          frames.pipe(
            Stream.scan(() => EMPTY_COMMAND_OUTPUT, applyCommandOutputFrame),
            Stream.filter((view) => view !== EMPTY_COMMAND_OUTPUT),
          ),
      }),
    },
    turnDiff: createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:orchestration:turn-diff",
      tag: ORCHESTRATION_V2_WS_METHODS.getTurnDiff,
    }),
    workflowScript: createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:orchestration:workflow-script",
      tag: ORCHESTRATION_V2_WS_METHODS.getWorkflowScript,
      staleTimeMs: 300_000,
      idleTtlMs: 300_000,
    }),
    stopWorkflow: createMachineRpcCommand(runtime, {
      label: "machine-data:orchestration:stop-workflow",
      tag: ORCHESTRATION_V2_WS_METHODS.stopWorkflow,
    }),
    turnItem: createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:orchestration:turn-item",
      tag: ORCHESTRATION_V2_WS_METHODS.getTurnItem,
      staleTimeMs: 60_000,
      idleTtlMs: 60_000,
    }),
    fullThreadDiff: createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:orchestration:full-thread-diff",
      tag: ORCHESTRATION_V2_WS_METHODS.getFullThreadDiff,
    }),
    threadFind: createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:orchestration:thread-find",
      tag: ORCHESTRATION_V2_WS_METHODS.searchThread,
      staleTimeMs: 0,
      idleTtlMs: 0,
    }),
    threadFindProgressive: createMachineRpcSubscriptionAtomFamily(runtime, {
      label: "machine-data:orchestration:thread-find-progressive",
      tag: ORCHESTRATION_V2_WS_METHODS.searchThreadStream,
      completeWhen: (result) => result.complete !== false,
      idleTtlMs: 0,
    }),
    threadSearch: createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:orchestration:thread-search",
      tag: ORCHESTRATION_V2_WS_METHODS.searchThreads,
      staleTimeMs: 30_000,
      idleTtlMs: 60_000,
    }),
    archivedShellSnapshot: createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:orchestration:archived-shell-snapshot",
      tag: ORCHESTRATION_V2_WS_METHODS.getArchivedShellSnapshot,
    }),
  };
}
