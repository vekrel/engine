import { WS_METHODS } from "@vekrel/contracts";
import * as Stream from "effect/Stream";
import { Atom } from "effect/reactivity";

import type { MachineRegistry } from "../connection/registry.ts";
import {
  createAtomCommandScheduler,
  createMachineRpcCommand,
  createMachineRpcQueryAtomFamily,
  createMachineRpcSubscriptionAtomFamily,
} from "./runtime.ts";

export function createPreviewMachineAtoms<R, E>(
  runtime: Atom.AtomRuntime<MachineRegistry | R, E>,
) {
  const lifecycleScheduler = createAtomCommandScheduler();
  const statusScheduler = createAtomCommandScheduler();
  const lifecycleConcurrency = {
    mode: "serial" as const,
    key: ({ machineId, input }: { machineId: string; input: { threadId: string } }) =>
      JSON.stringify([machineId, input.threadId]),
  };
  return {
    list: createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:preview:list",
      tag: WS_METHODS.previewList,
      staleTimeMs: 5_000,
    }),
    events: createMachineRpcSubscriptionAtomFamily(runtime, {
      label: "machine-data:preview:events",
      tag: WS_METHODS.subscribePreviewEvents,
      // Stream atoms publish only the last value per chunk; every preview event must arrive.
      transform: (stream) => stream.pipe(Stream.rechunk(1)),
    }),
    discoveredServers: createMachineRpcSubscriptionAtomFamily(runtime, {
      label: "machine-data:preview:discovered-servers",
      tag: WS_METHODS.subscribeDiscoveredLocalServers,
      // Configured URLs are part of this atom's key. Dispose immediately so
      // unmounted projects stop contributing probe candidates on the server.
      idleTtlMs: 0,
    }),
    open: createMachineRpcCommand(runtime, {
      label: "machine-data:preview:open",
      tag: WS_METHODS.previewOpen,
      scheduler: lifecycleScheduler,
      concurrency: lifecycleConcurrency,
    }),
    navigate: createMachineRpcCommand(runtime, {
      label: "machine-data:preview:navigate",
      tag: WS_METHODS.previewNavigate,
      scheduler: lifecycleScheduler,
      concurrency: lifecycleConcurrency,
    }),
    resize: createMachineRpcCommand(runtime, {
      label: "machine-data:preview:resize",
      tag: WS_METHODS.previewResize,
      scheduler: lifecycleScheduler,
      concurrency: lifecycleConcurrency,
    }),
    adjust: createMachineRpcCommand(runtime, {
      label: "machine-data:preview:adjust",
      tag: WS_METHODS.previewAdjust,
      scheduler: lifecycleScheduler,
      concurrency: lifecycleConcurrency,
    }),
    refresh: createMachineRpcCommand(runtime, {
      label: "machine-data:preview:refresh",
      tag: WS_METHODS.previewRefresh,
      scheduler: lifecycleScheduler,
      concurrency: lifecycleConcurrency,
    }),
    close: createMachineRpcCommand(runtime, {
      label: "machine-data:preview:close",
      tag: WS_METHODS.previewClose,
      scheduler: lifecycleScheduler,
      concurrency: lifecycleConcurrency,
    }),
    clearProfile: createMachineRpcCommand(runtime, {
      label: "machine-data:preview:clear-profile",
      tag: WS_METHODS.previewClearProfile,
    }),
    reportProfiles: createMachineRpcCommand(runtime, {
      label: "machine-data:preview:report-profiles",
      tag: WS_METHODS.previewReportProfiles,
      concurrency: { mode: "latest", key: ({ machineId }) => machineId },
    }),
    reportStatus: createMachineRpcCommand(runtime, {
      label: "machine-data:preview:report-status",
      tag: WS_METHODS.previewReportStatus,
      scheduler: statusScheduler,
      concurrency: {
        mode: "latest",
        key: ({ machineId, input }) =>
          JSON.stringify([machineId, input.threadId, input.tabId]),
      },
    }),
  };
}
