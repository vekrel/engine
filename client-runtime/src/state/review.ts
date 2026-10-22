import {
  type ReviewDiffPreviewInput,
  VcsUnsupportedOperationError,
  WS_METHODS,
} from "@vekrel/contracts";
import * as Effect from "effect/Effect";
import * as Semaphore from "effect/Semaphore";
import { request } from "../rpc/client.ts";
import { Atom } from "effect/reactivity";

import {
  createAtomCommandScheduler,
  createMachineRpcCommand,
  createMachineQueryAtomFamily,
  createMachineRpcQueryAtomFamily,
} from "./runtime.ts";
import type { MachineRegistry } from "../connection/registry.ts";

export function createReviewMachineAtoms<R, E>(
  runtime: Atom.AtomRuntime<MachineRegistry | R, E>,
) {
  const patchReads = Semaphore.makeUnsafe(4);
  const diffFileScheduler = createAtomCommandScheduler();
  return {
    diffPreview: createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:review:diff-preview",
      tag: WS_METHODS.reviewGetDiffPreview,
      staleTimeMs: 5_000,
    }),
    diffFilePatch: createMachineQueryAtomFamily(runtime, {
      label: "machine-data:review:diff-file-patch",
      staleTimeMs: 5 * 60_000,
      execute: (input: {
        request: ReviewDiffPreviewInput & { file: NonNullable<ReviewDiffPreviewInput["file"]> };
        cacheKey: string;
      }) =>
        request(WS_METHODS.reviewGetDiffPreview, input.request).pipe(
          patchReads.withPermit,
          Effect.flatMap((result) => {
            const source = result.sources.find(
              (source) => source.kind === input.request.file.sourceKind,
            );
            return source
              ? Effect.succeed(source)
              : Effect.fail(
                  new VcsUnsupportedOperationError({
                    operation: "review.diffFilePatch",
                    kind: "git",
                    detail: "Diff no longer available. Refresh the comparison.",
                  }),
                );
          }),
        ),
    }),
    diffFileContents: createMachineRpcCommand(runtime, {
      label: "machine-data:review:diff-file-contents",
      tag: WS_METHODS.reviewGetDiffFileContents,
      scheduler: diffFileScheduler,
      concurrency: {
        mode: "singleFlight",
        key: ({ machineId, input }) =>
          JSON.stringify([
            machineId,
            input.cwd,
            input.sourceKind,
            input.baseRef,
            input.headRef,
            input.oldPath,
            input.newPath,
            input.changeType,
          ]),
      },
    }),
  };
}
