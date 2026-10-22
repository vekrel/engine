import {
  type MachineId,
  ProjectId,
  type ProjectReadFileResult,
  type ScopedProjectRef,
  WS_METHODS,
} from "@vekrel/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { Atom, AtomRegistry } from "effect/reactivity";

import {
  type MachineRpcInput,
  type MachineRpcSuccess,
  request,
  requestGuarded,
} from "../rpc/client.ts";
import type { MachineProject } from "./models.ts";
import {
  createAtomCommandScheduler,
  createMachineCommand,
  createMachineRpcCommand,
  createMachineRpcQueryAtomFamily,
} from "./runtime.ts";
import {
  type CreateProjectInput,
  type DeleteProjectInput,
  type UpdateProjectInput,
  createProject,
  deleteProject,
  updateProject,
} from "../operations/commands.ts";
import type { MachineRegistry } from "../connection/registry.ts";
import { createFileMetadataAtoms } from "./fileMetadata.ts";
import { resolveWorkspaceFilePath } from "@vekrel/shared/path";

export type {
  CreateProjectInput,
  DeleteProjectInput,
  UpdateProjectInput,
} from "../operations/commands.ts";

export interface OptimisticProjectFile {
  readonly data: ProjectReadFileResult;
  readonly confirmedAgainst: object | null | undefined;
}

export interface OptimisticProjectFileTarget {
  readonly machineId: MachineId;
  readonly cwd: string;
  readonly relativePath: string;
}

function optimisticProjectFileKey(target: OptimisticProjectFileTarget): string {
  return JSON.stringify([target.machineId, target.cwd, target.relativePath]);
}

function writtenFilePaths(
  input: MachineRpcInput<typeof WS_METHODS.projectsWriteFile>,
  result: MachineRpcSuccess<typeof WS_METHODS.projectsWriteFile>,
) {
  return new Set(
    [result.relativePath, input.relativePath].map((path) =>
      resolveWorkspaceFilePath(path, input.cwd),
    ),
  );
}

export class ScratchProjectNotLoadedError extends Schema.TaggedError<ScratchProjectNotLoadedError>()(
  "ScratchProjectNotLoadedError",
  { projectId: ProjectId },
) {
  override get message(): string {
    return "The folder for threads without a project has not reached this device yet. Try again.";
  }
}

export function createProjectMachineAtoms<R, E>(
  runtime: Atom.AtomRuntime<MachineRegistry | Crypto.Crypto | R, E>,
  options: {
    readonly projectAtom: (ref: ScopedProjectRef) => Atom.Atom<MachineProject | null>;
  },
) {
  const fileMetadata = createFileMetadataAtoms(runtime);
  const projectScheduler = createAtomCommandScheduler();
  const fileScheduler = createAtomCommandScheduler();
  const optimisticFileFamily = Atom.family((key: string) =>
    Atom.make<OptimisticProjectFile | null>(null).pipe(
      Atom.withLabel(`machine-data:projects:optimistic-file:${key}`),
    ),
  );
  const projectConcurrency = {
    mode: "serial" as const,
    key: ({ machineId, input }: { machineId: string; input: { projectId: string } }) =>
      JSON.stringify([machineId, input.projectId]),
  };
  return {
    fileMetadata: fileMetadata.metadata,
    fileMetadataThread: fileMetadata.retainThread,
    invalidateFileMetadata: createMachineCommand(runtime, {
      label: "machine-data:filesystem:invalidate-metadata",
      execute: (input: { paths: ReadonlyArray<string> }, registry, machineId) =>
        Effect.forEach([...new Set(input.paths)], (path) =>
          fileMetadata
            .invalidate(path)
            .pipe(
              Effect.tap(() =>
                Effect.sync(() => fileMetadata.refreshPath(machineId, path, registry)),
              ),
            ),
        ),
    }),
    searchEntries: createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:projects:search-entries",
      tag: WS_METHODS.projectsSearchEntries,
      staleTimeMs: 15_000,
      execute: (input) =>
        fileMetadata.seedAfterRead(
          request(WS_METHODS.projectsSearchEntries, input),
          (result, isCurrent) => fileMetadata.rememberEntries(input.cwd, result.entries, isCurrent),
        ),
    }),
    listEntries: createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:projects:list-entries",
      tag: WS_METHODS.projectsListEntries,
      staleTimeMs: 30_000,
      idleTtlMs: 5 * 60_000,
      execute: (input) =>
        fileMetadata.seedAfterRead(
          request(WS_METHODS.projectsListEntries, input),
          (result, isCurrent) => fileMetadata.rememberEntries(input.cwd, result.entries, isCurrent),
        ),
    }),
    listIgnoredFiles: createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:projects:list-ignored-files",
      tag: WS_METHODS.projectsListIgnoredFiles,
      staleTimeMs: 15_000,
    }),
    readFile: createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:projects:read-file",
      tag: WS_METHODS.projectsReadFile,
      staleTimeMs: 30_000,
      idleTtlMs: 5 * 60_000,
      execute: (input) =>
        fileMetadata.seedAfterRead(
          request(WS_METHODS.projectsReadFile, input),
          (result, isCurrent) =>
            result.exists === false
              ? Effect.void
              : fileMetadata.rememberFile(input.cwd, result, isCurrent),
        ),
    }),
    optimisticFile: (target: OptimisticProjectFileTarget) =>
      optimisticFileFamily(optimisticProjectFileKey(target)),
    create: createMachineCommand(runtime, {
      label: "machine-data:commands:project:create",
      execute: (input: CreateProjectInput) => createProject(input),
      scheduler: projectScheduler,
      concurrency: projectConcurrency,
    }),
    update: createMachineCommand(runtime, {
      label: "machine-data:commands:project:update",
      execute: (input: UpdateProjectInput) => updateProject(input),
      scheduler: projectScheduler,
      concurrency: projectConcurrency,
    }),
    delete: createMachineCommand(runtime, {
      label: "machine-data:commands:project:delete",
      execute: (input: DeleteProjectInput) => deleteProject(input),
      scheduler: projectScheduler,
      concurrency: projectConcurrency,
    }),
    // Finds or creates the machine's Scratch project and resolves once the
    // project is in the client store, since drafts key off its stored path.
    openScratch: createMachineCommand(runtime, {
      label: "machine-data:projects:open-scratch",
      execute: (
        input: MachineRpcInput<typeof WS_METHODS.projectsEnsureScratch>,
        registry,
        machineId,
      ) =>
        request(WS_METHODS.projectsEnsureScratch, input).pipe(
          Effect.flatMap(({ projectId }) =>
            AtomRegistry.toStream(registry, options.projectAtom({ machineId, projectId })).pipe(
              Stream.filter(Predicate.isNotNull),
              Stream.runHead,
              Effect.timeoutOption("10 seconds"),
              Effect.map(Option.flatten),
              Effect.flatMap(
                Option.match({
                  onSome: Effect.succeed,
                  onNone: () => Effect.fail(new ScratchProjectNotLoadedError({ projectId })),
                }),
              ),
            ),
          ),
        ),
      scheduler: projectScheduler,
      concurrency: { mode: "serial", key: ({ machineId }) => machineId },
    }),
    createNew: createMachineRpcCommand(runtime, {
      label: "machine-data:projects:create-new",
      tag: WS_METHODS.projectsCreateNew,
      scheduler: projectScheduler,
      concurrency: { mode: "serial", key: ({ machineId }) => machineId },
    }),
    writeFile: createMachineRpcCommand(runtime, {
      label: "machine-data:projects:write-file",
      tag: WS_METHODS.projectsWriteFile,
      scheduler: fileScheduler,
      concurrency: {
        mode: "serial",
        key: ({ machineId, input }) =>
          JSON.stringify([machineId, input.cwd, input.relativePath]),
      },
      execute: (input) =>
        requestGuarded(WS_METHODS.projectsWriteFile, input).pipe(
          Effect.tap((result) =>
            Effect.forEach(writtenFilePaths(input, result), fileMetadata.invalidate),
          ),
        ),
      onSuccess: ({ machineId, input }, registry, result) =>
        Effect.sync(() => {
          for (const path of writtenFilePaths(input, result))
            fileMetadata.refreshPath(machineId, path, registry);
        }),
    }),
  };
}
