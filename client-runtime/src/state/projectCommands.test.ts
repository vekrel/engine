import { describe, expect, it } from "@effect/vitest";
import {
  AuthFilesystemWriteScope,
  MachineId,
  ProjectId,
  ProjectReadFileInput,
  ProjectReadFileResult,
  ThreadId,
  type AuthSessionState,
  type ScopedProjectRef,
  WS_METHODS,
} from "@vekrel/contracts";
import { vi } from "vite-plus/test";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as Stream from "effect/Stream";
import { Atom, AtomRegistry, AsyncResult } from "effect/reactivity";
import { AVAILABLE_CONNECTION_STATE } from "../connection/model.ts";

import * as MachineRegistry from "../connection/registry.ts";
import * as MachineSupervisor from "../connection/supervisor.ts";
import type { RpcSession } from "../rpc/session.ts";
import type { MachineProject } from "./models.ts";
import { createProjectMachineAtoms } from "./projectCommands.ts";

const decodeReadFileInput = Schema.decodeUnknownSync(ProjectReadFileInput);
const decodeReadFileResult = Schema.decodeUnknownSync(ProjectReadFileResult);
const MACHINE_ID = MachineId.make("machine-1");
vi.mock("./session.ts", () => ({
  createMachineSessionAtoms: () => ({ sessionStateAtom: sessions }),
}));
const sessions = Atom.family((_id: MachineId) =>
  Atom.make<AsyncResult.AsyncResult<AuthSessionState>>(AsyncResult.initial()),
);
const PROJECT_ID = ProjectId.make("scratch");
const PROJECT = {
  id: PROJECT_ID,
  machineId: MACHINE_ID,
  workspaceRoot: "/scratch",
} as MachineProject;

const makeHarness = Effect.fn("TestProjectCommands.makeHarness")(function* () {
  const metadataRequests: string[][] = [];
  const file = { byteLength: 5 };
  const readResponse = {
    wait: Effect.void as Effect.Effect<void>,
    missing: false,
  };
  const supervisor = MachineSupervisor.MachineSupervisor.of({
    target: { machineId: MACHINE_ID },
    session: yield* SubscriptionRef.make(
      Option.some({
        client: {
          [WS_METHODS.projectsEnsureScratch]: () => Effect.succeed({ projectId: PROJECT_ID }),
          [WS_METHODS.projectsReadFile]: () =>
            Effect.gen(function* () {
              const byteLength = file.byteLength;
              yield* readResponse.wait;
              return decodeReadFileResult(
                readResponse.missing
                  ? {
                      relativePath: "foo",
                      byteLength: 0,
                      contents: "",
                      truncated: false,
                      exists: false,
                    }
                  : { relativePath: "foo", byteLength, contents: "old", truncated: false },
              );
            }),
          [WS_METHODS.projectsWriteFile]: () =>
            Effect.sync(() => {
              file.byteLength = 10;
              return { relativePath: "foo" };
            }),
          [WS_METHODS.filesystemGetMetadata]: (input: { paths: string[] }) =>
            Effect.sync(() => {
              metadataRequests.push(input.paths);
              return {
                entries: input.paths.map(() =>
                  readResponse.missing
                    ? null
                    : { kind: "file" as const, byteLength: file.byteLength },
                ),
              };
            }),
        },
      } as unknown as RpcSession),
    ),
    state: yield* SubscriptionRef.make({
      ...AVAILABLE_CONNECTION_STATE,
      phase: "connected" as const,
    }),
  } as MachineSupervisor.MachineSupervisor["Service"]);
  const runtime = Atom.runtime(
    Layer.mergeAll(
      Layer.succeed(MachineRegistry.MachineRegistry, {
        run: (_machineId, effect) =>
          Effect.provideService(effect, MachineSupervisor.MachineSupervisor, supervisor),
        followStream: (_machineId, stream) =>
          Stream.provideService(stream, MachineSupervisor.MachineSupervisor, supervisor),
      } as MachineRegistry.MachineRegistry["Service"]),
      Layer.succeed(
        Crypto.Crypto,
        Crypto.make({
          randomBytes: (size) => new Uint8Array(size),
          digest: (_algorithm, data) => Effect.succeed(data),
        }),
      ),
    ),
  );
  const storedProject = Atom.make<MachineProject | null>(null);
  const commands = createProjectMachineAtoms(runtime, {
    projectAtom: (ref: ScopedProjectRef) =>
      ref.machineId === MACHINE_ID && ref.projectId === PROJECT_ID
        ? storedProject
        : Atom.make(null),
  });
  const registry = AtomRegistry.make();
  yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()));
  registry.set(
    sessions(MACHINE_ID),
    AsyncResult.success({
      authenticated: true,
      auth: {
        policy: "remote-reachable" as const,
        bootstrapMethods: [],
        sessionMethods: [],
        sessionCookieName: "test",
      },
      scopes: [AuthFilesystemWriteScope],
      permissions: [AuthFilesystemWriteScope],
    }),
  );
  const openScratch = Effect.promise(() =>
    commands.openScratch.run(registry, { machineId: MACHINE_ID, input: {} }),
  );
  return { registry, storedProject, openScratch, commands, metadataRequests, readResponse };
});

describe("writeFile metadata", () => {
  it.effect("preserves optional read results without seeding metadata for an absent file", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness();
      h.readResponse.missing = true;
      const input = { cwd: "/repo", relativePath: "foo", allowMissing: true };
      expect.soft(decodeReadFileInput(input)).toEqual(input);
      const result = yield* AtomRegistry.getResult(
        h.registry,
        h.commands.readFile({ machineId: MACHINE_ID, input }),
        { suspendOnWaiting: true },
      );
      expect.soft(result).toEqual({
        relativePath: "foo",
        byteLength: 0,
        contents: "",
        truncated: false,
        exists: false,
      });
      const metadata = h.commands.fileMetadata({
        machineId: MACHINE_ID,
        input: { path: "/repo/foo" },
      });
      expect(
        yield* AtomRegistry.getResult(h.registry, metadata, { suspendOnWaiting: true }),
      ).toBeNull();
      expect(h.metadataRequests).toEqual([["/repo/foo"]]);
    }).pipe(Effect.scoped),
  );

  it.effect("does not restore old metadata from a read completed after a write", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness();
      const threadId = ThreadId.make("stale-read-thread");
      h.registry.mount(h.commands.fileMetadataThread({ machineId: MACHINE_ID, threadId }));
      const metadata = h.commands.fileMetadata({
        machineId: MACHINE_ID,
        input: { path: "/repo/foo", threadId },
      });
      h.registry.mount(metadata);
      const readMetadata = () =>
        AtomRegistry.getResult(h.registry, metadata, { suspendOnWaiting: true });
      yield* readMetadata();
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      h.readResponse.wait = Deferred.succeed(started, undefined).pipe(
        Effect.andThen(Deferred.await(release)),
      );
      const fileRead = yield* AtomRegistry.getResult(
        h.registry,
        h.commands.readFile({
          machineId: MACHINE_ID,
          input: { cwd: "/repo", relativePath: "foo" },
        }),
        { suspendOnWaiting: true },
      ).pipe(Effect.forkChild);
      yield* Deferred.await(started);
      const write = yield* Effect.promise(() =>
        h.commands.writeFile.run(h.registry, {
          machineId: MACHINE_ID,
          input: { cwd: "/repo", relativePath: "foo", contents: "new" },
        }),
      );
      expect(write._tag).toBe("Success");
      expect(yield* readMetadata()).toEqual({ kind: "file", byteLength: 10 });
      yield* Deferred.succeed(release, undefined);
      expect((yield* Fiber.join(fileRead)).byteLength).toBe(5);
      h.registry.refresh(metadata);
      expect(yield* readMetadata()).toEqual({ kind: "file", byteLength: 10 });
      expect(h.metadataRequests).toHaveLength(2);
    }).pipe(Effect.scoped),
  );

  it.effect("refreshes retained canonical and original paths after an aliased write", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness();
      const threadId = ThreadId.make("write-metadata-thread");
      h.registry.mount(h.commands.fileMetadataThread({ machineId: MACHINE_ID, threadId }));
      const atoms = ["/repo/foo", "/repo/./foo"].map((path) =>
        h.commands.fileMetadata({ machineId: MACHINE_ID, input: { path, threadId } }),
      );
      for (const atom of atoms) h.registry.mount(atom);
      const read = () =>
        Effect.forEach(
          atoms,
          (atom) => AtomRegistry.getResult(h.registry, atom, { suspendOnWaiting: true }),
          { concurrency: "unbounded" },
        );
      expect(yield* read()).toEqual([
        { kind: "file", byteLength: 5 },
        { kind: "file", byteLength: 5 },
      ]);
      const result = yield* Effect.promise(() =>
        h.commands.writeFile.run(h.registry, {
          machineId: MACHINE_ID,
          input: { cwd: "/repo", relativePath: "./foo", contents: "new" },
        }),
      );
      expect(result).toMatchObject({ _tag: "Success", value: { relativePath: "foo" } });
      expect(yield* read()).toEqual([
        { kind: "file", byteLength: 10 },
        { kind: "file", byteLength: 10 },
      ]);
      expect(h.metadataRequests.flat()).toEqual([
        "/repo/foo",
        "/repo/./foo",
        "/repo/foo",
        "/repo/./foo",
      ]);
    }).pipe(Effect.scoped),
  );
});

describe("openScratch", () => {
  it.effect("resolves once the created project reaches the client store", () =>
    Effect.gen(function* () {
      const { registry, storedProject, openScratch } = yield* makeHarness();
      const opening = yield* Effect.forkChild(openScratch);
      yield* Effect.yieldNow;
      registry.set(storedProject, PROJECT);
      const result = yield* Fiber.join(opening);
      expect(result).toMatchObject({ _tag: "Success", value: PROJECT });
    }).pipe(Effect.scoped),
  );
});
