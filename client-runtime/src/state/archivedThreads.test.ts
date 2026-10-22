import { MachineId, type OrchestrationV2ArchivedShellSnapshot } from "@vekrel/contracts";
import * as Cause from "effect/Cause";
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity";
import { expect, it } from "vite-plus/test";

import {
  createArchivedThreadSnapshotsAtomFamily,
  makeArchivedThreadsMachineKey,
} from "./archivedThreads.ts";

it("does not expose an archived snapshot failure message", () => {
  const machineId = MachineId.make("env-sensitive");
  const snapshotsAtom = createArchivedThreadSnapshotsAtomFamily<Error>({
    getSnapshotAtom: () =>
      Atom.make(
        AsyncResult.failure<OrchestrationV2ArchivedShellSnapshot, Error>(
          Cause.fail(new Error("credential=secret-value")),
        ),
      ),
    labelPrefix: "test:archived-thread-snapshots",
  });
  const registry = AtomRegistry.make();

  expect(registry.get(snapshotsAtom(makeArchivedThreadsMachineKey([machineId])))).toEqual({
    snapshots: [],
    error: "Failed to load archived threads.",
    isLoading: false,
  });

  registry.dispose();
});
