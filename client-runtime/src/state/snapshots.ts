import type { MachineId, OrchestrationV2ShellSnapshot } from "@vekrel/contracts";
import * as Option from "effect/Option";
import { AsyncResult, Atom } from "effect/reactivity";

import type { MachineShellState } from "./shell.ts";

export function createMachineSnapshotAtom<E>(
  shellStateAtom: (
    machineId: MachineId,
  ) => Atom.Atom<AsyncResult.AsyncResult<MachineShellState, E>>,
) {
  return Atom.family((machineId: MachineId) =>
    Atom.make((get): OrchestrationV2ShellSnapshot | null =>
      Option.match(AsyncResult.value(get(shellStateAtom(machineId))), {
        onNone: () => null,
        onSome: (state) => Option.getOrNull(state.snapshot),
      }),
    ).pipe(Atom.withLabel(`machine-snapshot:${machineId}`)),
  );
}
