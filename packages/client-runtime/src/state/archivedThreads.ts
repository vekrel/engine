import { MachineId, type OrchestrationV2ArchivedShellSnapshot } from "@vekrel/contracts";
import * as Arr from "effect/Array";
import { pipe } from "effect/Function";
import * as Option from "effect/Option";
import * as Order from "effect/Order";
import { AsyncResult, Atom } from "effect/reactivity";

export interface ArchivedSnapshotEntry {
  readonly machineId: MachineId;
  readonly snapshot: OrchestrationV2ArchivedShellSnapshot;
}

export interface ArchivedThreadSnapshotsState {
  readonly snapshots: ReadonlyArray<ArchivedSnapshotEntry>;
  readonly error: string | null;
  readonly isLoading: boolean;
}

const ARCHIVED_THREADS_MACHINE_KEY_SEPARATOR = "\u001f";
const machineIdOrder = Order.String as Order.Order<MachineId>;

export function makeArchivedThreadsMachineKey(
  machineIds: ReadonlyArray<MachineId>,
): string {
  return pipe(machineIds, Arr.sort(machineIdOrder), (sortedMachineIds) =>
    sortedMachineIds.join(ARCHIVED_THREADS_MACHINE_KEY_SEPARATOR),
  );
}

function parseArchivedThreadsMachineKey(key: string): ReadonlyArray<MachineId> {
  if (key.length === 0) {
    return [];
  }
  return pipe(
    key.split(ARCHIVED_THREADS_MACHINE_KEY_SEPARATOR),
    Arr.map((machineId) => MachineId.make(machineId)),
  );
}

export function createArchivedThreadSnapshotsAtomFamily<E>(options: {
  readonly getSnapshotAtom: (
    machineId: MachineId,
  ) => Atom.Atom<AsyncResult.AsyncResult<OrchestrationV2ArchivedShellSnapshot, E>>;
  readonly labelPrefix: string;
}) {
  return Atom.family((machineKey: string) =>
    Atom.make((get): ArchivedThreadSnapshotsState => {
      const snapshots: ArchivedSnapshotEntry[] = [];
      let error: string | null = null;
      let isLoading = false;

      for (const machineId of parseArchivedThreadsMachineKey(machineKey)) {
        const result = get(options.getSnapshotAtom(machineId));
        isLoading ||= result.waiting;

        const snapshot = Option.getOrNull(AsyncResult.value(result));
        if (snapshot !== null) {
          snapshots.push({ machineId, snapshot });
        }

        if (error === null && result._tag === "Failure") {
          error = "Failed to load archived threads.";
        }
      }

      return { snapshots, error, isLoading };
    }).pipe(Atom.withLabel(`${options.labelPrefix}:${machineKey}`)),
  );
}
