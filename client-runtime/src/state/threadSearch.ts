import {
  MachineId,
  OrchestrationSearchThreadsInput,
  type OrchestrationSearchThreadsResult,
  type OrchestrationThreadSearchMatch,
} from "@vekrel/contracts";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { AsyncResult, Atom } from "effect/reactivity";

export interface MachineThreadSearchMatch extends OrchestrationThreadSearchMatch {
  readonly machineId: MachineId;
}

export interface ThreadSearchResultsState {
  readonly matches: ReadonlyArray<MachineThreadSearchMatch>;
  readonly isLoading: boolean;
}

const ThreadSearchKey = Schema.fromJsonString(
  Schema.Tuple([Schema.Array(MachineId), OrchestrationSearchThreadsInput.fields.query]),
);
const decodeThreadSearchKey = Schema.decodeUnknownOption(ThreadSearchKey);

export function makeThreadSearchKey(
  machineIds: ReadonlyArray<MachineId>,
  query: string,
): string {
  return JSON.stringify([
    [...machineIds].sort((left, right) => left.localeCompare(right)),
    query,
  ]);
}

function parseThreadSearchKey(key: string) {
  return decodeThreadSearchKey(key);
}

export function threadSearchMatchKey(
  match: Pick<MachineThreadSearchMatch, "machineId" | "threadId">,
): string {
  return JSON.stringify([match.machineId, match.threadId]);
}

export function createThreadSearchResultsAtomFamily<E>(options: {
  readonly getSearchAtom: (
    machineId: MachineId,
    query: string,
  ) => Atom.Atom<AsyncResult.AsyncResult<OrchestrationSearchThreadsResult, E>>;
  readonly labelPrefix: string;
}) {
  return Atom.family((key: string) =>
    Atom.make((get): ThreadSearchResultsState => {
      const parsedKey = parseThreadSearchKey(key);
      if (Option.isNone(parsedKey)) {
        return { matches: [], isLoading: false };
      }

      const [machineIds, query] = parsedKey.value;
      const matches: MachineThreadSearchMatch[] = [];
      let isLoading = false;

      for (const machineId of machineIds) {
        const result = get(options.getSearchAtom(machineId, query));
        isLoading ||= result.waiting;
        const value = Option.getOrNull(AsyncResult.value(result));
        if (value !== null) {
          matches.push(
            ...value.matches.map((match) => ({
              ...match,
              machineId,
            })),
          );
        }
      }

      return { matches, isLoading };
    }).pipe(Atom.withLabel(`${options.labelPrefix}:${key}`)),
  );
}
