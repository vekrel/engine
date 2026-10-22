import type { OrchestrationV2ThreadProjection } from "@vekrel/contracts";
import * as Option from "effect/Option";

import { EMPTY_THREAD_HISTORY_META, type ThreadHistoryMeta } from "./threadHistoryMerge.ts";

export type MachineThreadStatus = "empty" | "cached" | "synchronizing" | "live" | "deleted";

export interface MachineThreadState {
  readonly data: Option.Option<OrchestrationV2ThreadProjection>;
  readonly status: MachineThreadStatus;
  readonly error: Option.Option<string>;
  /**
   * Errors here are thread/history-local and must not be promoted into the machine
   * disconnect path.
   */
  readonly history: ThreadHistoryMeta;
}

export const EMPTY_MACHINE_THREAD_STATE: MachineThreadState = {
  data: Option.none(),
  status: "empty",
  error: Option.none(),
  history: EMPTY_THREAD_HISTORY_META,
};
