import type { ThreadId } from "@vekrel/contracts";
import * as KeyedLock from "@vekrel/shared/KeyedLock";
import * as Context from "effect/Context";
import * as Layer from "effect/Layer";

export class ThreadCommandExecutor extends Context.Service<
  ThreadCommandExecutor,
  KeyedLock.KeyedLock<ThreadId>
>()("vekrel-engine/orchestration-v2/ThreadCommandExecutor") {}

export const layer = Layer.effect(ThreadCommandExecutor, KeyedLock.make<ThreadId>());
