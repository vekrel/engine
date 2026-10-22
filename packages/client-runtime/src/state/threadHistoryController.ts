import type { MachineId, ThreadId } from "@vekrel/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";

import { threadKey } from "./entities.ts";

export type ThreadHistoryLoadEarlierResult =
  | { readonly _tag: "loaded" }
  | { readonly _tag: "noop" }
  | { readonly _tag: "busy" }
  | { readonly _tag: "error"; readonly message: string };

export type ThreadHistoryHandler = {
  readonly loadEarlier: (throughEntryId?: string) => Effect.Effect<ThreadHistoryLoadEarlierResult>;
};

export type ThreadHistoryRegistration = {
  readonly machineId: MachineId;
  readonly threadId: ThreadId;
  readonly handler: ThreadHistoryHandler;
};

export class ThreadHistoryController extends Context.Service<
  ThreadHistoryController,
  {
    readonly register: (
      machineId: MachineId,
      threadId: ThreadId,
      handler: ThreadHistoryHandler,
    ) => Effect.Effect<ThreadHistoryRegistration>;
    readonly unregister: (registration: ThreadHistoryRegistration) => Effect.Effect<void>;
    readonly loadEarlier: (
      machineId: MachineId,
      threadId: ThreadId,
      throughEntryId?: string,
    ) => Effect.Effect<ThreadHistoryLoadEarlierResult>;
  }
>()("@vekrel/client-runtime/state/threadHistoryController") {}

export const layer: Layer.Layer<ThreadHistoryController> = Layer.effect(
  ThreadHistoryController,
  Effect.gen(function* () {
    const handlers = yield* Ref.make(new Map<string, ThreadHistoryHandler>());
    return ThreadHistoryController.of({
      register: (machineId, threadId, handler) =>
        Ref.update(handlers, (current) => {
          const next = new Map(current);
          next.set(threadKey({ machineId, threadId }), handler);
          return next;
        }).pipe(
          Effect.as({
            machineId,
            threadId,
            handler,
          } satisfies ThreadHistoryRegistration),
        ),
      unregister: (registration) =>
        Ref.update(handlers, (current) => {
          const key = threadKey({
            machineId: registration.machineId,
            threadId: registration.threadId,
          });
          // Only remove when this registration still owns the slot. An older
          // fiber finalizer must not delete a newer handler for the same key.
          if (current.get(key) !== registration.handler) {
            return current;
          }
          const next = new Map(current);
          next.delete(key);
          return next;
        }),
      loadEarlier: (machineId, threadId, throughEntryId) =>
        Ref.get(handlers).pipe(
          Effect.flatMap((current) => {
            const handler = current.get(threadKey({ machineId, threadId }));
            if (handler === undefined) {
              return Effect.succeed({ _tag: "noop" } satisfies ThreadHistoryLoadEarlierResult);
            }
            return handler.loadEarlier(throughEntryId);
          }),
        ),
    });
  }),
);
