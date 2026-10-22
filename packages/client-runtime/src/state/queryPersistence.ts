import type { MachineId } from "@vekrel/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

export class MachineQueryStore extends Context.Reference<{
  readonly load: (
    machineId: MachineId,
    key: string,
  ) => Effect.Effect<Option.Option<string>>;
  readonly save: (
    machineId: MachineId,
    key: string,
    payload: string,
  ) => Effect.Effect<void>;
} | null>("@vekrel/client-runtime/MachineQueryStore", {
  defaultValue: () => null,
}) {}

export interface QueryPersistence<A> {
  readonly load: (machineId: MachineId, key: string) => Effect.Effect<Option.Option<A>>;
  readonly save: (machineId: MachineId, key: string, value: A) => Effect.Effect<void>;
}

export function persistQuery<A, I>(schema: Schema.Codec<A, I>): QueryPersistence<A> {
  const codec = Schema.fromJsonString(Schema.toCodecJson(schema));
  return {
    load: Effect.fn("queryPersistence.load")(function* (machineId, key) {
      const store = yield* MachineQueryStore;
      if (store === null) return Option.none<A>();
      const stored = yield* store.load(machineId, key);
      if (Option.isNone(stored)) return Option.none<A>();
      return yield* Schema.decodeUnknownEffect(codec)(stored.value).pipe(
        Effect.map(Option.some),
        Effect.catch(() => Effect.succeedNone),
      );
    }),
    save: Effect.fn("queryPersistence.save")(
      function* (machineId, key, value) {
        const store = yield* MachineQueryStore;
        if (store === null) return;
        const payload = yield* Schema.encodeEffect(codec)(value);
        yield* store.save(machineId, key, payload);
      },
      Effect.catch(() => Effect.logWarning("Offline query cache save failed")),
    ),
  };
}
