import type { MachineId as MachineIdType } from "@vekrel/contracts";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as MachineRegistry from "../connection/registry.ts";
import type * as MachineSupervisor from "../connection/supervisor.ts";

export function runStreamInMachine<A, E, R>(
  machineId: MachineIdType,
  stream: Stream.Stream<A, E, R>,
): Stream.Stream<
  A,
  E | MachineRegistry.MachineNotRegisteredError,
  MachineRegistry.MachineRegistry | Exclude<R, MachineSupervisor.MachineSupervisor>
> {
  return Stream.unwrap(
    MachineRegistry.MachineRegistry.pipe(
      Effect.map((registry) => registry.runStream(machineId, stream)),
    ),
  );
}

export function followStreamInMachine<A, E, R>(
  machineId: MachineIdType,
  stream: Stream.Stream<A, E, R>,
): Stream.Stream<
  A,
  E,
  MachineRegistry.MachineRegistry | Exclude<R, MachineSupervisor.MachineSupervisor>
> {
  return Stream.unwrap(
    MachineRegistry.MachineRegistry.pipe(
      Effect.map((registry) => registry.followStream(machineId, stream)),
    ),
  );
}
