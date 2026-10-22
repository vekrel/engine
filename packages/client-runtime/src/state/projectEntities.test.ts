import { MachineId, ProjectId, type ServerConfig } from "@vekrel/contracts";
import { Atom, AtomRegistry } from "effect/reactivity";
import * as Option from "effect/Option";
import { describe, expect, it } from "vite-plus/test";

import { PrimaryConnectionTarget } from "../connection/model.ts";
import { createMachineProjectAtoms } from "./projectEntities.ts";
import { v2Project, v2ShellSnapshot } from "./orchestrationV2TestFixtures.ts";

const machineId = MachineId.make("local");
const remoteMachineId = MachineId.make("remote");

function makeHarness(withConfig = true) {
  const snapshotAtom = Atom.family((_machineId: MachineId) =>
    Atom.make({
      ...v2ShellSnapshot,
      projects: [
        { ...v2Project, title: "No project" },
        {
          ...v2Project,
          title: "No project",
          id: ProjectId.make("ordinary"),
          workspaceRoot: "/repo",
        },
      ],
    }),
  );
  const configAtom = Atom.family((_machineId: MachineId) =>
    Atom.make<(Pick<ServerConfig, "scratchWorkspaceRoot"> & { revision?: number }) | null>(null),
  );
  const catalogValueAtom = Atom.make({
    isReady: true,
    entries: new Map(
      [machineId, remoteMachineId].map((id) => [
        id,
        {
          target: new PrimaryConnectionTarget({
            machineId: id,
            label: id,
            httpBaseUrl: `https://${id}.example.test`,
            wsBaseUrl: `wss://${id}.example.test`,
          }),
          profile: Option.none(),
          enabled: true,
        },
      ]),
    ),
  });
  const projects = createMachineProjectAtoms({
    catalogValueAtom,
    snapshotAtom,
    ...(withConfig ? { serverConfigValueAtom: configAtom } : {}),
  });
  return { registry: AtomRegistry.make(), projects, configAtom, snapshotAtom, catalogValueAtom };
}

describe("project scratch identity", () => {
  it("waits for enabled project configs and ignores a disabled machine with no config", () => {
    const { registry, projects, configAtom, catalogValueAtom } = makeHarness();
    try {
      expect(registry.get(projects.projectConfigsReadyAtom)).toBe(false);
      registry.set(configAtom(machineId), { scratchWorkspaceRoot: v2Project.workspaceRoot });
      expect(registry.get(projects.projectConfigsReadyAtom)).toBe(false);
      const catalog = registry.get(catalogValueAtom);
      registry.set(catalogValueAtom, {
        ...catalog,
        entries: new Map(
          [...catalog.entries].map(([id, entry]) => [
            id,
            { ...entry, enabled: id === machineId },
          ]),
        ),
      });
      expect(registry.get(projects.projectConfigsReadyAtom)).toBe(true);
      registry.set(catalogValueAtom, catalog);
      expect(registry.get(projects.projectConfigsReadyAtom)).toBe(false);
      registry.set(configAtom(remoteMachineId), {});
      expect(registry.get(projects.projectConfigsReadyAtom)).toBe(true);
      registry.set(catalogValueAtom, { ...catalog, isReady: false });
      expect(registry.get(projects.projectConfigsReadyAtom)).toBe(false);
    } finally {
      registry.dispose();
    }
  });

  it("updates a loaded project when config arrives, preserving unrelated projects and snapshots", () => {
    const { registry, projects, configAtom, snapshotAtom } = makeHarness();
    try {
      const initial = registry.get(projects.projectsAtom);
      const source = registry.get(snapshotAtom(machineId));
      registry.set(configAtom(machineId), {
        scratchWorkspaceRoot: `${v2Project.workspaceRoot}/`,
      });
      const next = registry.get(projects.projectsAtom);

      expect(next[0]?.isScratch).toBe(true);
      expect(next[0]).not.toBe(initial[0]);
      expect(next.slice(1)).toEqual(initial.slice(1));
      for (let index = 1; index < initial.length; index++) {
        expect(next[index]).toBe(initial[index]);
      }
      expect(registry.get(snapshotAtom(machineId))).toBe(source);
      expect(v2Project).not.toHaveProperty("isScratch");

      registry.set(configAtom(machineId), {
        scratchWorkspaceRoot: v2Project.workspaceRoot,
        revision: 1,
      });
      expect(registry.get(projects.projectsAtom)).toBe(next);

      registry.set(configAtom(machineId), { scratchWorkspaceRoot: "/repo" });
      const changedRoot = registry.get(projects.projectsAtom);
      expect(changedRoot[0]).not.toHaveProperty("isScratch");
      expect(changedRoot[1]?.isScratch).toBe(true);
      expect(changedRoot[2]).toBe(initial[2]);

      registry.set(configAtom(machineId), null);
      expect(registry.get(projects.projectsAtom).every((project) => !project.isScratch)).toBe(true);
    } finally {
      registry.dispose();
    }
  });
});
