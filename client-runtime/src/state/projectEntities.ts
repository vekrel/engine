import type {
  MachineId,
  OrchestrationProjectShell,
  OrchestrationV2ShellSnapshot,
  ProjectId,
  ScopedProjectRef,
  ServerConfig,
} from "@vekrel/contracts";
import { Atom } from "effect/reactivity";

import type { MachineProject } from "./models.ts";
import { scopeProject } from "./models.ts";
import { isScratchProject } from "./projects.ts";
import { type MachineCatalogState, enabledMachineIds } from "./connections.ts";
import { arrayElementsEqual, parseProjectKey, projectKey, projectRefsEqual } from "./entities.ts";

const EMPTY_PROJECTS: ReadonlyArray<OrchestrationProjectShell> = Object.freeze([]);
const EMPTY_PROJECT_INDEX: ReadonlyMap<ProjectId, OrchestrationProjectShell> = new Map();

export function createMachineProjectAtoms(input: {
  readonly catalogValueAtom: Atom.Atom<MachineCatalogState>;
  readonly snapshotAtom: (
    machineId: MachineId,
  ) => Atom.Atom<OrchestrationV2ShellSnapshot | null>;
  readonly serverConfigValueAtom?: (
    machineId: MachineId,
  ) => Atom.Atom<Pick<ServerConfig, "scratchWorkspaceRoot"> | null>;
}) {
  const machineProjectsAtom = Atom.family((machineId: MachineId) =>
    Atom.make(
      (get): ReadonlyArray<OrchestrationProjectShell> =>
        get(input.snapshotAtom(machineId))?.projects ?? EMPTY_PROJECTS,
    ).pipe(Atom.withLabel(`machine-projects:${machineId}`)),
  );

  const machineProjectIndexAtom = Atom.family((machineId: MachineId) =>
    Atom.make((get): ReadonlyMap<ProjectId, OrchestrationProjectShell> => {
      const projects = get(machineProjectsAtom(machineId));
      if (projects.length === 0) {
        return EMPTY_PROJECT_INDEX;
      }
      return new Map(projects.map((project) => [project.id, project] as const));
    }).pipe(Atom.withLabel(`machine-project-index:${machineId}`)),
  );

  const machineProjectRefsAtom = Atom.family((machineId: MachineId) => {
    let previous: ReadonlyArray<ScopedProjectRef> = [];
    return Atom.make((get) => {
      const next = get(machineProjectsAtom(machineId)).map((project) => ({
        machineId,
        projectId: project.id,
      }));
      if (projectRefsEqual(previous, next)) {
        return previous;
      }
      previous = next;
      return next;
    }).pipe(Atom.withLabel(`machine-project-refs:${machineId}`));
  });

  const projectAtomFamily = Atom.family((key: string) => {
    const ref = parseProjectKey(key);
    let previousSource: OrchestrationProjectShell | null = null;
    let previousIsScratch = false;
    let previousValue: MachineProject | null = null;
    return Atom.make((get) => {
      const source = get(machineProjectIndexAtom(ref.machineId)).get(ref.projectId) ?? null;
      const scratch =
        source !== null &&
        isScratchProject(
          source,
          input.serverConfigValueAtom
            ? get(input.serverConfigValueAtom(ref.machineId))?.scratchWorkspaceRoot
            : null,
        );
      if (source === previousSource && scratch === previousIsScratch) {
        return previousValue;
      }
      previousSource = source;
      previousIsScratch = scratch;
      previousValue =
        source === null
          ? null
          : {
              ...scopeProject(ref.machineId, source),
              ...(scratch ? { isScratch: true as const } : {}),
            };
      return previousValue;
    }).pipe(Atom.withLabel(`machine-project:${key}`));
  });

  let previousProjectRefs: ReadonlyArray<ScopedProjectRef> = [];
  const projectRefsAtom = Atom.make((get) => {
    const refs: ScopedProjectRef[] = [];
    for (const machineId of enabledMachineIds(get(input.catalogValueAtom))) {
      refs.push(...get(machineProjectRefsAtom(machineId)));
    }
    if (projectRefsEqual(previousProjectRefs, refs)) {
      return previousProjectRefs;
    }
    previousProjectRefs = refs;
    return refs;
  }).pipe(Atom.withLabel("machine-project-refs"));

  let previousProjects: ReadonlyArray<MachineProject> = [];
  const projectsAtom = Atom.make((get) => {
    const next = get(projectRefsAtom).flatMap((ref) => {
      const project = get(projectAtomFamily(projectKey(ref)));
      return project === null ? [] : [project];
    });
    if (arrayElementsEqual(previousProjects, next)) {
      return previousProjects;
    }
    previousProjects = next;
    return previousProjects;
  }).pipe(Atom.withLabel("machine-project-list"));

  const projectConfigsReadyAtom = Atom.make((get) => {
    const catalog = get(input.catalogValueAtom);
    if (!catalog.isReady) return false;
    for (const machineId of enabledMachineIds(catalog)) {
      if (input.serverConfigValueAtom && get(input.serverConfigValueAtom(machineId)) === null) {
        return false;
      }
    }
    return true;
  }).pipe(Atom.withLabel("machine-project-configs-ready"));

  return {
    machineProjectsAtom,
    machineProjectIndexAtom,
    machineProjectRefsAtom,
    projectRefsAtom,
    projectsAtom,
    projectConfigsReadyAtom,
    projectAtom: (ref: ScopedProjectRef) => projectAtomFamily(projectKey(ref)),
  };
}
