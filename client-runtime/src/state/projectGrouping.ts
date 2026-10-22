import { scopedProjectKey, scopeProjectRef } from "../machine/scoped.ts";
import {
  repositoryGroupingDisplayNameOf,
  repositoryGroupingKeyOf,
  type MachineId,
  type ScopedProjectRef,
  type SidebarProjectGroupingMode,
} from "@vekrel/contracts";
import type { ClientSettings } from "@vekrel/contracts/settings";

import type { MachineProject } from "./models.ts";
import { normalizeProjectPathForComparison } from "./projects.ts";

const SCRATCH_PROJECT_SCOPE_KEY = "\0no-project";

export interface ProjectGroupingSettings {
  readonly sidebarProjectGroupingMode: SidebarProjectGroupingMode;
  readonly sidebarProjectGroupingOverrides: Record<string, SidebarProjectGroupingMode>;
}

export type ProjectGroupingMode = SidebarProjectGroupingMode;

export function selectProjectGroupingSettings(settings: ClientSettings): ProjectGroupingSettings {
  return {
    sidebarProjectGroupingMode: settings.sidebarProjectGroupingMode,
    sidebarProjectGroupingOverrides: settings.sidebarProjectGroupingOverrides,
  };
}

function uniqueNonEmptyValues(values: ReadonlyArray<string | null | undefined>): string[] {
  const seen = new Set<string>();
  const unique: string[] = [];
  for (const value of values) {
    const trimmed = value?.trim();
    if (!trimmed || seen.has(trimmed)) {
      continue;
    }
    seen.add(trimmed);
    unique.push(trimmed);
  }
  return unique;
}

function deriveRepositoryRelativeProjectPath(
  project: Pick<MachineProject, "workspaceRoot" | "repositoryIdentity">,
): string | null {
  const rootPath = project.repositoryIdentity?.rootPath?.trim();
  if (!rootPath) {
    return null;
  }

  const normalizedProjectPath = normalizeProjectPathForComparison(project.workspaceRoot);
  const normalizedRootPath = normalizeProjectPathForComparison(rootPath);
  if (normalizedProjectPath.length === 0 || normalizedRootPath.length === 0) {
    return null;
  }

  if (normalizedProjectPath === normalizedRootPath) {
    return "";
  }

  const separator = normalizedRootPath.includes("\\") ? "\\" : "/";
  const rootPrefix = `${normalizedRootPath}${separator}`;
  if (!normalizedProjectPath.startsWith(rootPrefix)) {
    return null;
  }

  return normalizedProjectPath.slice(rootPrefix.length).replaceAll("\\", "/");
}

export function derivePhysicalProjectKeyFromPath(machineId: string, cwd: string): string {
  return `${machineId}:${normalizeProjectPathForComparison(cwd)}`;
}

export function derivePhysicalProjectKey(
  project: Pick<MachineProject, "machineId" | "workspaceRoot">,
): string {
  return derivePhysicalProjectKeyFromPath(project.machineId, project.workspaceRoot);
}

export function deriveProjectGroupingOverrideKey(
  project: Pick<MachineProject, "machineId" | "workspaceRoot">,
): string {
  return derivePhysicalProjectKey(project);
}

export function getProjectOrderKey(
  project: Pick<MachineProject, "machineId" | "workspaceRoot">,
): string {
  return derivePhysicalProjectKey(project);
}

function resolveProjectGroupingMode(
  project: Pick<MachineProject, "machineId" | "workspaceRoot">,
  settings: ProjectGroupingSettings,
): SidebarProjectGroupingMode {
  return (
    settings.sidebarProjectGroupingOverrides?.[deriveProjectGroupingOverrideKey(project)] ??
    settings.sidebarProjectGroupingMode
  );
}

function deriveRepositoryScopedKey(
  project: Pick<MachineProject, "workspaceRoot" | "repositoryIdentity">,
  groupingMode: SidebarProjectGroupingMode,
): string | null {
  const canonicalKey = project.repositoryIdentity
    ? repositoryGroupingKeyOf(project.repositoryIdentity)
    : null;
  // A NUL reserves the scratch key namespace outside valid repository identities.
  if (!canonicalKey || canonicalKey.includes("\0")) {
    return null;
  }

  if (groupingMode === "repository") {
    return canonicalKey;
  }

  const relativeProjectPath = deriveRepositoryRelativeProjectPath(project);
  if (relativeProjectPath === null) {
    return canonicalKey;
  }

  return relativeProjectPath.length === 0
    ? canonicalKey
    : `${canonicalKey}::${relativeProjectPath}`;
}

function deriveLogicalProjectKey(
  project: Pick<
    MachineProject,
    "machineId" | "id" | "workspaceRoot" | "repositoryIdentity" | "isScratch"
  >,
  options?: {
    readonly groupingMode?: SidebarProjectGroupingMode;
    readonly groupScratchProjects?: boolean;
  },
): string {
  if (options?.groupScratchProjects && project.isScratch) return SCRATCH_PROJECT_SCOPE_KEY;
  const groupingMode = options?.groupingMode ?? "repository";
  if (groupingMode === "separate") {
    return derivePhysicalProjectKey(project);
  }

  return (
    deriveRepositoryScopedKey(project, groupingMode) ??
    derivePhysicalProjectKey(project) ??
    scopedProjectKey(scopeProjectRef(project.machineId, project.id))
  );
}

export function deriveLogicalProjectKeyFromSettings(
  project: Pick<
    MachineProject,
    "machineId" | "id" | "workspaceRoot" | "repositoryIdentity" | "isScratch"
  >,
  settings: ProjectGroupingSettings,
  options?: { readonly groupScratchProjects?: boolean },
): string {
  return deriveLogicalProjectKey(project, {
    groupingMode: resolveProjectGroupingMode(project, settings),
    groupScratchProjects: options?.groupScratchProjects ?? false,
  });
}

export function resolveScratchProjectScopeKey(
  key: string | null,
  projects: ReadonlyArray<
    Pick<
      MachineProject,
      "machineId" | "id" | "workspaceRoot" | "repositoryIdentity" | "isScratch"
    >
  >,
  settings: ProjectGroupingSettings = {
    sidebarProjectGroupingMode: "repository",
    sidebarProjectGroupingOverrides: {},
  },
): string | null {
  if (key === null || key === SCRATCH_PROJECT_SCOPE_KEY) return key;
  if (
    projects.some(
      (project) =>
        deriveLogicalProjectKeyFromSettings(project, settings, { groupScratchProjects: true }) ===
        key,
    )
  ) {
    return key;
  }
  return projects.some((project) => {
    if (!project.isScratch) return false;
    const mode = resolveProjectGroupingMode(project, settings);
    return (
      derivePhysicalProjectKey(project) === key ||
      (mode !== "separate" && deriveRepositoryScopedKey(project, mode) === key)
    );
  })
    ? SCRATCH_PROJECT_SCOPE_KEY
    : key;
}

export function deriveProjectGroupLabel(input: {
  readonly representative: Pick<MachineProject, "title" | "repositoryIdentity">;
  readonly members: ReadonlyArray<Pick<MachineProject, "title" | "repositoryIdentity">>;
}): string {
  const sharedTitles = uniqueNonEmptyValues(input.members.map((member) => member.title));
  const sharedDisplayNames = uniqueNonEmptyValues(
    input.members.map((member) =>
      member.repositoryIdentity ? repositoryGroupingDisplayNameOf(member.repositoryIdentity) : null,
    ),
  );
  const sharedRepositoryNames = uniqueNonEmptyValues(
    input.members.map((member) => member.repositoryIdentity?.name),
  );
  const sharedTitle = sharedTitles[0];
  if (
    sharedTitles.length === 1 &&
    sharedTitle !== undefined &&
    !sharedDisplayNames.includes(sharedTitle) &&
    !sharedRepositoryNames.includes(sharedTitle)
  ) {
    return sharedTitle;
  }
  if (sharedDisplayNames.length === 1) {
    return sharedDisplayNames[0]!;
  }

  if (sharedRepositoryNames.length === 1) {
    return sharedRepositoryNames[0]!;
  }

  return input.representative.title;
}

export interface ProjectGroupMember<TProject extends MachineProject = MachineProject> {
  readonly physicalProjectKey: string;
  readonly project: TProject;
}

export interface ProjectGroup<TProject extends MachineProject = MachineProject> {
  readonly key: string;
  readonly label: string;
  readonly representative: TProject;
  readonly members: ReadonlyArray<ProjectGroupMember<TProject>>;
  readonly memberProjectRefs: ReadonlyArray<ScopedProjectRef>;
}

function projectFreshnessTime(project: MachineProject): number {
  const updatedAtTime = Date.parse(project.updatedAt);
  if (Number.isFinite(updatedAtTime)) {
    return updatedAtTime;
  }
  const createdAtTime = Date.parse(project.createdAt);
  return Number.isFinite(createdAtTime) ? createdAtTime : 0;
}

function shouldReplacePhysicalProjectWinner<TProject extends MachineProject>(
  existing: TProject,
  candidate: TProject,
): boolean {
  const freshnessDelta = projectFreshnessTime(candidate) - projectFreshnessTime(existing);
  return freshnessDelta > 0 || (freshnessDelta === 0 && candidate.id > existing.id);
}

function selectProjectIdentitySource<TProject extends MachineProject>(
  projects: ReadonlyArray<TProject>,
  winner: TProject,
): TProject {
  if (winner.repositoryIdentity !== null) {
    return winner;
  }

  let freshestIdentifiedProject: TProject | null = null;
  for (const project of projects) {
    if (project.repositoryIdentity === null) {
      continue;
    }
    if (
      freshestIdentifiedProject === null ||
      shouldReplacePhysicalProjectWinner(freshestIdentifiedProject, project)
    ) {
      freshestIdentifiedProject = project;
    }
  }
  return freshestIdentifiedProject ?? winner;
}

export function buildProjectGroups<TProject extends MachineProject>(input: {
  readonly projects: ReadonlyArray<TProject>;
  readonly settings: ProjectGroupingSettings;
  readonly preferredMachineId?: MachineId | null;
  readonly groupScratchProjects?: boolean;
}): ReadonlyArray<ProjectGroup<TProject>> {
  const projectsByPhysicalKey = new Map<string, TProject[]>();
  for (const project of input.projects) {
    const physicalProjectKey = derivePhysicalProjectKey(project);
    const existing = projectsByPhysicalKey.get(physicalProjectKey);
    if (existing) {
      existing.push(project);
    } else {
      projectsByPhysicalKey.set(physicalProjectKey, [project]);
    }
  }

  const logicalKeyByPhysicalKey = new Map<string, string>();
  const groupedMembers = new Map<string, ProjectGroupMember<TProject>[]>();
  for (const [physicalProjectKey, physicalProjects] of projectsByPhysicalKey) {
    const winner = physicalProjects.reduce((current, candidate) =>
      shouldReplacePhysicalProjectWinner(current, candidate) ? candidate : current,
    );
    const identitySource = selectProjectIdentitySource(physicalProjects, winner);
    const logicalKey = deriveLogicalProjectKey(identitySource, {
      groupingMode: resolveProjectGroupingMode(winner, input.settings),
      groupScratchProjects: input.groupScratchProjects ?? false,
    });
    logicalKeyByPhysicalKey.set(physicalProjectKey, logicalKey);
    const member = { physicalProjectKey, project: winner };
    const existing = groupedMembers.get(logicalKey);
    if (existing) {
      existing.push(member);
    } else {
      groupedMembers.set(logicalKey, [member]);
    }
  }

  const projectRefsByLogicalKey = new Map<string, ScopedProjectRef[]>();
  const seenProjectRefs = new Set<string>();
  for (const project of input.projects) {
    const physicalProjectKey = derivePhysicalProjectKey(project);
    const logicalKey =
      logicalKeyByPhysicalKey.get(physicalProjectKey) ??
      deriveLogicalProjectKeyFromSettings(project, input.settings, {
        groupScratchProjects: input.groupScratchProjects ?? false,
      });
    const projectRefKey = scopedProjectKey(scopeProjectRef(project.machineId, project.id));
    if (seenProjectRefs.has(projectRefKey)) continue;
    seenProjectRefs.add(projectRefKey);
    const projectRef = scopeProjectRef(project.machineId, project.id);
    const existing = projectRefsByLogicalKey.get(logicalKey);
    if (existing) {
      existing.push(projectRef);
    } else {
      projectRefsByLogicalKey.set(logicalKey, [projectRef]);
    }
  }

  const preferredMachineId = input.preferredMachineId ?? null;
  return Array.from(groupedMembers, ([key, members]) => {
    const representative =
      (preferredMachineId
        ? members.find((member) => member.project.machineId === preferredMachineId)?.project
        : null) ?? members[0]!.project;
    return {
      key,
      label:
        input.groupScratchProjects && representative.isScratch
          ? "No project"
          : members.length > 1
            ? deriveProjectGroupLabel({
                representative,
                members: members.map((member) => member.project),
              })
            : representative.title,
      representative,
      members,
      memberProjectRefs: projectRefsByLogicalKey.get(key) ?? [],
    };
  });
}

export function sortProjectsByGroupOrder<
  TProject extends { readonly id: string; readonly machineId: string },
>(
  projects: ReadonlyArray<TProject>,
  groups: ReadonlyArray<{ readonly memberProjectRefs: ReadonlyArray<ScopedProjectRef> }>,
): TProject[] {
  const rankByProject = new Map<string, number>();
  groups.forEach((group, rank) => {
    for (const ref of group.memberProjectRefs) {
      const key = `${ref.machineId}\0${ref.projectId}`;
      if (!rankByProject.has(key)) rankByProject.set(key, rank);
    }
  });
  const rankOf = (project: TProject) =>
    rankByProject.get(`${project.machineId}\0${project.id}`) ?? groups.length;
  return [...projects].sort((left, right) => rankOf(left) - rankOf(right));
}
