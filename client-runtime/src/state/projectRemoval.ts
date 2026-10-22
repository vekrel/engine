import type { MachineProject, MachineThreadShell } from "./models.ts";

export function planProjectRemoval<
  Thread extends Pick<MachineThreadShell, "machineId" | "projectId">,
>(options: {
  readonly members: ReadonlyArray<
    Pick<MachineProject, "id" | "machineId" | "title" | "workspaceRoot"> & {
      readonly machineLabel?: string | null;
    }
  >;
  readonly groupMemberCount: number;
  readonly hasOtherMembers: boolean;
  readonly displayName: string;
  readonly threads: ReadonlyArray<Thread>;
}) {
  const { members, groupMemberCount, hasOtherMembers, displayName, threads } = options;
  const memberKeys = new Set(members.map((member) => `${member.machineId}:${member.id}`));
  const projectThreads = threads.filter((thread) =>
    memberKeys.has(`${thread.machineId}:${thread.projectId}`),
  );
  const isWholeGroup = members.length === groupMemberCount;
  const targetKind = hasOtherMembers || !isWholeGroup ? "checkout" : "project";
  const singleMember = members.length === 1 ? members[0]! : null;
  const targetLabel = singleMember?.title ?? displayName;
  const confirmationMessage = [
    projectThreads.length > 0
      ? `Remove ${targetKind} "${targetLabel}" and delete its ${projectThreads.length} thread${projectThreads.length === 1 ? "" : "s"}?`
      : `Remove ${targetKind} "${targetLabel}"?`,
    ...(singleMember
      ? [
          `Path: ${singleMember.workspaceRoot}`,
          ...(singleMember.machineLabel
            ? [`Machine: ${singleMember.machineLabel}`]
            : []),
        ]
      : [`This removes ${members.length} grouped project entries.`]),
    ...(projectThreads.length > 0
      ? ["This permanently clears conversation history for those threads and any archived threads."]
      : ["This permanently clears any archived conversation history."]),
    isWholeGroup && !hasOtherMembers
      ? "This removes only the project entries, not the files on disk."
      : "Other entries in this grouped project are unaffected.",
    "This action cannot be undone.",
  ].join("\n");

  return { projectThreads, isWholeGroup, targetKind, confirmationMessage };
}
