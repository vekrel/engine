import type { MachineId, ScopedThreadRef, ThreadId } from "@vekrel/contracts";

const COMPOSER_THREAD_RESULT_LIMIT = 5;

export interface ComposerThreadCandidate {
  readonly machineId: MachineId;
  readonly id: ThreadId;
  readonly title: string;
  readonly updatedAt: string;
  readonly archivedAt: string | null;
}

export interface ComposerThreadItem {
  readonly id: string;
  readonly type: "thread";
  readonly thread: ScopedThreadRef;
  readonly label: string;
  readonly description: string;
}

/**
 * The agent can only read threads on its own server, so candidates stay within the
 * composer's machine.
 */
export function matchComposerThreadItems(input: {
  shells: ReadonlyArray<ComposerThreadCandidate>;
  machineId: MachineId;
  excludeThreadId: ThreadId | null;
  query: string;
}): ComposerThreadItem[] {
  const query = input.query.trim().toLowerCase();
  if (query.length === 0) return [];
  return input.shells
    .filter(
      (shell) =>
        shell.machineId === input.machineId &&
        shell.id !== input.excludeThreadId &&
        shell.archivedAt === null &&
        shell.title.toLowerCase().includes(query),
    )
    .sort((left, right) => right.updatedAt.localeCompare(left.updatedAt))
    .slice(0, COMPOSER_THREAD_RESULT_LIMIT)
    .map((shell) => ({
      id: `thread:${shell.machineId}:${shell.id}`,
      type: "thread",
      thread: { machineId: shell.machineId, threadId: shell.id },
      label: shell.title,
      description: "Thread",
    }));
}
