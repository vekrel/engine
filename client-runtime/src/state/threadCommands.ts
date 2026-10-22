import type { ThreadId } from "@vekrel/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { Atom } from "effect/reactivity";
import {
  WS_METHODS,
  type MachineId,
  type OrchestrationV2ShellSnapshot,
} from "@vekrel/contracts";

import { createOptimisticThreadLifecycle } from "./threadLifecycle.ts";
import * as DateTime from "effect/DateTime";

import {
  createAtomCommandScheduler,
  createMachineCommand,
  createMachineRpcCommand,
} from "./runtime.ts";
import {
  type ArchiveThreadInput,
  type CancelQueuedRunInput,
  type RetryWorkspacePreparationInput,
  type CreateThreadInput,
  type DeleteThreadInput,
  type EditQueuedRunInput,
  type InterruptThreadTurnInput,
  type MarkThreadUnreadInput,
  type ForkThreadFromRunInput,
  type MergeThreadBackInput,
  type PromoteQueuedRunInput,
  type ReorderQueuedRunInput,
  type ResumeThreadQueueInput,
  type LinkThreadPullRequestInput,
  type RespondToThreadApprovalInput,
  type RespondToThreadUserInputInput,
  type DismissThreadUserInputInput,
  type RevertThreadCheckpointInput,
  type SetThreadInteractionModeInput,
  type SetThreadRuntimeModeInput,
  type PinThreadInput,
  type ReorderPinnedThreadInput,
  type ReorderActiveThreadInput,
  type SetThreadAutoSettleInput,
  type SettleThreadInput,
  type SnoozeThreadInput,
  type StartThreadTurnInput,
  type StopThreadSessionInput,
  type UnarchiveThreadInput,
  type UnlinkThreadPullRequestInput,
  type UnpinThreadInput,
  type WatchThreadPullRequestInput,
  type UnsettleThreadInput,
  type UnsnoozeThreadInput,
  type UpdateThreadMetadataInput,
  type VisitThreadInput,
  archiveThread,
  cancelQueuedRun,
  createThread,
  deleteThread,
  editQueuedRun,
  interruptThreadTurn,
  forkThreadFromRun,
  markThreadUnread,
  mergeThreadBack,
  promoteQueuedRun,
  reorderQueuedRun,
  resumeThreadQueue,
  retryWorkspacePreparation,
  linkThreadPullRequest,
  respondToThreadApproval,
  respondToThreadUserInput,
  dismissThreadUserInput,
  revertThreadCheckpoint,
  setThreadInteractionMode,
  setThreadRuntimeMode,
  pinThread,
  reorderPinnedThread,
  reorderActiveThread,
  setThreadAutoSettle,
  settleThread,
  snoozeThread,
  startThreadTurn,
  stopThreadSession,
  unarchiveThread,
  unlinkThreadPullRequest,
  unpinThread,
  unsettleThread,
  unsnoozeThread,
  updateThreadMetadata,
  visitThread,
  watchThreadPullRequest,
} from "../operations/commands.ts";
import type { MachineRegistry } from "../connection/registry.ts";
import * as MachineSupervisor from "../connection/supervisor.ts";
import * as ThreadHistoryController from "./threadHistoryController.ts";

export type LoadEarlierThreadHistoryInput = {
  readonly threadId: ThreadId;
  readonly throughEntryId?: string;
};

export type {
  ArchiveThreadInput,
  CancelQueuedRunInput,
  CreateThreadInput,
  DeleteThreadInput,
  EditQueuedRunInput,
  InterruptThreadTurnInput,
  MarkThreadUnreadInput,
  ForkThreadFromRunInput,
  MergeThreadBackInput,
  PromoteQueuedRunInput,
  ReorderQueuedRunInput,
  LinkThreadPullRequestInput,
  RespondToThreadApprovalInput,
  RespondToThreadUserInputInput,
  DismissThreadUserInputInput,
  RetryWorkspacePreparationInput,
  RevertThreadCheckpointInput,
  SetThreadInteractionModeInput,
  SetThreadRuntimeModeInput,
  PinThreadInput,
  ReorderPinnedThreadInput,
  ReorderActiveThreadInput,
  SetThreadAutoSettleInput,
  SettleThreadInput,
  SnoozeThreadInput,
  StartThreadTurnInput,
  StopThreadSessionInput,
  ThreadCommandInput,
  UnarchiveThreadInput,
  UnlinkThreadPullRequestInput,
  UnpinThreadInput,
  UnsettleThreadInput,
  UnsnoozeThreadInput,
  UpdateThreadMetadataInput,
  VisitThreadInput,
  WatchThreadPullRequestInput,
} from "../operations/commands.ts";

export function createThreadMachineAtoms<R, E>(
  runtime: Atom.AtomRuntime<MachineRegistry | Crypto.Crypto | R, E>,
  snapshotAtom: (machineId: MachineId) => Atom.Atom<OrchestrationV2ShellSnapshot | null>,
) {
  const scheduler = createAtomCommandScheduler();
  const concurrency = {
    mode: "serial" as const,
    key: ({ machineId, input }: { machineId: string; input: { threadId: string } }) =>
      JSON.stringify([machineId, input.threadId]),
  };
  const loadEarlierHistory = Effect.fn("ThreadCommands.loadEarlierHistory")(function* (
    input: LoadEarlierThreadHistoryInput,
  ) {
    const supervisor = yield* MachineSupervisor.MachineSupervisor;
    const controller = yield* Effect.serviceOption(ThreadHistoryController.ThreadHistoryController);
    if (Option.isNone(controller)) {
      return {
        _tag: "noop",
      } satisfies ThreadHistoryController.ThreadHistoryLoadEarlierResult;
    }
    return yield* controller.value.loadEarlier(
      supervisor.target.machineId,
      input.threadId,
      input.throughEntryId,
    );
  });

  const commands = {
    create: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:create",
      execute: (input: CreateThreadInput) => createThread(input),
      scheduler,
      concurrency,
    }),
    delete: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:delete",
      execute: (input: DeleteThreadInput) => deleteThread(input),
      scheduler,
      concurrency,
    }),
    archive: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:archive",
      execute: (input: ArchiveThreadInput) => archiveThread(input),
      scheduler,
      concurrency,
    }),
    unarchive: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:unarchive",
      execute: (input: UnarchiveThreadInput) => unarchiveThread(input),
      scheduler,
      concurrency,
    }),
    settle: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:settle",
      execute: (input: SettleThreadInput) => settleThread(input),
      scheduler,
      concurrency,
    }),
    unsettle: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:unsettle",
      execute: (input: UnsettleThreadInput) => unsettleThread(input),
      scheduler,
      concurrency,
    }),
    snooze: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:snooze",
      execute: (input: SnoozeThreadInput) => snoozeThread(input),
      scheduler,
      concurrency,
    }),
    unsnooze: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:unsnooze",
      execute: (input: UnsnoozeThreadInput) => unsnoozeThread(input),
      scheduler,
      concurrency,
    }),
    pin: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:pin",
      execute: (input: PinThreadInput) => pinThread(input),
      scheduler,
      concurrency,
    }),
    unpin: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:unpin",
      execute: (input: UnpinThreadInput) => unpinThread(input),
      scheduler,
      concurrency,
    }),
    reorderPin: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:reorder-pin",
      execute: (input: ReorderPinnedThreadInput) => reorderPinnedThread(input),
      scheduler,
      concurrency,
    }),
    setAutoSettle: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:set-auto-settle",
      execute: (input: SetThreadAutoSettleInput) => setThreadAutoSettle(input),
      scheduler,
      concurrency,
    }),
    reorderActive: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:reorder-active",
      execute: (input: ReorderActiveThreadInput) => reorderActiveThread(input),
      scheduler,
      concurrency,
    }),
    visit: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:visit",
      execute: (input: VisitThreadInput) => visitThread(input),
      scheduler,
      concurrency,
    }),
    markUnread: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:mark-unread",
      execute: (input: MarkThreadUnreadInput) => markThreadUnread(input),
      scheduler,
      concurrency,
    }),
    updateMetadata: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:update-metadata",
      execute: (input: UpdateThreadMetadataInput) => updateThreadMetadata(input),
      scheduler,
      concurrency,
    }),
    linkPullRequest: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:link-pull-request",
      execute: (input: LinkThreadPullRequestInput) => linkThreadPullRequest(input),
      scheduler,
      concurrency,
    }),
    unlinkPullRequest: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:unlink-pull-request",
      execute: (input: UnlinkThreadPullRequestInput) => unlinkThreadPullRequest(input),
      scheduler,
      concurrency,
    }),
    watchPullRequest: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:watch-pull-request",
      execute: (input: WatchThreadPullRequestInput) => watchThreadPullRequest(input),
      scheduler,
      concurrency,
    }),
    setRuntimeMode: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:set-runtime-mode",
      execute: (input: SetThreadRuntimeModeInput) => setThreadRuntimeMode(input),
      scheduler,
      concurrency,
    }),
    setInteractionMode: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:set-interaction-mode",
      execute: (input: SetThreadInteractionModeInput) => setThreadInteractionMode(input),
      scheduler,
      concurrency,
    }),
    startTurn: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:start-turn",
      execute: (input: StartThreadTurnInput) => startThreadTurn(input),
      scheduler,
      concurrency,
    }),
    interruptTurn: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:interrupt-turn",
      execute: (input: InterruptThreadTurnInput) => interruptThreadTurn(input),
      scheduler,
      concurrency,
    }),
    respondToApproval: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:respond-to-approval",
      execute: (input: RespondToThreadApprovalInput) => respondToThreadApproval(input),
      scheduler,
      concurrency,
    }),
    respondToUserInput: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:respond-to-user-input",
      execute: (input: RespondToThreadUserInputInput) => respondToThreadUserInput(input),
      scheduler,
      concurrency,
    }),
    dismissUserInput: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:dismiss-user-input",
      execute: (input: DismissThreadUserInputInput) => dismissThreadUserInput(input),
      scheduler,
      concurrency,
    }),
    revertCheckpoint: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:revert-checkpoint",
      execute: (input: RevertThreadCheckpointInput) => revertThreadCheckpoint(input),
      scheduler,
      concurrency,
    }),
    stopSession: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:stop-session",
      execute: (input: StopThreadSessionInput) => stopThreadSession(input),
      scheduler,
      concurrency,
    }),
    forkFromRun: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:fork-from-run",
      execute: (input: ForkThreadFromRunInput) => forkThreadFromRun(input),
      scheduler,
      concurrency: {
        mode: "serial",
        key: ({ machineId, input }) => JSON.stringify([machineId, input.sourceThreadId]),
      },
    }),
    mergeBack: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:merge-back",
      execute: (input: MergeThreadBackInput) => mergeThreadBack(input),
      scheduler,
      concurrency: {
        mode: "serial",
        key: ({ machineId, input }) =>
          JSON.stringify([machineId, input.sourceThreadId, input.targetThreadId]),
      },
    }),
    resumeThreadQueue: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:resume-queue",
      execute: (input: ResumeThreadQueueInput) => resumeThreadQueue(input),
      scheduler,
      concurrency,
    }),
    reorderQueuedRun: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:reorder-queued-run",
      execute: (input: ReorderQueuedRunInput) => reorderQueuedRun(input),
      scheduler,
      concurrency,
    }),
    promoteQueuedRun: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:promote-queued-run",
      execute: (input: PromoteQueuedRunInput) => promoteQueuedRun(input),
      scheduler,
      concurrency,
    }),
    cancelQueuedRun: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:cancel-queued-run",
      execute: (input: CancelQueuedRunInput) => cancelQueuedRun(input),
      scheduler,
      concurrency,
    }),
    retryWorkspacePreparation: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:retry-workspace-preparation",
      execute: (input: RetryWorkspacePreparationInput) => retryWorkspacePreparation(input),
      scheduler,
      concurrency,
    }),
    editQueuedRun: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:edit-queued-run",
      execute: (input: EditQueuedRunInput) => editQueuedRun(input),
      scheduler,
      concurrency,
    }),
    loadEarlierHistory: createMachineCommand(runtime, {
      label: "machine-data:commands:thread:load-earlier-history",
      execute: loadEarlierHistory,
      scheduler,
      concurrency: {
        mode: "serial",
        key: ({ machineId, input }) => JSON.stringify([machineId, input.threadId]),
      },
    }),
    uploadFeedback: createMachineRpcCommand(runtime, {
      label: "machine-data:commands:thread:upload-feedback",
      tag: WS_METHODS.providerUploadFeedback,
      scheduler,
      concurrency,
    }),
  };
  const optimistic = createOptimisticThreadLifecycle(snapshotAtom);
  return {
    ...commands,
    snapshotAtom: optimistic.snapshotAtom,
    settle: optimistic.wrap(commands.settle, (thread, _input, now, accepted) =>
      !accepted &&
      (thread.pendingRuntimeRequest !== null ||
        ["preparing", "queued", "starting", "running", "waiting"].includes(thread.status))
        ? thread
        : {
            ...thread,
            pendingRuntimeRequest: null,
            settledOverride: "settled",
            settledAt: thread.settledOverride === "settled" ? (thread.settledAt ?? now) : now,
            unsettledAt: null,
            activeOrderKey: null,
            pinnedAt: null,
            pinOrderKey: null,
            snoozedAt: null,
            snoozedUntil: null,
          },
    ),
    unsettle: optimistic.wrap(commands.unsettle, (thread, input, now) => ({
      ...thread,
      settledOverride: input.reason === "user" ? "active" : null,
      settledAt: null,
      unsettledAt: thread.settledOverride === "active" ? (thread.unsettledAt ?? null) : now,
    })),
    snooze: optimistic.wrap(commands.snooze, (thread, input, now, accepted) =>
      (!accepted &&
        (thread.pendingRuntimeRequest !== null ||
          ["preparing", "queued", "starting"].includes(thread.status))) ||
      !(Date.parse(input.snoozedUntil) > DateTime.toEpochMillis(now))
        ? thread
        : {
            ...thread,
            pendingRuntimeRequest: null,
            snoozedUntil: DateTime.makeUnsafe(input.snoozedUntil),
            snoozedAt: now,
          },
    ),
    unsnooze: optimistic.wrap(commands.unsnooze, (thread) => ({
      ...thread,
      snoozedUntil: null,
      snoozedAt: null,
    })),
    setAutoSettle: optimistic.wrap(commands.setAutoSettle, (thread, input, now) => ({
      ...thread,
      autoSettleDisabledAt: input.enabled ? null : (thread.autoSettleDisabledAt ?? now),
    })),
    pin: optimistic.wrap(commands.pin, (thread, input, now) => ({
      ...thread,
      pinnedAt: thread.pinnedAt ?? now,
      pinOrderKey: thread.pinnedAt == null ? (input.orderKey ?? null) : thread.pinOrderKey,
      ...(thread.settledOverride === "settled"
        ? {
            settledOverride: "active" as const,
            settledAt: null,
            unsettledAt: now,
          }
        : {}),
      snoozedUntil: null,
      snoozedAt: null,
    })),
    unpin: optimistic.wrap(commands.unpin, (thread) => ({
      ...thread,
      pinnedAt: null,
      pinOrderKey: null,
    })),
    reorderPin: optimistic.wrap(commands.reorderPin, (thread, input) => ({
      ...thread,
      pinOrderKey: input.orderKey,
    })),
    reorderActive: optimistic.wrap(commands.reorderActive, (thread, input) => ({
      ...thread,
      activeOrderKey: input.orderKey,
    })),
  };
}
