import {
  WS_METHODS,
  PullRequestActivity,
  PullRequestDetail as PullRequestDetailSchema,
  PullRequestPreview,
  PullRequestReviewGuideResult,
  PullRequestListResult,
  PullRequestChecks,
  PullRequestDiffResult,
  PullRequestChangeBreakdown,
  PullRequestFilesViewedResult,
  PullRequestLinkedThreadsResult,
  type MachineId,
  type PullRequestActor,
  type PullRequestActionInput,
  type PullRequestDetail,
  type PullRequestDiffInput,
  type PullRequestRef,
  type PullRequestMergeMethod,
  PullRequestOperationError,
  type PullRequestSummary,
  type VcsStatusResult,
} from "@vekrel/contracts";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { persistPullRequestStats } from "./pullRequestStatsPersistence.ts";
import { persistQuery } from "./queryPersistence.ts";
import * as Option from "effect/Option";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity";

import {
  createAtomCommandScheduler,
  createMachineRpcCommand,
  createMachineRpcQueryAtomFamily,
  createMachineRpcSubscriptionAtomFamily,
  createMachineQueryAtomFamily,
} from "./runtime.ts";
import { createPullRequestRouter } from "./pullRequestRouting.ts";
import * as PullRequestDiffLoader from "./pullRequestDiffHttp.ts";
import type { MachineRegistry } from "../connection/registry.ts";
import * as MachineSupervisor from "../connection/supervisor.ts";

export {
  type PullRequestDiffLoadError,
  PullRequestDiffCredentialRejectedError,
} from "./pullRequestDiffHttp.ts";
export * as PullRequestDiffLoader from "./pullRequestDiffHttp.ts";

/** @public Required to name the error in consumers' inferred pull request results. */
export class MachineHttpConnectionNotReadyError extends Data.TaggedError(
  "MachineHttpConnectionNotReadyError",
)<{ readonly message: string }> {}

const LINKED_PULL_REQUEST_IDLE_TTL_MS = 5_000;

function writableQueryFamily<A, E>(
  family: (target: {
    readonly machineId: MachineId;
    readonly input: PullRequestRef;
  }) => Atom.Atom<AsyncResult.AsyncResult<A, E>>,
) {
  const writable = Atom.family((source: Atom.Atom<AsyncResult.AsyncResult<A, E>>) =>
    Atom.writable(
      (get) => {
        const result = get(source);
        if (result._tag === "Success" && !result.waiting) return result;
        const previous = get.self<AsyncResult.AsyncResult<A, E>>();
        const value = Option.flatMap(previous, AsyncResult.value);
        if (Option.isNone(value)) return result;
        return result._tag === "Failure"
          ? AsyncResult.failureWithPrevious(result.cause, { previous, waiting: result.waiting })
          : AsyncResult.success<A, E>(value.value, result);
      },
      (context, value: AsyncResult.AsyncResult<A, E>) => context.setSelf(value),
      (refresh) => refresh(source),
    ).pipe(Atom.setIdleTTL(5 * 60_000)),
  );
  return ({
    machineId,
    input: { projectId, host, repository, number, allowStale },
  }: Parameters<typeof family>[0]) =>
    writable(
      family({
        machineId,
        input: {
          projectId,
          ...(host === undefined ? {} : { host }),
          repository,
          number,
          ...(allowStale === undefined ? {} : { allowStale }),
        },
      }),
    );
}

/** Restart pre-mutation reads before patching so they cannot restore stale values. */
function updateCached<A, E>(
  registry: AtomRegistry.AtomRegistry,
  atom: Atom.Writable<AsyncResult.AsyncResult<A, E>>,
  update: (value: A) => A,
  refresh = false,
) {
  if (refresh || registry.get(atom).waiting) registry.refresh(atom);
  registry.update(atom, AsyncResult.map(update));
}

function createPullRequestRefreshAtomFamily<R, E>(
  runtime: Atom.AtomRuntime<MachineRegistry | R, E>,
) {
  return createMachineRpcSubscriptionAtomFamily(runtime, {
    label: "machine-data:pull-requests:turn-refreshes",
    tag: WS_METHODS.pullRequestsSubscribeRefreshes,
  });
}

export function createLinkedPullRequestSummaryAtomFamily<R, E>(
  runtime: Atom.AtomRuntime<MachineRegistry | R, E>,
  refreshes = createPullRequestRefreshAtomFamily(runtime),
) {
  const routedRequest = createPullRequestRouter();
  return createMachineRpcQueryAtomFamily(runtime, {
    label: "machine-data:pull-requests:linked-summary",
    tag: WS_METHODS.pullRequestsSummary,
    execute: (input) => routedRequest(WS_METHODS.pullRequestsSummary, input),
    staleTimeMs: 60_000,
    refreshIntervalMs: 60_000,
    idleTtlMs: LINKED_PULL_REQUEST_IDLE_TTL_MS,
    refreshTrigger: ({ machineId }) => refreshes({ machineId, input: {} }),
    isFinal: (summary) => summary.state === "merged",
  });
}

export function createPullRequestStackAtomFamily<R, E>(
  runtime: Atom.AtomRuntime<MachineRegistry | R, E>,
  refreshes = createPullRequestRefreshAtomFamily(runtime),
) {
  const routedRequest = createPullRequestRouter();
  return createMachineRpcQueryAtomFamily(runtime, {
    label: "machine-data:pull-requests:stack",
    tag: WS_METHODS.pullRequestsStack,
    execute: (input) => routedRequest(WS_METHODS.pullRequestsStack, input),
    staleTimeMs: 60_000,
    idleTtlMs: LINKED_PULL_REQUEST_IDLE_TTL_MS,
    refreshTrigger: ({ machineId }) => refreshes({ machineId, input: {} }),
  });
}

export function pullRequestDetailToVcsStatus(
  detail: PullRequestDetail | PullRequestSummary,
): NonNullable<VcsStatusResult["pr"]> {
  return {
    number: detail.number,
    title: detail.title,
    url: detail.url,
    baseRef: detail.baseBranch,
    headRef: detail.headBranch,
    state: detail.state,
    ...(detail.isDraft === true ? { isDraft: true } : {}),
    updatedAt: detail.updatedAt,
  };
}

export function createPullRequestMachineAtoms<R, E>(
  runtime: Atom.AtomRuntime<
    MachineRegistry | PullRequestDiffLoader.PullRequestDiffLoader | R,
    E
  >,
) {
  const refreshes = createPullRequestRefreshAtomFamily(runtime);
  const commandScheduler = createAtomCommandScheduler();
  const routedRequest = createPullRequestRouter();
  const serialPerMachine = {
    mode: "serial",
    key: ({ machineId }: { readonly machineId: string }) => machineId,
  } as const;
  const activity = writableQueryFamily(
    createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:pull-requests:activity",
      persistence: persistQuery(PullRequestActivity),
      tag: WS_METHODS.pullRequestsActivity,
      execute: (input) => routedRequest(WS_METHODS.pullRequestsActivity, input),
      staleTimeMs: 60_000,
      refreshTrigger: ({ machineId }) => refreshes({ machineId, input: {} }),
    }),
  );
  const detail = writableQueryFamily(
    createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:pull-requests:detail",
      persistence: persistQuery(PullRequestDetailSchema),
      tag: WS_METHODS.pullRequestsDetail,
      execute: (input) => routedRequest(WS_METHODS.pullRequestsDetail, input),
      staleTimeMs: 60_000,
      refreshTrigger: ({ machineId }) => refreshes({ machineId, input: {} }),
    }),
  );
  const preview = writableQueryFamily(
    createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:pull-requests:preview",
      persistence: persistQuery(PullRequestPreview),
      tag: WS_METHODS.pullRequestsPreview,
      execute: (input) => routedRequest(WS_METHODS.pullRequestsPreview, input),
      staleTimeMs: 60_000,
      refreshTrigger: ({ machineId }) => refreshes({ machineId, input: {} }),
    }),
  );
  const labelCandidates = writableQueryFamily(
    createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:pull-requests:label-candidates",
      tag: WS_METHODS.pullRequestsLabelCandidates,
      execute: (input) => routedRequest(WS_METHODS.pullRequestsLabelCandidates, input),
      staleTimeMs: 60_000,
    }),
  );
  const reviewGuide = writableQueryFamily(
    createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:pull-requests:review-guide",
      persistence: persistQuery(PullRequestReviewGuideResult),
      tag: WS_METHODS.pullRequestsReviewGuide,
      execute: (input) => routedRequest(WS_METHODS.pullRequestsReviewGuide, input),
      staleTimeMs: 60_000,
      refreshTrigger: ({ machineId }) => refreshes({ machineId, input: {} }),
    }),
  );
  const reviewerCandidates = writableQueryFamily(
    createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:pull-requests:reviewer-candidates",
      tag: WS_METHODS.pullRequestsReviewerCandidates,
      execute: (input) => routedRequest(WS_METHODS.pullRequestsReviewerCandidates, input),
      staleTimeMs: 60_000,
    }),
  );
  const loadDiff = Effect.fn("PullRequests.loadDiff")(function* (input: PullRequestDiffInput) {
    const supervisor = yield* MachineSupervisor.MachineSupervisor;
    const loader = yield* PullRequestDiffLoader.PullRequestDiffLoader;
    const prepared = yield* SubscriptionRef.get(supervisor.prepared);
    if (Option.isNone(prepared)) {
      return yield* new MachineHttpConnectionNotReadyError({
        message: "The machine HTTP connection is not ready.",
      });
    }
    return yield* loader.load(prepared.value, input);
  });

  const runAction = Effect.fn("PullRequests.runAction")(function* (
    input: PullRequestActionInput & {
      readonly resolveMergeMethod?: (detail: PullRequestDetail) => PullRequestMergeMethod;
    },
  ) {
    const { resolveMergeMethod, ...actionInput } = input;
    let preparedInput = actionInput;
    if (actionInput.action === "merge" && resolveMergeMethod !== undefined) {
      const { projectId, host, repository, number } = actionInput;
      const reference = { projectId, host, repository, number, allowStale: false };
      const detail = yield* routedRequest(WS_METHODS.pullRequestsDetail, reference);
      if (
        detail.state !== "open" ||
        detail.isDraft ||
        !detail.capabilities.actions.includes("merge") ||
        !detail.viewerPermissions.actions.includes("merge")
      ) {
        return yield* new PullRequestOperationError({
          operation: "runAction",
          detail: "This pull request cannot be merged.",
        });
      }
      if (detail.capabilities.stackActions) {
        const stack = yield* routedRequest(WS_METHODS.pullRequestsStack, reference);
        if (stack !== null) {
          return yield* new PullRequestOperationError({
            operation: "runAction",
            detail: "Open this pull request to merge its stack.",
          });
        }
      }
      const mergeMethod = yield* Effect.try({
        try: () => resolveMergeMethod(detail),
        catch: (cause) =>
          new PullRequestOperationError({
            operation: "runAction",
            detail: cause instanceof Error ? cause.message : "Could not choose a merge method.",
          }),
      });
      preparedInput = { ...actionInput, mergeMethod };
    }
    return yield* routedRequest(WS_METHODS.pullRequestsRunAction, preparedInput);
  });

  return {
    refreshes,
    linkedThreads: createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:pull-requests:linked-threads",
      persistence: persistQuery(PullRequestLinkedThreadsResult),
      tag: WS_METHODS.pullRequestsLinkedThreads,
      staleTimeMs: 0,
      refreshIntervalMs: 10_000,
      refreshTrigger: ({ machineId }) => refreshes({ machineId, input: {} }),
    }),
    list: createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:pull-requests:list",
      persistence: persistQuery(PullRequestListResult),
      tag: WS_METHODS.pullRequestsList,
      staleTimeMs: 30_000,
      refreshTrigger: ({ machineId, input }) =>
        input.cursors === undefined ? refreshes({ machineId, input: {} }) : undefined,
    }),
    /**
     * Its own query because the listing is quicker without them — measured over twelve
     * repositories, ~4.0s against ~7.1s — so the rows arrive first and their stats a moment
     * later.
     */
    listStats: createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:pull-requests:list-stats",
      persistence: persistPullRequestStats(),
      tag: WS_METHODS.pullRequestsListStats,
      staleTimeMs: 60_000,
      refreshTrigger: ({ machineId }) => refreshes({ machineId, input: {} }),
    }),
    detail,
    preview,
    checks: createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:pull-requests:checks",
      persistence: persistQuery(Schema.NullOr(PullRequestChecks)),
      tag: WS_METHODS.pullRequestsChecks,
      execute: (input) => routedRequest(WS_METHODS.pullRequestsChecks, input),
      staleTimeMs: 45_000,
      refreshTrigger: ({ machineId }) => refreshes({ machineId, input: {} }),
    }),
    activity,
    threadComments: createMachineRpcCommand(runtime, {
      label: "machine-data:pull-requests:thread-comments",
      tag: WS_METHODS.pullRequestsThreadComments,
      execute: (input) => routedRequest(WS_METHODS.pullRequestsThreadComments, input),
      scheduler: commandScheduler,
      concurrency: {
        mode: "singleFlight",
        key: ({ machineId, input }) =>
          JSON.stringify([machineId, input.threadId, input.cursor]),
      },
    }),
    diff: createMachineQueryAtomFamily(runtime, {
      label: "machine-data:pull-requests:diff",
      persistence: persistQuery(PullRequestDiffResult),
      staleTimeMs: 60_000,
      execute: loadDiff,
    }),
    diffFileContents: createMachineRpcCommand(runtime, {
      label: "machine-data:pull-requests:diff-file-contents",
      tag: WS_METHODS.pullRequestsDiffFileContents,
      execute: (input) => routedRequest(WS_METHODS.pullRequestsDiffFileContents, input),
      scheduler: commandScheduler,
      concurrency: {
        mode: "singleFlight",
        key: ({ machineId, input }) =>
          JSON.stringify([
            machineId,
            input.projectId,
            input.host?.toLowerCase() ?? null,
            input.repository,
            input.number,
            input.commit ?? null,
            input.changeType,
            input.oldPath,
            input.newPath,
          ]),
      },
    }),
    filesViewed: createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:pull-requests:files-viewed",
      persistence: persistQuery(PullRequestFilesViewedResult),
      tag: WS_METHODS.pullRequestsFilesViewed,
      execute: (input) => routedRequest(WS_METHODS.pullRequestsFilesViewed, input),
      staleTimeMs: 15_000,
    }),
    changeBreakdown: createMachineRpcQueryAtomFamily(runtime, {
      label: "machine-data:pull-requests:change-breakdown",
      persistence: persistQuery(PullRequestChangeBreakdown),
      tag: WS_METHODS.pullRequestsChangeBreakdown,
      execute: (input) => routedRequest(WS_METHODS.pullRequestsChangeBreakdown, input),
      staleTimeMs: 60_000,
      refreshTrigger: ({ machineId }) => refreshes({ machineId, input: {} }),
    }),
    /**
     * One write in flight per change request: the host applies these in order, and a reader
     * ticking down a file list faster than the round trip would otherwise race their own presses.
     */
    setFilesViewed: createMachineRpcCommand(runtime, {
      label: "machine-data:pull-requests:set-files-viewed",
      tag: WS_METHODS.pullRequestsSetFilesViewed,
      execute: (input) => routedRequest(WS_METHODS.pullRequestsSetFilesViewed, input),
      scheduler: commandScheduler,
      concurrency: {
        mode: "serial",
        key: ({ machineId, input }) =>
          JSON.stringify([machineId, input.projectId, input.repository, input.number]),
      },
    }),
    runAction: createMachineRpcCommand(runtime, {
      tag: WS_METHODS.pullRequestsRunAction,
      label: "machine-data:pull-requests:run-action",
      // Preparation belongs to the write's lane. Refreshable queries would restart it after
      // every preceding action, and preparing outside the lane could reorder the clicks.
      execute: runAction,
      onSuccess: ({ machineId, input }, registry) =>
        Effect.sync(() => registry.refresh(preview({ machineId, input }))),
      scheduler: commandScheduler,
      concurrency: serialPerMachine,
    }),
    update: createMachineRpcCommand(runtime, {
      label: "machine-data:pull-requests:update",
      tag: WS_METHODS.pullRequestsUpdate,
      execute: (input) => routedRequest(WS_METHODS.pullRequestsUpdate, input),
      onSuccess: (target, registry) => Effect.sync(() => registry.refresh(preview(target))),
      scheduler: commandScheduler,
      concurrency: serialPerMachine,
    }),
    comment: createMachineRpcCommand(runtime, {
      label: "machine-data:pull-requests:comment",
      tag: WS_METHODS.pullRequestsComment,
      execute: (input) => routedRequest(WS_METHODS.pullRequestsComment, input),
      scheduler: commandScheduler,
      concurrency: serialPerMachine,
    }),
    updateComment: createMachineRpcCommand(runtime, {
      label: "machine-data:pull-requests:update-comment",
      tag: WS_METHODS.pullRequestsUpdateComment,
      execute: (input) => routedRequest(WS_METHODS.pullRequestsUpdateComment, input),
      scheduler: commandScheduler,
      concurrency: serialPerMachine,
      onSuccess: ({ machineId, input: { projectId, host, repository, number } }, registry) =>
        Effect.sync(() =>
          registry.refresh(
            activity({
              machineId,
              input: { projectId, ...(host === undefined ? {} : { host }), repository, number },
            }),
          ),
        ),
    }),
    submitReview: createMachineRpcCommand(runtime, {
      label: "machine-data:pull-requests:submit-review",
      tag: WS_METHODS.pullRequestsSubmitReview,
      execute: (input) => routedRequest(WS_METHODS.pullRequestsSubmitReview, input),
      scheduler: commandScheduler,
      concurrency: serialPerMachine,
    }),
    replyToThread: createMachineRpcCommand(runtime, {
      label: "machine-data:pull-requests:reply-to-thread",
      tag: WS_METHODS.pullRequestsReplyToThread,
      execute: (input) => routedRequest(WS_METHODS.pullRequestsReplyToThread, input),
      scheduler: commandScheduler,
      concurrency: serialPerMachine,
    }),
    reviewerCandidates,
    requestReviewers: createMachineRpcCommand(runtime, {
      label: "machine-data:pull-requests:request-reviewers",
      tag: WS_METHODS.pullRequestsRequestReviewers,
      execute: (input) => routedRequest(WS_METHODS.pullRequestsRequestReviewers, input),
      scheduler: commandScheduler,
      concurrency: serialPerMachine,
      onSuccess: (target, registry) =>
        Effect.sync(() => {
          const { reviewers, requested } = target.input;
          const candidatesAtom = reviewerCandidates(target);
          const candidates = Option.getOrNull(AsyncResult.value(registry.get(candidatesAtom)));
          const selected =
            candidates?.candidates.filter((candidate) =>
              reviewers.some(
                (reviewer) => reviewer.id === candidate.id && reviewer.kind === candidate.kind,
              ),
            ) ?? [];
          const missingIdentities = selected.length < reviewers.length;
          updateCached(registry, candidatesAtom, (value) => ({
            ...value,
            candidates: value.candidates.map((candidate) =>
              selected.includes(candidate) ? { ...candidate, isRequested: requested } : candidate,
            ),
          }));
          const selectedLogins = new Set(
            selected.map((candidate) => candidate.login.toLowerCase()),
          );
          const updateReviewers = (
            actors: ReadonlyArray<PullRequestActor>,
            keep = (_actor: PullRequestActor) => false,
          ) =>
            requested
              ? [
                  ...actors,
                  ...selected
                    .filter(
                      (candidate) =>
                        !actors.some(
                          (actor) => actor.login.toLowerCase() === candidate.login.toLowerCase(),
                        ),
                    )
                    .map(({ login, name, avatarUrl }) => ({ login, name, avatarUrl })),
                ]
              : actors.filter(
                  (actor) => !selectedLogins.has(actor.login.toLowerCase()) || keep(actor),
                );
          updateCached(
            registry,
            detail(target),
            (value) => ({
              ...value,
              reviewers: updateReviewers(value.reviewers),
            }),
            missingIdentities,
          );
          updateCached(
            registry,
            activity(target),
            (value) => ({
              ...value,
              reviewers:
                value.reviewers === undefined
                  ? undefined
                  : updateReviewers(value.reviewers, (actor) =>
                      value.comments.some(
                        (comment) =>
                          (comment.kind === "review" || comment.kind === "review-comment") &&
                          comment.author?.login.toLowerCase() === actor.login.toLowerCase(),
                      ),
                    ),
            }),
            missingIdentities,
          );
        }),
    }),
    reviewGuide,
    generateReviewGuide: createMachineRpcCommand(runtime, {
      label: "machine-data:pull-requests:generate-review-guide",
      tag: WS_METHODS.pullRequestsGenerateReviewGuide,
      execute: (input) => routedRequest(WS_METHODS.pullRequestsGenerateReviewGuide, input),
      onSuccess: (target, registry, result) =>
        Effect.sync(() => {
          updateCached(registry, reviewGuide(target), () => result, true);
        }),
    }),
    labelCandidates,
    setLabels: createMachineRpcCommand(runtime, {
      label: "machine-data:pull-requests:set-labels",
      tag: WS_METHODS.pullRequestsSetLabels,
      execute: (input) => routedRequest(WS_METHODS.pullRequestsSetLabels, input),
      scheduler: commandScheduler,
      concurrency: serialPerMachine,
      onSuccess: (target, registry) =>
        Effect.sync(() => {
          const { labels, applied } = target.input;
          const candidatesAtom = labelCandidates(target);
          const candidates = Option.getOrNull(AsyncResult.value(registry.get(candidatesAtom)));
          const names = new Set(labels);
          updateCached(registry, candidatesAtom, (value) => ({
            ...value,
            candidates: value.candidates.map((candidate) =>
              names.has(candidate.name) ? { ...candidate, isApplied: applied } : candidate,
            ),
          }));
          updateCached(registry, detail(target), (value) => ({
            ...value,
            labels: applied
              ? [
                  ...value.labels,
                  ...labels
                    .filter((name) => !value.labels.some((label) => label.name === name))
                    .map((name) => ({
                      name,
                      color:
                        candidates?.candidates.find((candidate) => candidate.name === name)
                          ?.color ?? null,
                    })),
                ]
              : value.labels.filter((label) => !names.has(label.name)),
          }));
        }),
    }),
    setThreadResolution: createMachineRpcCommand(runtime, {
      label: "machine-data:pull-requests:set-thread-resolution",
      tag: WS_METHODS.pullRequestsSetThreadResolution,
      execute: (input) => routedRequest(WS_METHODS.pullRequestsSetThreadResolution, input),
      scheduler: commandScheduler,
      concurrency: serialPerMachine,
    }),
    setReaction: createMachineRpcCommand(runtime, {
      label: "machine-data:pull-requests:set-reaction",
      tag: WS_METHODS.pullRequestsSetReaction,
      execute: (input) => routedRequest(WS_METHODS.pullRequestsSetReaction, input),
      scheduler: commandScheduler,
      concurrency: serialPerMachine,
    }),
    /**
     * A separate request rather than a flag on a read, so only a person's refresh spends
     * host requests while every silent re-read shares the cache.
     */
    invalidate: createMachineRpcCommand(runtime, {
      label: "machine-data:pull-requests:invalidate",
      tag: WS_METHODS.pullRequestsInvalidate,
      execute: (input) => routedRequest(WS_METHODS.pullRequestsInvalidate, input),
      onSuccess: ({ machineId, input }, registry) =>
        Effect.sync(() => {
          if (input.reference !== undefined) {
            registry.refresh(preview({ machineId, input: input.reference }));
          }
        }),
      scheduler: commandScheduler,
      concurrency: serialPerMachine,
    }),
  };
}
