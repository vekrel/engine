import type { UsageProviderKind, UsageSummary } from "@vekrel/contracts";

interface UsageProgressMachine {
  readonly label: string;
  readonly isConnected: boolean;
  readonly isPending: boolean;
  readonly error: string | null;
  readonly summary: UsageSummary | null;
}

export function usageMachineProgress(
  machine: UsageProgressMachine,
  refreshing = false,
) {
  const { summary } = machine;
  if (!machine.isConnected || machine.error !== null) return { phase: "inactive" } as const;
  if (summary === null) return { phase: "loading" } as const;
  if (refreshing) return { phase: "stale" } as const;
  if (!machine.isPending) return { phase: "ready" } as const;
  const providers = [
    ...new Set(
      summary.sources.flatMap((source) => (source.refreshing ? [source.fingerprint.provider] : [])),
    ),
  ];
  return providers.length > 0
    ? ({ phase: "partway", providers } as const)
    : ({ phase: "stale" } as const);
}

export function updatingProvidersLabel(
  providers: readonly UsageProviderKind[],
  providerLabel: (provider: UsageProviderKind) => string,
) {
  return providers.length === 1
    ? `Updating ${providerLabel(providers[0]!)}…`
    : `Updating ${providers.length} providers…`;
}

export function usageProgress(
  machines: readonly UsageProgressMachine[],
  {
    refreshing = false,
    providerLabel,
  }: {
    readonly refreshing?: boolean;
    readonly providerLabel: (provider: UsageProviderKind) => string;
  },
) {
  const progress = machines.map((machine) => ({
    label: machine.label,
    ...usageMachineProgress(machine, refreshing),
  }));
  const waiting = progress.filter(({ phase }) => phase === "loading" || phase === "stale");
  const providers = [
    ...new Set(progress.flatMap((entry) => (entry.phase === "partway" ? entry.providers : []))),
  ];
  const answered = progress.some(({ phase }) => phase === "partway" || phase === "ready");
  return {
    dimmed: !answered && progress.some(({ phase }) => phase === "stale"),
    label:
      waiting.length === 0
        ? providers.length === 0
          ? null
          : updatingProvidersLabel(providers, providerLabel)
        : machines.length === 1
          ? "Updating…"
          : waiting.length === 1
            ? `Updating ${waiting[0]!.label}…`
            : `Updating ${waiting.length} machines…`,
  };
}

export function usageLoadingState(
  machines: readonly UsageProgressMachine[],
  refreshing = false,
) {
  const providers = new Set<UsageProviderKind>();
  let everyProvider = false;
  for (const machine of machines) {
    const progress = usageMachineProgress(machine, refreshing);
    if (progress.phase === "loading" || progress.phase === "stale") everyProvider = true;
    if (progress.phase === "partway")
      for (const provider of progress.providers) providers.add(provider);
  }
  return { partial: everyProvider || providers.size > 0, everyProvider, providers };
}
