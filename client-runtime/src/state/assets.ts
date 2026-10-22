import {
  type AssetCreateUrlInput,
  type AssetCreateUrlResult,
  type AssetImageDimensions,
  AssetResource,
  MachineId,
  type ProjectCloneSnapshot,
  WS_METHODS,
} from "@vekrel/contracts";
import { mediaMimeTypeFromExtension } from "@vekrel/shared/filePreview";
import { isWindowsAbsolutePath } from "@vekrel/shared/path";
import {
  getProjectFaviconResourceKey,
  isProjectFaviconFallbackUrl,
} from "@vekrel/shared/projectFavicon";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity";

import * as MachineRegistry from "../connection/registry.ts";
import * as MachineSupervisor from "../connection/supervisor.ts";
import { request } from "../rpc/client.ts";
import type { ProjectFaviconCache, ProjectFaviconTarget } from "../projectFaviconCache.ts";
import { createMachineQueryAtomFamily } from "./runtime.ts";

const ASSET_URL_REFRESH_INTERVAL_MS = 30 * 60_000;
const ASSET_URL_STALE_TIME_MS = 5 * 60_000;
const ASSET_URL_IDLE_TTL_MS = 60 * 60_000;

export class InvalidAssetCollectionKeyError extends Schema.TaggedError<InvalidAssetCollectionKeyError>()(
  "InvalidAssetCollectionKeyError",
  {
    key: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Invalid asset collection atom key: ${JSON.stringify(this.key)}.`;
  }
}

const decodeAssetCollectionKey = Schema.decodeUnknownSync(
  Schema.Tuple([MachineId, Schema.Array(AssetResource)]),
);

function parseAssetCollectionKey(
  key: string,
): readonly [MachineId, ReadonlyArray<AssetResource>] {
  try {
    return decodeAssetCollectionKey(JSON.parse(key));
  } catch (cause) {
    throw new InvalidAssetCollectionKeyError({ key, cause });
  }
}

export function resolveAssetUrl(httpBaseUrl: string, relativeUrl: string): string | null {
  try {
    return new URL(relativeUrl, httpBaseUrl).toString();
  } catch {
    return null;
  }
}

export const EMPTY_ASSET_URL_ATOM = Atom.make(AsyncResult.initial<never, never>(false)).pipe(
  Atom.withLabel("asset-url:empty"),
);

export type AssetUrlState =
  | { readonly _tag: "Loading" }
  | {
      readonly _tag: "Failure";
      readonly reason?: "missing-file-access";
    }
  | {
      readonly _tag: "Success";
      readonly url: string;
      readonly expiresAt: number;
      readonly sourcePath?: string;
      readonly imageDimensions?: AssetImageDimensions;
      readonly fileRevision?: string;
    };

export const MISSING_FILE_ACCESS_MESSAGE =
  "This connection cannot read host files. Pair again with a new link to view it.";

export function blockedAssetUrlState(
  resource: AssetResource | null,
  fileAccess: {
    readonly canReadFiles: boolean;
    readonly isPending: boolean;
    readonly error: string | null;
  },
): AssetUrlState | null {
  if (
    resource === null ||
    fileAccess.canReadFiles ||
    (resource._tag !== "workspace-file" &&
      resource._tag !== "media-file" &&
      resource._tag !== "draft-workspace-file")
  ) {
    return null;
  }
  if (fileAccess.isPending) return { _tag: "Loading" };
  return fileAccess.error === null
    ? { _tag: "Failure", reason: "missing-file-access" }
    : { _tag: "Failure" };
}

export function assetUrlStateFromResult(
  result: AsyncResult.AsyncResult<AssetCreateUrlResult, unknown>,
  httpBaseUrl: string | null,
): AssetUrlState {
  if (result._tag === "Failure") return { _tag: "Failure" };
  if (httpBaseUrl === null || result._tag !== "Success") return { _tag: "Loading" };
  const url = resolveAssetUrl(httpBaseUrl, result.value.relativeUrl);
  if (url === null) return { _tag: "Failure" };
  return {
    _tag: "Success",
    url,
    expiresAt: result.value.expiresAt,
    ...(result.value.sourcePath !== undefined ? { sourcePath: result.value.sourcePath } : {}),
    ...(result.value.imageDimensions !== undefined
      ? { imageDimensions: result.value.imageDimensions }
      : {}),
    ...(result.value.fileRevision !== undefined ? { fileRevision: result.value.fileRevision } : {}),
  };
}

export function createAssetMachineAtoms<R, E>(
  runtime: Atom.AtomRuntime<MachineRegistry.MachineRegistry | R, E>,
  localMediaMachine?: Atom.Atom<{
    readonly machineId: MachineId;
    readonly httpBaseUrl: string;
  } | null>,
) {
  const execute = Effect.fn("assets.createUrl")(function* (input: AssetCreateUrlInput) {
    const result = yield* request(WS_METHODS.assetsCreateUrl, input).pipe(Effect.result);
    if (Result.isSuccess(result)) return result.success;
    const error = result.failure;
    const resource = input.resource;
    const local = localMediaMachine
      ? (yield* AtomRegistry.AtomRegistry).get(localMediaMachine)
      : null;
    const supervisor = yield* MachineSupervisor.MachineSupervisor;
    if (
      !local ||
      local.machineId === supervisor.target.machineId ||
      !(
        error._tag === "AssetWorkspaceAssetNotFoundError" ||
        error._tag === "AssetWorkspaceAssetInspectionError" ||
        error._tag === "AssetWorkspaceContextNotFoundError"
      ) ||
      resource._tag !== "media-file" ||
      !(resource.path.startsWith("/") || isWindowsAbsolutePath(resource.path)) ||
      mediaMimeTypeFromExtension(resource.path.slice(resource.path.lastIndexOf("."))) === null
    )
      return yield* error;
    const registry = yield* MachineRegistry.MachineRegistry;
    // Map the full cause so retry defects and interruption survive.
    const asset = yield* registry
      .run(local.machineId, request(WS_METHODS.assetsCreateUrl, input))
      .pipe(Effect.catchCause((cause) => Effect.failCause(Cause.map(cause, () => error))));
    return { ...asset, relativeUrl: new URL(asset.relativeUrl, local.httpBaseUrl).href };
  });
  const createUrl = createMachineQueryAtomFamily(runtime, {
    label: "machine-data:assets:create-url",
    execute,
    staleTimeMs: ASSET_URL_STALE_TIME_MS,
    idleTtlMs: ASSET_URL_IDLE_TTL_MS,
    refreshIntervalMs: ASSET_URL_REFRESH_INTERVAL_MS,
    refreshTrigger: ({ input }) =>
      input.resource._tag === "media-file" ? localMediaMachine : undefined,
  });
  const createUrlsFamily = Atom.family((key: string) => {
    const [machineId, resources] = parseAssetCollectionKey(key);
    return Atom.make((get) =>
      resources.map((resource) =>
        get(
          createUrl({
            machineId,
            input: { resource },
          }),
        ),
      ),
    ).pipe(
      Atom.setIdleTTL(ASSET_URL_IDLE_TTL_MS),
      Atom.withLabel(`machine-data:assets:create-urls:${key}`),
    );
  });

  return {
    createUrl,
    createUrls: (target: {
      readonly machineId: MachineId;
      readonly resources: ReadonlyArray<AssetResource>;
    }) => createUrlsFamily(JSON.stringify([target.machineId, target.resources])),
  };
}

export function createProjectFaviconUrlAtomFamily(input: {
  readonly imageCache?: ProjectFaviconCache;
  readonly createUrl: (target: {
    readonly machineId: MachineId;
    readonly input: { readonly resource: AssetResource };
  }) => Atom.Atom<AsyncResult.AsyncResult<AssetCreateUrlResult, unknown>>;
  readonly preparedConnection: (
    machineId: MachineId,
  ) => Atom.Atom<Option.Option<{ readonly httpBaseUrl: string }>>;
  readonly projectClones?: (
    machineId: MachineId,
  ) => Atom.Atom<ReadonlyArray<ProjectCloneSnapshot>>;
}) {
  const decodeKey = Schema.decodeUnknownSync(
    Schema.Tuple([MachineId, Schema.String, Schema.NullOr(Schema.String)]),
  );
  const projectClones = input.projectClones;
  const family = Atom.family((key: string) => {
    const [machineId, cwd, path] = decodeKey(JSON.parse(key));
    const resource = { _tag: "project-favicon" as const, cwd, ...(path ? { path } : {}) };
    const query = input.createUrl({ machineId, input: { resource } });
    // A cloned project exists before its files do, and the server reports its
    // icon missing until the clone lands. Ask again whenever the clone's phase
    // changes: the first list a client sees may already say done.
    const request = projectClones
      ? query.pipe(
          Atom.makeRefreshOnSignal(
            Atom.make(
              (get) =>
                get(projectClones(machineId)).find((clone) => clone.destinationPath === cwd)
                  ?.phase ?? null,
            ),
          ),
        )
      : query;
    const resolvedUrl = Atom.make((get): string | null => {
      const result = get(request);
      const connection = get(input.preparedConnection(machineId));
      const state = assetUrlStateFromResult(
        result,
        Option.isSome(connection) ? connection.value.httpBaseUrl : null,
      );
      return state._tag === "Success" ? state.url : Option.getOrNull(get.self<string | null>());
    }).pipe(Atom.setIdleTTL(ASSET_URL_IDLE_TTL_MS));
    const cache = input.imageCache;
    if (!cache) return resolvedUrl;

    const target = { machineId, cwd, faviconPath: path };
    const image = Atom.make((get) => {
      get(request);
      const url = get(resolvedUrl);
      return Effect.promise((signal) => cache.resolve(target, url, signal));
    }).pipe(Atom.setIdleTTL(ASSET_URL_IDLE_TTL_MS));

    return Atom.make((get): string | null => {
      const result = get(image);
      if (isProjectFaviconFallbackUrl(get(resolvedUrl))) return null;
      return Option.getOrElse(AsyncResult.value(result), () => cache.peek(target));
    }).pipe(Atom.setIdleTTL(ASSET_URL_IDLE_TTL_MS));
  });
  return (target: ProjectFaviconTarget) =>
    family(getProjectFaviconResourceKey(target.machineId, target.cwd, target.faviconPath));
}
