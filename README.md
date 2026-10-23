<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset=".github/brand/logo-horizontal-on-dark.png">
    <img src=".github/brand/logo-horizontal-on-light.png" alt="Vekrel" width="360">
  </picture>
</p>

# Vekrel-Engine

The Node server that runs agents and serves the clients. Highly inspired by T3 Code (MIT, see NOTICE).

`packages/` is a git subtree of Vekrel-Shared's `packages/` (split with `git subtree split --prefix=packages`).

## Deviations from upstream T3 Code 8d3b061
- Root package name is `@vekrel/engine-root`; server package is `vekrel-engine`.
- `@vekrel/web` devDependency and the `build` task's `dependsOn: ["@vekrel/web#build"]` removed (web lives in another repo).
- `scripts` is not a workspace member (its typecheck references app files); kept on disk.
- Effect service keys `t3/...` renamed to `vekrel-engine/...` (required by the deterministicKeys check after the package rename).
- `SessionStore.awaitInvalidation` also re-reads the session row every 5 s while a socket is open, so revokes/expiry done outside Node (Rust daemon, CLI) close sockets and publish `clientRemoved`.
- Runtime CLI/executable names (`"t3"` in invocation.ts, serviceLauncher.ts, binCli.ts, etc.) are NOT renamed yet.
- Removed `patchedDependencies` entries (and patch files) for packages not installed here: 
  - react-native-image-viewing@0.2.2
  - alchemy@2.0.0-beta.80
  - expo-audio@58.0.9
  - expo-network@58.0.3
  - "@clerk/expo-native-components@0.2.0"
  - "@expo/metro-config@58.0.14"
  - expo-sharing@58.0.19
  - expo-widgets@58.0.20
  - "@legendapp/list@3.3.5"
  - "@react-native-ai/apple@0.12.0"
  - "@react-native-menu/menu@2.0.0"
  - "@react-navigation/native-stack@7.21.0"
  - dbus-next@0.10.2
  - react-native-gesture-handler@3.3.0
  - react-native-reanimated@4.7.1
  - react-native-keyboard-controller@1.22.7
  - react-native-pdf-jsi@5.0.0
  - react-native-webview@14.0.1
  - "@expo/ui@58.0.20"
  - expo-modules-core@58.0.22
  - "@expo/fingerprint@0.21.5"
  - app-builder-lib@26.16.1
## Default home
The default data directory is `~/.vekrel` (not `~/.t3`), so a machine with T3 Code installed does not share its database or config. Override with `VEKREL_HOME` or `--base-dir`. The dev-worktree `.t3` fallback (packages/shared/src/devHome.ts) and remote-host paths (`~/.t3/device`) are unchanged.

## Known upstream names
Deferred; these files still use `"t3"` as CLI, executable or npm-launcher name:
README.md
apps/server/scripts/acp-mock-agent.ts
apps/server/src/binCli.ts
apps/server/src/cli/config.test.ts
apps/server/src/cli/invocation.test.ts
apps/server/src/cli/invocation.ts
apps/server/src/cli/pair.test.ts
apps/server/src/cli/update.test.ts
apps/server/src/cloud/pinnedRuntime.test.ts
apps/server/src/cloud/pinnedRuntime.ts
apps/server/src/cloud/selfUpdate.test.ts
apps/server/src/entrypoint.test.ts
apps/server/src/environment/ServerEnvironment.test.ts
apps/server/src/orchestration-v2/Adapters/AcpAdapterV2.test.ts
apps/server/src/provider/Drivers/codexUsage.test.ts
apps/server/src/provider/ProviderCredentialStore.ts
apps/server/src/serverRuntimeState.test.ts
apps/server/src/serviceLauncher.test.ts
apps/server/src/serviceLauncher.ts
apps/server/vite.config.ts
scripts/build-cli-archive.ts
scripts/build-npm-launcher.test.ts
scripts/build-npm-launcher.ts
scripts/npm-launcher/t3.mjs
scripts/smoke-cli-archive.ts

## Updating packages/ from Vekrel-Shared
`packages/` is a subtree of Shared's `packages/` directory, so split it first (Shared's root is not the prefix):
```bash
git clone <shared-url-or-path> /tmp/vsplit && cd /tmp/vsplit && git subtree split --prefix=packages -b packages-only
cd <this repo>
git subtree add  --prefix=packages /tmp/vsplit packages-only --squash   # first time (done)
git subtree pull --prefix=packages /tmp/vsplit packages-only --squash   # later updates
```
