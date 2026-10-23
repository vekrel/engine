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
- `VEKREL_ADVERTISED_ORIGIN` (an http(s) origin; set by the Rust daemon to its public origin) replaces the engine's own origin in the startup `Connection string:`, `Pairing URL:`, QR and `pairingUrl:`, and is recorded as `advertisedOrigin` in the runtime state for `vekrel pair` to print. `origin` is unchanged. Unset or invalid (one warning): upstream behaviour.
- Internal T3 identifiers renamed to Vekrel (full mapping: ArchBoss `.superpowers/sdd/2026-10-23-plan-6/rename-identifiers-report.md`): root CLI command and standalone executable `vekrel`, cookies `vekrel_session*` / `vekrel_dev_session_*`, env vars and constants `T3_*` -> `VEKREL_*`, git refs `refs/vekrel/*`, service ids `vekrel.service` / `com.vekrel.vekrel.service` / `vekrel-server`, credential owner `"vekrel"`, provider client name `Vekrel`. The npm launcher package name is one constant, `NPM_LAUNCHER_PACKAGE_NAME` in packages/shared/src/cliRelease.ts (still `t3`, see the report).
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
The default data directory is `~/.vekrel` (not `~/.t3`), so a machine with T3 Code installed does not share its database or config. Override with `VEKREL_HOME` or `--base-dir`. Dev worktrees use their own `.vekrel`, and remote hosts use `~/.vekrel` (device, ssh-launch, runtime). There is no automatic move of `~/.t3`: that directory belongs to T3 Code.


## Updating packages/ from Vekrel-Shared
`packages/` is a subtree of Shared's `packages/` directory, so split it first (Shared's root is not the prefix):
```bash
git clone <shared-url-or-path> /tmp/vsplit && cd /tmp/vsplit && git subtree split --prefix=packages -b packages-only
cd <this repo>
git subtree add  --prefix=packages /tmp/vsplit packages-only --squash   # first time (done)
git subtree pull --prefix=packages /tmp/vsplit packages-only --squash   # later updates
```
