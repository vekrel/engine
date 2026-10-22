# Vekrel-Engine

The Node server that runs agents and serves the clients; derived from T3 Code (MIT, see NOTICE).

`packages/` is a git subtree of Vekrel-Shared's `packages/` (split with `git subtree split --prefix=packages`).

## Deviations from upstream T3 Code 8d3b061
- Root package name is `@vekrel/engine-root`; server package is `vekrel-engine`.
- `@vekrel/web` devDependency and the `build` task's `dependsOn: ["@vekrel/web#build"]` removed (web lives in another repo).
- `scripts` is not a workspace member (its typecheck references app files); kept on disk.
- Effect service keys `t3/...` renamed to `vekrel-engine/...` (required by the deterministicKeys check after the package rename).
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