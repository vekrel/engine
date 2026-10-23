import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as HostProcess from "@vekrel/shared/HostProcess";
import * as Effect from "effect/Effect";
import { vi } from "vite-plus/test";

import { resolveCliCommand } from "./invocation.ts";

// A name nothing hardcodes, so the suggestion can only come from the shared constant.
vi.mock("@vekrel/shared/cliRelease", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@vekrel/shared/cliRelease")>()),
  NPM_LAUNCHER_PACKAGE_NAME: "launcher-name-probe",
}));

it.layer(NodeServices.layer)("suggested runner commands", (it) => {
  it.effect("name the npm launcher package from NPM_LAUNCHER_PACKAGE_NAME", () =>
    Effect.gen(function* () {
      for (const [entry, runner] of [
        ["/home/u/.npm/_npx/abc/node_modules/x/dist/bin.mjs", "npx"],
        ["/home/u/.bun/install/cache/x/dist/bin.mjs", "bunx"],
      ] as const) {
        const command = yield* resolveCliCommand("pair").pipe(
          Effect.provideService(HostProcess.Arguments, ["node", entry]),
          Effect.provideService(HostProcess.IsExecutable, false),
          Effect.provideService(HostProcess.Environment, {}),
        );
        expect(command).toBe(`${runner} launcher-name-probe pair`);
      }
    }),
  );
});
