// The npm launcher package (scripts/build-npm-launcher.ts). Every install check, npm registry
// lookup and suggested `npx`/`bunx`/`npm install` command names it through this one constant.
// Kept free of Node types so browser clients can import it. Not yet published: claim it on npm
// before the first release.
export const NPM_LAUNCHER_PACKAGE_NAME = "vekrel";
