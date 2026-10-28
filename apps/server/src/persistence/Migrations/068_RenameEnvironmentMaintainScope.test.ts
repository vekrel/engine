import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@vekrel/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";

const layer = it.layer(NodeSqliteClient.layer({ filename: ":memory:" }));

layer("068_RenameEnvironmentMaintainScope", (it) => {
  it.effect("renames the legacy scope once and leaves other rows alone", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 67 });
      const insertSession = (id: string, scopes: ReadonlyArray<string>) =>
        sql`INSERT INTO auth_sessions (session_id, subject, scopes, method, issued_at, expires_at)
            VALUES (${id}, 'owner', ${JSON.stringify(scopes)}, 'pairing', '2026-10-01', '2027-10-01')`;
      yield* insertSession("legacy", ["orchestration:read", "environment:maintain"]);
      yield* insertSession("both", ["environment:maintain", "machine:maintain"]);
      yield* insertSession("current", ["orchestration:read", "machine:maintain"]);
      yield* sql`INSERT INTO auth_pairing_links (id, credential, method, scopes, subject, created_at, expires_at)
            VALUES ('link', 'secret', 'pairing', '["environment:maintain"]', 'owner', '2026-10-01', '2027-10-01')`;

      yield* runMigrations({ toMigrationInclusive: 68 });

      const sessions = yield* sql<{ session_id: string; scopes: string }>`
        SELECT session_id, scopes FROM auth_sessions ORDER BY session_id`;
      assert.deepStrictEqual(
        sessions.map((row) => [row.session_id, JSON.parse(row.scopes)]),
        [
          ["both", ["machine:maintain"]],
          ["current", ["orchestration:read", "machine:maintain"]],
          ["legacy", ["orchestration:read", "machine:maintain"]],
        ],
      );
      const [link] = yield* sql<{ scopes: string }>`SELECT scopes FROM auth_pairing_links`;
      assert.deepStrictEqual(JSON.parse(link!.scopes), ["machine:maintain"]);
    }),
  );
});
