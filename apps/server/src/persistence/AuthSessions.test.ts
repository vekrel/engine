import { assert, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";

import * as AuthSessions from "./AuthSessions.ts";
import * as SqlitePersistence from "./Sqlite.ts";

const layer = it.layer(AuthSessions.layer.pipe(Layer.provideMerge(SqlitePersistence.layerMemory)));

layer("AuthSessionRepository", (it) => {
  it.effect("reads sessions issued before the environment -> machine scope rename", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const sessions = yield* AuthSessions.AuthSessionRepository;
      yield* sql`
        INSERT INTO auth_sessions (session_id, subject, scopes, method, issued_at, expires_at)
        VALUES ('legacy', 'owner', '["orchestration:read","environment:maintain"]',
          'browser-session-cookie', '2026-10-01T00:00:00.000Z', '2099-01-01T00:00:00.000Z')
      `;

      const active = yield* sessions.listActive({
        now: DateTime.makeUnsafe("2026-10-28T00:00:00.000Z"),
      });

      assert.deepStrictEqual(
        active.map((session) => session.scopes),
        [["orchestration:read", "machine:maintain"]],
      );
    }),
  );
});
