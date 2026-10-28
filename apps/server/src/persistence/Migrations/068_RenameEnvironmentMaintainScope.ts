import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

// The rename from "environment" to "machine" changed the scope literal; rows issued
// before it no longer decode, which breaks listing every session.
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  for (const table of ["auth_sessions", "auth_pairing_links"]) {
    yield* sql`
      UPDATE ${sql(table)}
      SET scopes = (
        SELECT json_group_array(DISTINCT CASE value
          WHEN 'environment:maintain' THEN 'machine:maintain'
          ELSE value
        END)
        FROM json_each(${sql(table)}.scopes)
      )
      WHERE scopes LIKE '%"environment:maintain"%'
    `;
  }
});
