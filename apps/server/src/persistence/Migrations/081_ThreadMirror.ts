import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  // Names this database. A wiped or replaced one gets a new id, so a copy of its thread
  // list held elsewhere is never ordered against the new one by sequence.
  yield* sql`
    CREATE TABLE IF NOT EXISTS server_database_identity (
      singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
      database_id TEXT NOT NULL
    )
  `;
  yield* sql`
    INSERT OR IGNORE INTO server_database_identity (singleton, database_id)
    VALUES (1, lower(hex(randomblob(16))))
  `;

  // Thread lists other environments chose this one to hold. Kept apart from this
  // environment's own projections, one row per source, and never merged into them.
  yield* sql`
    CREATE TABLE IF NOT EXISTS thread_mirror_copies (
      source_environment_id TEXT PRIMARY KEY,
      source_database_id TEXT NOT NULL,
      source_label TEXT NOT NULL,
      source_sequence INTEGER NOT NULL,
      snapshot_json TEXT NOT NULL,
      reader_grant_hashes_json TEXT NOT NULL,
      writer_secret_hash TEXT NOT NULL,
      received_at TEXT NOT NULL,
      expires_at TEXT NOT NULL
    )
  `;
});
