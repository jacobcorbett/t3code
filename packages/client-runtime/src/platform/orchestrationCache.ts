import {
  EnvironmentId,
  OrchestrationV2ShellSnapshotJson,
  OrchestrationV2ThreadDetailSnapshot,
  OrchestrationV2ThreadProjectionJson,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

export const ORCHESTRATION_CACHE_SCHEMA_VERSION = 3 as const;
// Older caches may contain human turns that bypassed server history budgets.
export const ORCHESTRATION_THREAD_CACHE_SCHEMA_VERSION = 5 as const;

/**
 * The server snapshot the cached rows descend from. Written only for rows that have one, so a
 * cache from before it existed decodes and never resumes.
 */
export const ShellCacheOrigin = Schema.Struct({
  serverVersion: Schema.String,
  databaseMigration: Schema.NullOr(Schema.Int),
  omitIdleSubagents: Schema.Boolean,
  omitOlderSettled: Schema.Boolean,
  /** Epoch milliseconds of that snapshot; replay does not renew it. */
  snapshotAt: Schema.Number,
  /** The server's database when the rows were taken; a replaced one never resumes from them. */
  databaseId: Schema.optionalKey(Schema.String),
  /**
   * Where a copy of the rows is kept while the server is off, and this client's key to it, as the
   * server issued them with these rows.
   */
  mirror: Schema.optionalKey(
    Schema.Struct({ hostEnvironmentId: EnvironmentId, grant: Schema.String }),
  ),
  /**
   * The rows came from the host's copy, so no cursor of theirs can resume a live stream. Kept with
   * them so a restart still says where they came from and drops them once the host's lease ends.
   */
  fromMirror: Schema.optionalKey(
    Schema.Struct({
      sourceLabel: Schema.String,
      /** Epoch milliseconds when the host last heard from the source. */
      syncedAt: Schema.Number,
      /** Epoch milliseconds when the host stops serving this copy. */
      expiresAt: Schema.Number,
    }),
  ),
});
export type ShellCacheOrigin = typeof ShellCacheOrigin.Type;

export const StoredOrchestrationShellSnapshot = Schema.Struct({
  schemaVersion: Schema.Literal(ORCHESTRATION_CACHE_SCHEMA_VERSION),
  environmentId: EnvironmentId,
  snapshot: OrchestrationV2ShellSnapshotJson,
  origin: Schema.optionalKey(ShellCacheOrigin),
});

export const StoredOrchestrationThreadSnapshot = Schema.Struct({
  schemaVersion: Schema.Literal(ORCHESTRATION_THREAD_CACHE_SCHEMA_VERSION),
  environmentId: EnvironmentId,
  threadId: ThreadId,
  snapshot: OrchestrationV2ThreadDetailSnapshot.mapFields((fields) => ({
    ...fields,
    projection: OrchestrationV2ThreadProjectionJson,
  })),
});

export function decodeOrDiscardOrchestrationCache<A, E, R, E2, R2>(
  decode: Effect.Effect<Option.Option<A>, E, R>,
  discard: Effect.Effect<void, E2, R2>,
) {
  return decode.pipe(Effect.catch(() => discard.pipe(Effect.as(Option.none<A>()))));
}
