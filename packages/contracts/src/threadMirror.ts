import * as Schema from "effect/Schema";

import { EnvironmentId, NonNegativeInt, TrimmedNonEmptyString } from "./baseSchemas.ts";
import { OrchestrationV2ShellSnapshot } from "./orchestrationV2.ts";

/** Most threads one exported thread list may hold; a larger one is not stored. */
export const THREAD_MIRROR_MAX_THREADS = 1500;
/** Largest export request body the host reads. */
export const THREAD_MIRROR_MAX_BODY_BYTES = 16 * 1024 * 1024;
/** Most readers one export names; readers past this cannot use the cached copy. */
export const THREAD_MIRROR_MAX_READERS = 64;

/**
 * Where a thread list stands at its source: the source's database and the
 * sequence it had reached there. Sequences only compare inside one database.
 */
export const ThreadMirrorPosition = Schema.Struct({
  sourceDatabaseId: TrimmedNonEmptyString,
  sourceSequence: NonNegativeInt,
});
export type ThreadMirrorPosition = typeof ThreadMirrorPosition.Type;

/**
 * A complete, bounded thread list the source environment sends to the one host
 * it chose. The host keeps it read-only and apart from its own threads.
 */
export const ThreadMirrorExport = Schema.Struct({
  ...ThreadMirrorPosition.fields,
  sourceLabel: TrimmedNonEmptyString,
  snapshot: OrchestrationV2ShellSnapshot,
  /**
   * SHA-256 (hex) of each reader grant the source accepts. The host stores only these, but sees a
   * grant in plain text each time a client presents it to read.
   */
  readerGrantHashes: Schema.Array(TrimmedNonEmptyString),
  /** After this the host stops serving the copy and forgets it. */
  expiresAt: Schema.DateTimeUtc,
});
export type ThreadMirrorExport = typeof ThreadMirrorExport.Type;

/** `stale` means the host already holds a newer copy from this database and kept it. */
export const ThreadMirrorAck = Schema.Struct({
  outcome: Schema.Literals(["stored", "stale"]),
  heldSequence: NonNegativeInt,
});
export type ThreadMirrorAck = typeof ThreadMirrorAck.Type;

/** What a reader gets from the host: the source's list and when the host last heard from it. */
export const ThreadMirrorCopy = Schema.Struct({
  ...ThreadMirrorPosition.fields,
  sourceEnvironmentId: EnvironmentId,
  sourceLabel: TrimmedNonEmptyString,
  snapshot: OrchestrationV2ShellSnapshot,
  receivedAt: Schema.DateTimeUtc,
  expiresAt: Schema.DateTimeUtc,
});
export type ThreadMirrorCopy = typeof ThreadMirrorCopy.Type;

/**
 * Issued by the source to a signed-in client of its own, so that client alone
 * can read the source's copy on the host while the source is offline. `null`
 * host means the source does not export.
 */
export const ThreadMirrorReaderGrant = Schema.Struct({
  hostEnvironmentId: Schema.NullOr(EnvironmentId),
  sourceDatabaseId: TrimmedNonEmptyString,
  grant: Schema.NullOr(TrimmedNonEmptyString),
});
export type ThreadMirrorReaderGrant = typeof ThreadMirrorReaderGrant.Type;

export const THREAD_MIRROR_GRANT_HEADER = "x-t3-thread-mirror-grant";

/**
 * A random secret the source keeps and sends with every write. The host stores its hash on first
 * write and refuses other writers while the copy lives. The first claim on a source id is trusted:
 * a linked environment could claim another's id before that one exports.
 */
export const THREAD_MIRROR_WRITER_HEADER = "x-t3-thread-mirror-writer";

export const ThreadMirrorRemoveResult = Schema.Struct({ removed: Schema.Boolean });
export type ThreadMirrorRemoveResult = typeof ThreadMirrorRemoveResult.Type;

/**
 * Whether a client replaces the rows it holds for a source with the host's copy. Only a strictly
 * newer sequence in the same database does: with another or unknown database nothing says which is
 * newer, and wall-clock time must not decide. Nothing held takes any copy.
 */
export function threadMirrorSupersedes(
  held: { readonly databaseId: string | undefined; readonly sequence: number } | null,
  copy: ThreadMirrorPosition,
): boolean {
  if (held === null) return true;
  return held.databaseId === copy.sourceDatabaseId && copy.sourceSequence > held.sequence;
}
