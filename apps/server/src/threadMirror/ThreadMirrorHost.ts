import {
  type EnvironmentId,
  OrchestrationV2ShellSnapshotJson,
  THREAD_MIRROR_MAX_READERS,
  THREAD_MIRROR_MAX_THREADS,
  type ThreadMirrorAck,
  type ThreadMirrorCopy,
  type ThreadMirrorExport,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Digest from "effect/crypto/Digest";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

export class ThreadMirrorHostError extends Schema.TaggedError<ThreadMirrorHostError>()(
  "ThreadMirrorHostError",
  {
    reason: Schema.Literals(["not_found", "too_large", "expired", "writer_mismatch", "storage"]),
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {
  override get message(): string {
    switch (this.reason) {
      case "not_found":
        return "No thread list copy is available for this environment.";
      case "too_large":
        return "The exported thread list is too large to keep.";
      case "expired":
        return "The exported thread list has already expired.";
      case "writer_mismatch":
        return "Another writer already exports this environment's thread list here.";
      case "storage":
        return "Could not read or write the thread list copy.";
    }
  }
}

interface CopyRow {
  readonly source_environment_id: string;
  readonly source_database_id: string;
  readonly source_label: string;
  readonly source_sequence: number;
  readonly snapshot_json: string;
  readonly reader_grant_hashes_json: string;
  readonly writer_secret_hash: string;
  readonly received_at: string;
  readonly expires_at: string;
}

const decodeHashes = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Array(Schema.String)));
const encodeHashes = Schema.encodeEffect(Schema.fromJsonString(Schema.Array(Schema.String)));
const decodeSnapshot = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2ShellSnapshotJson),
);
const encodeSnapshot = Schema.encodeEffect(Schema.fromJsonString(OrchestrationV2ShellSnapshotJson));

/**
 * Read-only copies of other environments' thread lists, held for their signed-in clients. A copy
 * never touches this environment's own projections: it sits in its own table, one row per source,
 * and only the source's own reader grants open it.
 */
export class ThreadMirrorHost extends Context.Service<
  ThreadMirrorHost,
  {
    /**
     * Keeps `copy` unless the host holds a newer one from the same database. The first writer to
     * export a source owns it, proven by the secret it keeps; the secret is trusted on first use.
     * Another writer takes over only once the held copy has expired.
     */
    readonly accept: (input: {
      readonly sourceEnvironmentId: EnvironmentId;
      readonly writerSecret: string;
      readonly copy: ThreadMirrorExport;
    }) => Effect.Effect<ThreadMirrorAck, ThreadMirrorHostError>;
    /** The copy when `grant` is one the source named and the copy has not expired. */
    readonly read: (input: {
      readonly sourceEnvironmentId: EnvironmentId;
      readonly grant: string;
    }) => Effect.Effect<ThreadMirrorCopy, ThreadMirrorHostError>;
    /** Drops a copy for its writer; the source stops exporting separately. */
    readonly remove: (input: {
      readonly sourceEnvironmentId: EnvironmentId;
      readonly writerSecret: string;
    }) => Effect.Effect<boolean, ThreadMirrorHostError>;
  }
>()("t3/threadMirror/ThreadMirrorHost") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const digest = yield* Digest.Digest;

  const storage = (cause: unknown) => new ThreadMirrorHostError({ reason: "storage", cause });

  const load = (sourceEnvironmentId: string) =>
    sql<CopyRow>`
      SELECT * FROM thread_mirror_copies WHERE source_environment_id = ${sourceEnvironmentId}
    `.pipe(
      Effect.map((rows) => Option.fromNullishOr(rows[0])),
      Effect.mapError(storage),
    );

  const drop = (sourceEnvironmentId: string) =>
    sql`DELETE FROM thread_mirror_copies WHERE source_environment_id = ${sourceEnvironmentId}`.pipe(
      Effect.mapError(storage),
    );

  const expired = (expiresAt: string, now: DateTime.Utc) =>
    DateTime.toEpochMillis(DateTime.makeUnsafe(expiresAt)) <= DateTime.toEpochMillis(now);

  const sha256Hex = (value: string) =>
    digest
      .digest(Digest.sha256, new TextEncoder().encode(value))
      .pipe(Effect.map(Hex.encode), Effect.orDie);

  const accept = Effect.fn("ThreadMirrorHost.accept")(function* (input: {
    readonly sourceEnvironmentId: EnvironmentId;
    readonly writerSecret: string;
    readonly copy: ThreadMirrorExport;
  }) {
    const { copy } = input;
    if (
      copy.snapshot.threads.length + copy.snapshot.archivedThreads.length >
        THREAD_MIRROR_MAX_THREADS ||
      copy.readerGrantHashes.length > THREAD_MIRROR_MAX_READERS
    ) {
      return yield* new ThreadMirrorHostError({ reason: "too_large" });
    }
    const now = yield* DateTime.now;
    if (DateTime.toEpochMillis(copy.expiresAt) <= DateTime.toEpochMillis(now)) {
      return yield* new ThreadMirrorHostError({ reason: "expired" });
    }
    const secretHash = yield* sha256Hex(input.writerSecret);
    const snapshotJson = yield* encodeSnapshot(copy.snapshot).pipe(Effect.mapError(storage));
    const hashesJson = yield* encodeHashes(copy.readerGrantHashes).pipe(Effect.mapError(storage));
    const nowIso = DateTime.formatIso(now);
    // One statement decides owner and order, so concurrent writes cannot overwrite a newer copy.
    const written = yield* sql<{ readonly source_sequence: number }>`
      INSERT INTO thread_mirror_copies (
        source_environment_id, source_database_id, source_label, source_sequence,
        snapshot_json, reader_grant_hashes_json, writer_secret_hash, received_at, expires_at
      ) VALUES (
        ${input.sourceEnvironmentId}, ${copy.sourceDatabaseId}, ${copy.sourceLabel},
        ${copy.sourceSequence}, ${snapshotJson}, ${hashesJson}, ${secretHash},
        ${nowIso}, ${DateTime.formatIso(copy.expiresAt)}
      )
      ON CONFLICT (source_environment_id) DO UPDATE SET
        source_database_id = excluded.source_database_id,
        source_label = excluded.source_label,
        source_sequence = excluded.source_sequence,
        snapshot_json = excluded.snapshot_json,
        reader_grant_hashes_json = excluded.reader_grant_hashes_json,
        writer_secret_hash = excluded.writer_secret_hash,
        received_at = excluded.received_at,
        expires_at = excluded.expires_at
      WHERE thread_mirror_copies.expires_at <= ${nowIso}
        OR (
          thread_mirror_copies.writer_secret_hash = excluded.writer_secret_hash
          AND (
            thread_mirror_copies.source_database_id <> excluded.source_database_id
            OR excluded.source_sequence >= thread_mirror_copies.source_sequence
          )
        )
      RETURNING source_sequence
    `.pipe(Effect.mapError(storage));
    if (written.length > 0) {
      return { outcome: "stored", heldSequence: copy.sourceSequence } as const;
    }
    const held = yield* load(input.sourceEnvironmentId);
    if (Option.isNone(held) || held.value.writer_secret_hash !== secretHash) {
      return yield* new ThreadMirrorHostError({ reason: "writer_mismatch" });
    }
    return { outcome: "stale", heldSequence: held.value.source_sequence } as const;
  });

  const read = Effect.fn("ThreadMirrorHost.read")(function* (input: {
    readonly sourceEnvironmentId: EnvironmentId;
    readonly grant: string;
  }) {
    // Every refusal is the same "not found", so a reader cannot probe which sources exist.
    const notFound = new ThreadMirrorHostError({ reason: "not_found" });
    const held = yield* load(input.sourceEnvironmentId);
    if (Option.isNone(held)) return yield* notFound;
    const row = held.value;
    const now = yield* DateTime.now;
    if (expired(row.expires_at, now)) {
      yield* drop(input.sourceEnvironmentId);
      return yield* notFound;
    }
    const hashes = yield* decodeHashes(row.reader_grant_hashes_json).pipe(Effect.mapError(storage));
    if (!hashes.includes(yield* sha256Hex(input.grant))) return yield* notFound;
    const snapshot = yield* decodeSnapshot(row.snapshot_json).pipe(Effect.mapError(storage));
    return {
      sourceEnvironmentId: input.sourceEnvironmentId,
      sourceDatabaseId: row.source_database_id,
      sourceSequence: row.source_sequence,
      sourceLabel: row.source_label,
      snapshot,
      receivedAt: DateTime.makeUnsafe(row.received_at),
      expiresAt: DateTime.makeUnsafe(row.expires_at),
    };
  });

  const remove = Effect.fn("ThreadMirrorHost.remove")(function* (input: {
    readonly sourceEnvironmentId: EnvironmentId;
    readonly writerSecret: string;
  }) {
    const secretHash = yield* sha256Hex(input.writerSecret);
    const removed = yield* sql`
      DELETE FROM thread_mirror_copies
      WHERE source_environment_id = ${input.sourceEnvironmentId}
        AND writer_secret_hash = ${secretHash}
      RETURNING source_environment_id
    `.pipe(Effect.mapError(storage));
    if (removed.length > 0) return true;
    return Option.isSome(yield* load(input.sourceEnvironmentId))
      ? yield* new ThreadMirrorHostError({ reason: "writer_mismatch" })
      : false;
  });

  return ThreadMirrorHost.of({ accept, read, remove });
});

export const layer = Layer.effect(ThreadMirrorHost, make);
