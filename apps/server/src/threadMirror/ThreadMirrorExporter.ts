import {
  AuthOrchestrationReadScope,
  EnvironmentHttpApi,
  type EnvironmentId,
  ORCHESTRATION_PROTOCOL_HEADER,
  ORCHESTRATION_PROTOCOL_VERSION_TEXT,
  type OrchestrationV2ShellSnapshot,
  THREAD_MIRROR_WRITER_HEADER,
  type ThreadMirrorAck,
  type ThreadMirrorExport,
  type ThreadMirrorReaderGrant,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Digest from "effect/crypto/Digest";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpApiClient from "effect/http-api/HttpApiClient";
import * as SqlClient from "effect/sql/SqlClient";

import * as PayloadSigner from "../auth/PayloadSigner.ts";
import * as SessionStore from "../auth/SessionStore.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as PeerLinks from "../peer/PeerLinks.ts";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ThreadMirrorShellSource from "./ThreadMirrorShellSource.ts";

/** How long the host serves an export after the last one, if this environment never exports again. */
const THREAD_MIRROR_TTL = Duration.days(7);
/** An unchanged list is sent again this often, so the host's copy never nears its expiry. */
const THREAD_MIRROR_RENEW_EVERY = Duration.hours(6);
const RETRY_BASE = Duration.seconds(30);
const RETRY_MAX = Duration.minutes(10);
/** A streaming agent changes the list constantly; at most one export per this window. */
const CHANGES_WINDOW = Duration.seconds(15);
const READER_KEY_SECRET = "thread-mirror-reader-key";
const WRITER_SECRET = "thread-mirror-writer";

/**
 * The complete list as the host keeps it: titles, states, paths, branches and times. Message
 * text, error text, goals and background task descriptions stay on this environment, and
 * archived threads are not part of the list clients read first.
 */
export const buildThreadMirrorExport = (input: {
  readonly snapshot: OrchestrationV2ShellSnapshot;
  readonly sourceDatabaseId: string;
  readonly sourceLabel: string;
  readonly readerGrantHashes: ReadonlyArray<string>;
  readonly expiresAt: DateTime.Utc;
}): ThreadMirrorExport => ({
  sourceDatabaseId: input.sourceDatabaseId,
  sourceSequence: input.snapshot.snapshotSequence,
  sourceLabel: input.sourceLabel,
  snapshot: {
    ...input.snapshot,
    threads: input.snapshot.threads.map((thread) => ({
      ...thread,
      latestVisibleMessage: null,
      lastError: null,
      goal: null,
      pendingBackgroundTasks: [],
    })),
    archivedThreads: [],
  },
  readerGrantHashes: input.readerGrantHashes,
  expiresAt: input.expiresAt,
});

export class ThreadMirrorExportError extends Schema.TaggedError<ThreadMirrorExportError>()(
  "ThreadMirrorExportError",
  {
    stage: Schema.Literals(["snapshot", "grants", "peer", "push"]),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Could not export the thread list (${this.stage}).`;
  }
}

/**
 * Sends this environment's thread list to the one host the user chose, and tells its own signed-in
 * clients how to read it there. The host only ever holds a copy: threads, commands and live state
 * stay here.
 */
export class ThreadMirrorExporter extends Context.Service<
  ThreadMirrorExporter,
  {
    /** The caller's key to this environment's copy on the host; it names no one else's. */
    readonly readerGrant: (
      sessionId: string,
    ) => Effect.Effect<ThreadMirrorReaderGrant, ThreadMirrorExportError>;
    /** Pushes the current list. Nothing when no host is chosen. */
    readonly exportNow: Effect.Effect<Option.Option<ThreadMirrorAck>, ThreadMirrorExportError>;
    /** Asks a host to forget this environment's copy. */
    readonly withdraw: (
      hostEnvironmentId: EnvironmentId,
    ) => Effect.Effect<void, ThreadMirrorExportError>;
    /**
     * Exports at startup, as the list, readers or host choice change, and every few hours so the
     * copy never expires; retries failures with a bounded delay and withdraws from a host no
     * longer chosen. Never ends.
     */
    readonly run: Effect.Effect<never>;
  }
>()("t3/threadMirror/ThreadMirrorExporter") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const source = yield* ThreadMirrorShellSource.ThreadMirrorShellSource;
  const sessions = yield* SessionStore.SessionStore;
  const links = yield* PeerLinks.PeerLinks;
  const environment = yield* ServerEnvironment.ServerEnvironment;
  const settings = yield* ServerSettings.ServerSettingsService;
  const secrets = yield* ServerSecretStore.ServerSecretStore;
  const digest = yield* Digest.Digest;
  const signer = yield* PayloadSigner.make(READER_KEY_SECRET);
  const httpClient = yield* HttpClient.HttpClient;
  // A redirect could carry the token to an address the link never vouched for.
  const peerHttpClient = HttpClient.transform(httpClient, (response) =>
    Effect.provideService(response, FetchHttpClient.RequestInit, { redirect: "manual" }),
  );

  const hostId = settings.getSettings.pipe(
    Effect.map((current) =>
      current.linkedEnvironmentsEnabled ? current.threadMirrorHostEnvironmentId : null,
    ),
    Effect.orElseSucceed(() => null),
  );

  const databaseId = sql<{ readonly database_id: string }>`
    SELECT database_id FROM server_database_identity
  `.pipe(
    Effect.map((rows) => rows[0]!.database_id),
    Effect.mapError((cause) => new ThreadMirrorExportError({ stage: "snapshot", cause })),
  );

  const grantFor = (databaseIdValue: string, sessionId: string) =>
    signer.sign(`${databaseIdValue}:${sessionId}`);

  const sha256Hex = (value: string) =>
    digest
      .digest(Digest.sha256, new TextEncoder().encode(value))
      .pipe(Effect.map(Hex.encode), Effect.orDie);

  const readerGrant = Effect.fn("ThreadMirrorExporter.readerGrant")(
    function* (sessionId: string) {
      const hostEnvironmentId = yield* hostId;
      const sourceDatabaseId = yield* databaseId;
      return {
        hostEnvironmentId,
        sourceDatabaseId,
        grant: hostEnvironmentId === null ? null : yield* grantFor(sourceDatabaseId, sessionId),
      };
    },
    Effect.mapError((cause) => new ThreadMirrorExportError({ stage: "grants", cause })),
  );

  const hostClient = Effect.fn("ThreadMirrorExporter.hostClient")(function* (
    hostEnvironmentId: EnvironmentId,
  ) {
    const resolved = yield* links
      .resolve(hostEnvironmentId)
      .pipe(Effect.mapError((cause) => new ThreadMirrorExportError({ stage: "peer", cause })));
    // Survives restarts and re-links, so this environment keeps writing to the copy it started.
    const writerSecret = yield* secrets.getOrCreateRandom(WRITER_SECRET, 32).pipe(
      Effect.map(Hex.encode),
      Effect.mapError((cause) => new ThreadMirrorExportError({ stage: "peer", cause })),
    );
    const client = yield* HttpApiClient.group(EnvironmentHttpApi, {
      httpClient: peerHttpClient,
      group: "threadMirror",
      baseUrl: resolved.url,
    });
    return {
      client,
      headers: {
        authorization: `Bearer ${resolved.token}`,
        [ORCHESTRATION_PROTOCOL_HEADER]: ORCHESTRATION_PROTOCOL_VERSION_TEXT,
        [THREAD_MIRROR_WRITER_HEADER]: writerSecret,
      },
    } as const;
  });

  // What the host last acknowledged, so an unchanged list is not sent again before it is due.
  const lastSent = yield* Ref.make<{ readonly key: string; readonly at: number } | null>(null);

  const readerHashes = (sourceDatabaseId: string) =>
    sessions.listActive().pipe(
      Effect.flatMap((active) =>
        Effect.forEach(
          active.filter((session) => session.scopes.includes(AuthOrchestrationReadScope)),
          (session) =>
            grantFor(sourceDatabaseId, session.sessionId).pipe(Effect.flatMap(sha256Hex)),
        ),
      ),
      Effect.map((hashes) => hashes.toSorted()),
      Effect.mapError((cause) => new ThreadMirrorExportError({ stage: "grants", cause })),
    );

  const push = Effect.fn("ThreadMirrorExporter.push")(function* (force: boolean) {
    const hostEnvironmentId = yield* hostId;
    if (hostEnvironmentId === null) return Option.none<ThreadMirrorAck>();
    const sourceDatabaseId = yield* databaseId;
    const readerGrantHashes = yield* readerHashes(sourceDatabaseId);
    const now = yield* DateTime.now;
    const keyAt = (sequence: number) =>
      `${hostEnvironmentId}|${sourceDatabaseId}|${sequence}|${readerGrantHashes.join(",")}`;
    if (!force) {
      const previous = yield* Ref.get(lastSent);
      const latest = yield* source.latestSequence.pipe(
        Effect.mapError((cause) => new ThreadMirrorExportError({ stage: "snapshot", cause })),
      );
      if (
        previous !== null &&
        previous.key === keyAt(latest) &&
        DateTime.toEpochMillis(now) - previous.at < Duration.toMillis(THREAD_MIRROR_RENEW_EVERY)
      ) {
        return Option.none<ThreadMirrorAck>();
      }
    }
    const snapshot = yield* source.snapshot.pipe(
      Effect.mapError((cause) => new ThreadMirrorExportError({ stage: "snapshot", cause })),
    );
    const descriptor = yield* environment.getDescriptor;
    const { client, headers } = yield* hostClient(hostEnvironmentId);
    const ack = yield* client
      .put({
        headers,
        params: { sourceEnvironmentId: descriptor.environmentId },
        payload: buildThreadMirrorExport({
          snapshot,
          sourceDatabaseId,
          sourceLabel: descriptor.label,
          readerGrantHashes,
          expiresAt: DateTime.addDuration(now, THREAD_MIRROR_TTL),
        }),
      })
      .pipe(Effect.mapError((cause) => new ThreadMirrorExportError({ stage: "push", cause })));
    if (ack.outcome === "stored") {
      yield* Ref.set(lastSent, {
        key: keyAt(snapshot.snapshotSequence),
        at: DateTime.toEpochMillis(now),
      });
    }
    return Option.some(ack);
  });

  const withdraw = Effect.fn("ThreadMirrorExporter.withdraw")(function* (
    hostEnvironmentId: EnvironmentId,
  ) {
    const descriptor = yield* environment.getDescriptor;
    const { client, headers } = yield* hostClient(hostEnvironmentId);
    yield* client
      .delete({ headers, params: { sourceEnvironmentId: descriptor.environmentId } })
      .pipe(Effect.mapError((cause) => new ThreadMirrorExportError({ stage: "push", cause })));
  });

  const run = Effect.scoped(
    Effect.gen(function* () {
      const wake = yield* Queue.sliding<void>(1);
      const poke = Queue.offer(wake, undefined);
      // Subscribe before reading the current choice, so a change in between is not lost.
      const hostChanges = yield* settings.subscribeChanges;
      // The host this environment last exported to, which only a withdrawal after a change retires.
      const trackedHost = yield* Ref.make(yield* hostId);
      const pendingWithdrawals = yield* Ref.make<ReadonlyArray<EnvironmentId>>([]);
      const logFailure = (message: string) => (error: ThreadMirrorExportError) =>
        Effect.logWarning(message).pipe(
          Effect.as(true),
          Effect.annotateLogs({ stage: error.stage }),
        );

      // Every wake-up lands here, so startup, host changes, list changes, readers and retries all
      // take one path. Returns whether anything failed and wants a retry.
      const reconcile = Effect.gen(function* () {
        const host = yield* hostId;
        const previous = yield* Ref.getAndSet(trackedHost, host);
        if (previous !== null && previous !== host) {
          yield* Ref.update(pendingWithdrawals, (held) => [...held, previous]);
          yield* Ref.set(lastSent, null);
        }
        const owed = (yield* Ref.get(pendingWithdrawals)).filter((id) => id !== host);
        yield* Ref.set(pendingWithdrawals, owed);
        let failed = false;
        for (const old of new Set(owed)) {
          const stillOwed = yield* withdraw(old).pipe(
            Effect.as(false),
            Effect.catch(logFailure("Could not withdraw the thread list from its old host.")),
          );
          if (stillOwed) failed = true;
          else yield* Ref.update(pendingWithdrawals, (held) => held.filter((id) => id !== old));
        }
        if (host !== null) {
          failed =
            (yield* push(false).pipe(
              Effect.as(false),
              Effect.catch(logFailure("Could not export the thread list.")),
            )) || failed;
        }
        return failed;
      });

      const loop = Effect.gen(function* () {
        let failures = 0;
        while (true) {
          failures = (yield* reconcile) ? failures + 1 : 0;
          const wait =
            failures === 0
              ? THREAD_MIRROR_RENEW_EVERY
              : Duration.min(RETRY_MAX, Duration.times(RETRY_BASE, 2 ** (failures - 1)));
          // Any change wakes it at once, so a long backoff never holds up a settings change.
          yield* Queue.take(wake).pipe(Effect.timeoutOption(wait));
        }
      });
      const onListChanges = source.changes.pipe(
        Stream.groupedWithin(1000, CHANGES_WINDOW),
        Stream.runForEach(() => poke),
      );
      const onReaders = sessions.streamChanges.pipe(Stream.runForEach(() => poke));
      const onHostChosen = hostChanges.pipe(
        Stream.map((current) =>
          current.linkedEnvironmentsEnabled ? current.threadMirrorHostEnvironmentId : null,
        ),
        Stream.changes,
        Stream.runForEach(() => poke),
      );
      yield* Effect.all([loop, onListChanges, onReaders, onHostChosen], {
        concurrency: "unbounded",
        discard: true,
      });
      return yield* Effect.never;
    }),
  );

  return ThreadMirrorExporter.of({
    readerGrant,
    exportNow: push(true),
    withdraw,
    run,
  });
});

export const layer = Layer.effect(ThreadMirrorExporter, make);
