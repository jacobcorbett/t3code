import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  type OrchestrationV2ShellSnapshot,
  type OrchestrationV2ThreadShell,
  ThreadId,
  threadMirrorSupersedes,
} from "@t3tools/contracts";
import * as Digest from "effect/crypto/Digest";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";

import * as Sqlite from "../persistence/Sqlite.ts";
import { buildThreadMirrorExport } from "./ThreadMirrorExporter.ts";
import * as ThreadMirrorHost from "./ThreadMirrorHost.ts";

const layer = ThreadMirrorHost.layer.pipe(
  Layer.provideMerge(Sqlite.layerMemory),
  Layer.provideMerge(NodeCrypto.layer),
);

const mac = EnvironmentId.make("mac");
const writerA = "a".repeat(64);
const writerB = "b".repeat(64);
const phoneGrant = "phone-grant";
const thread = "thread-1";

const shell = (
  overrides: Partial<OrchestrationV2ThreadShell> = {},
  id = thread,
): OrchestrationV2ThreadShell => {
  const now = DateTime.makeUnsafe("2026-06-20T00:00:00.000Z");
  const threadId = ThreadId.make(id);
  return {
    id: threadId,
    projectId: "project-1" as OrchestrationV2ThreadShell["projectId"],
    title: id,
    providerInstanceId: "codex" as OrchestrationV2ThreadShell["providerInstanceId"],
    modelSelection: {
      instanceId: "codex" as OrchestrationV2ThreadShell["providerInstanceId"],
      model: "gpt-5.4",
    },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { rootThreadId: threadId, parentThreadId: null, relationshipToParent: null },
    forkedFrom: null,
    createdBy: "user",
    creationSource: "web",
    latestRunId: null,
    activeRunId: null,
    status: "idle",
    pendingRuntimeRequest: null,
    latestVisibleMessage: {
      id: "message-1" as NonNullable<OrchestrationV2ThreadShell["latestVisibleMessage"]>["id"],
      role: "assistant",
      text: "a conversation body that must stay on the source",
      updatedAt: now,
    },
    latestUserMessageAt: null,
    hasActionableProposedPlan: false,
    itemCount: 0,
    visibleItemCount: 0,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
    ...overrides,
  };
};

const snapshot = (
  sequence: number,
  threads: ReadonlyArray<OrchestrationV2ThreadShell>,
): OrchestrationV2ShellSnapshot => ({
  schemaVersion: 1,
  snapshotSequence: sequence,
  projects: [],
  threads,
  archivedThreads: [],
});

const sha256Hex = (value: string) =>
  Effect.gen(function* () {
    const digest = yield* Digest.Digest;
    return Hex.encode(
      yield* digest.digest(Digest.sha256, new TextEncoder().encode(value)).pipe(Effect.orDie),
    );
  });

const exportOf = (input: {
  readonly sequence: number;
  readonly threads: ReadonlyArray<OrchestrationV2ThreadShell>;
  readonly databaseId?: string;
  readonly grants?: ReadonlyArray<string>;
  readonly ttl?: Duration.Duration;
}) =>
  Effect.gen(function* () {
    const now = yield* DateTime.now;
    return buildThreadMirrorExport({
      snapshot: snapshot(input.sequence, input.threads),
      sourceDatabaseId: input.databaseId ?? "db-1",
      sourceLabel: "Mac",
      readerGrantHashes: yield* Effect.forEach(input.grants ?? [phoneGrant], sha256Hex),
      expiresAt: DateTime.addDuration(now, input.ttl ?? Duration.days(7)),
    });
  });

const accept = (copy: ReturnType<typeof exportOf>, writer = writerA) =>
  Effect.gen(function* () {
    const host = yield* ThreadMirrorHost.ThreadMirrorHost;
    return yield* host.accept({
      sourceEnvironmentId: mac,
      writerSecret: writer,
      copy: yield* copy,
    });
  });

const read = (grant = phoneGrant, source = mac) =>
  Effect.gen(function* () {
    const host = yield* ThreadMirrorHost.ThreadMirrorHost;
    return yield* host.read({ sourceEnvironmentId: source, grant });
  });

const failure = <A, R>(effect: Effect.Effect<A, ThreadMirrorHost.ThreadMirrorHostError, R>) =>
  effect.pipe(
    Effect.flip,
    Effect.map((error) => error.reason),
  );

it.effect(
  "a phone with a stale list reads the source's settle and snooze from the host once the source is off",
  () =>
    Effect.gen(function* () {
      // The phone last synchronised with the Mac at sequence 5, before anything below.
      const phoneHeld = { databaseId: "db-1", sequence: 5 };
      const snoozedUntil = DateTime.makeUnsafe("2026-06-21T09:00:00.000Z");
      const settledAt = DateTime.makeUnsafe("2026-06-20T10:00:00.000Z");

      // The Mac settles one thread and snoozes another, exports to Yoga, and Yoga acknowledges.
      const ack = yield* accept(
        exportOf({
          sequence: 9,
          threads: [
            shell({ settledOverride: "settled", settledAt }, "settled-thread"),
            shell({ snoozedUntil, snoozedAt: settledAt }, "snoozed-thread"),
          ],
        }),
      );
      expect(ack).toEqual({ outcome: "stored", heldSequence: 9 });

      // The Mac is now off. The phone reads Yoga with the grant the Mac gave it.
      const copy = yield* read();
      expect(threadMirrorSupersedes(phoneHeld, copy)).toBe(true);
      expect(copy.sourceEnvironmentId).toBe(mac);
      expect(copy.sourceSequence).toBe(9);
      const settled = copy.snapshot.threads.find((entry) => entry.id === "settled-thread");
      const snoozed = copy.snapshot.threads.find((entry) => entry.id === "snoozed-thread");
      expect(settled?.settledOverride).toBe("settled");
      expect(settled?.settledAt).toEqual(settledAt);
      expect(snoozed?.snoozedUntil).toEqual(snoozedUntil);
      // Message text never leaves the source.
      expect(copy.snapshot.threads.map((entry) => entry.latestVisibleMessage)).toEqual([
        null,
        null,
      ]);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "keeps a newer copy when an older export arrives, and never offers it over fresher rows",
  () =>
    Effect.gen(function* () {
      yield* accept(exportOf({ sequence: 9, threads: [shell()] }));
      const stale = yield* accept(exportOf({ sequence: 7, threads: [shell({ title: "old" })] }));
      expect(stale).toEqual({ outcome: "stale", heldSequence: 9 });
      expect((yield* read()).snapshot.threads[0]?.title).toBe(thread);

      // A client already at sequence 12 in the same database keeps its own rows.
      expect(threadMirrorSupersedes({ databaseId: "db-1", sequence: 12 }, yield* read())).toBe(
        false,
      );
      // An export that adds nothing but a new reader is not stale, so the reader can be added.
      expect(
        yield* accept(
          exportOf({ sequence: 9, threads: [shell()], grants: [phoneGrant, "tablet"] }),
        ),
      ).toEqual({ outcome: "stored", heldSequence: 9 });
      expect((yield* read("tablet")).sourceSequence).toBe(9);
    }).pipe(Effect.provide(layer)),
);

it.effect("orders by sequence only within one source database", () =>
  Effect.gen(function* () {
    yield* accept(exportOf({ sequence: 900, threads: [shell()], databaseId: "db-1" }));
    // The Mac's database was replaced: its sequence starts again, and the new database wins.
    const replaced = yield* accept(
      exportOf({ sequence: 3, threads: [shell({ title: "new" })], databaseId: "db-2" }),
    );
    expect(replaced).toEqual({ outcome: "stored", heldSequence: 3 });
    const copy = yield* read();
    expect(copy.sourceDatabaseId).toBe("db-2");
    expect(copy.snapshot.threads[0]?.title).toBe("new");

    // A client holding rows from the old database, or from before databases were named, or
    // from the new database already ahead of the copy, is not overwritten on a guess.
    expect(threadMirrorSupersedes({ databaseId: "db-1", sequence: 1 }, copy)).toBe(false);
    expect(threadMirrorSupersedes({ databaseId: undefined, sequence: 1 }, copy)).toBe(false);
    expect(threadMirrorSupersedes({ databaseId: "db-2", sequence: 8 }, copy)).toBe(false);
    expect(threadMirrorSupersedes({ databaseId: "db-2", sequence: 2 }, copy)).toBe(true);
    expect(threadMirrorSupersedes(null, copy)).toBe(true);
  }).pipe(Effect.provide(layer)),
);

it.effect("a later complete list drops threads the source removed", () =>
  Effect.gen(function* () {
    yield* accept(exportOf({ sequence: 4, threads: [shell({}, "kept"), shell({}, "deleted")] }));
    yield* accept(exportOf({ sequence: 6, threads: [shell({}, "kept")] }));
    expect((yield* read()).snapshot.threads.map((entry) => entry.id)).toEqual(["kept"]);
  }).pipe(Effect.provide(layer)),
);

it.effect("answers an unknown grant, source or expired copy as not found", () =>
  Effect.gen(function* () {
    yield* accept(exportOf({ sequence: 4, threads: [shell()], ttl: Duration.hours(1) }));
    expect(yield* failure(read("someone-elses-grant"))).toBe("not_found");
    expect(yield* failure(read(phoneGrant, EnvironmentId.make("other-source")))).toBe("not_found");
    expect((yield* read()).sourceSequence).toBe(4);

    yield* TestClock.adjust(Duration.hours(2));
    expect(yield* failure(read())).toBe("not_found");
    // Forgotten, not just hidden: a fresh export is stored from scratch.
    expect(yield* accept(exportOf({ sequence: 1, threads: [shell()] }))).toEqual({
      outcome: "stored",
      heldSequence: 1,
    });
  }).pipe(Effect.provide(layer)),
);

it.effect("refuses an export that has already expired or is too large", () =>
  Effect.gen(function* () {
    expect(
      yield* failure(
        accept(exportOf({ sequence: 1, threads: [shell()], ttl: Duration.seconds(-1) })),
      ),
    ).toBe("expired");
    expect(
      yield* failure(
        accept(
          exportOf({
            sequence: 1,
            threads: Array.from({ length: 1501 }, (_, index) => shell({}, `t${index}`)),
          }),
        ),
      ),
    ).toBe("too_large");
    expect(yield* failure(read())).toBe("not_found");
  }).pipe(Effect.provide(layer)),
);

it.effect("keeps a source for the writer that first claimed it until the copy expires", () =>
  Effect.gen(function* () {
    const host = yield* ThreadMirrorHost.ThreadMirrorHost;
    yield* accept(exportOf({ sequence: 2, threads: [shell()], ttl: Duration.hours(1) }));
    // Another writer cannot take over or remove the copy, however fresh its list...
    expect(yield* failure(accept(exportOf({ sequence: 3, threads: [shell()] }), writerB))).toBe(
      "writer_mismatch",
    );
    expect(yield* failure(host.remove({ sourceEnvironmentId: mac, writerSecret: writerB }))).toBe(
      "writer_mismatch",
    );
    // ...the same writer keeps writing across any number of sign-ins or re-links...
    expect((yield* accept(exportOf({ sequence: 3, threads: [shell()] }))).outcome).toBe("stored");
    // ...and once the copy has expired a new writer may claim the source.
    yield* TestClock.adjust(Duration.days(8));
    expect((yield* accept(exportOf({ sequence: 1, threads: [shell()] }), writerB)).outcome).toBe(
      "stored",
    );
    expect(yield* host.remove({ sourceEnvironmentId: mac, writerSecret: writerB })).toBe(true);
    expect(yield* failure(read())).toBe("not_found");
    expect(yield* host.remove({ sourceEnvironmentId: mac, writerSecret: writerB })).toBe(false);
  }).pipe(Effect.provide(layer)),
);

it.effect("concurrent exports never leave an older list over a newer one", () =>
  Effect.gen(function* () {
    const exports = yield* Effect.forEach([3, 9, 5, 7, 1, 8], (sequence) =>
      exportOf({ sequence, threads: [shell({ title: `at-${sequence}` })] }),
    );
    yield* Effect.forEach(exports, (copy) => accept(Effect.succeed(copy)), {
      concurrency: "unbounded",
    });
    const held = yield* read();
    expect(held.sourceSequence).toBe(9);
    expect(held.snapshot.threads[0]?.title).toBe("at-9");
    // The stale answer names the sequence the host kept.
    expect(yield* accept(exportOf({ sequence: 2, threads: [shell()] }))).toEqual({
      outcome: "stale",
      heldSequence: 9,
    });
  }).pipe(Effect.provide(layer)),
);
