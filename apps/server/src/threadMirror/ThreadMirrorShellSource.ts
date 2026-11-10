import type { OrchestrationV2ShellSnapshot } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";

import * as OrchestrationEventStore from "../persistence/OrchestrationEventStore.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import { loadSubscriberShellSnapshot } from "../orchestration-v2/ShellStream.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";

export class ThreadMirrorShellSourceError extends Schema.TaggedError<ThreadMirrorShellSourceError>()(
  "ThreadMirrorShellSourceError",
  { cause: Schema.Defect() },
) {
  override get message(): string {
    return "Could not read this environment's thread list.";
  }
}

/** Where the exported thread list comes from, and when it may have changed. */
export class ThreadMirrorShellSource extends Context.Service<
  ThreadMirrorShellSource,
  {
    /** Every active thread, with idle subagents and older settled threads left out and counted. */
    readonly snapshot: Effect.Effect<OrchestrationV2ShellSnapshot, ThreadMirrorShellSourceError>;
    /** The sequence the list is at now; cheap, so an unchanged list is not read again. */
    readonly latestSequence: Effect.Effect<number, ThreadMirrorShellSourceError>;
    /** Emits when the list may have changed. */
    readonly changes: Stream.Stream<unknown>;
  }
>()("t3/threadMirror/ThreadMirrorShellSource") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const threadManagement = yield* ThreadManagementService.ThreadManagementService;
  const applicationEvents = yield* OrchestrationEventStore.OrchestrationEventStore;
  const projectStore = yield* ProjectStore.ProjectStoreV2;

  return ThreadMirrorShellSource.of({
    snapshot: loadSubscriberShellSnapshot({
      sql,
      listShellIndex: threadManagement.listActiveShellIndex(),
      readThreads: (threadIds) =>
        threadManagement.readShellSnapshot({
          location: "active",
          ...(threadIds === undefined ? {} : { threadIds }),
        }),
      listProjects: projectStore.listShells(),
      latestSequence: applicationEvents.latestApplicationSequence,
      options: { omitIdleSubagents: true, omitOlderSettled: true },
    }).pipe(
      Effect.map(({ snapshot }) => snapshot),
      Effect.mapError((cause) => new ThreadMirrorShellSourceError({ cause })),
    ),
    latestSequence: applicationEvents.latestApplicationSequence.pipe(
      Effect.mapError((cause) => new ThreadMirrorShellSourceError({ cause })),
    ),
    // A dropped stream resumes from the live tail; the next export is a complete list anyway.
    changes: applicationEvents
      .streamApplicationEvents()
      .pipe(Stream.retry(Schedule.spaced("5 seconds")), Stream.orDie),
  });
});

export const layer = Layer.effect(ThreadMirrorShellSource, make);
