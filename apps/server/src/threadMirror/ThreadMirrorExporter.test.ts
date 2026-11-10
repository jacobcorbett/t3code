import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  AuthOrchestrationReadScope,
  type AuthClientSession,
  EnvironmentId,
  type ExecutionEnvironmentDescriptor,
  type ServerSettings as ServerSettingsValue,
  ORCHESTRATION_PROTOCOL_HEADER,
  type OrchestrationV2ShellSnapshot,
  THREAD_MIRROR_WRITER_HEADER,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Fiber from "effect/Fiber";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as TestClock from "effect/testing/TestClock";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Context from "effect/Context";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as HttpClient from "effect/http/HttpClient";
import type * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpClientResponse from "effect/http/HttpClientResponse";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as SessionStore from "../auth/SessionStore.ts";
import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as PeerLinks from "../peer/PeerLinks.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ThreadMirrorExporter from "./ThreadMirrorExporter.ts";
import * as ThreadMirrorShellSource from "./ThreadMirrorShellSource.ts";

const yoga = EnvironmentId.make("yoga");
const macDescriptor: ExecutionEnvironmentDescriptor = {
  environmentId: EnvironmentId.make("mac"),
  label: "Mac",
  platform: { os: "darwin", arch: "arm64" },
  serverVersion: "0.0.0-test",
  capabilities: { repositoryIdentity: true },
};

const session = (sessionId: string, scopes: AuthClientSession["scopes"]) =>
  ({ sessionId, scopes }) as unknown as AuthClientSession;

const bob = EnvironmentId.make("bob");

type Sent = {
  readonly request: HttpClientRequest.HttpClientRequest;
  readonly body: string;
};

const settingsWithChanges = (host: EnvironmentId | null) =>
  Layer.effect(
    ServerSettings.ServerSettingsService,
    Effect.gen(function* () {
      const base = Context.get(
        yield* Layer.build(
          ServerSettings.layerTest({
            linkedEnvironmentsEnabled: true,
            threadMirrorHostEnvironmentId: host,
          }),
        ),
        ServerSettings.ServerSettingsService,
      );
      const changes = yield* PubSub.unbounded<ServerSettingsValue>();
      return ServerSettings.ServerSettingsService.of({
        ...base,
        updateSettings: (patch) =>
          base
            .updateSettings(patch)
            .pipe(Effect.tap((updated) => PubSub.publish(changes, updated))),
        subscribeChanges: PubSub.subscribe(changes).pipe(
          Effect.map((subscription) => Stream.fromSubscription(subscription)),
        ),
      });
    }),
  );

const harness = (host: EnvironmentId | null) =>
  Effect.gen(function* () {
    const requests = yield* Queue.unbounded<Sent>();
    const seen = yield* Ref.make<ReadonlyArray<Sent>>([]);
    const failing = yield* Ref.make(false);
    const sequence = yield* Ref.make(9);
    const listChanges = yield* Queue.unbounded<unknown>();
    const sessionChanges = yield* Queue.unbounded<never>();
    const active = yield* Ref.make<ReadonlyArray<AuthClientSession>>([
      session("phone-session", [AuthOrchestrationReadScope]),
      session("no-read-session", []),
    ]);
    const now = DateTime.makeUnsafe("2026-06-20T00:00:00.000Z");
    const snapshotAt = (snapshotSequence: number): OrchestrationV2ShellSnapshot => ({
      schemaVersion: 1,
      snapshotSequence,
      projects: [],
      archivedThreads: [],
      threads: [
        {
          id: ThreadId.make("thread-1"),
          projectId: "project-1",
          title: "Thread",
          providerInstanceId: "codex",
          modelSelection: { instanceId: "codex", model: "gpt-5.4" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          activeProviderThreadId: null,
          lineage: {
            rootThreadId: ThreadId.make("thread-1"),
            parentThreadId: null,
            relationshipToParent: null,
          },
          forkedFrom: null,
          createdBy: "user",
          creationSource: "web",
          latestRunId: null,
          activeRunId: null,
          status: "idle",
          lastError: "private error text",
          goal: { objective: "private goal text", status: "active" },
          pendingBackgroundTasks: [
            { taskId: "task-1", kind: "command", description: "private command text" },
          ],
          pendingRuntimeRequest: null,
          latestVisibleMessage: {
            id: "message-1",
            role: "assistant",
            text: "private conversation body",
            updatedAt: now,
          },
          latestUserMessageAt: null,
          hasActionableProposedPlan: false,
          itemCount: 0,
          visibleItemCount: 0,
          createdAt: now,
          updatedAt: now,
          archivedAt: null,
          settledOverride: "settled",
          settledAt: now,
          lastVisitedAt: null,
          deletedAt: null,
        } as unknown as OrchestrationV2ShellSnapshot["threads"][number],
      ],
    });
    const layerHttp = Layer.succeed(
      HttpClient.HttpClient,
      HttpClient.make((request) =>
        Effect.gen(function* () {
          const body =
            request.body._tag === "Uint8Array" ? new TextDecoder().decode(request.body.body) : "";
          const sent = { request, body };
          yield* Ref.update(seen, (all) => [...all, sent]);
          yield* Queue.offer(requests, sent);
          if (yield* Ref.get(failing)) {
            return HttpClientResponse.fromWeb(request, new Response("down", { status: 503 }));
          }
          return HttpClientResponse.fromWeb(
            request,
            new Response(
              JSON.stringify(
                request.method === "DELETE"
                  ? { removed: true }
                  : { outcome: "stored", heldSequence: 9 },
              ),
              { status: 200, headers: { "content-type": "application/json" } },
            ),
          );
        }),
      ),
    );
    const layer = ThreadMirrorExporter.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          layerHttp,
          Layer.mock(ThreadMirrorShellSource.ThreadMirrorShellSource)({
            snapshot: Ref.get(sequence).pipe(Effect.map(snapshotAt)),
            latestSequence: Ref.get(sequence),
            changes: Stream.fromQueue(listChanges),
          }),
          Layer.mock(SessionStore.SessionStore)({
            cookieName: "t3-test-session",
            legacyCookieName: undefined,
            listActive: () => Ref.get(active),
            streamChanges: Stream.fromQueue(sessionChanges),
          }),
          Layer.mock(PeerLinks.PeerLinks)({
            resolve: (id: EnvironmentId) =>
              Effect.succeed({
                link: {} as never,
                url: `https://${id}.example`,
                token: `${id}-peer-token`,
              }),
          }),
          Layer.mock(ServerEnvironment.ServerEnvironment)({
            getDescriptor: Effect.succeed(macDescriptor),
          }),
        ),
      ),
      Layer.provideMerge(settingsWithChanges(host)),
      Layer.provideMerge(Sqlite.layerMemory),
      Layer.provide(ServerSecretStore.layer),
      Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "t3-thread-mirror-" })),
      Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
    );
    return { requests, seen, failing, sequence, listChanges, sessionChanges, active, layer };
  });

const take = (requests: Queue.Queue<Sent>) => Queue.take(requests);

it.effect(
  "exports the list with readers only the source's own sessions can use, and no private text",
  () =>
    Effect.gen(function* () {
      const { seen, layer } = yield* harness(yoga);
      yield* Effect.gen(function* () {
        const exporter = yield* ThreadMirrorExporter.ThreadMirrorExporter;
        const sql = yield* SqlClient.SqlClient;
        const [{ database_id: databaseId }] = yield* sql<{ readonly database_id: string }>`
          SELECT database_id FROM server_database_identity
        `.pipe(Effect.map((rows) => rows as [{ readonly database_id: string }]));

        const phone = yield* exporter.readerGrant("phone-session");
        const other = yield* exporter.readerGrant("other-session");
        expect(phone).toMatchObject({ hostEnvironmentId: yoga, sourceDatabaseId: databaseId });
        expect(phone.grant).not.toBeNull();
        expect(phone.grant).not.toBe(other.grant);

        expect(yield* exporter.exportNow).toEqual(
          Option.some({ outcome: "stored", heldSequence: 9 }),
        );
        const [sent] = yield* Ref.get(seen);
        expect(sent?.request.method).toBe("PUT");
        expect(sent?.request.url).toBe("https://yoga.example/api/thread-mirror/mac");
        expect(sent?.request.headers.authorization).toBe("Bearer yoga-peer-token");
        expect(sent?.request.headers[ORCHESTRATION_PROTOCOL_HEADER]).toBeDefined();
        expect(sent?.request.headers[THREAD_MIRROR_WRITER_HEADER]).toMatch(/^[0-9a-f]{64}$/);
        const payload = JSON.parse(sent?.body ?? "{}");
        expect(payload.sourceDatabaseId).toBe(databaseId);
        expect(payload.sourceSequence).toBe(9);
        expect(payload.snapshot.threads[0].settledOverride).toBe("settled");
        expect(payload.snapshot.threads[0].latestVisibleMessage).toBeNull();
        expect(payload.snapshot.threads[0].lastError).toBeNull();
        expect(payload.snapshot.threads[0].goal).toBeNull();
        expect(payload.snapshot.threads[0].pendingBackgroundTasks).toEqual([]);
        for (const text of ["conversation body", "error text", "goal text", "command text"]) {
          expect(sent?.body).not.toContain(`private ${text}`);
        }
        // One reader hash, for the session allowed to read here; never the grant itself.
        expect(payload.readerGrantHashes).toHaveLength(1);
        expect(sent?.body).not.toContain(phone.grant ?? "unreachable");
      }).pipe(Effect.provide(layer));
    }),
);

it.effect("sends nothing and issues no grants when no host is chosen", () =>
  Effect.gen(function* () {
    const { seen, layer } = yield* harness(null);
    yield* Effect.gen(function* () {
      const exporter = yield* ThreadMirrorExporter.ThreadMirrorExporter;
      expect(yield* exporter.exportNow).toEqual(Option.none());
      expect(yield* exporter.readerGrant("phone-session")).toMatchObject({
        hostEnvironmentId: null,
        grant: null,
      });
      expect(yield* Ref.get(seen)).toEqual([]);
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("withdraws with the same writer secret every export used", () =>
  Effect.gen(function* () {
    const { seen, layer } = yield* harness(yoga);
    yield* Effect.gen(function* () {
      const exporter = yield* ThreadMirrorExporter.ThreadMirrorExporter;
      yield* exporter.exportNow;
      yield* exporter.withdraw(yoga).pipe(Effect.ignore);
      const [put, deleted] = yield* Ref.get(seen);
      expect(deleted?.request.method).toBe("DELETE");
      expect(deleted?.request.url).toBe("https://yoga.example/api/thread-mirror/mac");
      expect(deleted?.request.headers[THREAD_MIRROR_WRITER_HEADER]).toBe(
        put?.request.headers[THREAD_MIRROR_WRITER_HEADER],
      );
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("exports at startup and withdraws from the old host when the choice changes", () =>
  Effect.gen(function* () {
    const { requests, layer } = yield* harness(yoga);
    yield* Effect.gen(function* () {
      const exporter = yield* ThreadMirrorExporter.ThreadMirrorExporter;
      const settings = yield* ServerSettings.ServerSettingsService;
      const running = yield* Effect.forkChild(exporter.run);

      // A restart sends the list without waiting for a change.
      expect((yield* take(requests)).request.url).toBe(
        "https://yoga.example/api/thread-mirror/mac",
      );

      // Changing hosts after that restart still retires the copy the first host holds.
      yield* settings.updateSettings({ threadMirrorHostEnvironmentId: bob });
      const withdrawn = yield* take(requests);
      expect([withdrawn.request.method, withdrawn.request.url]).toEqual([
        "DELETE",
        "https://yoga.example/api/thread-mirror/mac",
      ]);
      const moved = yield* take(requests);
      expect([moved.request.method, moved.request.url]).toEqual([
        "PUT",
        "https://bob.example/api/thread-mirror/mac",
      ]);

      // Turning it off withdraws and sends nothing more, even when renewal comes due.
      yield* settings.updateSettings({ threadMirrorHostEnvironmentId: null });
      const off = yield* take(requests);
      expect([off.request.method, off.request.url]).toEqual([
        "DELETE",
        "https://bob.example/api/thread-mirror/mac",
      ]);
      yield* settings.updateSettings({ threadMirrorHostEnvironmentId: yoga });
      expect((yield* take(requests)).request.method).toBe("PUT");
      yield* settings.updateSettings({ linkedEnvironmentsEnabled: false });
      expect(yield* exporter.readerGrant("phone-session")).toMatchObject({
        hostEnvironmentId: null,
        grant: null,
      });
      expect((yield* take(requests)).request.method).toBe("DELETE");
      expect(yield* exporter.exportNow).toEqual(Option.none());
      yield* TestClock.adjust(Duration.hours(13));
      expect(yield* Queue.size(requests)).toBe(0);
      yield* Fiber.interrupt(running);
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("retries a failed export soon, without holding up a settings change, then renews", () =>
  Effect.gen(function* () {
    const { requests, failing, layer } = yield* harness(yoga);
    yield* Effect.gen(function* () {
      const exporter = yield* ThreadMirrorExporter.ThreadMirrorExporter;
      const settings = yield* ServerSettings.ServerSettingsService;
      yield* Ref.set(failing, true);
      const running = yield* Effect.forkChild(exporter.run);
      expect((yield* take(requests)).request.method).toBe("PUT");

      // The host is down: the next attempt follows within the first backoff, then the host recovers.
      yield* TestClock.adjust(Duration.seconds(30));
      expect((yield* take(requests)).request.method).toBe("PUT");
      yield* Ref.set(failing, false);
      yield* TestClock.adjust(Duration.minutes(1));
      expect((yield* take(requests)).request.method).toBe("PUT");

      // Nothing changed, so nothing is sent until the copy is due for renewal, well before 7 days.
      yield* TestClock.adjust(Duration.hours(5));
      expect(yield* Queue.size(requests)).toBe(0);
      yield* TestClock.adjust(Duration.hours(1));
      expect((yield* take(requests)).request.method).toBe("PUT");

      // A failing host never holds up the user's change of mind.
      yield* Ref.set(failing, true);
      yield* TestClock.adjust(Duration.hours(6));
      expect((yield* take(requests)).request.method).toBe("PUT");
      yield* settings.updateSettings({ threadMirrorHostEnvironmentId: null });
      expect((yield* take(requests)).request.method).toBe("DELETE");
      // The withdrawal that failed is still owed, and goes out once the host answers.
      yield* Ref.set(failing, false);
      yield* TestClock.adjust(Duration.minutes(1));
      expect((yield* take(requests)).request.method).toBe("DELETE");
      yield* TestClock.adjust(Duration.hours(13));
      expect(yield* Queue.size(requests)).toBe(0);
      yield* Fiber.interrupt(running);
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("exports again when a reader session appears, without any thread changing", () =>
  Effect.gen(function* () {
    const { requests, active, sessionChanges, layer } = yield* harness(yoga);
    yield* Effect.gen(function* () {
      const exporter = yield* ThreadMirrorExporter.ThreadMirrorExporter;
      const running = yield* Effect.forkChild(exporter.run);
      const first = JSON.parse((yield* take(requests)).body);
      expect(first.readerGrantHashes).toHaveLength(1);

      yield* Ref.update(active, (all) => [
        ...all,
        session("tablet-session", [AuthOrchestrationReadScope]),
      ]);
      yield* Queue.offer(sessionChanges, undefined as never);
      const second = JSON.parse((yield* take(requests)).body);
      expect(second.readerGrantHashes).toHaveLength(2);

      // The tablet signs out: its key stops working on the host at the next export.
      yield* Ref.set(active, [session("phone-session", [AuthOrchestrationReadScope])]);
      yield* Queue.offer(sessionChanges, undefined as never);
      expect(JSON.parse((yield* take(requests)).body).readerGrantHashes).toHaveLength(1);
      yield* Fiber.interrupt(running);
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("sends a streaming list once per window, not per event", () =>
  Effect.gen(function* () {
    const { requests, sequence, listChanges, layer } = yield* harness(yoga);
    yield* Effect.gen(function* () {
      const exporter = yield* ThreadMirrorExporter.ThreadMirrorExporter;
      const running = yield* Effect.forkChild(exporter.run);
      yield* take(requests);
      for (let event = 10; event < 60; event += 1) {
        yield* Ref.set(sequence, event);
        yield* Queue.offer(listChanges, event);
      }
      yield* TestClock.adjust(Duration.seconds(14));
      expect(yield* Queue.size(requests)).toBe(0);
      yield* TestClock.adjust(Duration.seconds(1));
      expect(JSON.parse((yield* take(requests)).body).sourceSequence).toBe(59);
      yield* TestClock.adjust(Duration.seconds(30));
      expect(yield* Queue.size(requests)).toBe(0);
      yield* Fiber.interrupt(running);
    }).pipe(Effect.provide(layer));
  }),
);
