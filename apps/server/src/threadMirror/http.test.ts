import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  EnvironmentHttpApi,
  EnvironmentId,
  ORCHESTRATION_PROTOCOL_HEADER,
  ORCHESTRATION_PROTOCOL_VERSION_TEXT,
  THREAD_MIRROR_GRANT_HEADER,
  THREAD_MIRROR_MAX_BODY_BYTES,
  THREAD_MIRROR_WRITER_HEADER,
  ThreadMirrorExport,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Digest from "effect/crypto/Digest";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import * as Schema from "effect/Schema";
import * as Etag from "effect/http/Etag";
import * as HttpPlatform from "effect/http/HttpPlatform";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";

import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as AuthHttp from "../auth/http.ts";
import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as SessionStore from "../auth/SessionStore.ts";
import * as ManagedPolicy from "../policy/ManagedPolicy.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ThreadMirrorExporter from "./ThreadMirrorExporter.ts";
import * as ThreadMirrorHost from "./ThreadMirrorHost.ts";
import * as ThreadMirrorHttp from "./http.ts";

class MirrorTestApi extends HttpApi.make("environment").add(
  EnvironmentHttpApi.groups.threadMirror,
) {}

const mac = EnvironmentId.make("mac");
const grant = "phone-grant";
const secretA = "a".repeat(64);
const secretB = "b".repeat(64);
const encodeExport = Schema.encodeSync(Schema.toCodecJson(ThreadMirrorExport));

const layerEnvironmentAuth = EnvironmentAuth.layer.pipe(
  Layer.provide(ManagedPolicy.layerTest()),
  Layer.provide(SqlitePersistence.layerMemory),
  Layer.provide(ServerSecretStore.layer),
  Layer.provide(ServerEnvironment.layerIdentity),
  Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "t3-thread-mirror-http-" })),
);

// Real authentication and host store behind the real routes; the source side is not involved.
const layerServices = Layer.mergeAll(
  ThreadMirrorHost.layer,
  Layer.mock(ThreadMirrorExporter.ThreadMirrorExporter)({}),
  ServerSettings.layerTest({ linkedEnvironmentsEnabled: true }),
).pipe(
  Layer.provideMerge(layerEnvironmentAuth),
  Layer.provideMerge(SqlitePersistence.layerMemory),
  Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
);

const sha256Hex = (value: string) =>
  Effect.gen(function* () {
    const digest = yield* Digest.Digest;
    return Hex.encode(
      yield* digest.digest(Digest.sha256, new TextEncoder().encode(value)).pipe(Effect.orDie),
    );
  });

const exportBody = (sourceLabel = "Mac") =>
  Effect.gen(function* () {
    const now = yield* DateTime.now;
    return JSON.stringify(
      encodeExport(
        ThreadMirrorExporter.buildThreadMirrorExport({
          snapshot: {
            schemaVersion: 1,
            snapshotSequence: 1,
            projects: [],
            threads: [],
            archivedThreads: [],
          },
          sourceDatabaseId: "db-1",
          sourceLabel,
          readerGrantHashes: [yield* sha256Hex(grant)],
          expiresAt: DateTime.addDuration(now, Duration.days(7)),
        }),
      ),
    );
  });

const bearer = (token: string | null) => ({
  [ORCHESTRATION_PROTOCOL_HEADER]: ORCHESTRATION_PROTOCOL_VERSION_TEXT,
  ...(token === null ? {} : { authorization: `Bearer ${token}` }),
});

const mirrorRequest = (
  method: "GET" | "PUT" | "DELETE",
  token: string | null,
  options: {
    readonly grant?: string;
    readonly body?: string;
    readonly source?: string;
    readonly writer?: string | null;
  } = {},
) =>
  new Request(`http://127.0.0.1/api/thread-mirror/${options.source ?? mac}`, {
    method,
    headers: {
      ...bearer(token),
      ...(options.grant === undefined ? {} : { [THREAD_MIRROR_GRANT_HEADER]: options.grant }),
      ...(method === "GET" || options.writer === null
        ? {}
        : { [THREAD_MIRROR_WRITER_HEADER]: options.writer ?? secretA }),
      ...(options.body === undefined ? {} : { "content-type": "application/json" }),
    },
    ...(options.body === undefined ? {} : { body: options.body }),
  });

const serve = Effect.gen(function* () {
  // Session expiry is judged against the clock, so it starts at a real date.
  yield* TestClock.setTime(Date.parse("2026-11-10T00:00:00.000Z"));
  const services = yield* Layer.build(layerServices);
  const routes = HttpApiBuilder.layer(MirrorTestApi).pipe(
    Layer.provide(ThreadMirrorHttp.layer),
    Layer.provide(AuthHttp.layerAuthenticatedAuth),
    Layer.provide(AuthHttp.layerPeerLinkAuth),
    Layer.provide(Layer.succeedContext(services)),
    Layer.provide(HttpPlatform.layer),
    Layer.provide(Etag.layerWeak),
    Layer.provide(NodeServices.layer),
  );
  const { handler, dispose } = HttpRouter.toWebHandler(routes, { disableLogger: true });
  yield* Effect.addFinalizer(() => Effect.promise(dispose));
  const auth = yield* Effect.succeed(Context.get(services, EnvironmentAuth.EnvironmentAuth));
  const sessions = yield* Effect.succeed(Context.get(services, SessionStore.SessionStore));
  const send = (request: Request) => Effect.promise(() => handler(request));
  const peer = (peerLink: boolean) =>
    auth
      .issueMcpClientSession({
        label: "T3 Code · Mac",
        access: "read-only",
        peerLink,
        client: { deviceType: "bot" },
      })
      .pipe(Effect.map((issued) => issued.token));
  const client = (scopes: ReadonlyArray<"orchestration:read" | "settings:write">) =>
    sessions
      .issue({
        subject: "phone",
        method: "bearer-access-token",
        scopes: [...scopes],
        client: { deviceType: "mobile" },
      })
      .pipe(Effect.map((issued) => issued.token));
  return { send, peer, client };
});

it.effect("keeps copies for linked environments only, behind scope, grant and ownership", () =>
  Effect.gen(function* () {
    const { send, peer, client } = yield* serve;
    const writer = yield* peer(true);
    const otherWriter = yield* peer(true);
    const plainAgent = yield* peer(false);
    const phone = yield* client(["orchestration:read"]);
    const noScope = yield* client(["settings:write"]);
    const body = yield* exportBody();

    // Only a linked environment may write: an ordinary agent, a signed-in client, or nobody may not.
    for (const token of [plainAgent, phone, null]) {
      expect((yield* send(mirrorRequest("PUT", token, { body }))).status).toBe(401);
    }
    // A write without the writer secret is refused outright.
    expect((yield* send(mirrorRequest("PUT", writer, { body, writer: null }))).status).toBe(400);
    const stored = yield* send(mirrorRequest("PUT", writer, { body }));
    expect(stored.status).toBe(200);
    expect(yield* Effect.promise(() => stored.json())).toMatchObject({ outcome: "stored" });

    // Another writer secret cannot take over the source, whichever link carries it; the same
    // secret still can when it arrives on a fresh link.
    expect((yield* send(mirrorRequest("PUT", otherWriter, { body, writer: secretB }))).status).toBe(
      404,
    );
    expect((yield* send(mirrorRequest("PUT", otherWriter, { body }))).status).toBe(200);

    // Readers need the read scope and a grant the source named; an agent token never reads.
    const read = (token: string | null, readGrant: string) =>
      send(mirrorRequest("GET", token, { grant: readGrant }));
    const copy = yield* read(phone, grant);
    expect(copy.status).toBe(200);
    expect(yield* Effect.promise(() => copy.json())).toMatchObject({
      sourceEnvironmentId: "mac",
      sourceLabel: "Mac",
    });
    expect((yield* read(phone, "someone-elses-grant")).status).toBe(404);
    expect((yield* read(noScope, grant)).status).toBe(403);
    expect((yield* read(writer, grant)).status).toBe(401);
    expect((yield* read(null, grant)).status).toBe(401);
    expect((yield* send(mirrorRequest("GET", phone, { grant, source: "unknown" }))).status).toBe(
      404,
    );

    // Only the writer secret deletes; afterwards nothing is served.
    expect((yield* send(mirrorRequest("DELETE", otherWriter, { writer: secretB }))).status).toBe(
      404,
    );
    for (const token of [plainAgent, phone, null]) {
      expect((yield* send(mirrorRequest("DELETE", token))).status).toBe(401);
    }
    expect((yield* read(phone, grant)).status).toBe(200);
    const removed = yield* send(mirrorRequest("DELETE", writer));
    expect(yield* Effect.promise(() => removed.json())).toEqual({ removed: true });
    expect((yield* read(phone, grant)).status).toBe(404);
    const again = yield* send(mirrorRequest("DELETE", writer));
    expect(yield* Effect.promise(() => again.json())).toEqual({ removed: false });
  }).pipe(Effect.scoped, Effect.provide(NodeCrypto.layer)),
);

it.effect("refuses an export body over the size cap", () =>
  Effect.gen(function* () {
    const { send, peer } = yield* serve;
    const writer = yield* peer(true);
    const body = yield* exportBody("x".repeat(THREAD_MIRROR_MAX_BODY_BYTES));
    const response = yield* send(mirrorRequest("PUT", writer, { body, source: "huge" }));
    expect(response.status).toBeGreaterThanOrEqual(400);
    expect(response.status).toBeLessThan(500);
  }).pipe(Effect.scoped, Effect.provide(NodeCrypto.layer)),
);
