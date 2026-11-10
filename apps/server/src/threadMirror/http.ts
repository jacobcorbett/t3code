import {
  AuthOrchestrationReadScope,
  EnvironmentHttpApi,
  THREAD_MIRROR_GRANT_HEADER,
  THREAD_MIRROR_WRITER_HEADER,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";

import {
  annotateEnvironmentRequest,
  failEnvironmentInternal,
  failEnvironmentInvalidRequest,
  failEnvironmentNotFound,
  requireEnvironmentScope,
} from "../auth/http.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ThreadMirrorExporter from "./ThreadMirrorExporter.ts";
import * as ThreadMirrorHost from "./ThreadMirrorHost.ts";

const mapHostError = Effect.fnUntraced(function* (error: ThreadMirrorHost.ThreadMirrorHostError) {
  switch (error.reason) {
    case "too_large":
      return yield* failEnvironmentInvalidRequest("thread_mirror_too_large");
    case "expired":
      return yield* failEnvironmentInvalidRequest("thread_mirror_expired");
    // A writer that lost the source to another writer hears the same as a missing copy.
    case "not_found":
    case "writer_mismatch":
      return yield* failEnvironmentNotFound("thread_mirror_not_found");
    case "storage":
      return yield* failEnvironmentInternal("thread_mirror_failed", error);
  }
});

export const layer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "threadMirror",
  Effect.fnUntraced(function* (handlers) {
    const host = yield* ThreadMirrorHost.ThreadMirrorHost;
    const exporter = yield* ThreadMirrorExporter.ThreadMirrorExporter;
    const settings = yield* ServerSettings.ServerSettingsService;

    return handlers
      .handle(
        "readerGrant",
        Effect.fn("environment.threadMirror.readerGrant")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          const session = yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          return yield* exporter
            .readerGrant(session.sessionId)
            .pipe(Effect.catch((cause) => failEnvironmentInternal("thread_mirror_failed", cause)));
        }),
      )
      .handle(
        "put",
        Effect.fn("environment.threadMirror.put")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          // With linked environments off here, a link that already exists keeps nothing new.
          const linksOn = yield* settings.getSettings.pipe(
            Effect.map((current) => current.linkedEnvironmentsEnabled),
            Effect.orElseSucceed(() => false),
          );
          if (!linksOn) return yield* failEnvironmentNotFound("thread_mirror_not_found");
          return yield* host
            .accept({
              sourceEnvironmentId: args.params.sourceEnvironmentId,
              writerSecret: args.headers[THREAD_MIRROR_WRITER_HEADER],
              copy: args.payload,
            })
            .pipe(Effect.catch(mapHostError));
        }),
      )
      .handle(
        "get",
        Effect.fn("environment.threadMirror.get")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          return yield* host
            .read({
              sourceEnvironmentId: args.params.sourceEnvironmentId,
              grant: args.headers[THREAD_MIRROR_GRANT_HEADER],
            })
            .pipe(Effect.catch(mapHostError));
        }),
      )
      .handle(
        "delete",
        Effect.fn("environment.threadMirror.delete")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          const removed = yield* host
            .remove({
              sourceEnvironmentId: args.params.sourceEnvironmentId,
              writerSecret: args.headers[THREAD_MIRROR_WRITER_HEADER],
            })
            .pipe(Effect.catch(mapHostError));
          return { removed };
        }),
      );
  }),
);
