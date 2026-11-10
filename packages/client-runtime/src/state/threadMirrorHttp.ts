import {
  type EnvironmentId,
  THREAD_MIRROR_GRANT_HEADER,
  type ThreadMirrorCopy,
  type ThreadMirrorReaderGrant,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { HttpClient } from "effect/http";

import * as EnvironmentRegistry from "../connection/registry.ts";
import * as EnvironmentSupervisor from "../connection/supervisor.ts";
import {
  executeConnectedEnvironmentHttpRequest,
  withOrchestrationProtocolHeader,
} from "./environmentHttpAuth.ts";

const THREAD_MIRROR_TIMEOUT_MS = 15_000;

/**
 * The thread list another environment keeps for a source that may be off, and the client's key to
 * it. Both fail soft: a source that never exported, a host that is not connected, or a refused
 * key all mean there is no copy to show.
 */
export class ThreadMirrorLoader extends Context.Service<
  ThreadMirrorLoader,
  {
    /** This client's key to the supervised source's copy, from the source itself. */
    readonly readerGrant: Effect.Effect<
      Option.Option<ThreadMirrorReaderGrant>,
      never,
      EnvironmentSupervisor.EnvironmentSupervisor
    >;
    /** The source's copy, read from the connected host with `grant`. */
    readonly loadCopy: (input: {
      readonly hostEnvironmentId: EnvironmentId;
      readonly sourceEnvironmentId: EnvironmentId;
      readonly grant: string;
    }) => Effect.Effect<Option.Option<ThreadMirrorCopy>>;
  }
>()("@t3tools/client-runtime/state/threadMirrorHttp/ThreadMirrorLoader") {}

export const layer: Layer.Layer<
  ThreadMirrorLoader,
  never,
  HttpClient.HttpClient | EnvironmentRegistry.EnvironmentRegistry
> = Layer.effect(
  ThreadMirrorLoader,
  Effect.gen(function* () {
    const httpClient = yield* HttpClient.HttpClient;
    const registry = yield* EnvironmentRegistry.EnvironmentRegistry;
    const softly =
      (message: string) =>
      <A, R>(effect: Effect.Effect<A, unknown, R>) =>
        effect.pipe(
          Effect.asSome,
          Effect.provideService(HttpClient.HttpClient, httpClient),
          Effect.catchCause((cause) =>
            Effect.logDebug(message).pipe(
              Effect.annotateLogs({ cause: Cause.pretty(cause) }),
              Effect.as(Option.none<A>()),
            ),
          ),
        );

    return ThreadMirrorLoader.of({
      readerGrant: executeConnectedEnvironmentHttpRequest({
        group: "threadMirror",
        method: "GET",
        url: (urls) => urls.readerGrant(),
        timeoutMs: THREAD_MIRROR_TIMEOUT_MS,
        request: ({ client, headers }) =>
          client.readerGrant({ headers: withOrchestrationProtocolHeader(headers) }),
      }).pipe(softly("The source did not issue a thread list reader grant.")),
      loadCopy: Effect.fn("ThreadMirrorLoader.loadCopy")(function* (input) {
        return yield* registry
          .run(
            input.hostEnvironmentId,
            executeConnectedEnvironmentHttpRequest({
              group: "threadMirror",
              method: "GET",
              url: (urls) =>
                urls.get({ params: { sourceEnvironmentId: input.sourceEnvironmentId } }),
              timeoutMs: THREAD_MIRROR_TIMEOUT_MS,
              request: ({ client, headers }) =>
                client.get({
                  headers: {
                    ...withOrchestrationProtocolHeader(headers),
                    [THREAD_MIRROR_GRANT_HEADER]: input.grant,
                  },
                  params: { sourceEnvironmentId: input.sourceEnvironmentId },
                }),
            }),
          )
          .pipe(softly("The thread list copy could not be read from its host."));
      }),
    });
  }),
);
