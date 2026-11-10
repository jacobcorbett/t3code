import {
  ORCHESTRATION_V2_WS_METHODS,
  type EnvironmentId,
  type OrchestrationV2ShellSnapshot,
  type OrchestrationV2ShellStreamItem,
  type OrchestrationV2ThreadShell,
  type ServerConfig,
  type ThreadId,
  type ThreadMirrorCopy,
  type ThreadPullRequestLink,
  threadMirrorSupersedes,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { AsyncResult, Atom } from "effect/reactivity";

import * as EnvironmentRegistry from "../connection/registry.ts";
import { connectionProjectionPhase, type SupervisorConnectionState } from "../connection/model.ts";
import * as EnvironmentSupervisor from "../connection/supervisor.ts";
import * as ConnectionWakeups from "../connection/wakeups.ts";
import { safeErrorLogAttributes } from "../errors/safeLog.ts";
import * as Persistence from "../platform/persistence.ts";
import { runCachePersistence } from "./cachePersistence.ts";
import { subscribeDynamicWithSession } from "../rpc/client.ts";
import type { RpcSession } from "../rpc/session.ts";
import * as ShellSnapshotLoader from "./shellSnapshotHttp.ts";
import * as ThreadMirrorLoader from "./threadMirrorHttp.ts";
import type { DeferredShellSnapshot } from "./shellPullRequests.ts";
import {
  applyShellStreamEvents,
  type ShellStreamEvent,
  mergeShellSnapshotProjects,
  reuseUnchangedThreadShells,
  sameThreadPullRequests,
} from "./shellReducer.ts";
import { type EnvironmentCatalogState, enabledEnvironmentIds } from "./connections.ts";
import { followStreamInEnvironment } from "./runtime.ts";

export type EnvironmentShellStatus = "empty" | "cached" | "synchronizing" | "live";

export interface EnvironmentShellState {
  readonly snapshot: Option.Option<OrchestrationV2ShellSnapshot>;
  readonly status: EnvironmentShellStatus;
  readonly error: Option.Option<string>;
  /**
   * Changes with each authoritative snapshot (not enrichment refreshes); 0 until the first one.
   * Thread families read for an older generation may miss changes from before the latest snapshot,
   * so they are read again.
   */
  readonly generation: number;
  /** Sequences of the thread removals applied since the latest authoritative snapshot. */
  readonly removedAt: ReadonlyMap<ThreadId, number>;
  /**
   * Set while the rows are another environment's copy of this one's list, because this one is
   * unreachable. Live data replaces it; commands still go to this environment only.
   */
  readonly mirror: Option.Option<EnvironmentShellMirror>;
}

export interface EnvironmentShellMirror {
  readonly hostEnvironmentId: EnvironmentId;
  readonly sourceLabel: string;
  /** Epoch milliseconds when the host last heard from the source. */
  readonly syncedAt: number;
  /** Epoch milliseconds when the host stops serving this copy; the rows are dropped then. */
  readonly expiresAt: number;
}

const EMPTY_REMOVALS: ReadonlyMap<ThreadId, number> = new Map();
// Shared by every shell state, so a rebuilt one never reuses a generation another already used.
let lastShellGeneration = 0;

export const EMPTY_SHELL_STATE: EnvironmentShellState = {
  snapshot: Option.none(),
  status: "empty",
  error: Option.none(),
  generation: 0,
  removedAt: EMPTY_REMOVALS,
  mirror: Option.none(),
};

function shellStatusForSnapshot(
  snapshot: Option.Option<OrchestrationV2ShellSnapshot>,
): EnvironmentShellStatus {
  return Option.isSome(snapshot) ? "cached" : "empty";
}

function withoutDeferredPullRequests(value: DeferredShellSnapshot): OrchestrationV2ShellSnapshot {
  if (value.loadPullRequests === undefined) return value;
  const { loadPullRequests: _deferred, ...snapshot } = value;
  return snapshot;
}

// Exports reach the host every few hours or on change, so a slow poll loses little.
const MIRROR_POLL_SECONDS = 45;
const MIRROR_RETRY_MIN_SECONDS = 10;
const MIRROR_RETRY_MAX_SECONDS = 300;

/**
 * Only a source that is wanted and failing to answer may be read from its host. A refused
 * credential, an unsupported server, a disabled connection or a pending session is not offline.
 */
export function mirrorFallbackEligible(state: SupervisorConnectionState): boolean {
  return (
    state.desired &&
    state.phase === "backoff" &&
    state.lastFailure?._tag === "ConnectionTransientError" &&
    state.lastFailure.reason !== "session-pending"
  );
}

const SHELL_SYNCHRONIZATION_ERROR_MESSAGE = "Could not synchronize environment data.";

/**
 * Replay does not refresh the older-settled cut, rows that left the newest window, or
 * projections the server rebuilt, so drift grows with the age of the last server snapshot.
 */
const CACHE_RESUME_MAX_AGE_MILLIS = 12 * 60 * 60 * 1000;

type ShellOriginIdentity = Omit<Persistence.ShellCacheOrigin, "snapshotAt">;

interface ShellSubscription {
  readonly session: RpcSession;
  /** What a snapshot from this subscription descends from; null when the server did not say. */
  readonly identity: ShellOriginIdentity | null;
}

function canResumeFrom(
  origin: Persistence.ShellCacheOrigin | undefined,
  identity: ShellOriginIdentity | null,
  now: number,
): boolean {
  if (origin === undefined || identity === null) return false;
  const age = now - origin.snapshotAt;
  return (
    origin.fromMirror === undefined &&
    origin.databaseId === identity.databaseId &&
    origin.serverVersion === identity.serverVersion &&
    origin.databaseMigration === identity.databaseMigration &&
    origin.omitIdleSubagents === identity.omitIdleSubagents &&
    origin.omitOlderSettled === identity.omitOlderSettled &&
    age >= 0 &&
    age < CACHE_RESUME_MAX_AGE_MILLIS
  );
}

interface LiveShell {
  readonly snapshot: OrchestrationV2ShellSnapshot;
  // Read together with the rows, so a save never pairs them with another snapshot's origin.
  readonly origin: Persistence.ShellCacheOrigin | undefined;
}

export const makeEnvironmentShellState = Effect.fn("EnvironmentShellState.make")(function* () {
  const supervisor = yield* EnvironmentSupervisor.EnvironmentSupervisor;
  const cache = yield* Persistence.EnvironmentCacheStore;
  const snapshotLoader = yield* ShellSnapshotLoader.ShellSnapshotLoader;
  const mirrorLoader = yield* Effect.serviceOption(ThreadMirrorLoader.ThreadMirrorLoader);
  const wakeups = yield* Effect.serviceOption(ConnectionWakeups.ConnectionWakeups);
  const environmentId = supervisor.target.environmentId;
  const cached = yield* cache.loadShell(environmentId).pipe(
    Effect.catch((error) =>
      Effect.logWarning("Could not load cached environment shell.").pipe(
        Effect.annotateLogs({
          environmentId,
          ...safeErrorLogAttributes(error),
        }),
        Effect.as(Option.none<Persistence.CachedShellSnapshot>()),
      ),
    ),
  );
  // Rows that were a host's copy are dropped once the host's lease on them has ended.
  const loadedAt = yield* Clock.currentTimeMillis;
  const lapsed =
    Option.isSome(cached) &&
    cached.value.origin?.fromMirror !== undefined &&
    cached.value.origin.fromMirror.expiresAt <= loadedAt;
  const cachedOrigin = Option.isSome(cached) && !lapsed ? cached.value.origin : undefined;
  const cachedSnapshot = lapsed
    ? Option.none<OrchestrationV2ShellSnapshot>()
    : Option.map(cached, ({ origin: _origin, ...rows }) => withoutDeferredPullRequests(rows));
  const restoredMirror =
    cachedOrigin?.fromMirror === undefined || cachedOrigin.mirror === undefined
      ? Option.none<EnvironmentShellMirror>()
      : Option.some<EnvironmentShellMirror>({
          hostEnvironmentId: cachedOrigin.mirror.hostEnvironmentId,
          ...cachedOrigin.fromMirror,
        });
  const state = yield* SubscriptionRef.make<EnvironmentShellState>({
    ...EMPTY_SHELL_STATE,
    snapshot: cachedSnapshot,
    status: shellStatusForSnapshot(cachedSnapshot),
    mirror: restoredMirror,
  });
  const awaitingCompletion = yield* Ref.make(false);
  const activeSubscription = yield* Ref.make<ShellSubscription | null>(null);
  // The server snapshot the in-memory rows descend from, directly or through the cache.
  // Only a server snapshot renews it; `snapshot-required` drops it.
  const origin = yield* Ref.make(cachedOrigin);
  const reloadRequests = yield* Queue.unbounded<void>();
  const grantRefreshRequests = yield* Queue.sliding<void>(1);
  const latestLive = yield* Ref.make<Option.Option<LiveShell>>(Option.none());
  const persistence = yield* Queue.sliding<OrchestrationV2ShellSnapshot>(1);

  const persistenceLock = yield* Semaphore.make(1);
  let lastPersisted: LiveShell | undefined;
  // Rows still waiting for their deferred pull request links, which must not be saved:
  // the cache would keep them without links until the next server refresh.
  let pendingLinkRows: ReadonlySet<OrchestrationV2ThreadShell> = new Set();
  const persistLatest = Effect.fn("EnvironmentShellState.persistLatest")(function* (
    flush: boolean,
  ) {
    while (true) {
      const latest = yield* Ref.get(latestLive);
      if (Option.isNone(latest) || latest.value === lastPersisted) return;
      const { snapshot, origin } = latest.value;
      if (snapshot.threads.some((thread) => pendingLinkRows.has(thread))) return;
      if (
        lastPersisted !== undefined &&
        origin === lastPersisted.origin &&
        snapshot.snapshotSequence >= lastPersisted.snapshot.snapshotSequence &&
        snapshot.schemaVersion === lastPersisted.snapshot.schemaVersion &&
        snapshot.projects === lastPersisted.snapshot.projects &&
        snapshot.threads === lastPersisted.snapshot.threads &&
        snapshot.archivedThreads === lastPersisted.snapshot.archivedThreads
      ) {
        // Keep the live cursor without rewriting the rows. Remember it here too so a
        // later authoritative reset to a lower sequence still reaches the cache.
        lastPersisted = latest.value;
        return;
      }
      const saved = yield* cache.saveShell(environmentId, snapshot, origin).pipe(
        Effect.as(true),
        Effect.catch((error) =>
          Effect.logWarning("Could not persist environment shell cache.").pipe(
            Effect.annotateLogs({ environmentId, ...safeErrorLogAttributes(error) }),
            Effect.as(false),
          ),
        ),
      );
      if (!saved) return;
      lastPersisted = latest.value;
      if (!flush) return;
    }
  }, persistenceLock.withPermit);
  const persist = () => persistLatest(false);
  const flushLiveShellSnapshot = persistLatest(true);

  // Register before scoped worker fibers so reverse finalizer order interrupts
  // those fibers first and this flush sees a stable latestLiveSnapshot.
  yield* Effect.addFinalizer(() => flushLiveShellSnapshot);

  yield* runCachePersistence(persistence, persist).pipe(Effect.forkScoped);

  const setDisconnected = Ref.set(awaitingCompletion, false).pipe(
    Effect.andThen(
      SubscriptionRef.update(state, (current) => ({
        ...current,
        status: shellStatusForSnapshot(current.snapshot),
      })),
    ),
    Effect.andThen(flushLiveShellSnapshot.pipe(Effect.forkScoped)),
    Effect.asVoid,
  );
  const setSynchronizing = SubscriptionRef.update(state, (current) => ({
    ...current,
    status: "synchronizing" as const,
    error: Option.none(),
  }));
  const setReady = SubscriptionRef.update(state, (current) =>
    current.status === "live"
      ? current
      : {
          ...current,
          status: "synchronizing" as const,
          error: Option.none(),
        },
  );
  const setStreamError = (error: unknown) =>
    Ref.set(awaitingCompletion, false).pipe(
      Effect.andThen(Effect.logWarning("Could not synchronize the environment shell.")),
      Effect.annotateLogs({
        environmentId,
        ...safeErrorLogAttributes(error),
      }),
      Effect.andThen(
        SubscriptionRef.update(state, (current) => ({
          ...current,
          status: shellStatusForSnapshot(current.snapshot),
          error: Option.some(SHELL_SYNCHRONIZATION_ERROR_MESSAGE),
        })),
      ),
    );

  // Server items and deferred link fills both read the snapshot and write it back; one at a
  // time, so neither can overwrite what the other wrote in between.
  const snapshotWriteLock = yield* Semaphore.make(1);

  const applyItems = Effect.fn("EnvironmentShellState.applyItems")(function* (
    entries: ReadonlyArray<readonly [RpcSession, OrchestrationV2ShellStreamItem]>,
    pendingLinkIds?: ReadonlySet<ThreadId>,
  ) {
    // Read under the lock: items buffered across a session change must not land on the
    // rows the next session loaded.
    const subscription = yield* Ref.get(activeSubscription);
    const items = entries.flatMap(([session, item]) =>
      session === subscription?.session ? [item] : [],
    );
    const initial = yield* SubscriptionRef.get(state);
    let waiting = yield* Ref.get(awaitingCompletion);
    let next = initial;
    let receivedAuthoritativeSnapshot = false;
    let snapshotRequired = false;
    let deltas: Array<ShellStreamEvent> = [];
    const flushDeltas = () => {
      if (deltas.length > 0 && Option.isSome(next.snapshot)) {
        let sequence = next.snapshot.value.snapshotSequence;
        let removedAt: Map<ThreadId, number> | undefined;
        for (const delta of deltas) {
          if (delta.sequence <= sequence) continue;
          switch (delta.kind) {
            case "thread.removed":
              removedAt ??= new Map(next.removedAt);
              removedAt.set(delta.threadId, delta.sequence);
              break;
            case "thread.updated":
            case "project.updated":
            case "project.removed":
              break;
            default:
              continue;
          }
          sequence = delta.sequence;
        }
        next = {
          ...next,
          removedAt: removedAt ?? next.removedAt,
          snapshot: Option.some(applyShellStreamEvents(next.snapshot.value, deltas)),
          status: waiting ? "synchronizing" : "live",
          error: Option.none(),
          mirror: Option.none(),
        };
      }
      deltas = [];
    };
    for (const item of items) {
      if (item.kind === "snapshot-required") {
        snapshotRequired = true;
        continue;
      }
      if (item.kind !== "snapshot" && item.kind !== "synchronized") {
        deltas.push(item);
        continue;
      }
      // Snapshots can reset the cursor; completion markers change the sync status.
      flushDeltas();
      if (item.kind === "synchronized") {
        waiting = false;
        if (Option.isSome(next.snapshot)) {
          next = {
            ...next,
            status: "live",
            error: Option.none(),
            mirror: Option.none(),
            // A cache resume that caught up is as current as a snapshot for generation readers.
            generation: next.generation === 0 ? ++lastShellGeneration : next.generation,
          };
        }
        continue;
      }
      const authoritative = item.resolvedRepositoryIdentityRoots === undefined;
      const nextSnapshot = mergeShellSnapshotProjects(
        Option.getOrNull(next.snapshot),
        item.snapshot,
        item.resolvedRepositoryIdentityRoots === undefined
          ? undefined
          : { resolvedRepositoryIdentityRoots: item.resolvedRepositoryIdentityRoots },
      );
      if (authoritative) receivedAuthoritativeSnapshot = true;
      next = {
        snapshot: Option.some(nextSnapshot),
        status: waiting ? "synchronizing" : "live",
        error: Option.none(),
        generation: authoritative ? ++lastShellGeneration : next.generation,
        removedAt: authoritative ? EMPTY_REMOVALS : next.removedAt,
        mirror: Option.none(),
      };
    }
    flushDeltas();
    yield* Ref.set(awaitingCompletion, waiting);
    let nextOrigin = yield* Ref.get(origin);
    if (snapshotRequired) {
      // The rows stay on screen but reload before the next resume; the cache keeps the old
      // origin until that reload saves, which costs at most one more refusal.
      nextOrigin = undefined;
      yield* Ref.set(origin, nextOrigin);
      yield* Queue.offer(reloadRequests, undefined);
    } else if (receivedAuthoritativeSnapshot) {
      // The key held so far stays until the source issues a new one or withdraws it.
      const heldMirror = nextOrigin?.mirror;
      nextOrigin =
        subscription?.identity == null
          ? undefined
          : {
              ...subscription.identity,
              ...(heldMirror === undefined ? {} : { mirror: heldMirror }),
              snapshotAt: yield* Clock.currentTimeMillis,
            };
      yield* Ref.set(origin, nextOrigin);
      if (nextOrigin !== undefined) yield* Queue.offer(grantRefreshRequests, undefined);
    }
    if (next === initial) return;
    if (pendingLinkIds !== undefined && Option.isSome(next.snapshot)) {
      pendingLinkRows = new Set(
        next.snapshot.value.threads.filter((thread) => pendingLinkIds.has(thread.id)),
      );
    }
    if (Option.isSome(next.snapshot)) {
      yield* Ref.set(
        latestLive,
        Option.some({ snapshot: next.snapshot.value, origin: nextOrigin }),
      );
    }
    yield* SubscriptionRef.set(state, next);
    if (next.snapshot !== initial.snapshot && Option.isSome(next.snapshot)) {
      yield* Queue.offer(persistence, next.snapshot.value);
    }
  }, snapshotWriteLock.withPermit);

  const stateScope = yield* Effect.scope;
  // This client's key to the source's copy on the host, taken from the source while it answers.
  // It rides in the cache origin, so a restart with the source off can still read the copy.
  const refreshReaderGrant = Effect.fn("EnvironmentShellState.refreshReaderGrant")(function* () {
    if (Option.isNone(mirrorLoader)) return;
    const issued = yield* mirrorLoader.value.readerGrant.pipe(
      Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
    );
    // Unreachable or unsupported: keep the key held, which the host refuses once it is stale.
    if (Option.isNone(issued)) return;
    const { hostEnvironmentId, grant } = issued.value;
    yield* snapshotWriteLock.withPermit(
      Effect.gen(function* () {
        const held = yield* Ref.get(origin);
        const live = yield* Ref.get(latestLive);
        if (held === undefined || Option.isNone(live)) return;
        const { mirror: heldMirror, ...rest } = held;
        const mirror =
          hostEnvironmentId === null || grant === null ? undefined : { hostEnvironmentId, grant };
        if (
          heldMirror?.hostEnvironmentId === mirror?.hostEnvironmentId &&
          heldMirror?.grant === mirror?.grant
        ) {
          return;
        }
        const nextOrigin = mirror === undefined ? rest : { ...rest, mirror };
        yield* Ref.set(origin, nextOrigin);
        yield* Ref.set(
          latestLive,
          Option.some({ snapshot: live.value.snapshot, origin: nextOrigin }),
        );
        yield* Queue.offer(persistence, live.value.snapshot);
      }),
    );
  });

  // The host's copy replaces the rows only while this environment is not answering, and only
  // when it is strictly newer in the same source database; commands never go to the host. A copy
  // at the rows' own sequence only brings the notice and lease, since the list is the same.
  const applyMirrorCopy = Effect.fn("EnvironmentShellState.applyMirrorCopy")(function* (
    copy: ThreadMirrorCopy,
  ) {
    return yield* snapshotWriteLock.withPermit(
      Effect.gen(function* () {
        const current = yield* SubscriptionRef.get(state);
        const held = yield* Ref.get(origin);
        if (
          (current.status !== "cached" && current.status !== "empty") ||
          held?.mirror === undefined
        ) {
          return false;
        }
        const previous = Option.getOrNull(current.snapshot);
        const replaces = threadMirrorSupersedes(
          previous === null
            ? null
            : { databaseId: held.databaseId, sequence: previous.snapshotSequence },
          copy,
        );
        const sameList =
          previous !== null &&
          held.databaseId === copy.sourceDatabaseId &&
          copy.sourceSequence === previous.snapshotSequence;
        if (!replaces && !sameList) return false;
        const provenance = {
          sourceLabel: copy.sourceLabel,
          syncedAt: DateTime.toEpochMillis(copy.receivedAt),
          expiresAt: DateTime.toEpochMillis(copy.expiresAt),
        };
        const nextOrigin = { ...held, fromMirror: provenance };
        const mirror = Option.some({
          hostEnvironmentId: held.mirror.hostEnvironmentId,
          ...provenance,
        });
        const snapshot = replaces
          ? mergeShellSnapshotProjects(
              previous,
              reuseUnchangedThreadShells(previous, copy.snapshot),
            )
          : previous;
        if (snapshot === null) return false;
        yield* Ref.set(origin, nextOrigin);
        yield* Ref.set(latestLive, Option.some({ snapshot, origin: nextOrigin }));
        yield* SubscriptionRef.set(state, {
          ...current,
          snapshot: Option.some(snapshot),
          ...(replaces ? { generation: ++lastShellGeneration, removedAt: EMPTY_REMOVALS } : {}),
          mirror,
        });
        yield* Queue.offer(persistence, snapshot);
        return replaces;
      }),
    );
  });

  // Rows that came from the host stop being shown once the host's lease on the copy ends.
  const dropLapsedMirror = Effect.fn("EnvironmentShellState.dropLapsedMirror")(function* () {
    const now = yield* Clock.currentTimeMillis;
    yield* snapshotWriteLock.withPermit(
      Effect.gen(function* () {
        const current = yield* SubscriptionRef.get(state);
        if (Option.isNone(current.mirror) || current.mirror.value.expiresAt > now) return;
        yield* Ref.set(origin, undefined);
        yield* Ref.set(latestLive, Option.none());
        yield* SubscriptionRef.set(state, {
          ...current,
          snapshot: Option.none(),
          status: "empty",
          mirror: Option.none(),
        });
      }),
    );
  });

  // Reads the host again while the source is unreachable: soon after a failure, then slowly to
  // pick up later exports. Ends when the source connects or is blocked, which interrupts it.
  const mirrorFallback = Effect.gen(function* () {
    if (Option.isNone(mirrorLoader)) return;
    for (let failures = 0; ;) {
      const held = yield* Ref.get(origin);
      const copy =
        held?.mirror === undefined
          ? Option.none<ThreadMirrorCopy>()
          : yield* mirrorLoader.value.loadCopy({
              hostEnvironmentId: held.mirror.hostEnvironmentId,
              sourceEnvironmentId: environmentId,
              grant: held.mirror.grant,
            });
      if (Option.isSome(copy)) {
        failures = 0;
        yield* applyMirrorCopy(copy.value);
      } else {
        failures += 1;
      }
      yield* dropLapsedMirror();
      yield* Effect.sleep(
        Duration.seconds(
          failures === 0
            ? MIRROR_POLL_SECONDS
            : Math.min(MIRROR_RETRY_MAX_SECONDS, MIRROR_RETRY_MIN_SECONDS * 2 ** (failures - 1)),
        ),
      );
    }
  });
  let activeMirrorFallback: Fiber.Fiber<void> | undefined;
  const setMirrorFallback = (running: boolean) =>
    Effect.suspend(() => {
      const fiber = activeMirrorFallback;
      if (running === (fiber !== undefined)) return Effect.void;
      activeMirrorFallback = undefined;
      if (fiber !== undefined) return Fiber.interrupt(fiber);
      return Effect.forkIn(mirrorFallback, stateScope).pipe(
        Effect.tap((forked) => Effect.sync(() => void (activeMirrorFallback = forked))),
        Effect.asVoid,
      );
    });

  // Each full snapshot supersedes the previous one's fill; links from an older snapshot must
  // not land on rows a newer snapshot reused.

  let activeFill: Fiber.Fiber<void> | undefined;
  const cancelActiveFill = Effect.suspend(() => {
    const fill = activeFill;
    activeFill = undefined;
    return fill === undefined ? Effect.void : Fiber.interrupt(fill);
  });
  const fillDeferredPullRequests = Effect.fnUntraced(function* (
    source: DeferredShellSnapshot,
    applied: OrchestrationV2ShellSnapshot,
  ) {
    yield* cancelActiveFill;
    if (source.loadPullRequests === undefined) return;
    const arrivedRows = new Set(applied.threads);
    const arrivedIds = new Set(source.threads.map((thread) => thread.id));
    pendingLinkRows = new Set(applied.threads.filter((thread) => arrivedIds.has(thread.id)));
    activeFill = yield* source.loadPullRequests.pipe(
      Effect.flatMap((linksByThreadId) =>
        writeFilledLinks(linksByThreadId, arrivedRows, arrivedIds),
      ),
      Effect.forkIn(stateScope),
    );
  });
  const writeFilledLinks = Effect.fnUntraced(
    function* (
      linksByThreadId: ReadonlyMap<ThreadId, ReadonlyArray<ThreadPullRequestLink>>,
      arrivedRows: ReadonlySet<OrchestrationV2ThreadShell>,
      arrivedIds: ReadonlySet<ThreadId>,
    ) {
      const filled = yield* SubscriptionRef.modify(
        state,
        (current): [Option.Option<OrchestrationV2ShellSnapshot>, EnvironmentShellState] => {
          if (Option.isNone(current.snapshot)) return [Option.none(), current];
          const snapshot = current.snapshot.value;
          let changed = false;
          const threads = snapshot.threads.map((thread) => {
            if (!arrivedRows.has(thread) || !arrivedIds.has(thread.id)) return thread;
            const links = linksByThreadId.get(thread.id);
            if (sameThreadPullRequests(thread.pullRequests, links)) return thread;
            changed = true;
            if (links === undefined) {
              const { pullRequests: _previous, ...row } = thread;
              return row;
            }
            return { ...thread, pullRequests: links };
          });
          if (!changed) return [Option.none(), current];
          const next = { ...snapshot, threads };
          return [Option.some(next), { ...current, snapshot: Option.some(next) }];
        },
      );
      pendingLinkRows = new Set();
      const live = yield* Ref.get(latestLive);
      if (Option.isNone(live)) return;
      const latest = Option.isSome(filled) ? filled.value : live.value.snapshot;
      yield* Ref.set(latestLive, Option.some({ snapshot: latest, origin: live.value.origin }));
      yield* Queue.offer(persistence, latest);
    },
    (effect) => snapshotWriteLock.withPermit(effect),
  );
  if (Option.isSome(cached) && Option.isSome(cachedSnapshot)) {
    yield* fillDeferredPullRequests(cached.value, cachedSnapshot.value);
  }

  const resubscriptions = Stream.merge(
    Stream.fromQueue(reloadRequests),
    Option.match(wakeups, {
      onNone: () => Stream.never,
      onSome: (service) =>
        service.changes.pipe(Stream.filter(ConnectionWakeups.shouldResubscribeAfterWakeup)),
    }),
  );

  yield* Stream.fromQueue(grantRefreshRequests).pipe(
    Stream.runForEach(() => refreshReaderGrant()),
    Effect.forkScoped,
  );
  yield* setSynchronizing;
  yield* Effect.forkScoped(
    subscribeDynamicWithSession(
      ORCHESTRATION_V2_WS_METHODS.subscribeShell,
      Effect.fn("EnvironmentShellState.makeSubscribeInput")(function* (session) {
        const identity = yield* session.initialConfig.pipe(
          Effect.map((config): ShellOriginIdentity => {
            const omitIdleSubagents = config.shellThreadFamilies === true;
            return {
              serverVersion: config.environment.serverVersion,
              // Migrations rewrite projections without events, so a migrated server reloads.
              databaseMigration: config.environment.databaseMigration ?? null,
              ...(config.environment.databaseId === undefined
                ? {}
                : { databaseId: config.environment.databaseId }),
              omitIdleSubagents,
              omitOlderSettled: config.shellOlderSettled === true && omitIdleSubagents,
            };
          }),
          Effect.orElseSucceed(() => null),
        );
        const { omitIdleSubagents, omitOlderSettled } = identity ?? {
          omitIdleSubagents: false,
          omitOlderSettled: false,
        };
        // The source's own host choice changes without any thread changing, and the key held
        // would name the old host (or none), so a change asks it for a fresh one.
        yield* session.subscribeServerConfig().pipe(
          Stream.filterMap((event) =>
            event.type === "snapshot"
              ? Result.succeed(event.config.settings.threadMirrorHostEnvironmentId)
              : event.type === "settingsUpdated"
                ? Result.succeed(event.payload.settings.threadMirrorHostEnvironmentId)
                : Result.failVoid,
          ),
          Stream.changes,
          Stream.runForEach(() => Queue.offer(grantRefreshRequests, undefined)),
          Effect.raceFirst(session.closed),
          Effect.ignore,
          Effect.forkIn(stateScope),
        );
        yield* Ref.set(activeSubscription, { session, identity });
        yield* Ref.set(awaitingCompletion, true);
        yield* setSynchronizing;

        // Rows from a recent server snapshot with the same omissions and server build resume
        // from their cursor, in memory or from the cache. Anything else reloads the HTTP
        // snapshot so a valid cursor cannot preserve stale or incomplete data.
        let current = yield* SubscriptionRef.get(state);
        let canResume =
          Option.isSome(current.snapshot) &&
          canResumeFrom(yield* Ref.get(origin), identity, yield* Clock.currentTimeMillis);
        // Only rows the client already held may be refused; a cursor from the snapshot just
        // loaded falls back to the socket snapshot, so a refusal cannot loop.
        const resumingHeldRows = canResume;
        if (!canResume) {
          const prepared = yield* SubscriptionRef.get(supervisor.prepared).pipe(
            Effect.flatMap(
              Option.match({
                onSome: Effect.succeed,
                onNone: () =>
                  SubscriptionRef.changes(supervisor.prepared).pipe(
                    Stream.filter(Option.isSome),
                    Stream.map((value) => value.value),
                    Stream.runHead,
                    Effect.map(Option.getOrThrow),
                  ),
              }),
            ),
          );
          // HTTP and the socket must agree, or omitted children would return through the socket.
          const httpSnapshot = yield* snapshotLoader.load(prepared, {
            omitIdleSubagents,
            omitOlderSettled,
          });
          if (Option.isSome(httpSnapshot)) {
            yield* cancelActiveFill;
            const previous = yield* SubscriptionRef.get(state);
            const snapshot = reuseUnchangedThreadShells(
              Option.getOrNull(previous.snapshot),
              withoutDeferredPullRequests(httpSnapshot.value),
            );
            const pendingLinkIds = new Set(
              httpSnapshot.value.loadPullRequests === undefined
                ? []
                : httpSnapshot.value.threads.map((thread) => thread.id),
            );
            yield* applyItems([[session, { kind: "snapshot", snapshot }]], pendingLinkIds);
            canResume = true;
            current = yield* SubscriptionRef.get(state);
            if (Option.isSome(current.snapshot)) {
              yield* fillDeferredPullRequests(httpSnapshot.value, current.snapshot.value);
            }
          }
        }

        const omission = {
          ...(omitIdleSubagents ? { omitIdleSubagents: true as const } : {}),
          ...(omitOlderSettled ? { omitOlderSettled: true as const } : {}),
        };
        if (!canResume || Option.isNone(current.snapshot)) {
          return { ...omission, requestCompletionMarker: true as const };
        }
        return {
          afterSequence: current.snapshot.value.snapshotSequence,
          ...(resumingHeldRows ? { httpSnapshotFallback: true as const } : {}),
          ...omission,
          requestCompletionMarker: true as const,
        };
      }),
      {
        onExpectedFailure: (cause) => setStreamError(Cause.squash(cause)),
        retryExpectedFailureAfter: "250 millis",
        resubscribe: resubscriptions,
      },
    ).pipe(Stream.runForEachArray((entries) => applyItems(entries))),
  );
  yield* SubscriptionRef.changes(supervisor.state).pipe(
    Stream.runForEach((connectionState) => {
      switch (connectionProjectionPhase(connectionState)) {
        case "synchronizing":
          return setMirrorFallback(false).pipe(Effect.andThen(setSynchronizing));
        case "disconnected":
          return setDisconnected.pipe(
            Effect.andThen(setMirrorFallback(mirrorFallbackEligible(connectionState))),
          );
        case "ready":
          return setMirrorFallback(false).pipe(Effect.andThen(setReady));
      }
    }),
    Effect.forkScoped,
  );

  return state;
});

function shellStateChanges(environmentId: EnvironmentId) {
  return followStreamInEnvironment(
    environmentId,
    Stream.unwrap(makeEnvironmentShellState().pipe(Effect.map(SubscriptionRef.changes))),
  );
}

export interface EnvironmentShellSummary {
  readonly hasSnapshot: boolean;
  readonly hasSynchronizingShell: boolean;
  readonly firstError: string | null;
}

const EMPTY_ENVIRONMENT_SHELL_SUMMARY: EnvironmentShellSummary = Object.freeze({
  hasSnapshot: false,
  hasSynchronizingShell: false,
  firstError: null,
});

const EMPTY_SERVER_CONFIGS: ReadonlyMap<EnvironmentId, ServerConfig> = new Map();

function shellSummariesEqual(
  left: EnvironmentShellSummary,
  right: EnvironmentShellSummary,
): boolean {
  return (
    left.hasSnapshot === right.hasSnapshot &&
    left.hasSynchronizingShell === right.hasSynchronizingShell &&
    left.firstError === right.firstError
  );
}

function mapsEqual<K, V>(left: ReadonlyMap<K, V>, right: ReadonlyMap<K, V>): boolean {
  if (left.size !== right.size) {
    return false;
  }
  for (const [key, value] of left) {
    if (right.get(key) !== value) {
      return false;
    }
  }
  return true;
}

export function createEnvironmentShellSummaryAtom(input: {
  readonly catalogValueAtom: Atom.Atom<EnvironmentCatalogState>;
  readonly shellStateValueAtom: (environmentId: EnvironmentId) => Atom.Atom<EnvironmentShellState>;
}) {
  let previousSummary = EMPTY_ENVIRONMENT_SHELL_SUMMARY;
  return Atom.make((get) => {
    let hasSnapshot = false;
    let hasSynchronizingShell = false;
    let firstError: string | null = null;

    for (const environmentId of enabledEnvironmentIds(get(input.catalogValueAtom))) {
      const state = get(input.shellStateValueAtom(environmentId));
      hasSynchronizingShell ||= state.status === "synchronizing";
      if (firstError === null) {
        firstError = Option.getOrNull(state.error);
      }
      if (Option.isNone(state.snapshot)) {
        continue;
      }
      hasSnapshot = true;
    }

    const next: EnvironmentShellSummary = {
      hasSnapshot,
      hasSynchronizingShell,
      firstError,
    };
    if (shellSummariesEqual(previousSummary, next)) {
      return previousSummary;
    }
    previousSummary = next;
    return previousSummary;
  }).pipe(Atom.withLabel("environment-shell-summary"));
}

export interface EnvironmentMirrorSummaryEntry {
  readonly environmentId: EnvironmentId;
  readonly sourceLabel: string;
  readonly syncedAt: number;
}

const EMPTY_MIRROR_SUMMARY: ReadonlyArray<EnvironmentMirrorSummaryEntry> = Object.freeze([]);

/** Environments whose threads are a saved copy; shell pushes leave it untouched until one changes. */
export function createEnvironmentMirrorSummaryAtom(input: {
  readonly catalogValueAtom: Atom.Atom<EnvironmentCatalogState>;
  readonly shellStateValueAtom: (environmentId: EnvironmentId) => Atom.Atom<EnvironmentShellState>;
}) {
  let previous = EMPTY_MIRROR_SUMMARY;
  return Atom.make((get) => {
    const next: Array<EnvironmentMirrorSummaryEntry> = [];
    for (const environmentId of enabledEnvironmentIds(get(input.catalogValueAtom))) {
      const mirror = Option.getOrNull(get(input.shellStateValueAtom(environmentId)).mirror);
      if (mirror !== null) {
        next.push({ environmentId, sourceLabel: mirror.sourceLabel, syncedAt: mirror.syncedAt });
      }
    }
    if (
      next.length === previous.length &&
      next.every(
        (entry, index) =>
          entry.environmentId === previous[index]?.environmentId &&
          entry.sourceLabel === previous[index]?.sourceLabel &&
          entry.syncedAt === previous[index]?.syncedAt,
      )
    ) {
      return previous;
    }
    previous = next.length === 0 ? EMPTY_MIRROR_SUMMARY : next;
    return previous;
  }).pipe(Atom.withLabel("environment-mirror-summary"));
}

export function createEnvironmentServerConfigsAtom(input: {
  readonly catalogValueAtom: Atom.Atom<EnvironmentCatalogState>;
  readonly serverConfigValueAtom: (environmentId: EnvironmentId) => Atom.Atom<ServerConfig | null>;
}) {
  let previousServerConfigs = EMPTY_SERVER_CONFIGS;
  return Atom.make((get) => {
    const next = new Map<EnvironmentId, ServerConfig>();
    for (const environmentId of enabledEnvironmentIds(get(input.catalogValueAtom))) {
      const config = get(input.serverConfigValueAtom(environmentId));
      if (config !== null) {
        next.set(environmentId, config);
      }
    }
    if (mapsEqual(previousServerConfigs, next)) {
      return previousServerConfigs;
    }
    previousServerConfigs = next;
    return previousServerConfigs;
  }).pipe(Atom.withLabel("environment-server-configs"));
}

/**
 * Every shell push replaces the state, so views that gate on load progress read these
 * primitives instead and stay put while threads stream.
 */
function createEnvironmentShellProgressAtoms(
  stateValueAtom: (environmentId: EnvironmentId) => Atom.Atom<EnvironmentShellState>,
) {
  return {
    statusAtom: Atom.family((environmentId: EnvironmentId) =>
      Atom.make((get) => get(stateValueAtom(environmentId)).status).pipe(
        Atom.withLabel(`environment-shell-status:${environmentId}`),
      ),
    ),
    hasSnapshotAtom: Atom.family((environmentId: EnvironmentId) =>
      Atom.make((get) => Option.isSome(get(stateValueAtom(environmentId)).snapshot)).pipe(
        Atom.withLabel(`environment-shell-has-snapshot:${environmentId}`),
      ),
    ),
  };
}

export function createEnvironmentShellAtoms<R, E>(
  runtime: Atom.AtomRuntime<
    | EnvironmentRegistry.EnvironmentRegistry
    | Persistence.EnvironmentCacheStore
    | ShellSnapshotLoader.ShellSnapshotLoader
    | R,
    E
  >,
) {
  const stateAtom = Atom.family((environmentId: EnvironmentId) =>
    runtime.atom(shellStateChanges(environmentId), {
      initialValue: EMPTY_SHELL_STATE,
    }),
  );

  const stateValueAtom = Atom.family((environmentId: EnvironmentId) =>
    Atom.make((get) =>
      AsyncResult.getOrElse(get(stateAtom(environmentId)), () => EMPTY_SHELL_STATE),
    ).pipe(Atom.withLabel(`environment-shell-state-value:${environmentId}`)),
  );

  const { statusAtom, hasSnapshotAtom } = createEnvironmentShellProgressAtoms(stateValueAtom);

  return {
    stateAtom,
    stateValueAtom,
    statusAtom,
    hasSnapshotAtom,
  };
}

export * from "./models.ts";
export * from "./shellCommands.ts";
export * from "./shellReducer.ts";
export * from "./shellPullRequests.ts";
export * as ShellSnapshotLoader from "./shellSnapshotHttp.ts";
export * as ThreadMirrorLoader from "./threadMirrorHttp.ts";
export * from "./snapshots.ts";
