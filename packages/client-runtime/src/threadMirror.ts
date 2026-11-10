import type { EnvironmentId, PeerLinkSummary } from "@t3tools/contracts";

export const THREAD_MIRROR_SETTING_TITLE = "Keep a copy of this thread list on another machine";

export const THREAD_MIRROR_SETTING_DESCRIPTION =
  "Shares this machine's thread titles, status, project folders and branches with the machine you choose, so signed-in devices can still see them while this machine is off. Messages stay here. The copy is replaced as threads change and removed after 7 days without an update. Turning this off asks that machine to delete it; if it can't be reached, a device that already has access can keep seeing the list until the copy expires.";

export const THREAD_MIRROR_NEEDS_LINKS = "Turn on Linked environments in Labs to choose a machine.";

export interface ThreadMirrorHostOption {
  readonly environmentId: EnvironmentId;
  readonly label: string;
  /** Why this machine can't take the copy right now, or null. */
  readonly problem: string | null;
}

export interface ThreadMirrorHostChoices {
  readonly options: ReadonlyArray<ThreadMirrorHostOption>;
  /** The chosen machine is no longer linked; turning the setting off is still possible. */
  readonly selectedMissing: boolean;
}

/** The linked machines that can keep the copy, and whether the saved choice is still one of them. */
export function threadMirrorHostChoices(input: {
  readonly selectedId: EnvironmentId | null;
  readonly links: ReadonlyArray<PeerLinkSummary>;
}): ThreadMirrorHostChoices {
  return {
    options: input.links.map((link) => ({
      environmentId: link.environmentId,
      label: link.label,
      problem:
        link.status === "expired"
          ? "Link expired"
          : link.status === "unreachable"
            ? "Not answering right now"
            : null,
    })),
    selectedMissing:
      input.selectedId !== null &&
      !input.links.some((link) => link.environmentId === input.selectedId),
  };
}

/** What to tell a user whose list for `sourceLabel` is a saved copy from another machine. */
export function threadMirrorNotice(sourceLabel: string) {
  return {
    title: `${sourceLabel} is offline`,
    detail: "Showing saved threads. Reconnect to make changes.",
  } as const;
}
