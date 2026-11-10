import { useAtomValue } from "@effect/atom-react";
import { threadMirrorNotice } from "@t3tools/client-runtime/thread-mirror";

import { useNowMinute } from "../../hooks/useNowMinute";
import { environmentMirrorSummaryAtom } from "~/state/shell";
import { formatRelativeTimeLabel } from "~/timestampFormat";
import { ConnectionStatusDot } from "../ConnectionStatusDot";

/** Saved lists shown for environments that are off, so none reads as live. */
export function MirroredEnvironmentsNotice() {
  const mirrored = useAtomValue(environmentMirrorSummaryAtom);
  const nowMs = Date.parse(`${useNowMinute()}Z`);
  return mirrored.map((entry) => {
    const notice = threadMirrorNotice(entry.sourceLabel);
    const synced = formatRelativeTimeLabel(new Date(entry.syncedAt).toISOString(), nowMs);
    return (
      <div
        key={entry.environmentId}
        role="status"
        className="flex items-start gap-2 px-2 py-1.5 text-xs text-muted-foreground"
      >
        <span className="mt-0.5">
          <ConnectionStatusDot dotClassName="bg-muted-foreground/40" pingClassName={null} />
        </span>
        <p>
          <span className="font-medium text-foreground">{notice.title}.</span>{" "}
          {synced === "" ? "" : `Last synced ${synced}. `}
          {notice.detail}
        </p>
      </div>
    );
  });
}
