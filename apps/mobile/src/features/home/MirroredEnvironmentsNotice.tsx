import { useAtomValue } from "@effect/atom-react";
import { threadMirrorNotice } from "@t3tools/client-runtime/thread-mirror";
import { View } from "react-native";

import { AppText as Text } from "../../components/ui/AppText";
import { environmentMirrorSummaryAtom } from "../../state/shell";

/** Saved lists shown for environments that are off, so none reads as live. */
export function MirroredEnvironmentsNotice() {
  const mirrored = useAtomValue(environmentMirrorSummaryAtom);
  return mirrored.map((entry) => {
    const notice = threadMirrorNotice(entry.sourceLabel);
    return (
      <View
        key={entry.environmentId}
        accessibilityRole="summary"
        className="mx-4 mb-2 gap-0.5 rounded-xl bg-subtle px-3 py-2"
      >
        <Text textRole="footnote" className="font-t3-medium text-foreground">
          {notice.title}
        </Text>
        <Text textRole="footnote" className="text-foreground-muted">
          {`Last synced ${new Date(entry.syncedAt).toLocaleString()}. ${notice.detail}`}
        </Text>
      </View>
    );
  });
}
