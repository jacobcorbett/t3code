import { offersLinkedEnvironments } from "@t3tools/client-runtime/link-request";
import { peerLinkHealth } from "@t3tools/client-runtime/state/peerLinks";
import { useFocusEffect } from "@react-navigation/native";
import {
  THREAD_MIRROR_NEEDS_LINKS,
  THREAD_MIRROR_SETTING_DESCRIPTION,
  THREAD_MIRROR_SETTING_TITLE,
  threadMirrorHostChoices,
} from "@t3tools/client-runtime/thread-mirror";
import {
  AuthAccessReadScope,
  AuthSettingsWriteScope,
  type EnvironmentId,
  isManagedSettingPath,
  type PeerLinkSummary,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { useCallback, useState } from "react";
import { AppState, View } from "react-native";

import { AppText as Text } from "../../components/ui/AppText";
import { SymbolView } from "../../components/ui/AppSymbol";
import { ListRowLayout } from "../../components/ui/ListRowLayout";
import { RowPressable } from "../../components/ui/RowPressable";
import { serverEnvironment } from "../../state/server";
import { useAtomCommand } from "../../state/use-atom-command";
import { useEnvironmentServerConfig } from "../../state/entities";
import { peerLinkEnvironment } from "../../state/peerLinks";
import { useEnvironmentQuery } from "../../state/query";
import { useEnvironmentScope } from "../../state/session";
import { SettingsSection } from "../settings/components/SettingsSection";

export function LinkedEnvironmentsSection(props: {
  readonly environmentId: EnvironmentId;
  readonly environmentLabel: string;
}) {
  const config = useEnvironmentServerConfig(props.environmentId);
  const canRead = useEnvironmentScope(props.environmentId, AuthAccessReadScope);
  const supported = canRead && offersLinkedEnvironments(config);
  const links = useEnvironmentQuery(
    supported ? peerLinkEnvironment.list({ environmentId: props.environmentId, input: {} }) : null,
  );
  const [nowMs, setNowMs] = useState(Date.now);
  const hasLinks = supported && (links.data?.links.length ?? 0) > 0;
  useFocusEffect(
    useCallback(() => {
      if (!hasLinks) return;
      const updateNow = () => setNowMs(Date.now());
      updateNow();
      const timer = setInterval(updateNow, 60_000);
      const subscription = AppState.addEventListener("change", (state) => {
        if (state === "active") updateNow();
      });
      return () => {
        clearInterval(timer);
        subscription.remove();
      };
    }, [hasLinks]),
  );
  if (!supported || links.error || !links.data) return null;
  const now = DateTime.makeUnsafe(nowMs);
  return (
    <>
      {links.data.links.length === 0 ? null : (
        <View className="mt-5 gap-3">
          <SettingsSection title={`Linked from ${props.environmentLabel}`}>
            {links.data.links.map((link) => {
              const health = peerLinkHealth(link, now);
              const status =
                health.kind === "expired"
                  ? "Expired. Link it again with a new pairing code."
                  : health.kind === "unreachable"
                    ? (health.detail ?? "Not answering right now.")
                    : health.kind === "failing"
                      ? health.detail
                      : health.expiresInDays !== null
                        ? `Expires in ${health.expiresInDays} ${health.expiresInDays === 1 ? "day" : "days"}`
                        : "Reachable";
              return (
                <View key={link.environmentId} className="gap-0.5 p-4">
                  <Text className="text-base font-t3-bold text-foreground">{link.label}</Text>
                  <Text
                    className={
                      health.kind === "reachable" && health.expiresInDays === null
                        ? "text-xs text-foreground-muted"
                        : "text-xs text-danger-foreground"
                    }
                    numberOfLines={2}
                  >
                    {status}
                  </Text>
                </View>
              );
            })}
          </SettingsSection>
        </View>
      )}
      <ThreadMirrorSection
        environmentId={props.environmentId}
        environmentLabel={props.environmentLabel}
        links={links.data.links}
      />
    </>
  );
}

const MIRROR_OFF = "off";

function ThreadMirrorSection(props: {
  readonly environmentId: EnvironmentId;
  readonly environmentLabel: string;
  readonly links: ReadonlyArray<PeerLinkSummary>;
}) {
  const config = useEnvironmentServerConfig(props.environmentId);
  const canWrite = useEnvironmentScope(props.environmentId, AuthSettingsWriteScope);
  const updateSettings = useAtomCommand(serverEnvironment.updateSettings, {
    label: "thread list copy",
    reportFailure: true,
  });
  const settings = config?.settings;
  // Older servers don't know the setting, so there is nothing to choose.
  if (settings === undefined || config?.environment.capabilities.threadMirror !== true) return null;
  const selectedId = settings.threadMirrorHostEnvironmentId;
  const { options, selectedMissing } = threadMirrorHostChoices({
    selectedId,
    links: props.links,
  });
  if (options.length === 0 && selectedId === null) return null;
  const linksOff = !settings.linkedEnvironmentsEnabled;
  const locked =
    !canWrite || isManagedSettingPath(config?.managedPolicy, "threadMirrorHostEnvironmentId");
  const choose = (value: string) =>
    void updateSettings({
      environmentId: props.environmentId,
      input: {
        patch: {
          threadMirrorHostEnvironmentId: value === MIRROR_OFF ? null : (value as EnvironmentId),
        },
      },
    });
  const rows = [
    { id: MIRROR_OFF, label: "Off", description: undefined, disabled: false },
    ...options.map((option) => ({
      id: option.environmentId as string,
      label: option.label,
      description: option.problem ?? undefined,
      disabled: linksOff || option.problem === "Link expired",
    })),
    ...(selectedMissing && selectedId !== null
      ? [
          {
            id: selectedId as string,
            label: "Unavailable machine",
            description: "No longer linked. Choose Off or another machine.",
            disabled: true,
          },
        ]
      : []),
  ];
  const current = selectedId ?? MIRROR_OFF;
  return (
    <View className="mt-5 gap-3">
      <SettingsSection title={`${THREAD_MIRROR_SETTING_TITLE} · ${props.environmentLabel}`}>
        {rows.map((row) => (
          <RowPressable
            key={row.id}
            accessibilityRole="radio"
            accessibilityState={{
              checked: row.id === current,
              disabled: locked || (row.disabled && row.id !== current),
            }}
            disabled={locked || row.disabled || row.id === current}
            onPress={() => choose(row.id)}
            className="bg-grouped-card"
          >
            <ListRowLayout
              label={row.label}
              {...(row.description === undefined ? {} : { description: row.description })}
              disabled={locked || row.disabled}
              accessory={
                row.id === current ? (
                  <SymbolView
                    name="checkmark"
                    size={16}
                    tintColorClassName="accent-icon"
                    type="monochrome"
                    weight="semibold"
                  />
                ) : undefined
              }
            />
          </RowPressable>
        ))}
      </SettingsSection>
      <Text className="px-2 text-sm text-foreground-muted">
        {THREAD_MIRROR_SETTING_DESCRIPTION}
        {linksOff && selectedId === null ? ` ${THREAD_MIRROR_NEEDS_LINKS}` : ""}
      </Text>
    </View>
  );
}
