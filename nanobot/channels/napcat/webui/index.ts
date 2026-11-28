import type { ChannelUiContribution } from "@/channel-plugins/types";
import { chatAppGuideUrl } from "@/channel-plugins/catalog";

export default {
  presentation: {
    displayName: "NapCat",
    initials: "NC",
    color: "#F97316",
    setup: {
      mode: "credentials",
      docsUrl: chatAppGuideUrl("napcat"),
      fields: [
        { key: "channels.napcat.wsUrl", section: "connection" },
        { key: "channels.napcat.accessToken", section: "credentials" },
        { key: "channels.napcat.groupPolicy", section: "behavior" },
        { key: "channels.napcat.groupAgent.enabled", section: "behavior" },
        { key: "channels.napcat.groupAgent.groups", section: "behavior" },
        { key: "channels.napcat.groupAgent.batchSeconds", section: "behavior" },
        { key: "channels.napcat.groupAgent.directedBatchSeconds", section: "behavior" },
        { key: "channels.napcat.allowFrom", section: "access" },
        { key: "channels.napcat.allowGroups", section: "access" },
      ],
    },
  },
} satisfies ChannelUiContribution;
