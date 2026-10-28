import type { Dispatch, SetStateAction } from "react";
import { useTranslation } from "react-i18next";

import { useEditCompletion, type EditCompletion } from "@/components/settings/shared/useEditCompletion";
import { RestartSettingsFooter, SettingsGroup, SettingsRow } from "@/components/settings/shared/SettingsControls";
import { Input } from "@/components/ui/input";
import type { SettingsPayload, SpeechSynthesisSettingsUpdate } from "@/lib/types";

export const DEFAULT_SPEECH_SYNTHESIS_FORM: SpeechSynthesisSettingsUpdate = {
  enabled: false,
  apiBase: "",
  apiKey: "",
  model: "qwen-audio-3.1-tts-plus",
  voice: "",
};

export function speechSynthesisFormFromPayload(payload: SettingsPayload): SpeechSynthesisSettingsUpdate {
  const speech = payload.speech_synthesis;
  return speech ? {
    enabled: speech.enabled,
    apiBase: speech.api_base,
    apiKey: "",
    model: speech.model,
    voice: speech.voice,
  } : DEFAULT_SPEECH_SYNTHESIS_FORM;
}

export function speechSynthesisDraftState(settings: SettingsPayload, form: SpeechSynthesisSettingsUpdate) {
  const saved = speechSynthesisFormFromPayload(settings);
  return {
    dirty: form.enabled !== saved.enabled || form.apiBase !== saved.apiBase || form.apiKey !== ""
      || form.model !== saved.model || form.voice !== saved.voice,
    ready: !form.enabled || Boolean(form.apiBase.trim() && form.model.trim() && form.voice.trim()
      && (form.apiKey.trim() || settings.speech_synthesis?.api_key_configured)),
  };
}

export function SpeechSynthesisSettings({
  settings, form, dirty, saving, error, onChangeForm, onSave, editCompletion,
}: {
  editCompletion?: EditCompletion;
  settings: SettingsPayload;
  form: SpeechSynthesisSettingsUpdate;
  dirty: boolean;
  saving: boolean;
  error?: string;
  onChangeForm: Dispatch<SetStateAction<SpeechSynthesisSettingsUpdate>>;
  onSave: () => void;
}) {
  const { t } = useTranslation();
  const localEdits = useEditCompletion(form, dirty, onSave, !editCompletion);
  const edits = editCompletion ?? localEdits;
  const fields = [
    { name: "apiBase", label: t("settings.byok.apiBase"), placeholder: "https://…/api/v1" },
    { name: "apiKey", label: t("settings.byok.apiKey"), placeholder: t(settings.speech_synthesis?.api_key_configured
      ? "settings.byok.apiKeyConfiguredPlaceholder" : "settings.byok.apiKeyPlaceholder") },
    { name: "model", label: t("settings.rows.transcriptionModel") },
    { name: "voice", label: t("settings.speechSynthesis.voice") },
  ] as const;
  return (
    <section onChangeCapture={edits.onChangeCapture} onFocusCapture={edits.onFocusCapture} onBlurCapture={edits.onBlurCapture}>
      <p className="settings-list-inset pb-4 text-[13px] text-muted-foreground">{t("settings.speechSynthesis.help")}</p>
      <SettingsGroup>
        {fields.map(({ name, label, ...field }) => (
          <SettingsRow key={name} title={label}>
            <Input
              aria-label={label}
              type={name === "apiKey" ? "password" : "text"}
              autoComplete="off"
              value={form[name]}
              {...field}
              onChange={(event) => onChangeForm((prev) => ({ ...prev, [name]: event.target.value }))}
              className="h-9 w-full rounded-full text-end text-[13px]"
            />
          </SettingsRow>
        ))}
        <RestartSettingsFooter error={Boolean(error)} message={error} autoSave dirty={dirty}
          saving={saving} pendingRestart={false} onSave={onSave} />
      </SettingsGroup>
    </section>
  );
}
