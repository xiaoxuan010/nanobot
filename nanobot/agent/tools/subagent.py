"""Tool for creating, messaging, cancelling, and inspecting session-owned tasks."""

# pyright: reportIncompatibleMethodOverride=false

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from nanobot.agent.subagent import SubagentControlError
from nanobot.agent.tools.base import Tool, ToolResult, tool_parameters
from nanobot.agent.tools.context import current_request_context, current_request_session_key
from nanobot.agent.tools.schema import (
    BooleanSchema,
    NumberSchema,
    StringSchema,
    tool_parameters_schema,
)
from nanobot.security.workspace_access import current_workspace_scope
from nanobot.session.manager import SessionPolicy

if TYPE_CHECKING:
    from nanobot.agent.model_runtime import ModelRuntimeResolver
    from nanobot.agent.subagent import SubagentManager
    from nanobot.agent.tools.context import ToolContext


@tool_parameters(tool_parameters_schema(
    action=StringSchema(
        "create starts a task; send queues a message; cancel requests an idempotent stop; "
        "check returns task status, receipts, and results.",
        enum=("create", "send", "cancel", "check"),
    ),
    task=StringSchema("Required for create: the task for the subagent to complete.", min_length=1),
    label=StringSchema("Display label for create."),
    model_preset=StringSchema(
        "For create: omit or null to use the configured subagent default; an empty string uses "
        "the parent model; otherwise use a preset name. Call check without task_id for available presets.",
        nullable=True,
    ),
    temperature=NumberSchema(
        description="Sampling temperature for create; inherits the selected model runtime if omitted.",
        minimum=0.0,
        maximum=2.0,
    ),
    wait=BooleanSchema(
        description=(
            "For create: true waits for the result; false runs in background with a completion notification. "
            "Temporary sessions and hosts unable to receive background results always wait."
        ),
        default=False,
    ),
    task_id=StringSchema(
        "Full task ID returned by create. Required for send/cancel; optional for check. "
        "Omit for check to list the current session's active and recently finished tasks.",
        min_length=1,
    ),
    message=StringSchema(
        "Required for send: text, at most 8192 UTF-8 bytes. Accepted means queued, not delivered. "
        "Delivery receipts mean injected into the task transcript, not that the task acted on the message.",
        max_length=8192,
    ),
    required=["action"],
))
class SubagentTool(Tool):
    """Manage the current session's private subagent tasks."""

    def __init__(self, manager: SubagentManager, model_runtime_resolver: ModelRuntimeResolver | None = None):
        self._manager = manager
        self._model_runtime_resolver = model_runtime_resolver

    @classmethod
    def create(cls, ctx: ToolContext) -> Tool:
        if ctx.subagent_manager is None:
            raise RuntimeError("SubagentTool requires an initialized subagent manager")
        return cls(ctx.subagent_manager, ctx.model_runtime_resolver)

    @property
    def name(self) -> str:
        return "subagent"

    @property
    def description(self) -> str:
        return (
            "Manage the current session's private subagent tasks. "
            "Background results arrive automatically; avoid repeated polling. "
            "Check with a known task_id to read older results. "
            "Check without task_id to see the configured subagent default and available model presets. "
            "Partial results or iteration limits do not mean completion."
        )

    @property
    def concurrency_safe(self) -> bool:
        return True

    def _model_options(self) -> dict[str, object]:
        resolver = self._model_runtime_resolver
        presets: list[dict[str, str]] = []
        if resolver is not None:
            presets = [
                {"name": name, "model": preset.model, "provider": preset.provider}
                for name, preset in resolver.model_presets.items()
            ]
        return {
            "default_model_preset": resolver.subagent_model_preset if resolver is not None else None,
            "model_presets": presets,
        }

    def _model_selection_hint(self) -> str:
        return (
            "For model_preset: omit or null uses the configured subagent default; "
            '"" uses the parent model; a preset name selects that preset. '
            "Available selections: " + json.dumps(self._model_options(), ensure_ascii=False)
        )

    def cast_params(self, params: dict[str, Any]) -> dict[str, Any]:
        casted = super().cast_params(params)
        if "model_preset" in params:
            casted["model_preset"] = params["model_preset"]
        return casted

    def validate_params(self, params: dict[str, Any]) -> list[str]:
        errors = super().validate_params(params)
        if any(error.startswith("model_preset ") for error in errors):
            errors.append(self._model_selection_hint())
        return errors

    async def execute(
        self, action: str, task_id: str | None = None, message: str | None = None,
        task: str | None = None, label: str | None = None,
        temperature: float | None = None, wait: bool = False,
        model_preset: str | None = None, **kwargs: Any,
    ) -> str:
        if action == "create":
            if not task or not task.strip():
                return ToolResult.error("Error: create requires a non-empty task")
            return await self._create_task(task, label, temperature, wait, model_preset)
        owner = current_request_session_key()
        try:
            if action == "check":
                if not owner:
                    raise SubagentControlError("task unavailable")
                if task_id is not None:
                    return json.dumps(self._manager.check(task_id, owner).as_dict(), ensure_ascii=False)
                return json.dumps({
                    **self._model_options(),
                    "tasks": [status.as_dict()
                              for status in self._manager.statuses_for_session(owner, include_history=False).values()],
                }, ensure_ascii=False)
            if action in {"send", "cancel"}:
                if not task_id:
                    return ToolResult.error(f"Error: {action} requires task_id")
                if action == "send":
                    return json.dumps(await self._manager.send(task_id, owner, message))
                status = await self._manager.cancel(task_id, owner)
                return json.dumps(status.as_dict(), ensure_ascii=False)
            return ToolResult.error("Error: unknown subagent action")
        except SubagentControlError as exc:
            return ToolResult.error(f"Error: {exc}")

    async def _create_task(
        self, task: str, label: str | None, temperature: float | None, wait: bool,
        model_preset: str | None,
    ) -> str:
        request_ctx = current_request_context()
        if request_ctx is None or request_ctx.runtime is None:
            return ToolResult.error("Error: create requires an active model runtime")
        resolver = self._model_runtime_resolver
        selected = resolver.subagent_model_preset if model_preset is None and resolver is not None else model_preset
        runtime = request_ctx.runtime
        if selected is not None and selected != "":
            try:
                if resolver is None:
                    raise ValueError("No model presets are configured")
                runtime = resolver.resolve_preset(selected)
            except (KeyError, ValueError) as exc:
                return ToolResult.error(f"Error: {exc}. {self._model_selection_hint()}")
        origin_channel = request_ctx.channel
        origin_chat_id = request_ctx.chat_id
        session_key = request_ctx.session_key or (
            f"{origin_channel}:{origin_chat_id}" if origin_channel and origin_chat_id else None
        )
        if not session_key:
            return ToolResult.error("Error: create requires an active session identity")
        inline = wait or not request_ctx.can_receive_background_results or not request_ctx.persist_session
        method = self._manager.run_inline if inline else self._manager.spawn
        return await method(
            task=task,
            runtime=runtime,
            label=label,
            origin_channel=origin_channel,
            origin_chat_id=origin_chat_id,
            session_key=session_key,
            origin_message_id=request_ctx.message_id,
            origin_turn_id=request_ctx.turn_id,
            temperature=temperature,
            workspace_scope=current_workspace_scope(),
            session_policy=SessionPolicy(
                persist=request_ctx.persist_session,
                log_content=request_ctx.log_content,
            ),
        )
