"""Remediation policy matrix and payload helpers (Gap 3)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping

from scripts.validation_remediation import RemediationAction


@dataclass(frozen=True)
class RemediationPolicyDecision:
    category: str
    retry_strategy: str
    severity: str
    route_hint: str


def resolve_policy(action: RemediationAction) -> RemediationPolicyDecision:
    stage = action.target_stage

    if stage.startswith("tiny_api_"):
        return RemediationPolicyDecision(
            category="tiny_api_missing",
            retry_strategy="replay_source",
            severity="high",
            route_hint="tiny_pipeline_retrigger",
        )

    if stage.startswith("raw_"):
        return RemediationPolicyDecision(
            category="raw_missing",
            retry_strategy="pipeline_retrigger",
            severity="high",
            route_hint="raw_reingest",
        )

    if stage.startswith("transform_"):
        return RemediationPolicyDecision(
            category="transform_missing",
            retry_strategy="downstream_recompute",
            severity="medium",
            route_hint="transform_retrigger",
        )

    if stage.startswith("points_"):
        return RemediationPolicyDecision(
            category="points_missing",
            retry_strategy="points_recompute",
            severity="medium",
            route_hint="points_retrigger",
        )

    return RemediationPolicyDecision(
        category="generic_missing",
        retry_strategy="generic_retrigger",
        severity="medium",
        route_hint="default",
    )


def build_action_payload(
    *,
    action: RemediationAction,
    decision: RemediationPolicyDecision,
    identity_context: Mapping[str, Any] | None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "target_type": action.target_type,
        "target_key": action.target_key,
        "target_stage": action.target_stage,
        "reason": action.reason,
        "policy": {
            "category": decision.category,
            "retry_strategy": decision.retry_strategy,
            "severity": decision.severity,
            "route_hint": decision.route_hint,
        },
    }
    if identity_context:
        payload["identity_context"] = dict(identity_context)
    return payload
