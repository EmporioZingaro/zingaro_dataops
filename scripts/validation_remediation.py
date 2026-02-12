"""Targeted remediation engine (Chunk 4).

Implements dry-run capable, bounded retry remediation with incremental revalidation
of unresolved UUID/pedido deltas.
"""

from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from scripts.validation_reconciliation import (
    PedidoCompletenessReport,
    UUIDPropagationReport,
)


@dataclass(frozen=True)
class RemediationAction:
    target_type: str  # "uuid" or "pedido"
    target_key: str
    target_stage: str
    reason: str


@dataclass(frozen=True)
class AttemptResult:
    attempt: int
    actions_planned: int
    actions_executed: int
    actions_skipped: int
    unresolved_uuid_count: int
    unresolved_pedido_count: int


@dataclass(frozen=True)
class RemediationRunResult:
    converged: bool
    attempts_run: int
    attempt_results: List[AttemptResult]
    final_unresolved_uuids: Dict[str, List[str]]
    final_unresolved_pedidos: Dict[str, List[str]]


def unresolved_uuid_by_stage(report: UUIDPropagationReport) -> Dict[str, Set[str]]:
    return {stage: set(values) for stage, values in report.missing_by_stage.items()}


def unresolved_pedido_by_stage(report: PedidoCompletenessReport) -> Dict[str, Set[str]]:
    return {stage: set(values) for stage, values in report.missing_pedidos_by_stage.items()}


def plan_remediation_actions(
    uuid_unresolved: Mapping[str, Set[str]],
    pedido_unresolved: Mapping[str, Set[str]],
) -> List[RemediationAction]:
    """Create deterministic remediation action plan for unresolved items."""

    actions: List[RemediationAction] = []

    for stage, keys in sorted(uuid_unresolved.items()):
        for key in sorted(keys):
            actions.append(
                RemediationAction(
                    target_type="uuid",
                    target_key=key,
                    target_stage=stage,
                    reason=f"missing_in_stage:{stage}",
                )
            )

    for stage, keys in sorted(pedido_unresolved.items()):
        for key in sorted(keys):
            actions.append(
                RemediationAction(
                    target_type="pedido",
                    target_key=key,
                    target_stage=stage,
                    reason=f"missing_in_stage:{stage}",
                )
            )

    return actions


def _delta_only(
    current: Mapping[str, Set[str]],
    previous: Optional[Mapping[str, Set[str]]],
) -> Dict[str, Set[str]]:
    if previous is None:
        return {stage: set(keys) for stage, keys in current.items()}

    delta: Dict[str, Set[str]] = {}
    for stage, keys in current.items():
        prev_keys = previous.get(stage, set())
        unresolved_now = set(keys)
        # Re-run for still unresolved keys from previous pass only.
        delta[stage] = unresolved_now & set(prev_keys)
    return delta


def _count_unresolved(mapping: Mapping[str, Set[str]]) -> int:
    return sum(len(values) for values in mapping.values())


def run_remediation_loop(
    *,
    validate_func: Callable[[Optional[Mapping[str, Set[str]]], Optional[Mapping[str, Set[str]]]], Tuple[UUIDPropagationReport, PedidoCompletenessReport]],
    execute_action_func: Callable[[RemediationAction], None],
    max_attempts: int = 3,
    dry_run: bool = True,
    wait_seconds: float = 0.0,
    sleep_func: Callable[[float], None] = time.sleep,
    action_filter_func: Optional[Callable[[RemediationAction], bool]] = None,
    max_actions_per_attempt: Optional[int] = None,
) -> RemediationRunResult:
    """Run bounded remediation/revalidation loop.

    - `validate_func` must return fresh reconciliation reports.
    - On attempt 1, full unresolved set is planned.
    - On subsequent attempts, only unresolved deltas are planned/executed.
    """

    previous_uuid_unresolved: Optional[Dict[str, Set[str]]] = None
    previous_pedido_unresolved: Optional[Dict[str, Set[str]]] = None

    attempt_results: List[AttemptResult] = []

    final_uuid_unresolved: Dict[str, Set[str]] = {}
    final_pedido_unresolved: Dict[str, Set[str]] = {}

    for attempt in range(1, max_attempts + 1):
        uuid_report, pedido_report = validate_func(previous_uuid_unresolved, previous_pedido_unresolved)
        current_uuid_unresolved = unresolved_uuid_by_stage(uuid_report)
        current_pedido_unresolved = unresolved_pedido_by_stage(pedido_report)

        final_uuid_unresolved = current_uuid_unresolved
        final_pedido_unresolved = current_pedido_unresolved

        if _count_unresolved(current_uuid_unresolved) == 0 and _count_unresolved(current_pedido_unresolved) == 0:
            attempt_results.append(
                AttemptResult(
                    attempt=attempt,
                    actions_planned=0,
                    actions_executed=0,
                    actions_skipped=0,
                    unresolved_uuid_count=0,
                    unresolved_pedido_count=0,
                )
            )
            return RemediationRunResult(
                converged=True,
                attempts_run=attempt,
                attempt_results=attempt_results,
                final_unresolved_uuids={k: sorted(v) for k, v in current_uuid_unresolved.items()},
                final_unresolved_pedidos={k: sorted(v) for k, v in current_pedido_unresolved.items()},
            )

        uuid_targets = _delta_only(current_uuid_unresolved, previous_uuid_unresolved)
        pedido_targets = _delta_only(current_pedido_unresolved, previous_pedido_unresolved)

        actions = plan_remediation_actions(uuid_targets, pedido_targets)

        if action_filter_func is not None:
            filtered_actions = [action for action in actions if action_filter_func(action)]
        else:
            filtered_actions = actions

        if max_actions_per_attempt is not None and max_actions_per_attempt >= 0:
            selected_actions = filtered_actions[:max_actions_per_attempt]
        else:
            selected_actions = filtered_actions

        skipped = len(actions) - len(selected_actions)

        executed = 0
        if not dry_run:
            for action in selected_actions:
                execute_action_func(action)
                executed += 1

        attempt_results.append(
            AttemptResult(
                attempt=attempt,
                actions_planned=len(selected_actions),
                actions_executed=executed,
                actions_skipped=skipped,
                unresolved_uuid_count=_count_unresolved(current_uuid_unresolved),
                unresolved_pedido_count=_count_unresolved(current_pedido_unresolved),
            )
        )

        previous_uuid_unresolved = current_uuid_unresolved
        previous_pedido_unresolved = current_pedido_unresolved

        if wait_seconds > 0 and attempt < max_attempts:
            sleep_func(wait_seconds)

    return RemediationRunResult(
        converged=False,
        attempts_run=max_attempts,
        attempt_results=attempt_results,
        final_unresolved_uuids={k: sorted(v) for k, v in final_uuid_unresolved.items()},
        final_unresolved_pedidos={k: sorted(v) for k, v in final_pedido_unresolved.items()},
    )
