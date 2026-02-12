import sys
from pathlib import Path

repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from scripts.validation_remediation import (
    RemediationAction,
    plan_remediation_actions,
    run_remediation_loop,
)
from scripts.validation_reconciliation import (
    PedidoCompletenessEntry,
    PedidoCompletenessReport,
    UUIDPropagationEntry,
    UUIDPropagationReport,
)


def _reports(uuid_missing, pedido_missing):
    return (
        UUIDPropagationReport(
            entries=[
                UUIDPropagationEntry(
                    store_prefix="z316",
                    uuid="00000000-0000-0000-0000-000000000001",
                    dados_id="1",
                    pedido_id="10",
                    event_timestamp="2025-02-11T01:02:03Z",
                    stage_presence={"raw": False},
                )
            ],
            missing_by_stage={"raw": list(uuid_missing)},
            uuid_orphans_by_stage={"raw": []},
        ),
        PedidoCompletenessReport(
            entries=[
                PedidoCompletenessEntry(
                    store_prefix="z316",
                    pedido_id="10",
                    expected_count=1,
                    stage_counts={"raw": 0},
                )
            ],
            missing_pedidos_by_stage={"raw": list(pedido_missing)},
            overrepresented_pedidos_by_stage={"raw": []},
        ),
    )


def test_plan_remediation_actions_is_deterministic():
    actions = plan_remediation_actions(
        {"raw": {"z316|uuid-2", "z316|uuid-1"}},
        {"points": {"z316|pedido-1"}},
    )

    assert [a.target_key for a in actions] == [
        "z316|uuid-1",
        "z316|uuid-2",
        "z316|pedido-1",
    ]
    assert [a.target_type for a in actions] == ["uuid", "uuid", "pedido"]


def test_run_remediation_loop_converges_in_dry_run_with_revalidation():
    calls = {"n": 0}

    def validate_func(prev_uuid, prev_pedido):
        calls["n"] += 1
        if calls["n"] == 1:
            return _reports({"z316|uuid-1"}, {"z316|pedido-1"})
        return _reports(set(), set())

    executed_actions = []

    def execute_action(action: RemediationAction):
        executed_actions.append(action)

    result = run_remediation_loop(
        validate_func=validate_func,
        execute_action_func=execute_action,
        max_attempts=3,
        dry_run=True,
    )

    assert result.converged is True
    assert result.attempts_run == 2
    assert result.attempt_results[0].actions_planned == 2
    assert result.attempt_results[0].actions_executed == 0
    assert result.attempt_results[1].actions_planned == 0
    assert executed_actions == []


def test_run_remediation_loop_executes_actions_when_not_dry_run():
    calls = {"n": 0}

    def validate_func(prev_uuid, prev_pedido):
        calls["n"] += 1
        if calls["n"] == 1:
            return _reports({"z316|uuid-1"}, set())
        return _reports(set(), set())

    executed_actions = []

    def execute_action(action: RemediationAction):
        executed_actions.append(action)

    result = run_remediation_loop(
        validate_func=validate_func,
        execute_action_func=execute_action,
        max_attempts=2,
        dry_run=False,
    )

    assert result.converged is True
    assert result.attempt_results[0].actions_executed == 1
    assert len(executed_actions) == 1
    assert executed_actions[0].target_type == "uuid"


def test_run_remediation_loop_respects_max_attempts_and_returns_unresolved():
    def validate_func(prev_uuid, prev_pedido):
        return _reports({"z316|uuid-1"}, {"z316|pedido-1"})

    def execute_action(action: RemediationAction):
        return None

    result = run_remediation_loop(
        validate_func=validate_func,
        execute_action_func=execute_action,
        max_attempts=2,
        dry_run=True,
    )

    assert result.converged is False
    assert result.attempts_run == 2
    assert result.final_unresolved_uuids["raw"] == ["z316|uuid-1"]
    assert result.final_unresolved_pedidos["raw"] == ["z316|pedido-1"]


def test_run_remediation_loop_waits_between_attempts():
    from scripts.validation_identity import EventIdentity, StageRecord
    from scripts.validation_reconciliation import build_dual_reconciliation_reports

    identity = EventIdentity(
        store_prefix="z316",
        uuid="00000000-0000-0000-0000-000000000001",
        dados_id="100",
        pedido_id="500",
        event_timestamp="2025-01-01T00:00:00Z",
    )
    expected = [identity]

    calls = {"count": 0}

    def validate_func(_prev_uuid, _prev_pedido):
        calls["count"] += 1
        if calls["count"] == 1:
            observed = {"raw": []}
        else:
            observed = {"raw": [StageRecord(identity=identity, stage_name="raw", exists=True, metadata={})]}
        return build_dual_reconciliation_reports(expected, observed)

    slept = []

    def fake_sleep(seconds: float):
        slept.append(seconds)

    result = run_remediation_loop(
        validate_func=validate_func,
        execute_action_func=lambda _action: None,
        max_attempts=3,
        dry_run=True,
        wait_seconds=1.5,
        sleep_func=fake_sleep,
    )

    assert result.converged is True
    assert slept == [1.5]


def test_run_remediation_loop_applies_action_filter_and_cap():
    calls = {"n": 0}

    def validate_func(_prev_uuid, _prev_pedido):
        calls["n"] += 1
        if calls["n"] == 1:
            return _reports({"z316|uuid-1", "z316|uuid-2"}, {"z316|pedido-1"})
        return _reports(set(), set())

    executed = []

    def execute_action(action: RemediationAction):
        executed.append(action)

    result = run_remediation_loop(
        validate_func=validate_func,
        execute_action_func=execute_action,
        max_attempts=2,
        dry_run=False,
        action_filter_func=lambda action: action.target_type == "uuid",
        max_actions_per_attempt=1,
    )

    assert result.converged is True
    assert len(executed) == 1
    assert result.attempt_results[0].actions_planned == 1
    assert result.attempt_results[0].actions_skipped == 2
