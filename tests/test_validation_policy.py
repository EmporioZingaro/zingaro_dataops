import sys
from pathlib import Path

repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from scripts.validation_policy import build_action_payload, resolve_policy
from scripts.validation_remediation import RemediationAction


def test_resolve_policy_by_stage():
    raw = resolve_policy(RemediationAction("uuid", "k", "raw_pdv", "r"))
    points = resolve_policy(RemediationAction("pedido", "k", "points_sales", "r"))

    assert raw.category == "raw_missing"
    assert raw.retry_strategy == "pipeline_retrigger"
    assert points.category == "points_missing"


def test_build_action_payload_includes_policy_and_context():
    action = RemediationAction("uuid", "z316|u1", "raw_pdv", "missing")
    decision = resolve_policy(action)
    payload = build_action_payload(
        action=action,
        decision=decision,
        identity_context={"uuid": "u1", "pedido_id": "p1"},
    )

    assert payload["target_key"] == "z316|u1"
    assert payload["policy"]["category"] == "raw_missing"
    assert payload["identity_context"]["pedido_id"] == "p1"
