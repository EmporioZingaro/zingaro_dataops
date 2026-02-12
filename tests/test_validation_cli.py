import json
import subprocess
import sys
from argparse import Namespace
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _run_cli(args):
    cmd = [sys.executable, "-m", "scripts.validation_cli", *args]
    return subprocess.run(cmd, cwd=REPO_ROOT, check=False, capture_output=True, text=True)


def test_validate_command_writes_reports(tmp_path):
    expected_path = tmp_path / "expected.json"
    stages_path = tmp_path / "stages.json"
    output_dir = tmp_path / "out"

    expected_path.write_text(
        json.dumps(
            [
                {
                    "store_prefix": "z316",
                    "uuid": "00000000-0000-0000-0000-000000000001",
                    "dados_id": "10",
                    "pedido_id": "501",
                    "event_timestamp": "2025-01-01T00:00:00Z",
                }
            ]
        ),
        encoding="utf-8",
    )

    stages_path.write_text(
        json.dumps(
            {
                "raw_pdv": [
                    {
                        "store_prefix": "z316",
                        "uuid": "00000000-0000-0000-0000-000000000001",
                        "dados_id": "10",
                        "pedido_id": "501",
                        "event_timestamp": "2025-01-01T00:00:00Z",
                        "exists": True,
                        "metadata": {},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    result = _run_cli(
        [
            "validate",
            "--expected-json",
            str(expected_path),
            "--stages-json",
            str(stages_path),
            "--output-dir",
            str(output_dir),
        ]
    )

    assert result.returncode == 0, result.stderr
    assert (output_dir / "uuid_propagation_report.json").exists()
    assert (output_dir / "pedido_completeness_report.json").exists()


def test_dedupe_report_command_writes_report(tmp_path):
    candidates_path = tmp_path / "candidates.json"
    output_dir = tmp_path / "out"

    candidates_path.write_text(
        json.dumps(
            [
                {
                    "record_id": "a",
                    "store_prefix": "z316",
                    "pedido_id": "501",
                    "stage_name": "points_sales",
                    "uuid": "00000000-0000-0000-0000-000000000001",
                    "event_timestamp": "2025-01-01T00:00:00Z",
                },
                {
                    "record_id": "b",
                    "store_prefix": "z316",
                    "pedido_id": "501",
                    "stage_name": "points_sales",
                    "uuid": "00000000-0000-0000-0000-000000000002",
                    "event_timestamp": "2025-01-02T00:00:00Z",
                },
            ]
        ),
        encoding="utf-8",
    )

    result = _run_cli(
        [
            "dedupe-report",
            "--candidates-json",
            str(candidates_path),
            "--stage-name",
            "points_sales",
            "--output-dir",
            str(output_dir),
        ]
    )

    assert result.returncode == 0, result.stderr
    report_path = output_dir / "dedupe_report_points_sales.json"
    assert report_path.exists()
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["duplicate_groups"] == 1


def test_validate_command_requires_input_files_in_file_mode(tmp_path):
    output_dir = tmp_path / "out"

    result = _run_cli(
        [
            "validate",
            "--output-dir",
            str(output_dir),
        ]
    )

    assert result.returncode != 0
    assert "--expected-json and --stages-json are required" in result.stderr


def test_validate_live_mode_requires_live_parameters(tmp_path):
    output_dir = tmp_path / "out"

    result = _run_cli(
        [
            "validate",
            "--mode",
            "live",
            "--output-dir",
            str(output_dir),
        ]
    )

    assert result.returncode != 0
    assert "--webhook-bucket is required in --mode live" in result.stderr


def test_remediate_http_webhook_backend_requires_webhook_url(tmp_path):
    expected_path = tmp_path / "expected.json"
    stages_path = tmp_path / "stages.json"
    output_dir = tmp_path / "out"

    expected_path.write_text("[]", encoding="utf-8")
    stages_path.write_text("{}", encoding="utf-8")

    result = _run_cli(
        [
            "remediate",
            "--expected-json",
            str(expected_path),
            "--stages-json",
            str(stages_path),
            "--remediation-backend",
            "http_webhook",
            "--output-dir",
            str(output_dir),
        ]
    )

    assert result.returncode != 0
    assert "--webhook-url is required when --remediation-backend=http_webhook" in result.stderr


def test_run_remediation_command_reloads_runtime_data_per_attempt():
    from scripts.validation_cli import _run_remediation_command
    from scripts.validation_identity import EventIdentity, StageRecord

    args = Namespace(
        remediation_backend="noop",
        webhook_url=None,
        webhook_timeout_s=2.0,
        max_retries=3,
        dry_run=False,
        retry_wait_seconds=0.0,
        remediate_stage=[],
        max_actions_per_attempt=None,
        stage_webhook_url=[],
    )

    calls = {"count": 0}

    def runtime_loader(_args):
        calls["count"] += 1
        expected = [
            EventIdentity(
                store_prefix="z316",
                uuid="00000000-0000-0000-0000-000000000001",
                dados_id="10",
                pedido_id="501",
                event_timestamp="2025-01-01T00:00:00Z",
            )
        ]
        if calls["count"] == 1:
            observed = {"raw_pdv": []}
        else:
            observed = {
                "raw_pdv": [
                    StageRecord(
                        identity=expected[0],
                        stage_name="raw_pdv",
                        exists=True,
                        metadata={},
                    )
                ]
            }
        return expected, observed, {"loader_calls": calls["count"]}

    result, diagnostics = _run_remediation_command(args=args, runtime_loader=runtime_loader)

    assert result.converged is True
    assert result.attempts_run == 2
    assert calls["count"] == 2
    assert len(diagnostics["attempt_diagnostics"]) == 2
    assert diagnostics["action_log"]


def test_run_remediation_command_filters_and_caps_actions():
    from scripts.validation_cli import _run_remediation_command
    from scripts.validation_identity import EventIdentity

    args = Namespace(
        remediation_backend="noop",
        webhook_url=None,
        webhook_timeout_s=2.0,
        max_retries=1,
        dry_run=False,
        retry_wait_seconds=0.0,
        remediate_stage=["raw_pdv"],
        max_actions_per_attempt=1,
        stage_webhook_url=[],
    )

    identity1 = EventIdentity(
        store_prefix="z316",
        uuid="00000000-0000-0000-0000-000000000001",
        dados_id="10",
        pedido_id="501",
        event_timestamp="2025-01-01T00:00:00Z",
    )
    identity2 = EventIdentity(
        store_prefix="z316",
        uuid="00000000-0000-0000-0000-000000000002",
        dados_id="11",
        pedido_id="502",
        event_timestamp="2025-01-01T00:00:00Z",
    )

    def runtime_loader(_args):
        expected = [identity1, identity2]
        observed = {"raw_pdv": []}
        return expected, observed, {}

    result, diagnostics = _run_remediation_command(args=args, runtime_loader=runtime_loader)

    assert result.attempt_results[0].actions_executed == 1
    assert result.attempt_results[0].actions_skipped >= 1
    assert diagnostics["allowed_stages"] == ["raw_pdv"]


def test_remediate_command_writes_manual_queue_and_verdict(tmp_path):
    expected_path = tmp_path / "expected.json"
    stages_path = tmp_path / "stages.json"
    output_dir = tmp_path / "out"

    expected_path.write_text(
        json.dumps([
            {
                "store_prefix": "z316",
                "uuid": "00000000-0000-0000-0000-000000000001",
                "dados_id": "10",
                "pedido_id": "501",
                "event_timestamp": "2025-01-01T00:00:00Z",
            }
        ]),
        encoding="utf-8",
    )
    stages_path.write_text(json.dumps({"raw_pdv": []}), encoding="utf-8")

    result = _run_cli([
        "remediate",
        "--expected-json",
        str(expected_path),
        "--stages-json",
        str(stages_path),
        "--max-retries",
        "1",
        "--output-dir",
        str(output_dir),
    ])

    assert result.returncode == 0, result.stderr
    assert (output_dir / "manual_intervention_queue.json").exists()
    assert (output_dir / "remediation_run_verdict.json").exists()
