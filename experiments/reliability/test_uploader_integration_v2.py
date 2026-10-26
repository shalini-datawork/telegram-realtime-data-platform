import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

sys.path.insert(
    0,
    str(ROOT / "ingestion" / "sync")
)

import bronze_to_databricks_v2 as uploader
import databricks_job_trigger_v2 as trigger


with tempfile.TemporaryDirectory() as directory:

    root = Path(directory)

    # Redirect production checkpoints.
    uploader.STATE_FILE = root / "uploads.json"

    uploader.REGISTRATION_STATE_FILE = (
        root / "registered.json"
    )

    trigger.STATE_FILE = root / "trigger.json"

    trigger.TRIGGER_EXECUTION_LOCK = (
        root / "trigger_execution"
    )

    # Simulate historical files plus two new uploads.
    uploaded_files = [
        "historical_file.parquet",
        "new_file_001.parquet",
        "new_file_002.parquet",
    ]

    uploader.STATE_FILE.write_text(
        json.dumps({
            "source": str(uploader.SOURCE),
            "destination": uploader.DESTINATION,
            "uploaded": uploaded_files,
        })
    )

    uploader.REGISTRATION_STATE_FILE.write_text(
        json.dumps({
            "registered": [
                "historical_file.parquet"
            ]
        })
    )

    # Mock uploader's Databricks operations.
    uploader.verify_destination = lambda: None

    # No actual files need uploading:
    # The checkpoint simulates successful uploads.
    uploader.upload_new_files = lambda state: None

    # Mock Databricks job operations.
    submissions = []

    def mock_cli(*args):

        if args[:2] == ("jobs", "list-runs"):
            return []

        if args[:2] == ("jobs", "run-now"):

            submissions.append(args)

            return {
                "run_id": 987654321
            }

        if args[:2] == ("jobs", "get-run"):

            return {
                "state": {
                    "life_cycle_state": "TERMINATED",
                    "result_state": "SUCCESS",
                }
            }

        raise AssertionError(
            f"Unexpected CLI call: {args}"
        )

    trigger.cli = mock_cli

    print("========== TEST 1: UPLOAD RECONCILIATION ==========")

    uploader.sync_once()

    state = trigger.load_state()

    assert len(submissions) == 1

    assert state["active_run_id"] == 987654321

    assert set(state["inflight_files"]) == {
        "new_file_001.parquet",
        "new_file_002.parquet",
    }

    print("PASS: New uploads registered and job triggered")

    print("\n========== TEST 2: JOB COMPLETION ==========")

    uploader.sync_once()

    state = trigger.load_state()

    assert state["active_run_id"] is None

    assert state["inflight_files"] == []

    assert set(state["completed_files"]) == {
        "new_file_001.parquet",
        "new_file_002.parquet",
    }

    assert len(submissions) == 1

    print("PASS: Job completion recorded")
    print("PASS: No duplicate submission")

    print("\n========== TEST 3: REGISTRATION CHECKPOINT ==========")

    registered = json.loads(
        uploader.REGISTRATION_STATE_FILE.read_text()
    )["registered"]

    assert len(registered) == 3

    print("PASS: Registration checkpoint synchronized")

print("\nALL UPLOADER INTEGRATION TESTS PASSED")
