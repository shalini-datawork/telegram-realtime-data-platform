import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

sys.path.insert(
    0,
    str(ROOT / "ingestion" / "sync")
)

import databricks_job_trigger_v2 as trigger


with tempfile.TemporaryDirectory() as directory:

    root = Path(directory)

    trigger.STATE_FILE = root / "trigger.json"

    trigger.TRIGGER_EXECUTION_LOCK = (
        root / "trigger_execution"
    )

    state = trigger.load_state()

    state["pending_files"] = [
        "file_001.parquet",
        "file_002.parquet",
    ]

    trigger.save_state(state)

    accepted_tokens = {}
    submissions = []

    def mock_cli(*args):

        if args[:2] == ("jobs", "list-runs"):
            return []

        if args[:2] == ("jobs", "run-now"):

            token = args[
                args.index("--idempotency-token") + 1
            ]

            submissions.append(token)

            if token not in accepted_tokens:

                accepted_tokens[token] = 987654321

                raise ConnectionError(
                    "Simulated lost submission response"
                )

            return {
                "run_id": accepted_tokens[token]
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

    print("========== TEST 1: INTERRUPTED SUBMISSION ==========")

    try:
        trigger.trigger_if_needed()

    except ConnectionError:
        print("Expected submission interruption")

    state = trigger.load_state()

    assert len(state["inflight_files"]) == 2
    assert state["pending_files"] == []
    assert state["trigger_token"] is not None

    original_token = state["trigger_token"]

    print("PASS: Submission batch preserved")

    print("\n========== TEST 2: NEW FILE REGISTRATION ==========")

    trigger.register_uploaded_files(
        ["file_003.parquet"]
    )

    state = trigger.load_state()

    assert state["pending_files"] == [
        "file_003.parquet"
    ]

    print("PASS: New file preserved")

    print("\n========== TEST 3: SUBMISSION RECOVERY ==========")

    trigger.trigger_if_needed()

    state = trigger.load_state()

    assert state["active_run_id"] == 987654321
    assert state["trigger_token"] == original_token
    assert len(state["inflight_files"]) == 2
    assert state["pending_files"] == [
        "file_003.parquet"
    ]

    assert len(accepted_tokens) == 1

    print("PASS: Original job recovered")
    print("PASS: New pending file preserved")
    print("PASS: No duplicate accepted job")

    print("\n========== TEST 4: SUCCESSFUL COMPLETION ==========")

    state = trigger.check_active_run(
        trigger.load_state()
    )

    assert state["active_run_id"] is None
    assert state["inflight_files"] == []

    assert set(state["completed_files"]) == {
        "file_001.parquet",
        "file_002.parquet",
    }

    assert state["pending_files"] == [
        "file_003.parquet"
    ]

    print("PASS: Completed files recorded")
    print("PASS: Pending files preserved")

    print("\n========== TEST 5: DUPLICATE REGISTRATION ==========")

    trigger.register_uploaded_files([
        "file_001.parquet",
        "file_003.parquet",
    ])

    state = trigger.load_state()

    assert state["pending_files"] == [
        "file_003.parquet"
    ]

    print("PASS: Completed files not re-registered")

print("\nALL TRIGGER HARDENING TESTS PASSED")
