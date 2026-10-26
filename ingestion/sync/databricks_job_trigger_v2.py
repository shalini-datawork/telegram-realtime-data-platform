import json
import subprocess
import uuid

from pathlib import Path

from checkpoint_lock_v2 import checkpoint_lock


# ==================================================
# 1. CONFIGURATION
# ==================================================

ROOT_DIR = Path(__file__).resolve().parents[2]

JOB_ID = "921048933612524"
PROFILE = "TELEGRAM_PROJECT"

STATE_FILE = (
    ROOT_DIR
    / "data"
    / "checkpoints"
    / "databricks_job_trigger_v2.json"
)

TRIGGER_EXECUTION_LOCK = (
    ROOT_DIR
    / "data"
    / "checkpoints"
    / "databricks_trigger_execution_v2"
)


# ==================================================
# 2. DATABRICKS CLI
# ==================================================

def cli(*args):

    result = subprocess.run(
        [
            "databricks",
            *args,
            "--profile",
            PROFILE,
            "--output",
            "json",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )

    if result.returncode != 0:
        raise RuntimeError(
            result.stderr or result.stdout
        )

    return json.loads(result.stdout)


# ==================================================
# 3. CHECKPOINT MANAGEMENT
# ==================================================

def load_state():

    default = {
        "pending_files": [],
        "inflight_files": [],
        "completed_files": [],
        "trigger_token": None,
        "active_run_id": None,
        "last_successful_run_id": None,
        "retry_paused": False,
    }

    if not STATE_FILE.exists():
        return default

    state = json.loads(
        STATE_FILE.read_text(encoding="utf-8")
    )

    return {**default, **state}


def save_state(state):

    STATE_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp = STATE_FILE.with_suffix(".tmp")

    temp.write_text(
        json.dumps(state, indent=2),
        encoding="utf-8",
    )

    temp.replace(STATE_FILE)


# ==================================================
# 4. REGISTER NEW UPLOADS
# ==================================================

def register_uploaded_files(filenames):

    with checkpoint_lock(STATE_FILE):

        state = load_state()

        pending = set(state["pending_files"])
        inflight = set(state["inflight_files"])
        completed = set(state["completed_files"])

        pending.update(
            name
            for name in filenames
            if name not in inflight
            and name not in completed
        )

        state["pending_files"] = sorted(pending)

        save_state(state)


# ==================================================
# 5. CHECK ACTIVE JOB STATUS
# ==================================================

def check_active_run(state):

    run_id = state["active_run_id"]

    if not run_id:
        return load_state()

    # Do not hold the checkpoint lock
    # while contacting Databricks.
    run = cli(
        "jobs",
        "get-run",
        str(run_id),
    )

    run_state = run.get("state", {})

    lifecycle = run_state.get("life_cycle_state")
    result = run_state.get("result_state")

    terminal_states = {
        "TERMINATED",
        "SKIPPED",
        "INTERNAL_ERROR",
    }

    if lifecycle not in terminal_states:

        print(
            f"Databricks run {run_id} "
            f"still active: {lifecycle}"
        )

        return load_state()

    with checkpoint_lock(STATE_FILE):

        latest_state = load_state()

        # Avoid applying an outdated API response.
        if latest_state["active_run_id"] != run_id:
            return latest_state

        if result == "SUCCESS":

            print(
                f"Databricks run {run_id} SUCCESS"
            )

            completed = set(
                latest_state["completed_files"]
            )

            completed.update(
                latest_state["inflight_files"]
            )

            latest_state["completed_files"] = sorted(
                completed
            )

            latest_state["last_successful_run_id"] = run_id

        else:

            print(
                f"Databricks run {run_id} "
                f"FAILED: {result}"
            )

            pending = set(
                latest_state["pending_files"]
            )

            pending.update(
                latest_state["inflight_files"]
            )

            latest_state["pending_files"] = sorted(
                pending
            )

            # Manual review required before retry.
            latest_state["retry_paused"] = True

        latest_state["inflight_files"] = []
        latest_state["active_run_id"] = None
        latest_state["trigger_token"] = None

        save_state(latest_state)

        return latest_state


# ==================================================
# 6. PREPARE SUBMISSION ATOMICALLY
# ==================================================

def prepare_submission():

    with checkpoint_lock(STATE_FILE):

        state = load_state()

        if state["retry_paused"]:
            return state

        if state["active_run_id"]:
            return state

        # An interrupted submission already has
        # a durable token and inflight batch.
        if state["trigger_token"]:
            return state

        if not state["pending_files"]:
            return state

        state["trigger_token"] = str(uuid.uuid4())

        state["inflight_files"] = list(
            state["pending_files"]
        )

        state["pending_files"] = []

        save_state(state)

        return state


# ==================================================
# 7. PERSIST RETURNED RUN ID
# ==================================================

def persist_run_id(token, run_id):

    with checkpoint_lock(STATE_FILE):

        state = load_state()

        if state["trigger_token"] != token:
            raise RuntimeError(
                "Submission token changed unexpectedly"
            )

        if (
            state["active_run_id"] is not None
            and state["active_run_id"] != run_id
        ):
            raise RuntimeError(
                "Conflicting Databricks run ID"
            )

        state["active_run_id"] = run_id

        save_state(state)

        return state


# ==================================================
# 8. CHECK EXTERNAL ACTIVE RUNS
# ==================================================

def has_external_active_run():

    response = cli(
        "jobs",
        "list-runs",
        "--job-id",
        JOB_ID,
        "--active-only",
    )

    if isinstance(response, list):
        runs = response

    elif isinstance(response, dict):
        runs = response.get("runs", [])

    else:
        raise RuntimeError(
            "Unexpected Databricks list-runs response"
        )

    return bool(runs)


# ==================================================
# 9. TRIGGER DATABRICKS JOB
# ==================================================

def trigger_if_needed():

    # Only one cooperating trigger process
    # can execute this workflow at a time.
    with checkpoint_lock(TRIGGER_EXECUTION_LOCK):

        state = load_state()

        state = check_active_run(state)

        if state["retry_paused"]:

            print(
                "Databricks V2 automatic triggering "
                "is paused. Manual review required."
            )

            return

        if state["active_run_id"]:
            return

        if (
            not state["pending_files"]
            and not state["inflight_files"]
        ):

            print(
                "No pending files. Job trigger skipped."
            )

            return

        recovering_submission = bool(
            state["trigger_token"]
        )

        # Only check for external active runs
        # when preparing a brand-new submission.
        #
        # For interrupted submissions, retry
        # using the original idempotency token.
        if not recovering_submission:

            if has_external_active_run():

                print(
                    "Another Databricks V2 run "
                    "is active. Waiting."
                )

                return

        state = prepare_submission()

        if state["retry_paused"]:
            return

        if state["active_run_id"]:
            return

        token = state["trigger_token"]

        if not token:
            return

        if not state["inflight_files"]:
            raise RuntimeError(
                "Submission token exists without "
                "an inflight file batch"
            )

        result = cli(
            "jobs",
            "run-now",
            JOB_ID,
            "--idempotency-token",
            token,
            "--no-wait",
        )

        run_id = result.get("run_id")

        if not run_id:
            raise RuntimeError(
                "Databricks returned no run_id"
            )

        persist_run_id(token, run_id)

        print(
            f"Triggered Databricks V2 run: {run_id}"
        )
