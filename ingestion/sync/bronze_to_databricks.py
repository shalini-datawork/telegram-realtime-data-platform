
from pathlib import Path
import json
import subprocess
import time
import uuid

SOURCE = Path("data/bronze/kafka_messages_live")
DESTINATION = "dbfs:/Volumes/workspace/default/telegram_raw/bronze_kafka"
STATE_FILE = Path("data/checkpoints/databricks_uploads.json")

PROFILE = "TELEGRAM_PROJECT"
JOB_ID = 910754402805023

SYNC_INTERVAL = 60
MIN_FILE_AGE = 60


def cli(*args):
    result = subprocess.run(
        ["databricks", *args, "--profile", PROFILE],
        capture_output=True,
        text=True,
        timeout=120,
    )

    if result.returncode != 0:
        raise RuntimeError(result.stderr or result.stdout)

    return result.stdout


def load_state():
    state = {
        "uploaded": [],
        "upload_version": 0,
        "processed_version": 0,
        "active_run": None,
        "job_failed": False,
    }

    if not STATE_FILE.exists():
        return state

    saved = json.loads(STATE_FILE.read_text())

    # Migrate the original list-based checkpoint.
    if isinstance(saved, list):
        state["uploaded"] = saved
        return state

    # Migrate the old pending_job format safely.
    if "upload_version" not in saved:
        if saved.get("run_id") or saved.get("pending_job"):
            raise RuntimeError(
                "Old checkpoint has unfinished work. "
                "Resolve it before migration."
            )

        state["uploaded"] = saved.get("uploaded", [])
        state["job_failed"] = saved.get("job_failed", False)
        return state

    return {**state, **saved}


def save_state(state):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

    temp = STATE_FILE.with_suffix(".tmp")
    temp.write_text(json.dumps(state, indent=2))
    temp.replace(STATE_FILE)


def upload_new_files(state):
    uploaded = set(state["uploaded"])

    for file in sorted(SOURCE.glob("*.parquet")):
        if file.name in uploaded:
            continue

        if time.time() - file.stat().st_mtime < MIN_FILE_AGE:
            continue

        cli("fs", "cp", str(file), f"{DESTINATION}/{file.name}","--overwrite",)

        uploaded.add(file.name)
        state["uploaded"] = sorted(uploaded)
        state["upload_version"] += 1
        save_state(state)

        print(f"Uploaded: {file.name}", flush=True)


def submit_job(state):
    active = state["active_run"]

    output = cli(
        "jobs", "run-now", str(JOB_ID),
        "--no-wait",
        "--idempotency-token", active["token"],
        "--output", "json",
    )

    active["run_id"] = json.loads(output)["run_id"]
    save_state(state)

    print(f"Triggered Job: {active['run_id']}", flush=True)


def check_job(state):
    active = state["active_run"]

    if active is None:
        return

    if active["run_id"] is None:
        submit_job(state)
        return

    output = cli(
        "jobs", "get-run", str(active["run_id"]),
        "--output", "json",
    )

    status = json.loads(output).get("state", {})
    lifecycle = status.get("life_cycle_state")
    result = status.get("result_state")

    if lifecycle not in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
        print(f"Job {active['run_id']} still running", flush=True)
        return

    if result == "SUCCESS":
        state["processed_version"] = max(
            state["processed_version"],
            active["target_version"],
        )
        state["active_run"] = None
        save_state(state)

        print(f"Job {active['run_id']} succeeded", flush=True)

    else:
        state["job_failed"] = True
        save_state(state)
        print(f"Job {active['run_id']} failed: {result}", flush=True)


def trigger_job(state):
    if state["job_failed"] or state["active_run"] is not None:
        return

    if state["upload_version"] <= state["processed_version"]:
        return

    # Save the submission intent before calling Databricks.
    state["active_run"] = {
        "run_id": None,
        "target_version": state["upload_version"],
        "token": uuid.uuid4().hex,
    }
    save_state(state)

    submit_job(state)


def sync_once():
    state = load_state()
    check_job(state)
    upload_new_files(state)
    trigger_job(state)


if __name__ == "__main__":
    print("Starting automated Bronze → Databricks pipeline", flush=True)

    while True:
        try:
            sync_once()
        except Exception as error:
            print(f"Pipeline error: {error}", flush=True)

        time.sleep(SYNC_INTERVAL)
