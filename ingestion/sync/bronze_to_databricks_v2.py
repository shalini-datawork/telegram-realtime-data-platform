
import json
import subprocess
import time
from pathlib import Path
from databricks_job_trigger_v2 import (
    register_uploaded_files,
    trigger_if_needed,
)


# ==================================================
# 1. PROJECT CONFIGURATION
# ==================================================

ROOT_DIR = Path(__file__).resolve().parents[2]

SOURCE = (
    ROOT_DIR
    / "data"
    / "bronze"
    / "kafka_messages_v2"
)

DESTINATION = (
    "dbfs:/Volumes/workspace/default/"
    "telegram_raw_v2/bronze_kafka"
)

STATE_FILE = (
    ROOT_DIR
    / "data"
    / "checkpoints"
    / "databricks_uploads_v2.json"
)

PROFILE = "TELEGRAM_PROJECT"

REGISTRATION_STATE_FILE = (
    ROOT_DIR
    / "data"
    / "checkpoints"
    / "databricks_registered_uploads_v2.json"
)

SYNC_INTERVAL = 60
MIN_FILE_AGE = 60


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
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )

    if result.returncode != 0:
        raise RuntimeError(
            result.stderr or result.stdout
        )

    return result.stdout


# ==================================================
# 3. LOAD UPLOAD CHECKPOINT
# ==================================================

def load_state():

    default_state = {
        "source": str(SOURCE),
        "destination": DESTINATION,
        "uploaded": [],
    }

    if not STATE_FILE.exists():
        return default_state

    state = json.loads(
        STATE_FILE.read_text(encoding="utf-8")
    )

    if state.get("source") != str(SOURCE):
        raise ValueError(
            "V2 checkpoint source mismatch"
        )

    if state.get("destination") != DESTINATION:
        raise ValueError(
            "V2 checkpoint destination mismatch"
        )

    if not isinstance(state.get("uploaded"), list):
        raise ValueError(
            "Invalid V2 upload checkpoint"
        )

    return state


# ==================================================
# 4. SAVE UPLOAD CHECKPOINT
# ==================================================

def save_state(state):

    STATE_FILE.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    temp_file = STATE_FILE.with_suffix(".tmp")

    with temp_file.open(
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            state,
            file,
            indent=2
        )

        file.flush()

    temp_file.replace(STATE_FILE)

# ==================================================
# 5. RECONCILE UPLOADED FILES WITH JOB REGISTRATION
# ==================================================

def reconcile_uploaded_files():

    upload_state = load_state()

    if not REGISTRATION_STATE_FILE.exists():
        raise RuntimeError(
            "V2 registration checkpoint is missing. "
            "Refusing to re-register historical files."
        )

    registration_state = json.loads(
        REGISTRATION_STATE_FILE.read_text(
            encoding="utf-8"
        )
    )

    uploaded = set(upload_state["uploaded"])

    registered_files = registration_state.get(
        "registered"
    )

    if not isinstance(registered_files, list):
        raise ValueError(
            "Invalid V2 registration checkpoint"
        )

    registered = set(registered_files)

    if not registered.issubset(uploaded):
        raise ValueError(
            "Registration checkpoint contains "
            "unknown uploaded files"
        )

    unregistered = sorted(uploaded - registered)

    if not unregistered:
        print(
            "Upload registration checkpoint synchronized.",
            flush=True,
        )
        return

    # First persist files in the job-trigger checkpoint.
    register_uploaded_files(unregistered)

    # Only then mark registration as complete.
    registration_state["registered"] = sorted(
        registered | set(unregistered)
    )

    temp_file = REGISTRATION_STATE_FILE.with_suffix(
        ".tmp"
    )

    with temp_file.open(
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            registration_state,
            file,
            indent=2,
        )

        file.flush()

    temp_file.replace(REGISTRATION_STATE_FILE)

    print(
        f"Registered {len(unregistered)} "
        "uploaded files for Databricks processing.",
        flush=True,
    )

# ==================================================
# 6. VERIFY DATABRICKS DESTINATION
# ==================================================

def verify_destination():

    print(
        f"Checking destination: {DESTINATION}",
        flush=True
    )

    cli("fs", "ls", DESTINATION)

    print(
        "Databricks V2 destination verified.",
        flush=True
    )


# ==================================================
# 7. UPLOAD NEW BRONZE FILES
# ==================================================

def upload_new_files(state):

    uploaded = set(state["uploaded"])

    parquet_files = sorted(
        SOURCE.glob("*.parquet")
    )

    if not parquet_files:
        print(
            "No Bronze V2 Parquet files found.",
            flush=True
        )
        return

    new_uploads = 0

    for file in parquet_files:

        if file.name in uploaded:
            continue

        file_age = (
            time.time() - file.stat().st_mtime
        )

        if file_age < MIN_FILE_AGE:
            continue

        destination_file = (
            f"{DESTINATION}/{file.name}"
        )

        cli(
            "fs",
            "cp",
            str(file),
            destination_file,
            "--overwrite",
        )

        uploaded.add(file.name)

        state["uploaded"] = sorted(uploaded)

        save_state(state)

        new_uploads += 1

        print(
            f"Uploaded: {file.name}",
            flush=True
        )

    print(
        f"New files uploaded: {new_uploads}",
        flush=True
    )

    print(
        f"Total files tracked: "
        f"{len(state['uploaded'])}",
        flush=True
    )


# ==================================================
# 8. RUN ONE SYNCHRONIZATION
# ==================================================

def sync_once():

    state = load_state()

    # Verify Databricks destination.
    verify_destination()

    # Recover uploads from interrupted cycles.
    reconcile_uploaded_files()

    # Upload newly generated Parquet files.
    upload_new_files(state)

    # Register newly uploaded files.
    reconcile_uploaded_files()

    # Check existing jobs and submit new work if needed.
    trigger_if_needed()

# ==================================================
# 9. CONTINUOUS SYNCHRONIZATION
# ==================================================

if __name__ == "__main__":

    print("=" * 60)
    print("TELEGRAM BRONZE → DATABRICKS V2")
    print("=" * 60)

    print(f"Source: {SOURCE}")
    print(f"Destination: {DESTINATION}")
    print(f"Checkpoint: {STATE_FILE}")

    print("\nAutomatic Databricks job triggering: ENABLED")

    while True:

        try:
            sync_once()

        except KeyboardInterrupt:
            print(
                "\nStopping V2 synchronization..."
            )
            break

        except Exception as error:
            print(
                f"V2 synchronization error: {error}",
                flush=True
            )

        time.sleep(SYNC_INTERVAL)
