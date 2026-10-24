
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[2] / "ingestion/sync"),
)

import bronze_to_databricks as sync


class TestSyncReliability(unittest.TestCase):

    def test_upload_during_active_job(self):
        # Job A covers upload version 2.
        state = {
            "uploaded": ["file_a.parquet"],
            "upload_version": 3,
            "processed_version": 1,
            "active_run": {
                "run_id": 12345,
                "target_version": 2,
                "token": "test-token",
            },
            "job_failed": False,
        }

        # Simulate Job A completing successfully.
        response = json.dumps({
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "SUCCESS",
            }
        })

        with patch.object(sync, "cli", return_value=response), \
             patch.object(sync, "save_state"):
            sync.check_job(state)

        # Job A must not mark version 3 as processed.
        self.assertEqual(state["processed_version"], 2)
        self.assertEqual(state["upload_version"], 3)
        self.assertIsNone(state["active_run"])
        self.assertFalse(state["job_failed"])

        # Version 3 must trigger Job B.
        new_run = json.dumps({"run_id": 67890})

        with patch.object(sync, "cli", return_value=new_run), \
             patch.object(sync, "save_state"):
            sync.trigger_job(state)

        self.assertEqual(state["active_run"]["run_id"], 67890)
        self.assertEqual(state["active_run"]["target_version"], 3)
        self.assertEqual(state["processed_version"], 2)

        print("PASS: New uploads remain pending after Job A.")


    def test_failed_job_pauses_automatic_triggering(self):
        state = {
            "uploaded": ["file_a.parquet"],
            "upload_version": 3,
            "processed_version": 2,
            "active_run": {
                "run_id": 12345,
                "target_version": 3,
                "token": "test-failure-token",
            },
            "job_failed": False,
        }

        # Simulate a failed Databricks Job.
        response = json.dumps({
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "FAILED",
            }
        })

        with patch.object(sync, "cli", return_value=response), \
             patch.object(sync, "save_state"):
            sync.check_job(state)

        self.assertTrue(state["job_failed"])
        self.assertEqual(state["processed_version"], 2)

        # A failed Job must not trigger another Job automatically.
        with patch.object(sync, "cli") as mock_cli, \
             patch.object(sync, "save_state"):
            sync.trigger_job(state)

        mock_cli.assert_not_called()

        print("PASS: Failed Job pauses automatic triggering.")


    def test_restart_recovers_active_job(self):
        from tempfile import TemporaryDirectory

        # Simulate a checkpoint saved before a restart.
        state = {
            "uploaded": ["file_a.parquet"],
            "upload_version": 3,
            "processed_version": 2,
            "active_run": {
                "run_id": 12345,
                "target_version": 3,
                "token": "restart-test-token",
            },
            "job_failed": False,
        }

        with TemporaryDirectory() as directory:
            checkpoint = Path(directory) / "state.json"
            checkpoint.write_text(json.dumps(state))

            # Restart: reload the saved checkpoint.
            with patch.object(sync, "STATE_FILE", checkpoint):
                recovered = sync.load_state()

                self.assertEqual(
                    recovered["active_run"]["run_id"], 12345
                )

                # Simulate the existing Job still running.
                response = json.dumps({
                    "state": {
                        "life_cycle_state": "RUNNING",
                    }
                })

                with patch.object(sync, "cli", return_value=response) as mock_cli:
                    sync.check_job(recovered)
                    sync.trigger_job(recovered)

                # Only the existing Job is checked.
                # No new Job should be submitted.
                self.assertEqual(mock_cli.call_count, 1)
                self.assertEqual(mock_cli.call_args.args[0:2],
                                 ("jobs", "get-run"))

                self.assertEqual(
                    recovered["active_run"]["run_id"], 12345
                )

        print("PASS: Restart recovers active Job without duplicate submission.")


    def test_upload_retry_after_checkpoint_failure(self):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as directory:
            source = Path(directory) / "bronze"
            source.mkdir()

            parquet_file = source / "test_file.parquet"
            parquet_file.write_bytes(b"test-data")

            state = {
                "uploaded": [],
                "upload_version": 0,
                "processed_version": 0,
                "active_run": None,
                "job_failed": False,
            }

            # Simulate a successful upload followed by
            # a failure while saving the checkpoint.
            with patch.object(sync, "SOURCE", source), \
                 patch.object(sync, "MIN_FILE_AGE", 0), \
                 patch.object(sync, "cli") as mock_cli, \
                 patch.object(
                     sync,
                     "save_state",
                     side_effect=OSError("Checkpoint write failed"),
                 ):
                with self.assertRaises(OSError):
                    sync.upload_new_files(state)

            # Simulate restarting from the old checkpoint.
            state = {
                "uploaded": [],
                "upload_version": 0,
                "processed_version": 0,
                "active_run": None,
                "job_failed": False,
            }

            # Retry must overwrite the same remote path.
            with patch.object(sync, "SOURCE", source), \
                 patch.object(sync, "MIN_FILE_AGE", 0), \
                 patch.object(sync, "cli") as mock_cli, \
                 patch.object(sync, "save_state"):
                sync.upload_new_files(state)

            self.assertEqual(mock_cli.call_count, 1)
            self.assertIn("--overwrite", mock_cli.call_args.args)
            self.assertEqual(state["upload_version"], 1)
            self.assertEqual(len(state["uploaded"]), 1)

        print("PASS: Interrupted upload can be retried safely.")


if __name__ == "__main__":
    unittest.main()
