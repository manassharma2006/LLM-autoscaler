import subprocess
import unittest
from unittest.mock import patch

import run_autoscaler


class AutoscalerControllerTests(unittest.TestCase):
    def test_running_workers_accepts_compose_json_array(self):
        result = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout='[{"State":"running"},{"State":"exited"}]',
            stderr="",
        )
        with patch("run_autoscaler.subprocess.run", return_value=result):
            self.assertEqual(run_autoscaler.get_running_workers(), 1)

    def test_scale_workers_retries_and_confirms_actual_replicas(self):
        failed = subprocess.CompletedProcess([], 1, "", "No such container")
        succeeded = subprocess.CompletedProcess([], 0, "", "")
        running = subprocess.CompletedProcess(
            [], 0, '{"State":"running"}\n{"State":"running"}', ""
        )
        with (
            patch(
                "run_autoscaler.subprocess.run",
                side_effect=[failed, succeeded, running],
            ),
            patch("run_autoscaler.time.sleep"),
        ):
            self.assertTrue(run_autoscaler.scale_workers(2))

    def test_scale_down_requires_no_queued_or_active_work(self):
        self.assertFalse(
            run_autoscaler.should_scale_down(
                {"queue_depth": 1, "active_requests": 0}, 1, 2
            )
        )
        self.assertFalse(
            run_autoscaler.should_scale_down(
                {"queue_depth": 0, "active_requests": 1}, 1, 2
            )
        )
        self.assertTrue(
            run_autoscaler.should_scale_down(
                {"queue_depth": 0, "active_requests": 0}, 1, 2
            )
        )


if __name__ == "__main__":
    unittest.main()
