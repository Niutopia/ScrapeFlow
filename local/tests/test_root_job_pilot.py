"""Pure fail-closed rules for the durable/Compose RootJob intersection."""

from __future__ import annotations

import unittest

from local.scrapeflow_api.root_job_pilot import (
    ROOT_JOB_PILOT_ENV,
    RootJobPilotError,
    disabled_scope,
    environment_root_job_pilot,
    root_job_allowed,
    single_root_scope,
)


class RootJobPilotTests(unittest.TestCase):
    def test_environment_ceiling_intersects_instead_of_widening_scope(self) -> None:
        scope = single_root_scope("root-a")

        self.assertTrue(
            root_job_allowed(scope, "root-a", environment_root_job_id="root-a"),
        )
        self.assertFalse(
            root_job_allowed(scope, "root-a", environment_root_job_id="root-b"),
        )
        self.assertFalse(
            root_job_allowed(scope, "root-b", environment_root_job_id="root-a"),
        )
        self.assertFalse(root_job_allowed(disabled_scope(), "root-a"))

    def test_blank_environment_ceiling_is_absent_and_malformed_value_fails_closed(self) -> None:
        self.assertIsNone(environment_root_job_pilot({ROOT_JOB_PILOT_ENV: "  "}))
        self.assertEqual(
            environment_root_job_pilot({ROOT_JOB_PILOT_ENV: "root-a"}),
            "root-a",
        )
        with self.assertRaises(RootJobPilotError):
            environment_root_job_pilot({ROOT_JOB_PILOT_ENV: "../all-roots"})


if __name__ == "__main__":
    unittest.main()
