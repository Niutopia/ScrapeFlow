"""Focused tests for the credential-safe recursive redaction projection.

The regression guard below exists because ``redact_value`` used to rescan the
process environment for every string in the tree, which made one full
``/api/jobs`` projection over legacy records cost tens of seconds.
"""

from __future__ import annotations

import unittest
from unittest import mock

from local.scrapeflow_api.redaction import (
    redact_error,
    redact_text,
    redact_value,
    runtime_secret_values,
)


class RedactionSinglePassTests(unittest.TestCase):
    def test_redact_value_resolves_secrets_once_per_tree(self) -> None:
        calls = {"count": 0}
        real = runtime_secret_values

        def counting() -> tuple[str, ...]:
            calls["count"] += 1
            return real()

        payload = {
            "items": [
                {"title": f"episode {index}", "note": "plain text"} for index in range(120)
            ],
            "nested": {"deep": [{"leaf": "value"}] * 40},
        }
        with mock.patch(
            "local.scrapeflow_api.redaction.runtime_secret_values",
            side_effect=counting,
        ):
            redact_value(payload)

        self.assertEqual(calls["count"], 1)

    def test_redact_value_still_removes_secrets_and_sensitive_keys(self) -> None:
        with mock.patch.dict(
            "os.environ",
            {"SCRAPEFLOW_FAKE_PASSWORD": "hunter2secret"},
            clear=False,
        ):
            output = redact_value({
                "token": "hunter2secret",
                "note": "see hunter2secret here",
                "items": [{"x": "hunter2secret"}],
            })
        self.assertEqual(output["token"], "<redacted>")
        self.assertEqual(output["note"], "see <redacted> here")
        self.assertEqual(output["items"][0]["x"], "<redacted>")

    def test_redact_text_accepts_precomputed_secrets(self) -> None:
        self.assertEqual(
            redact_text("use hunter2secret now", secrets=("hunter2secret",)),
            "use <redacted> now",
        )

    def test_redact_error_accepts_precomputed_secrets(self) -> None:
        error = ValueError("bad hunter2secret value")
        self.assertEqual(
            redact_error(error, secrets=("hunter2secret",)),
            "bad <redacted> value",
        )


if __name__ == "__main__":
    unittest.main()
