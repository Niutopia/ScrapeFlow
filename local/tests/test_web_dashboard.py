"""Surfaces the console owes the operator: wired endpoints and panels.

The 09-07 web/backend divergence audit found seven operator capabilities
with a backend but no console entry.  These regressions pin the wiring so
the surfaces cannot silently regress to curl-only again.
"""

from __future__ import annotations

import unittest

from local.web_dashboard import dashboard_html


class OperatorSurfaceTest(unittest.TestCase):
    def _html(self) -> str:
        return dashboard_html().decode("utf-8")

    def test_ops_endpoints_have_buttons(self) -> None:
        html = self._html()
        for action in (
            "rebuild-uncertain",
            "rebuild-boundaries",
            "reopen-orphan",
            "repair-artifacts",
        ):
            self.assertIn(f'data-action="{action}"', html)
        # The global orphan-selection clear rides the topbar button.
        self.assertIn('id="orphanButton"', html)
        self.assertIn("/api/control/clear-orphan-selection", html)

    def test_disc_ruling_form_targets_the_endpoint(self) -> None:
        html = self._html()
        self.assertIn("data-ruling-form", html)
        self.assertIn("/file-disc-ruling", html)
        # The ruling may explicitly skip bonus playlists (946b7a9).
        self.assertIn("data-ruling-skipped", html)

    def test_browse_panel_consumes_the_read_only_endpoint(self) -> None:
        html = self._html()
        self.assertIn('data-view="browse"', html)
        self.assertIn('id="browseBoard"', html)
        self.assertIn("/api/browse", html)
        # Quick links cover the four shelves and the three staging roots.
        for label in ("待刮削", "展开", "补源", "归档"):
            self.assertIn(label, html)


if __name__ == "__main__":
    unittest.main()
