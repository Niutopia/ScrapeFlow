import importlib.util
import os
import sys
import tempfile
import unittest
from pathlib import Path


SERVER_PATH = Path(__file__).resolve().parents[1] / "server.py"
spec = importlib.util.spec_from_file_location("scrapeflow_local_server", SERVER_PATH)
assert spec and spec.loader
server = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = server
spec.loader.exec_module(server)


class LocalServerTests(unittest.TestCase):
    def test_normalize_remote_path(self):
        self.assertEqual(
            server.normalize_remote_input("http://127.0.0.1:5244/quark/%E5%BD%B1%E8%A7%86/%E7%95%AA%E5%89%A7"),
            "/quark/影视/番剧",
        )
        self.assertEqual(server.normalize_remote_input("/quark/影视/番剧/"), "/quark/影视/番剧")
        with self.assertRaises(ValueError):
            server.normalize_remote_input("quark/影视")
        with self.assertRaises(ValueError):
            server.normalize_remote_input("/quark/../影视")

    def test_default_parent(self):
        self.assertEqual(server.default_parent("/quark/影视/番剧/作品"), "/quark/影视/番剧")

    def test_digest_is_stable(self):
        self.assertEqual(server.canonical_digest({"b": 2, "a": 1}), server.canonical_digest({"a": 1, "b": 2}))

    def test_media_summary_never_exposes_more_than_250_files(self):
        plan = {
            "mode": "tv",
            "source_root": "/source",
            "target_root": "/target",
            "metadata": {"title": "Example", "tmdb_id": 1},
            "files": [
                {"source_path": f"/source/{index}.mkv", "target_dir": "/target", "final_name": f"S01E{index:03}.mkv"}
                for index in range(300)
            ],
        }
        summary = server.summarize_media_plan(plan)
        self.assertEqual(summary["file_count"], 300)
        self.assertEqual(len(summary["files"]), 250)
        self.assertTrue(summary["truncated"])

    def test_redacts_runtime_secrets(self):
        previous = os.environ.get("ALIST_PASSWORD")
        os.environ["ALIST_PASSWORD"] = "unit-test-secret"
        try:
            self.assertEqual(server.redact("value=unit-test-secret"), "value=[REDACTED]")
        finally:
            if previous is None:
                os.environ.pop("ALIST_PASSWORD", None)
            else:
                os.environ["ALIST_PASSWORD"] = previous


if __name__ == "__main__":
    unittest.main()
