import json
from pathlib import Path
import tempfile
import unittest

from engine.scrapeflow.archive import ArchiveLimits
from engine.scrapeflow.archive_preprocessing import (
    ArchivePreprocessingAdapter,
    ArchivePreprocessingError,
    prepare_ordinary_archive,
    prepare_provider_archive,
)

from local.tests.test_archive_domain import FakeRunner


class RemoteArchivePort:
    """Small combined AList source/sink double for the shared adapter."""

    def __init__(self, payload: bytes, *, marker: bytes = b""):
        self.payload = payload
        self.marker = marker
        self.remote: dict[str, bytes] = {}
        self.mkdir_calls: list[str] = []
        self.prefix_calls: list[str] = []
        self.download_calls: list[str] = []

    def list(self, path: str, refresh: bool = False):
        del refresh
        if path == "/incoming":
            return [
                {"name": "movie.7z", "is_dir": False, "size": len(self.payload)},
                {"name": "密码提示.txt", "is_dir": False, "size": len(self.marker)},
            ]
        return []

    def read_file_prefix(self, path: str, *, max_bytes: int):
        self.prefix_calls.append(path)
        if path.endswith("密码提示.txt"):
            return self.marker[:max_bytes]
        return self.payload[:max_bytes]

    def download_file_to_path(self, path: str, destination: Path, *, expected_size: int):
        self.download_calls.append(path)
        if not path.endswith("movie.7z") or expected_size != len(self.payload):
            raise AssertionError(path)
        destination.write_bytes(self.payload)

    def mkdir(self, path: str):
        self.mkdir_calls.append(path)

    def upload_file(self, target_path: str, source: Path, content_type: str = "application/octet-stream"):
        del content_type
        self.remote[target_path] = source.read_bytes()

    def exact_file_info(self, path: str):
        payload = self.remote.get(path)
        return None if payload is None else {"size": len(payload), "version": "fake"}


class ArchivePreprocessingTests(unittest.TestCase):
    def _adapter(self, runner=None, **kwargs):
        return ArchivePreprocessingAdapter(
            runner or FakeRunner(),
            limits=ArchiveLimits(min_free_bytes=0),
            video_validator=lambda *_args: True,
            **kwargs,
        )

    def _archive(self, root: Path, name: str = "movie.7z") -> Path:
        path = root / name
        path.write_bytes(b"7z\xbc\xaf'\x1cfixture")
        return path

    def test_ordinary_and_provider_facades_share_one_domain_boundary(self):
        runner = FakeRunner()
        adapter = self._adapter(runner)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "source"
            root.mkdir()
            source = self._archive(root)
            ordinary = prepare_ordinary_archive(adapter, source, Path(temp) / "ordinary")
            provider = prepare_provider_archive(adapter, source, Path(temp) / "provider")
        self.assertEqual(ordinary.ingress, "ordinary")
        self.assertEqual(provider.ingress, "provider")
        self.assertIs(adapter.inspector.runner, adapter.extractor.runner)
        self.assertIs(adapter.inspector.runner, runner)
        self.assertEqual(len([call for call, _ in runner.calls if call[0] == "l"]), 2)
        self.assertEqual(len([call for call, _ in runner.calls if call[0] == "x"]), 2)
        self.assertNotIn("_password", ordinary.to_dict())

    def test_local_provider_archive_never_removes_original_and_only_returns_staging(self):
        adapter = self._adapter()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "source"
            root.mkdir()
            source = self._archive(root, "密码:local-secret.7z")
            result = adapter.prepare_provider_local(source, Path(temp) / "task")
            self.assertTrue(source.exists())
            self.assertTrue(result.changed)
            task_root = str((Path(temp) / "task").resolve())
            self.assertTrue(result.source_path.startswith(task_root))
            self.assertTrue(all(path.path.startswith(task_root) for path in result.files))
            self.assertNotIn("fixture", json.dumps(result.to_dict()))
            self.assertNotIn("local-secret", json.dumps(result.to_dict(), ensure_ascii=False))

    def test_remote_archive_preflight_reads_prefix_and_uploads_only_task_staging(self):
        payload = b"7z\xbc\xaf'\x1cfixture"
        port = RemoteArchivePort(payload, marker="密码:remote-secret".encode())
        adapter = self._adapter(staging_root_validator=lambda path: path == "/tasks/job/attempt" or path.startswith("/tasks/job/attempt/"))
        with tempfile.TemporaryDirectory() as temp:
            result = adapter.prepare_ordinary_remote(
                "/incoming/movie.7z",
                port,
                Path(temp) / "task",
                remote_staging_root="/tasks/job/attempt",
            )
        self.assertEqual(result.ingress, "ordinary")
        self.assertEqual(port.download_calls, ["/incoming/movie.7z"])
        self.assertIn("/incoming/movie.7z", port.prefix_calls)
        self.assertIn("/incoming/密码提示.txt", port.prefix_calls)
        self.assertTrue(port.remote)
        self.assertTrue(all(path.startswith("/tasks/job/attempt/") for path in port.remote))
        self.assertEqual(result.password_sources, ("source-tree-marker",))
        self.assertNotIn("remote-secret", json.dumps(result.to_dict(), ensure_ascii=False))

    def test_remote_formal_library_root_is_rejected_before_upload(self):
        port = RemoteArchivePort(b"7z\xbc\xaf'\x1cfixture")
        adapter = self._adapter(staging_root_validator=lambda path: path.startswith("/tasks/"))
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaises(ArchivePreprocessingError):
                adapter.prepare_provider_remote(
                    "/incoming/movie.7z",
                    port,
                    Path(temp) / "task",
                    remote_staging_root="/library/影视",
                )
        self.assertEqual(port.download_calls, [])
        self.assertEqual(port.remote, {})

    def test_ordinary_unknown_residual_is_passthrough_but_provider_rejects_it(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "source"
            root.mkdir()
            unknown = root / "readme.dat"
            unknown.write_bytes(b"not an archive")
            ordinary = self._adapter().prepare_ordinary_local(unknown, Path(temp) / "task")
            self.assertFalse(ordinary.changed)
            self.assertEqual(ordinary.files, ())
            with self.assertRaises(Exception):
                self._adapter().prepare_provider_local(unknown, Path(temp) / "provider-task")


if __name__ == "__main__":
    unittest.main()
