import json
from pathlib import Path
import tempfile
import unittest

from engine.scrapeflow.archive import (
    AListArchiveSource,
    ArchiveExtractor,
    ArchiveInspector,
    ArchiveLimits,
    ArchiveMagicError,
)
from engine.scrapeflow.archive_preprocessing import (
    ArchivePreprocessingAdapter,
    ArchivePreprocessingError,
    ArchiveMultiplicityError,
    ArchivePauseRequested,
    _ensure_local_staging,
    _fresh_child,
    prepare_ordinary_archive,
    prepare_provider_archive,
)

from local.tests.test_archive_domain import (
    FakeRunner,
    PasswordFallbackRunner,
    iso9660_prefix,
)


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


class RemoteTreePort:
    """Remote source double for ambiguity checks that must not download."""

    def __init__(self, direct_name: str):
        self.direct_name = direct_name
        self.download_calls: list[str] = []

    def list(self, path: str):
        if path == "/incoming":
            return [
                {"name": "movie.7z", "is_dir": False, "size": 64},
                {"name": self.direct_name, "is_dir": False, "size": 64},
            ]
        return []

    def read_prefix(self, path: str, *, max_bytes: int):
        del max_bytes
        if path.endswith(".7z"):
            return b"7z\xbc\xaf'\x1cfixture"
        # AVI/TS/M2TS have no single magic handled by the archive detector.
        # Their canonical extension must still participate in the tree-level
        # archive-vs-direct-media ambiguity check.
        return b"unclassified direct-media fixture"

    def download(self, path: str, destination: Path, *, expected_size: int):
        del destination, expected_size
        self.download_calls.append(path)
        raise AssertionError("ambiguous source tree must not download")


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

    def test_iso_and_renamed_exe_are_safely_listed_and_selected_into_task_staging(self):
        """Disc images use the same bounded 7-Zip lane, never a mount."""

        for name in ("feature.iso", "feature.exe"):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temp:
                root = Path(temp) / "source"
                root.mkdir()
                source = root / name
                source.write_bytes(iso9660_prefix())
                runner = FakeRunner()
                result = self._adapter(runner).prepare_ordinary_local(
                    source,
                    Path(temp) / "task",
                )

                self.assertTrue(result.changed)
                self.assertEqual([args[0] for args, _password in runner.calls], ["l", "x"])
                self.assertTrue(all(file.path.startswith(result.source_path) for file in result.files))
                self.assertTrue(source.exists())

    def test_renamed_video_exe_is_copied_to_staging_with_media_extension(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "source"
            root.mkdir()
            source = root / "[FSH] Show - 01 [BD].exe"
            source.write_bytes(b"\x1a\x45\xdf\xa3" + b"matroska-payload")
            runner = FakeRunner()
            result = self._adapter(runner).prepare_ordinary_local(
                source,
                Path(temp) / "task",
            )
            self.assertTrue(result.changed)
            self.assertEqual(len(result.files), 1)
            self.assertEqual(result.files[0].kind, "video")
            self.assertEqual(result.files[0].relative_path, "[FSH] Show - 01 [BD].mkv")
            self.assertTrue(Path(result.files[0].path).name.endswith(".mkv"))
            self.assertTrue(source.exists())
            self.assertEqual(runner.calls, [])

    def test_remote_tree_renames_masquerade_exe_to_media_extension(self):
        class MasqueradePort:
            def __init__(self):
                self.payload = b"\x1a\x45\xdf\xa3" + b"matroska-payload"
                self.remote = {}
                self.downloaded: list[str] = []
                self.uploaded: list[str] = []

            def list(self, path: str, refresh: bool = False):
                del refresh
                if path == "/incoming":
                    return [{"name": "Show S1", "is_dir": True}]
                if path == "/incoming/Show S1":
                    return [{"name": "[FSH] Show - 01 [BD].exe", "is_dir": False, "size": len(self.payload)}]
                return []

            def read_file_prefix(self, path: str, *, max_bytes: int):
                del max_bytes
                return self.payload

            def download_file_to_path(self, path: str, destination: Path, *, expected_size: int):
                if expected_size != len(self.payload):
                    raise AssertionError(path)
                destination.write_bytes(self.payload)
                self.downloaded.append(path)

            def mkdir(self, path: str):
                pass

            def upload_file(self, target_path: str, source: Path, content_type: str = "application/octet-stream"):
                del content_type
                self.remote[target_path] = source.read_bytes()
                self.uploaded.append(target_path)

            def exact_file_info(self, path: str):
                payload = self.remote.get(path)
                return None if payload is None else {"size": len(payload), "version": "fake"}

        adapter = self._adapter(staging_root_validator=lambda path: path.startswith("/tasks/"))
        port = MasqueradePort()
        with tempfile.TemporaryDirectory() as temp:
            result = adapter.prepare_ordinary_remote_tree(
                "/incoming",
                port,
                Path(temp) / "task",
                remote_staging_root="/tasks/job/attempt",
            )
            self.assertTrue(result.changed)
            self.assertEqual(len(result.files), 1)
            self.assertEqual(result.files[0].kind, "video")
            self.assertTrue(result.files[0].relative_path.endswith(".mkv"))
            self.assertEqual(port.downloaded, ["/incoming/Show S1/[FSH] Show - 01 [BD].exe"])
            self.assertEqual(len(port.uploaded), 1)
            self.assertTrue(port.uploaded[0].endswith(".mkv"))

    def test_archive_parent_relative_keeps_season_folder(self):
        from engine.scrapeflow.archive_preprocessing import _archive_parent_relative
        self.assertEqual(
            _archive_parent_relative("/incoming/出包王女 S1/01.exe", "/incoming"),
            "出包王女 S1",
        )
        self.assertEqual(
            _archive_parent_relative("/incoming/01.exe", "/incoming"),
            "",
        )

    def test_real_exe_is_not_executed_or_treated_as_an_ordinary_residual(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "source"
            root.mkdir()
            source = root / "real.exe"
            source.write_bytes(b"MZ\x90\x00real-program")
            runner = FakeRunner()
            with self.assertRaises(ArchiveMagicError):
                self._adapter(runner).prepare_ordinary_local(source, Path(temp) / "task")
        self.assertEqual(runner.calls, [])

    def test_unknown_disc_or_executable_suffix_cannot_be_silently_skipped(self):
        for name in ("opaque.iso", "opaque.img", "opaque.exe"):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temp:
                root = Path(temp) / "source"
                root.mkdir()
                source = root / name
                source.write_bytes(b"not a proven container")
                with self.assertRaises(ArchiveMagicError):
                    self._adapter().prepare_ordinary_local(source, Path(temp) / "task")

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

    def test_pause_after_archive_listing_blocks_local_extraction_subprocess(self):
        """A pause between `7z l` and `7z x` must not create payload files."""
        paused = {"value": False}

        class PauseAfterListingRunner(FakeRunner):
            def run(self, args, **kwargs):
                result = super().run(args, **kwargs)
                if args[0] == "l":
                    paused["value"] = True
                return result

        adapter = self._adapter(PauseAfterListingRunner())
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "source"
            root.mkdir()
            source = self._archive(root)
            staging = Path(temp) / "task"
            with self.assertRaises(ArchivePauseRequested):
                adapter.prepare_ordinary_local(
                    source,
                    staging,
                    pause_requested=lambda: paused["value"],
                )

        self.assertEqual(
            [args[0] for args, _password in adapter.runner.calls],
            ["l"],
        )

    def test_pause_after_remote_prefix_blocks_download_and_staging_upload(self):
        """The remote adapter checks again before its download/upload steps."""
        paused = {"value": False}

        class PauseAfterPrefixPort(RemoteArchivePort):
            def read_file_prefix(self, path: str, *, max_bytes: int):
                value = super().read_file_prefix(path, max_bytes=max_bytes)
                if path.endswith("movie.7z"):
                    paused["value"] = True
                return value

        port = PauseAfterPrefixPort(b"7z\xbc\xaf'\x1cfixture")
        adapter = self._adapter(
            staging_root_validator=(
                lambda path: path == "/tasks/job/attempt"
                or path.startswith("/tasks/job/attempt/")
            ),
        )
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaises(ArchivePauseRequested):
                adapter.prepare_ordinary_remote(
                    "/incoming/movie.7z",
                    port,
                    Path(temp) / "task",
                    remote_staging_root="/tasks/job/attempt",
                    pause_requested=lambda: paused["value"],
                )

        self.assertEqual(port.download_calls, [])
        self.assertEqual(port.mkdir_calls, [])
        self.assertEqual(port.remote, {})

    def test_pause_inside_local_staging_helpers_creates_no_directory(self):
        """A callback flip at the actual local mkdir boundary is fail-closed."""
        with tempfile.TemporaryDirectory() as temp:
            staging = Path(temp) / "task"
            with self.assertRaises(ArchivePauseRequested):
                _ensure_local_staging(staging, pause_requested=lambda: True)
            self.assertFalse(staging.exists())

            staging.mkdir()
            with self.assertRaises(ArchivePauseRequested):
                _fresh_child(
                    staging,
                    "archive/movie",
                    pause_requested=lambda: True,
                )
            self.assertFalse((staging / "archive").exists())

    def test_remote_inspector_pause_before_input_mkdir_creates_no_staging(self):
        """The input-root helper rechecks after metadata reads, before mkdir."""
        port = RemoteArchivePort(b"7z\xbc\xaf'\x1cfixture")
        checks = {"count": 0}

        def pause_checkpoint() -> None:
            checks["count"] += 1
            if checks["count"] == 4:
                raise ArchivePauseRequested("scope withdrawn")

        inspector = ArchiveInspector(
            FakeRunner(), limits=ArchiveLimits(min_free_bytes=0),
        )
        with tempfile.TemporaryDirectory() as temp:
            staging = Path(temp) / "input"
            with self.assertRaises(ArchivePauseRequested):
                inspector.inspect_remote(
                    AListArchiveSource(port),
                    "/incoming/movie.7z",
                    staging,
                    pause_checkpoint=pause_checkpoint,
                )
            self.assertFalse(staging.exists())
        self.assertEqual(port.download_calls, [])

    def test_remote_inspector_pause_inside_download_helper_skips_download(self):
        """A scope flip after input-root creation cannot start the transfer."""
        port = RemoteArchivePort(b"7z\xbc\xaf'\x1cfixture")
        checks = {"count": 0}

        def pause_checkpoint() -> None:
            checks["count"] += 1
            if checks["count"] == 6:
                raise ArchivePauseRequested("scope withdrawn")

        inspector = ArchiveInspector(
            FakeRunner(), limits=ArchiveLimits(min_free_bytes=0),
        )
        with tempfile.TemporaryDirectory() as temp:
            staging = Path(temp) / "input"
            with self.assertRaises(ArchivePauseRequested):
                inspector.inspect_remote(
                    AListArchiveSource(port),
                    "/incoming/movie.7z",
                    staging,
                    pause_checkpoint=pause_checkpoint,
                )
            self.assertTrue(staging.is_dir())
            self.assertEqual(list(staging.iterdir()), [])
        self.assertEqual(port.download_calls, [])

    def test_extractor_pause_inside_output_root_helper_skips_7z_and_mkdir(self):
        """Extraction rechecks at its own output-root mkdir boundary."""
        runner = FakeRunner()
        inspector = ArchiveInspector(runner, limits=ArchiveLimits(min_free_bytes=0))
        extractor = ArchiveExtractor(
            runner,
            limits=ArchiveLimits(min_free_bytes=0),
            video_validator=lambda *_args: True,
        )
        checks = {"count": 0}

        def pause_checkpoint() -> None:
            checks["count"] += 1
            if checks["count"] == 2:
                raise ArchivePauseRequested("scope withdrawn")

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "source"
            root.mkdir()
            source = self._archive(root)
            listing = inspector.inspect(source)
            staging = Path(temp) / "output"
            with self.assertRaises(ArchivePauseRequested):
                extractor.extract(
                    listing,
                    staging,
                    pause_checkpoint=pause_checkpoint,
                )
            self.assertFalse(staging.exists())
        self.assertEqual([args[0] for args, _password in runner.calls], ["l"])

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

    def test_remote_tree_detects_all_canonical_direct_media_extensions_alongside_archive(self):
        adapter = self._adapter(staging_root_validator=lambda path: path.startswith("/tasks/"))
        for direct_name in ("bonus.avi", "bonus.ts", "bonus.m2ts"):
            with self.subTest(direct_name=direct_name), tempfile.TemporaryDirectory() as temp:
                port = RemoteTreePort(direct_name)
                with self.assertRaises(ArchiveMultiplicityError):
                    adapter.prepare_ordinary_remote_tree(
                        "/incoming",
                        port,
                        Path(temp) / "task",
                        remote_staging_root="/tasks/job/attempt",
                )
                self.assertEqual(port.download_calls, [])

    def test_archive_bearing_document_residual_does_not_block_direct_media(self):
        """DOCX is ZIP internally, but remains a source residual, not media.

        This guards the generic boundary between actual/renamed media
        containers and ordinary documents accompanying an episode set.  The
        document must neither be extracted nor make the directory look like a
        mixed archive/direct-media source.
        """
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "source"
            root.mkdir()
            (root / "01.mp4").write_bytes(b"\x00\x00\x00\x18ftypisom")
            (root / "resource.docx").write_bytes(b"PK\x03\x04office")
            result = self._adapter().prepare_ordinary_tree(root, Path(temp) / "task")
            self.assertFalse(result.changed)
            self.assertEqual(result.archives, ())
            self.assertEqual([item.relative_path for item in result.files], ["01.mp4"])

        class DocumentResidualPort:
            def __init__(self) -> None:
                self.download_calls: list[str] = []

            def list(self, path: str):
                if path != "/incoming":
                    raise AssertionError(path)
                return [
                    {"name": "01.mp4", "is_dir": False, "size": 16},
                    {"name": "resource.docx", "is_dir": False, "size": 16},
                ]

            def read_prefix(self, path: str, *, max_bytes: int):
                del max_bytes
                if path.endswith("01.mp4"):
                    return b"\x00\x00\x00\x18ftypisom"
                if path.endswith("resource.docx"):
                    return b"PK\x03\x04office"
                raise AssertionError(path)

            def download(self, path: str, destination: Path, *, expected_size: int):
                del destination, expected_size
                self.download_calls.append(path)
                raise AssertionError("residual-only tree must not download")

        port = DocumentResidualPort()
        with tempfile.TemporaryDirectory() as temp:
            result = self._adapter(
                staging_root_validator=lambda path: path.startswith("/tasks/")
            ).prepare_ordinary_remote_tree(
                "/incoming",
                port,
                Path(temp) / "task",
                remote_staging_root="/tasks/job/attempt",
            )
        self.assertFalse(result.changed)
        self.assertEqual(result.source_path, "/incoming")
        self.assertEqual(port.download_calls, [])

    def test_font_installer_exe_is_a_residual_and_does_not_block_direct_media(self):
        """A ``[Fonts].exe`` self-extracting font installer stays at source.

        It is an executable resource (never executed, never expanded), not a
        disguised media container, so it must not make the directory look like
        a mixed archive/direct-media source.
        """
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "source"
            root.mkdir()
            (root / "01.mp4").write_bytes(b"\x00\x00\x00\x18ftypisom")
            (root / "[Fonts].exe").write_bytes(b"MZ" + b"stub" * 8 + b"PK\x03\x04payload")
            result = self._adapter().prepare_ordinary_tree(root, Path(temp) / "task")
            self.assertFalse(result.changed)
            self.assertEqual(result.archives, ())
            self.assertEqual([item.relative_path for item in result.files], ["01.mp4"])

    def test_font_installer_pure_pe_exe_is_excluded_not_a_blocking_executable(self):
        """A pure PE ``[Fonts].exe`` (no ZIP payload) is still a font residual.

        The ``MZ`` header alone must not trip the executable boundary before
        the font-residual classification can keep it at source.
        """
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "source"
            root.mkdir()
            (root / "01.mp4").write_bytes(b"\x00\x00\x00\x18ftypisom")
            (root / "[Fonts].exe").write_bytes(b"MZ\x90\x00" + b"\x00" * 60)
            result = self._adapter().prepare_ordinary_tree(root, Path(temp) / "task")
            self.assertFalse(result.changed)
            self.assertEqual(result.archives, ())
            self.assertEqual([item.relative_path for item in result.files], ["01.mp4"])

    def test_remote_tree_skips_unreadable_declared_empty_residual(self):
        """A zero-byte readme cannot carry archive/executable magic.

        Providers can list an empty explanatory file but reject a file-link
        Range request for it.  The ordinary tree scanner must not turn that
        residual into a planning failure, while non-empty objects retain the
        existing magic scan.
        """
        class EmptyResidualPort:
            def __init__(self) -> None:
                self.prefix_calls: list[str] = []

            def list(self, path: str):
                self.assert_path(path)
                return [{"name": "provider-note", "is_dir": False, "size": 0}]

            @staticmethod
            def assert_path(path: str) -> None:
                if path != "/incoming":
                    raise AssertionError(path)

            def read_prefix(self, path: str, *, max_bytes: int):
                del max_bytes
                self.prefix_calls.append(path)
                raise AssertionError("declared-empty residual must not be read")

            def download(self, path: str, destination: Path, *, expected_size: int):
                del path, destination, expected_size
                raise AssertionError("residual must not download")

        port = EmptyResidualPort()
        adapter = self._adapter(staging_root_validator=lambda path: path.startswith("/tasks/"))
        with tempfile.TemporaryDirectory() as temp:
            result = adapter.prepare_ordinary_remote_tree(
                "/incoming", port, Path(temp) / "task",
                remote_staging_root="/tasks/job/attempt",
            )
        self.assertFalse(result.changed)
        self.assertEqual(result.source_path, "/incoming")
        self.assertEqual(port.prefix_calls, [])

    def test_remote_tree_rejects_declared_empty_archive_without_reading(self):
        class EmptyArchivePort:
            prefix_calls: list[str] = []

            def list(self, path: str):
                if path != "/incoming":
                    raise AssertionError(path)
                return [{"name": "empty.7z", "is_dir": False, "size": 0}]

            def read_prefix(self, path: str, *, max_bytes: int):
                del path, max_bytes
                self.prefix_calls.append("unexpected")
                raise AssertionError("declared-empty archive must fail before read")

            def download(self, path: str, destination: Path, *, expected_size: int):
                del path, destination, expected_size
                raise AssertionError("declared-empty archive must not download")

        port = EmptyArchivePort()
        adapter = self._adapter(staging_root_validator=lambda path: path.startswith("/tasks/"))
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaises(ArchiveMagicError):
                adapter.prepare_ordinary_remote_tree(
                    "/incoming", port, Path(temp) / "task",
                    remote_staging_root="/tasks/job/attempt",
                )
        self.assertEqual(port.prefix_calls, [])

    def test_remote_tree_keeps_empty_video_in_archive_multiplicity_check(self):
        class MixedPort:
            def __init__(self) -> None:
                self.prefix_calls: list[str] = []

            def list(self, path: str):
                if path != "/incoming":
                    raise AssertionError(path)
                return [
                    {"name": "empty.mkv", "is_dir": False, "size": 0},
                    {"name": "payload.7z", "is_dir": False, "size": 64},
                ]

            def read_prefix(self, path: str, *, max_bytes: int):
                del max_bytes
                self.prefix_calls.append(path)
                if path.endswith("payload.7z"):
                    return b"7z\xbc\xaf'\x1cfixture"
                raise AssertionError(path)

            def download(self, path: str, destination: Path, *, expected_size: int):
                del path, destination, expected_size
                raise AssertionError("mixed tree must not download")

        port = MixedPort()
        adapter = self._adapter(staging_root_validator=lambda path: path.startswith("/tasks/"))
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaises(ArchiveMultiplicityError):
                adapter.prepare_ordinary_remote_tree(
                    "/incoming", port, Path(temp) / "task",
                    remote_staging_root="/tasks/job/attempt",
                )
        self.assertEqual(port.prefix_calls, ["/incoming/payload.7z"])

    def test_local_tree_rejects_child_file_and_directory_links(self):
        adapter = self._adapter()
        for is_directory in (False, True):
            with self.subTest(is_directory=is_directory), tempfile.TemporaryDirectory() as temp:
                root = Path(temp) / "source"
                root.mkdir()
                target = Path(temp) / ("outside-dir" if is_directory else "outside.mkv")
                if is_directory:
                    target.mkdir()
                    link = root / "linked-directory"
                else:
                    target.write_bytes(b"outside")
                    link = root / "linked-file.mkv"
                link.symlink_to(target, target_is_directory=is_directory)
                with self.assertRaises(ArchivePreprocessingError):
                    adapter.prepare_ordinary_tree(root, Path(temp) / "task")

    def test_preprocessing_reports_the_candidate_that_actually_extracted(self):
        runner = PasswordFallbackRunner()
        adapter = self._adapter(runner)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "source"
            root.mkdir()
            source = self._archive(root)
            (root / "password.txt").write_text("password: correct", encoding="utf-8")
            result = adapter.prepare_ordinary_local(
                source,
                Path(temp) / "task",
                retry_password="wrong",
            )
        self.assertEqual(result.password_sources, ("source-tree-marker",))


if __name__ == "__main__":
    unittest.main()
