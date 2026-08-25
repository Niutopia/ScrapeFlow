import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

from engine.scrapeflow.archive import (
    ArchiveBudgetError,
    ArchiveCollisionError,
    ArchiveCorruptionError,
    ArchiveExtractor,
    ArchiveLinkError,
    ArchiveMagicError,
    ArchiveLimits,
    ArchivePasswordConflict,
    ArchivePathError,
    ArchiveVolumeError,
    PasswordCandidate,
    RunnerResult,
    Subprocess7zRunner,
    detect_magic,
    discover_volume_paths,
    member_from_mapping,
    normalize_member_path,
    parse_7z_slt_listing,
    password_candidates,
    validate_archive_members,
    validate_volume_names,
)


LISTING = """\
Path = movie.7z
Type = 7z
Physical Size = 99

----------
Path = Movie/movie.mkv
Size = 4
Packed Size = 3
Attributes = A....
Encrypted = -

Path = Movie/movie.srt
Size = 38
Packed Size = 20
Attributes = A....
Encrypted = -

Path = Movie/readme.txt
Size = 6
Packed Size = 6
Attributes = A....
Encrypted = -
"""

ENCRYPTED_LISTING = LISTING.replace("Encrypted = -", "Encrypted = +")


def iso9660_prefix() -> bytes:
    """Small non-mountable ISO-9660 signature fixture for magic tests."""

    data = bytearray(16 * 2048 + 7)
    data[16 * 2048] = 1
    data[16 * 2048 + 1:16 * 2048 + 6] = b"CD001"
    data[16 * 2048 + 6] = 1
    return bytes(data)


def udf_prefix() -> bytes:
    """Small UDF VRS fixture: NSR02 at the fixed sector descriptor slot."""

    data = bytearray(17 * 2048 + 6)
    data[16 * 2048 + 1:16 * 2048 + 6] = b"BEA01"
    data[17 * 2048 + 1:17 * 2048 + 6] = b"NSR02"
    return bytes(data)


class FakeRunner:
    def __init__(self, listing: str = LISTING):
        self.listing = listing
        self.calls: list[tuple[tuple[str, ...], str]] = []
        self.timeouts: list[float] = []

    def run(self, args, *, password="", cwd=None, timeout=0):
        del cwd
        self.calls.append((tuple(args), password))
        self.timeouts.append(timeout)
        if args[0] == "l":
            return RunnerResult(0, self.listing, "")
        if args[0] == "x":
            output = next(Path(arg[2:]) for arg in args if arg.startswith("-o"))
            selected = [arg for arg in args if arg in {"Movie/movie.mkv", "Movie/movie.srt"}]
            output.joinpath("Movie").mkdir(parents=True, exist_ok=True)
            if "Movie/movie.mkv" in selected:
                output.joinpath("Movie/movie.mkv").write_bytes(b"1234")
            if "Movie/movie.srt" in selected:
                output.joinpath("Movie/movie.srt").write_text(
                    "1\n00:00:00,000 --> 00:00:01,000\nhello\n",
                    encoding="utf-8",
                )
            return RunnerResult(0, "", "")
        return RunnerResult(2, "", "failure")


class PasswordFallbackRunner(FakeRunner):
    """A 7-Zip double whose listing succeeds before payload password proof."""

    def __init__(self, *, corruption: bool = False, ambiguous_password_failure: bool = False):
        super().__init__(ENCRYPTED_LISTING)
        self.corruption = corruption
        self.ambiguous_password_failure = ambiguous_password_failure

    def run(self, args, *, password="", cwd=None, timeout=0):
        if args[0] == "x" and (self.corruption or password != "correct"):
            del cwd, timeout
            self.calls.append((tuple(args), password))
            if self.corruption:
                return RunnerResult(2, "", "ERROR: Headers Error")
            if self.ambiguous_password_failure:
                return RunnerResult(2, "", "ERROR: Data Error in encrypted file")
            return RunnerResult(2, "", "ERROR: Wrong password")
        return super().run(args, password=password, cwd=cwd, timeout=timeout)


class FakeRemoteSource:
    def __init__(self, payload: bytes):
        self.payload = payload
        self.downloads: list[str] = []

    def list(self, path):
        self.parent = path
        return [{"name": "movie.7z", "is_dir": False, "size": len(self.payload)}]

    def read_prefix(self, path, *, max_bytes):
        del path
        return self.payload[:max_bytes]

    def download(self, path, destination, *, expected_size):
        self.downloads.append(path)
        assert expected_size == len(self.payload)
        destination.write_bytes(self.payload)


class FakeRemoteImageSource:
    """A bounded remote ISO source used without any mount-capable port."""

    def __init__(self, payload: bytes, *, name: str = "feature.iso"):
        self.payload = payload
        self.name = name
        self.downloads: list[str] = []

    def list(self, path):
        if path != "/incoming":
            raise AssertionError(path)
        return [{"name": self.name, "is_dir": False, "size": len(self.payload)}]

    def read_prefix(self, path, *, max_bytes):
        if path != f"/incoming/{self.name}":
            raise AssertionError(path)
        return self.payload[:max_bytes]

    def download(self, path, destination, *, expected_size):
        if path != f"/incoming/{self.name}" or expected_size != len(self.payload):
            raise AssertionError(path)
        self.downloads.append(path)
        destination.write_bytes(self.payload)


class ArchiveDomainTests(unittest.TestCase):
    def test_magic_detects_archive_media_and_mz_without_execution(self):
        self.assertTrue(detect_magic(b"7z\xbc\xaf'\x1canything").is_archive)
        self.assertEqual(detect_magic(b"\x1aE\xdf\xa3rest").kind, "media")
        self.assertEqual(detect_magic(b"MZ\x90\x00not-a-container").kind, "executable")
        self.assertTrue(
            detect_magic(b"MZ" + b"x" * 32 + b"PK\x03\x04payload").self_extracting
        )

    def test_magic_recognizes_iso9660_and_udf_only_at_filesystem_offsets(self):
        iso = detect_magic(iso9660_prefix(), filename="feature.exe")
        self.assertEqual((iso.format, iso.kind, iso.offset), ("iso", "archive", 16 * 2048 + 1))
        self.assertTrue(iso.is_disc_image)
        self.assertTrue(iso.is_archive)

        udf = detect_magic(udf_prefix(), filename="feature.img")
        self.assertEqual((udf.format, udf.kind, udf.offset), ("udf", "archive", 17 * 2048 + 1))
        self.assertTrue(udf.is_disc_image)

        # These bytes are deliberately not at a volume-descriptor boundary.
        self.assertEqual(detect_magic(b"prefix-CD001-not-an-image").kind, "unknown")

    def test_iso_listing_uses_7z_without_mounting_the_image(self):
        from engine.scrapeflow.archive import ArchiveInspector

        runner = FakeRunner()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            image = root / "feature.iso"
            image.write_bytes(iso9660_prefix())
            listing = ArchiveInspector(
                runner,
                limits=ArchiveLimits(min_free_bytes=0),
            ).inspect(image)

        self.assertEqual(listing.archive_format, "iso")
        self.assertTrue(listing.selected_media)
        self.assertEqual([args[0] for args, _password in runner.calls], ["l"])
        # The only external boundary is argv-based 7-Zip listing; no shell,
        # mount command, or executable wrapper is ever invoked.
        self.assertEqual(runner.calls[0][0][1:4], ("-slt", "-sccUTF-8", "-y"))

    def test_udf_listing_uses_the_same_bounded_7z_lane(self):
        from engine.scrapeflow.archive import ArchiveInspector

        runner = FakeRunner()
        with tempfile.TemporaryDirectory() as temp:
            image = Path(temp) / "feature.udf"
            image.write_bytes(udf_prefix())
            listing = ArchiveInspector(
                runner,
                limits=ArchiveLimits(min_free_bytes=0),
            ).inspect(image)
        self.assertEqual(listing.archive_format, "udf")
        self.assertEqual([args[0] for args, _password in runner.calls], ["l"])

    def test_self_extracting_and_renamed_exe_containers_are_listed_not_run(self):
        from engine.scrapeflow.archive import ArchiveInspector

        runner = FakeRunner()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            sfx = root / "release.exe"
            sfx.write_bytes(b"MZ" + b"stub" * 8 + b"PK\x03\x04payload")
            sfx_listing = ArchiveInspector(
                runner,
                limits=ArchiveLimits(min_free_bytes=0),
            ).inspect(sfx)

            renamed_image = root / "disc.exe"
            renamed_image.write_bytes(iso9660_prefix())
            renamed_listing = ArchiveInspector(
                runner,
                limits=ArchiveLimits(min_free_bytes=0),
            ).inspect(renamed_image)

            embedded_image = root / "disc-wrapper.exe"
            embedded_image.write_bytes(b"MZ" + b"stub" * 8 + iso9660_prefix())
            embedded_listing = ArchiveInspector(
                runner,
                limits=ArchiveLimits(min_free_bytes=0),
            ).inspect(embedded_image)

        self.assertEqual(sfx_listing.archive_format, "zip")
        self.assertEqual(renamed_listing.archive_format, "iso")
        self.assertEqual(embedded_listing.archive_format, "iso")
        self.assertEqual([args[0] for args, _password in runner.calls], ["l", "l", "l"])
        self.assertTrue(all(args[0] == "l" for args, _password in runner.calls))

    def test_real_executable_fails_closed_before_7z(self):
        from engine.scrapeflow.archive import ArchiveInspector

        runner = FakeRunner()
        with tempfile.TemporaryDirectory() as temp:
            executable = Path(temp) / "real.exe"
            executable.write_bytes(b"MZ\x90\x00a-real-pe-without-an-archive")
            with self.assertRaises(ArchiveMagicError):
                ArchiveInspector(
                    runner,
                    limits=ArchiveLimits(min_free_bytes=0),
                ).inspect(executable)
            # An executable containing an ISO-looking string without a full
            # descriptor is still an executable, not an accepted container.
            deceptive = bytearray(b"MZ" + b"stub" * 8 + b"\x00" * len(iso9660_prefix()))
            marker = len(b"MZ" + b"stub" * 8) + 16 * 2048 + 1
            deceptive[marker:marker + 5] = b"CD001"
            self.assertEqual(detect_magic(bytes(deceptive)).kind, "executable")
        self.assertEqual(runner.calls, [])

    def test_iso_member_path_traversal_is_rejected_before_extraction(self):
        from engine.scrapeflow.archive import ArchiveInspector

        malicious_listing = """\
Path = feature.iso
Type = Iso

----------
Path = ../escape.m2ts
Size = 4
Attributes = A....
"""
        runner = FakeRunner(malicious_listing)
        with tempfile.TemporaryDirectory() as temp:
            image = Path(temp) / "feature.iso"
            image.write_bytes(iso9660_prefix())
            with self.assertRaises(ArchivePathError):
                ArchiveInspector(
                    runner,
                    limits=ArchiveLimits(min_free_bytes=0),
                ).inspect(image)
        self.assertEqual([args[0] for args, _password in runner.calls], ["l"])

    def test_disc_limits_are_independent_from_ordinary_archive_limits(self):
        limits = ArchiveLimits(
            max_archive_bytes=10,
            max_expanded_bytes=10,
            max_member_bytes=10,
            max_disc_image_bytes=64,
            max_disc_expanded_bytes=64,
            max_disc_member_bytes=64,
            min_free_bytes=0,
        )
        member = {"path": "BDMV/STREAM/00001.m2ts", "size": 32}
        self.assertEqual(
            validate_archive_members(
                [member],
                limits=limits,
                archive_size=32,
                archive_format="iso",
            )[0].size,
            32,
        )
        with self.assertRaises(ArchiveBudgetError):
            validate_archive_members(
                [member],
                limits=limits,
                archive_size=32,
                archive_format="zip",
            )
        with self.assertRaises(ArchiveBudgetError):
            validate_archive_members(
                [{"path": "BDMV/STREAM/00001.m2ts", "size": 1}],
                limits=limits,
                archive_size=65,
                archive_format="iso",
            )

    def test_disc_listing_uses_disc_timeout_and_source_budget(self):
        from engine.scrapeflow.archive import ArchiveInspector

        runner = FakeRunner()
        limits = ArchiveLimits(
            max_archive_bytes=1024,
            max_disc_image_bytes=64 * 1024,
            command_timeout_seconds=1,
            disc_command_timeout_seconds=2,
            min_free_bytes=0,
        )
        with tempfile.TemporaryDirectory() as temp:
            image = Path(temp) / "feature.iso"
            image.write_bytes(iso9660_prefix())
            ArchiveInspector(runner, limits=limits).inspect(image)
            ordinary = Path(temp) / "ordinary.7z"
            ordinary.write_bytes(b"7z\xbc\xaf'\x1c" + b"x" * (32 * 1024))
            with self.assertRaises(ArchiveBudgetError):
                ArchiveInspector(runner, limits=limits).inspect(ordinary)
        self.assertEqual(runner.timeouts, [2])

    def test_remote_iso_is_staged_only_after_fresh_disk_budget_check(self):
        from collections import namedtuple
        from engine.scrapeflow.archive import ArchiveInspector

        payload = iso9660_prefix()
        source = FakeRemoteImageSource(payload)
        DiskUsage = namedtuple("DiskUsage", "total used free")
        with tempfile.TemporaryDirectory() as temp:
            staging = Path(temp) / "archive-input"
            with mock.patch(
                "engine.scrapeflow.archive.shutil.disk_usage",
                return_value=DiskUsage(total=len(payload), used=0, free=len(payload)),
            ):
                with self.assertRaises(ArchiveBudgetError):
                    ArchiveInspector(
                        FakeRunner(),
                        limits=ArchiveLimits(min_free_bytes=1),
                    ).inspect_remote(source, "/incoming/feature.iso", staging)
            self.assertTrue(staging.is_dir())
            self.assertEqual(list(staging.iterdir()), [])
        self.assertEqual(source.downloads, [])

    def test_remote_disc_rechecks_live_space_after_source_staging_before_extracting(self):
        from collections import namedtuple
        from engine.scrapeflow.archive import ArchiveInspector

        payload = iso9660_prefix()
        source = FakeRemoteImageSource(payload)
        runner = FakeRunner()
        DiskUsage = namedtuple("DiskUsage", "total used free")
        limits = ArchiveLimits(min_free_bytes=1)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            input_root = root / "archive-input"
            with mock.patch(
                "engine.scrapeflow.archive.shutil.disk_usage",
                return_value=DiskUsage(total=len(payload) * 2, used=0, free=len(payload) + 1),
            ):
                listing = ArchiveInspector(runner, limits=limits).inspect_remote(
                    source,
                    "/incoming/feature.iso",
                    input_root,
                )
            # The source now occupies task staging.  A later free-space check
            # must reject the selected member output before `7z x` starts.
            with mock.patch(
                "engine.scrapeflow.archive.shutil.disk_usage",
                return_value=DiskUsage(total=100, used=99, free=1),
            ):
                with self.assertRaises(ArchiveBudgetError):
                    ArchiveExtractor(
                        runner,
                        limits=limits,
                        video_validator=lambda *_args: True,
                    ).extract(listing, root / "selected-output")
        self.assertEqual(source.downloads, ["/incoming/feature.iso"])
        self.assertEqual([args[0] for args, _password in runner.calls], ["l"])

    def test_archive_limits_environment_is_bounded_and_invalid_values_fail_closed(self):
        limits = ArchiveLimits.from_environment({
            "SCRAPEFLOW_ARCHIVE_MIN_FREE_BYTES": "17",
            "SCRAPEFLOW_DISC_IMAGE_MAX_SOURCE_BYTES": "123",
            "SCRAPEFLOW_DISC_IMAGE_COMMAND_TIMEOUT_SECONDS": "8.5",
        })
        self.assertEqual(limits.min_free_bytes, 17)
        self.assertEqual(limits.max_disc_image_bytes, 123)
        self.assertEqual(limits.disc_command_timeout_seconds, 8.5)
        with self.assertRaises(ValueError):
            ArchiveLimits.from_environment({"SCRAPEFLOW_DISC_IMAGE_MAX_SOURCE_BYTES": "many"})
        with self.assertRaises(ValueError):
            ArchiveLimits.from_environment({"SCRAPEFLOW_DISC_IMAGE_COMMAND_TIMEOUT_SECONDS": "nan"})
        with self.assertRaises(ValueError):
            Subprocess7zRunner(executable="7z", max_timeout_seconds=float("nan"))

    def test_member_paths_and_collision_are_fail_closed(self):
        for value in (
            "/absolute.mkv", "C:/drive.mkv", "../escape.mkv", "a/../b.mkv",
            "a\\..\\b.mkv", "bad:name.mkv", "@members.mkv", "wild*.mkv",
        ):
            with self.subTest(value=value), self.assertRaises(ArchivePathError):
                normalize_member_path(value)
        with self.assertRaises(ArchiveCollisionError):
            validate_archive_members(
                [
                    {"path": "Show/É.mkv", "size": 1},
                    {"path": "show/é.mkv", "size": 1},
                ],
                limits=ArchiveLimits(min_free_bytes=0),
            )
        with self.assertRaises(ArchiveCollisionError):
            validate_archive_members(
                [{"path": "show", "size": 0}, {"path": "show/a.mkv", "size": 1}],
                limits=ArchiveLimits(min_free_bytes=0),
            )

    def test_links_and_budgets_are_rejected_before_extraction(self):
        with self.assertRaises(ArchiveLinkError):
            validate_archive_members([{"path": "movie.mkv", "size": 1, "symlink": True}])
        # These are the exact keys emitted by ``7z l -slt``.  They must not
        # bypass the generic lower-case adapter keys above.
        with self.assertRaises(ArchiveLinkError):
            validate_archive_members([{"path": "movie.mkv", "size": 1, "Symbolic Link": "target"}])
        with self.assertRaises(ArchiveLinkError):
            validate_archive_members([{"path": "movie.mkv", "size": 1, "Hard Link": "target"}])
        with self.assertRaises(ArchiveBudgetError):
            validate_archive_members(
                [{"path": "movie.mkv", "size": 100}],
                limits=ArchiveLimits(max_expanded_bytes=10, min_free_bytes=0),
            )
        with self.assertRaises(ArchiveBudgetError):
            validate_archive_members(
                [{"path": "movie.mkv", "size": 1000}],
                archive_size=1,
                limits=ArchiveLimits(max_expansion_ratio=2, min_free_bytes=0),
            )

    def test_split_volume_sequence_requires_first_and_contiguous_parts(self):
        self.assertEqual(
            validate_volume_names(["show.7z.002", "show.7z.001", "other.7z.001"]),
            ("show.7z.001", "show.7z.002"),
        )
        with self.assertRaises(ArchiveVolumeError):
            validate_volume_names(["show.7z.002", "show.7z.004"], "show.7z.002")
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "show.7z.001").write_bytes(b"a")
            (root / "show.7z.003").write_bytes(b"c")
            with self.assertRaises(ArchiveVolumeError):
                discover_volume_paths(root / "show.7z.001")
        with self.assertRaises(ArchiveVolumeError):
            validate_volume_names(["show.r00", "show.rar"])
        self.assertEqual(
            validate_volume_names(["other.r00", "show.rar"], "show.rar"),
            ("show.rar",),
        )

    def test_password_candidates_are_bounded_conflict_aware_and_redacted(self):
        with self.assertRaises(ArchivePasswordConflict):
            password_candidates(path="/x/密码:one/密码:two/a.7z")
        with self.assertRaises(ArchivePasswordConflict):
            password_candidates(path="/密码:one/a.7z", sibling_names=("密码:two.txt",))
        rows = password_candidates(
            retry_password="retry-secret",
            path="/密码:path/a.7z",
            parent_names=("parent-one", "parent-two", "parent-three"),
            max_candidates=3,
        )
        self.assertEqual([row.source for row in rows], ["retry", "path-marker", "none"])
        self.assertNotIn("retry-secret", repr(rows))

    def test_inspector_uses_bounded_local_marker_candidates_by_default(self):
        runner = FakeRunner()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            archive = root / "movie.7z"
            archive.write_bytes(b"7z\xbc\xaf'\x1cfixture")
            (root / "密码:local-secret.txt").write_text("hint", encoding="utf-8")
            # The fake runner records the in-memory candidate; the listing/UI
            # projection must still contain only the source label.
            from engine.scrapeflow.archive import ArchiveInspector

            listing = ArchiveInspector(
                runner,
                limits=ArchiveLimits(min_free_bytes=0),
            ).inspect(archive)
            self.assertEqual(listing.password_source, "sibling-marker")
            safe = listing.to_dict()
            self.assertEqual(safe["password_source"], "sibling-marker")
            self.assertNotIn("local-secret", json.dumps(safe["members"]))

    def test_remote_preflight_rejects_unknown_bytes_before_download_or_staging(self):
        from engine.scrapeflow.archive import ArchiveInspector

        source = FakeRemoteSource(b"not an archive")
        with tempfile.TemporaryDirectory() as temp:
            staging = Path(temp) / "archive-input"
            with self.assertRaises(ArchiveMagicError):
                ArchiveInspector(
                    FakeRunner(),
                    limits=ArchiveLimits(min_free_bytes=0),
                ).inspect_remote(source, "/incoming/movie.bin", staging)
            self.assertEqual(source.downloads, [])
            self.assertFalse(staging.exists())

    def test_remote_inspection_reuses_the_local_listing_boundary(self):
        from engine.scrapeflow.archive import ArchiveInspector

        payload = b"7z\xbc\xaf'\x1cfixture"
        source = FakeRemoteSource(payload)
        with tempfile.TemporaryDirectory() as temp:
            listing = ArchiveInspector(
                FakeRunner(),
                limits=ArchiveLimits(min_free_bytes=0),
            ).inspect_remote(source, "/incoming/movie.7z", Path(temp) / "archive-input")
        self.assertEqual(listing.archive_format, "7z")
        self.assertEqual(source.downloads, ["/incoming/movie.7z"])

    def test_listing_and_selected_extraction_use_fake_runner_and_fresh_staging(self):
        runner = FakeRunner()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            archive = root / "movie.7z"
            archive.write_bytes(b"7z\xbc\xaf'\x1cfixture")
            staging = root / "task-staging"
            limits = ArchiveLimits(min_free_bytes=0)
            from engine.scrapeflow.archive import ArchiveInspector

            listing = ArchiveInspector(runner, limits=limits).inspect(
                archive,
                password_candidates=[PasswordCandidate("secret", "retry")],
            )
            self.assertEqual(listing.password_source, "retry")
            self.assertNotIn("secret", json.dumps(listing.to_dict(), ensure_ascii=False))
            result = ArchiveExtractor(
                runner,
                limits=limits,
                video_validator=lambda path, member=None: True,
            ).extract(listing, staging)
            self.assertEqual([path.name for path in result.files], ["movie.mkv", "movie.srt"])
            extract_call = next(args for args, _password in runner.calls if args[0] == "x")
            self.assertNotIn("secret", extract_call)
            self.assertIn("Movie/movie.mkv", extract_call)
            self.assertFalse((staging / "Movie" / "readme.txt").exists())

    def test_extractor_falls_through_untried_password_candidates(self):
        from engine.scrapeflow.archive import ArchiveInspector

        runner = PasswordFallbackRunner(ambiguous_password_failure=True)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            archive = root / "movie.7z"
            archive.write_bytes(b"7z\xbc\xaf'\x1cfixture")
            listing = ArchiveInspector(runner, limits=ArchiveLimits(min_free_bytes=0)).inspect(
                archive,
                password_candidates=(
                    PasswordCandidate("wrong", "retry"),
                    PasswordCandidate("correct", "source-tree-marker"),
                ),
            )
            result = ArchiveExtractor(
                runner,
                limits=ArchiveLimits(min_free_bytes=0),
                video_validator=lambda *_args: True,
            ).extract(listing, root / "staging")
        self.assertEqual(result.password_source, "source-tree-marker")
        self.assertEqual(
            [password for args, password in runner.calls if args[0] == "x"],
            ["wrong", "correct"],
        )
        safe_listing = json.dumps(listing.to_dict(), ensure_ascii=False)
        self.assertNotIn("wrong", safe_listing)
        self.assertNotIn("correct", safe_listing)

    def test_extractor_reports_structural_corruption_without_trying_more_passwords(self):
        from engine.scrapeflow.archive import ArchiveInspector

        runner = PasswordFallbackRunner(corruption=True)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            archive = root / "movie.7z"
            archive.write_bytes(b"7z\xbc\xaf'\x1cfixture")
            listing = ArchiveInspector(runner, limits=ArchiveLimits(min_free_bytes=0)).inspect(
                archive,
                password_candidates=(
                    PasswordCandidate("wrong", "retry"),
                    PasswordCandidate("correct", "source-tree-marker"),
                ),
            )
            with self.assertRaises(ArchiveCorruptionError):
                ArchiveExtractor(
                    runner,
                    limits=ArchiveLimits(min_free_bytes=0),
                    video_validator=lambda *_args: True,
                ).extract(listing, root / "staging")
        self.assertEqual(
            [password for args, password in runner.calls if args[0] == "x"],
            ["wrong"],
        )

    def test_unexpected_output_and_existing_staging_are_rejected(self):
        runner = FakeRunner()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            archive = root / "movie.7z"
            archive.write_bytes(b"7z\xbc\xaf'\x1cfixture")
            from engine.scrapeflow.archive import ArchiveInspector

            listing = ArchiveInspector(runner, limits=ArchiveLimits(min_free_bytes=0)).inspect(archive)
            staging = root / "staging"
            staging.mkdir()
            (staging / "unexpected.txt").write_text("keep", encoding="utf-8")
            with self.assertRaises(Exception):
                ArchiveExtractor(
                    runner,
                    limits=ArchiveLimits(min_free_bytes=0),
                    video_validator=lambda *_args: True,
                ).extract(listing, staging)

    def test_archive_with_only_nested_archive_reports_explicit_boundary(self):
        from engine.scrapeflow.archive import NestedArchiveUnsupported, select_media_members

        with self.assertRaises(NestedArchiveUnsupported):
            select_media_members((member_from_mapping({"path": "inner.zip", "size": 10}),))

    def test_subprocess_runner_keeps_password_out_of_argv(self):
        completed = subprocess.CompletedProcess(["7z"], 0, b"ok", b"")
        with mock.patch("engine.scrapeflow.archive.subprocess.run", return_value=completed) as run:
            Subprocess7zRunner(executable="7z").run(("l", "-slt", "archive.7z"), password="secret")
        argv = run.call_args.args[0]
        self.assertIn("-p*", argv)
        self.assertNotIn("secret", argv)
        self.assertEqual(run.call_args.kwargs["input"], b"secret\n")


if __name__ == "__main__":
    unittest.main()
