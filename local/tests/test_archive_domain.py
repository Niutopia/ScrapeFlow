import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

from engine.scrapeflow.archive import (
    ArchiveBudgetError,
    ArchiveCollisionError,
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


class FakeRunner:
    def __init__(self, listing: str = LISTING):
        self.listing = listing
        self.calls: list[tuple[tuple[str, ...], str]] = []

    def run(self, args, *, password="", cwd=None, timeout=0):
        del cwd, timeout
        self.calls.append((tuple(args), password))
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


class ArchiveDomainTests(unittest.TestCase):
    def test_magic_detects_archive_media_and_mz_without_execution(self):
        self.assertTrue(detect_magic(b"7z\xbc\xaf'\x1canything").is_archive)
        self.assertEqual(detect_magic(b"\x1aE\xdf\xa3rest").kind, "media")
        self.assertEqual(detect_magic(b"MZ\x90\x00not-a-container").kind, "executable")
        self.assertTrue(
            detect_magic(b"MZ" + b"x" * 32 + b"PK\x03\x04payload").self_extracting
        )

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
