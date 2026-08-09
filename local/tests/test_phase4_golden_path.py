"""One-shot acceptance fixture for the phase-4 ordinary-ingress convergence.

This is intentionally a small fake-environment acceptance test, not a second
E2E framework.  It connects the real archive adapter and the real single
writer through ``SimpleEngineRunner`` and records the externally meaningful
checkpoint row required by T5.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import posixpath
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from engine.scrapeflow.archive import ArchiveError, ArchiveLimits, RunnerResult
from engine.scrapeflow.archive_preprocessing import ArchivePreprocessingAdapter
from engine.scrapeflow.models import Plan, PlannedFile, PlannedProblem
from local.scrapeflow_api.simple_engine_runner import (
    EngineExecutionError,
    EngineRequest,
    SimpleEngineRunner,
    SimplePlanExecutor,
)
from local.simple_server import SimpleApplication


VIDEO = b"v" * (1024 * 1024)
SUBTITLE = "1\n00:00:00,000 --> 00:00:01,000\n你好\n".encode()
TARGET_ROOT = "/library/电影/Movie (2020)"
TARGET_VIDEO = f"{TARGET_ROOT}/Movie (2020).mkv"
TARGET_SUBTITLE = f"{TARGET_ROOT}/Movie (2020).zh.srt"


def _magic(name: str) -> bytes:
    suffix = Path(name).suffix.casefold()
    if suffix == ".zip":
        return b"PK\x03\x04" + b"z" * 32768
    if suffix == ".rar":
        return b"Rar!\x1a\x07\x01\x00" + b"r" * 32768
    if suffix == ".exe":
        return b"MZ" + b"x" * 32 + b"PK\x03\x04" + b"e" * 32768
    if suffix == ".dat":
        return b"PK\x03\x04" + b"d" * 32768
    return b"7z\xbc\xaf'\x1c" + b"7" * 32768


def _listing_format(name: str | None) -> str:
    suffix = Path(name or "movie.7z").suffix.casefold()
    if suffix in {".zip", ".exe", ".dat"}:
        return "zip"
    if suffix == ".rar":
        return "rar"
    return "7z"


@dataclass(frozen=True)
class Member:
    path: str
    payload: bytes = VIDEO
    link: bool = False


class Fixture7zRunner:
    """Deterministic list/extract port used by the real archive domain."""

    def __init__(
        self, members: tuple[Member, ...], *, required_password: str | None = None,
        archive_format: str = "7z",
    ):
        self.members = members
        self.required_password = required_password
        self.archive_format = archive_format
        self.calls: list[str] = []

    def run(self, args, *, password="", cwd=None, timeout=0):
        del cwd, timeout
        self.calls.append(str(args[0]))
        if self.required_password is not None and password != self.required_password:
            return RunnerResult(2, "", "Wrong password")
        if args[0] == "l":
            rows = [
                "Path = fixture", f"Type = {self.archive_format}",
                "Physical Size = 32768", "",
            ]
            for member in self.members:
                rows.extend([
                    "----------",
                    f"Path = {member.path}",
                    f"Size = {len(member.payload)}",
                    f"Packed Size = {max(1, len(member.payload) // 2)}",
                    "Attributes = A....",
                    "Encrypted = " + ("+" if self.required_password else "-"),
                ])
                if member.link:
                    rows.append("Symbolic Link = target")
                rows.append("")
            return RunnerResult(0, "\n".join(rows), "")
        if args[0] == "x":
            output = next(Path(item[2:]) for item in args if str(item).startswith("-o"))
            selected = {str(item) for item in args[1:]}
            for member in self.members:
                if member.path not in selected:
                    continue
                target = output / member.path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(member.payload)
            return RunnerResult(0, "", "")
        return RunnerResult(2, "", "unsupported")


class FixtureAList:
    """Minimal remote tree plus the ports used by preprocessing and writer."""

    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}
        self.directories: set[str] = {"/", "/incoming", "/library"}
        self.moves: list[tuple[str, str, tuple[str, ...]]] = []

    def add_file(self, path: str, payload: bytes) -> None:
        self.files[path] = bytes(payload)
        parent = posixpath.dirname(path)
        while parent and parent != "/":
            self.directories.add(parent)
            parent = posixpath.dirname(parent)

    def exact_file_info(self, path: str):
        if path in self.files:
            return {"size": len(self.files[path]), "is_dir": False}
        if path in self.directories:
            return {"size": 0, "is_dir": True}
        return None

    def list(self, path: str, refresh: bool = False):
        del refresh
        prefix = path.rstrip("/") + "/"
        rows: dict[str, dict[str, object]] = {}
        for directory in self.directories:
            if directory.startswith(prefix):
                rest = directory[len(prefix):]
                if rest and "/" not in rest:
                    rows[rest] = {"name": rest, "is_dir": True, "size": 0}
        for full_path, payload in self.files.items():
            if full_path.startswith(prefix):
                rest = full_path[len(prefix):]
                if rest and "/" not in rest:
                    rows[rest] = {"name": rest, "is_dir": False, "size": len(payload)}
        return [rows[name] for name in sorted(rows)]

    try_list = list

    def read_file_prefix(self, path: str, *, max_bytes: int):
        return self.files[path][:max_bytes]

    def download_file_to_path(self, path: str, destination: Path, *, expected_size: int):
        payload = self.files[path]
        if len(payload) != expected_size:
            raise AssertionError("fixture size changed")
        destination.write_bytes(payload)

    def mkdir(self, path: str) -> None:
        self.directories.add(path)

    ensure_directory = mkdir

    def upload_file(self, target: str, source: Path, content_type="application/octet-stream"):
        del content_type
        self.add_file(target, source.read_bytes())

    def upload_bytes(self, target: str, data: bytes, content_type: str, *, overwrite=False):
        del content_type
        if overwrite or target not in self.files:
            self.add_file(target, data)

    def move(self, source_dir: str, target_dir: str, names: list[str]) -> None:
        self.moves.append((source_dir, target_dir, tuple(names)))
        self.mkdir(target_dir)
        for name in names:
            source = f"{source_dir.rstrip('/')}/{name}"
            target = f"{target_dir.rstrip('/')}/{name}"
            if source in self.files:
                self.files[target] = self.files.pop(source)
                continue
            if source in self.directories:
                nested = sorted(path for path in self.files if path.startswith(source + "/"))
                for old in nested:
                    self.files[target + old[len(source):]] = self.files.pop(old)
                old_dirs = sorted(
                    (path for path in self.directories if path == source or path.startswith(source + "/")),
                    key=len,
                    reverse=True,
                )
                for old in old_dirs:
                    self.directories.discard(old)
                    self.directories.add(target + old[len(source):])

    def rename(self, full_path: str, new_name: str) -> None:
        parent = posixpath.dirname(full_path)
        self.files[f"{parent}/{new_name}"] = self.files.pop(full_path)

    def remove(self, source_dir: str, names: list[str]) -> None:
        for name in names:
            target = f"{source_dir.rstrip('/')}/{name}"
            self.files.pop(target, None)
            self.directories.discard(target)


class RecordingArchiveAdapter:
    def __init__(self, adapter: ArchivePreprocessingAdapter, events: list[str]):
        self.adapter = adapter
        self.events = events

    def prepare_ordinary_request(self, request, **kwargs):
        self.events.append("preprocess")
        return self.adapter.prepare_ordinary_request(request, **kwargs)


class RecordingExecutor:
    def __init__(self, alist: FixtureAList, events: list[str]):
        self.delegate = SimplePlanExecutor(alist)
        self.events = events
        self.calls = 0

    def execute(self, plan):
        self.calls += 1
        self.events.append("writer")
        return self.delegate.execute(plan)


class Phase4GoldenPathTests(unittest.TestCase):
    maxDiff = None

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    @staticmethod
    def _planner(alist: FixtureAList, events: list[str], *, problem=False):
        def build(request: EngineRequest, _alist, _tmdb):
            events.append("planning")
            prefix = request.source_path.rstrip("/") + "/"
            sources = sorted(path for path in alist.files if path.startswith(prefix))
            video = next((path for path in sources if path.casefold().endswith(".mkv")), None)
            subtitle = next((path for path in sources if path.casefold().endswith(".srt")), None)
            files: list[PlannedFile] = []
            if video is not None:
                files.append(PlannedFile(
                    source_path=video, source_dir=posixpath.dirname(video),
                    original_name=posixpath.basename(video), final_name="Movie (2020).mkv",
                    target_dir=TARGET_ROOT, media_kind="video", source_size=len(alist.files[video]),
                ))
            if subtitle is not None:
                files.append(PlannedFile(
                    source_path=subtitle, source_dir=posixpath.dirname(subtitle),
                    original_name=posixpath.basename(subtitle), final_name="Movie (2020).zh.srt",
                    target_dir=TARGET_ROOT, media_kind="subtitle", source_size=len(alist.files[subtitle]),
                ))
            problems = []
            if problem:
                problems.append(PlannedProblem(
                    source_path=request.source_path,
                    reason="fixture problem_files gate",
                ))
            return Plan(
                mode="movie", source_root=request.source_path, target_root=TARGET_ROOT,
                files=files, problem_files=problems, warnings=[],
                metadata={
                    "tmdb_id": 1, "title": "Movie", "original_title": "Movie",
                    "year": "2020", "poster_path": None, "backdrop_path": None,
                },
            )
        return build

    def _runner(self, alist, archive_runner, events, *, limits=None, problem=False):
        policy = limits or ArchiveLimits(min_free_bytes=0, max_expansion_ratio=1000)
        adapter = ArchivePreprocessingAdapter(
            archive_runner,
            limits=policy,
            video_validator=lambda *_args: True,
            staging_root_validator=lambda path: path.startswith("/library/ScrapeFlow/归档/"),
        )
        executor = RecordingExecutor(alist, events)
        runner = SimpleEngineRunner(
            self.root, alist=alist, tmdb=object(),
            planner=self._planner(alist, events, problem=problem), validate=False,
            executor=executor, library_root="/library",
            archive_preprocessor=RecordingArchiveAdapter(adapter, events),
        )
        return runner, executor

    @staticmethod
    def _identity(events):
        def match(*_args, **_kwargs):
            events.append("identity")
            return SimpleNamespace(
                media_type="movie", tmdb_id=1, title="Movie", year="2020",
                confidence=0.99, decision_trace={"fixture": "phase4"},
            ), []
        return match

    def _positive(self, name: str, archive_name: str | None, members: tuple[Member, ...]):
        alist = FixtureAList()
        source = f"/library/待刮削/{name}"
        if archive_name is None:
            alist.add_file(f"{source}/movie.mkv", VIDEO)
        else:
            alist.add_file(f"{source}/{archive_name}", _magic(archive_name))
        events: list[str] = []
        archive_runner = Fixture7zRunner(
            members,
            archive_format=_listing_format(archive_name),
        )
        runner, executor = self._runner(alist, archive_runner, events)
        queued = runner.create_automatic_job(source, job_id=f"golden-{name}")
        with patch("engine.scraper.auto_match_tmdb", side_effect=self._identity(events)):
            planned = runner.plan_automatic_job(queued.id)
        writer_allowed = not planned.plan.get("problem_files")
        done = runner.execute_job(queued.id)
        restarted = SimpleEngineRunner(
            self.root, alist=alist, tmdb=object(), planner=self._planner(alist, []),
            validate=False, executor=executor, library_root="/library",
        ).execute_job(queued.id)
        runner.cleanup_terminal_job(queued.id)
        with patch.object(SimpleApplication, "_start_startup_thread"):
            application = SimpleApplication(
                state_root=self.root,
                remote_root="/library",
                remote=alist,
                engine_runner=runner,
                enforce_engine_roots=False,
            )
        try:
            application.set_paused(False)
            rescan_jobs = application._scan_inbound_once()  # noqa: SLF001 - golden intake boundary
        finally:
            application.close()
        formal = sorted(
            (path, len(payload)) for path, payload in alist.files.items()
            if path.startswith(TARGET_ROOT + "/") and path.endswith((".mkv", ".srt"))
        )
        processed = next((path for path in alist.directories if f"/{queued.id}/processed/" in path), None)
        consumption = (
            done.execution.get("archive_source_consumption")
            if isinstance(done.execution, dict)
            else None
        )
        staging_prefix = f"/library/ScrapeFlow/归档/{queued.id}/archive/"
        record = {
            "input_tree": [archive_name or "movie.mkv"],
            "preprocessing": {
                "changed": archive_name is not None,
                "source": "task_staging" if archive_name is not None else "input",
            },
            "identity": {"media_type": "movie", "tmdb_id": 1, "title": "Movie", "year": "2020"},
            "plan": {"mode": "movie", "files": len(members) if archive_name else 1, "problems": 0},
            "writer_allowed": writer_allowed,
            "formal": formal,
            "source_fate": (
                consumption.get("status")
                if isinstance(consumption, dict)
                else "not_recorded"
            ),
            "staging_fate": "empty_task_owned" if archive_name else "not_created",
            "phase": done.phase,
            "restart_phase": restarted.phase,
            "cleanup_rescan_jobs": rescan_jobs,
            "formal_copy_count": sum(path == TARGET_VIDEO for path, _size in formal),
        }
        self.assertEqual(executor.calls, 1, "restart must not invoke the writer twice")
        self.assertEqual(rescan_jobs, [], "cleanup followed by intake scan must not recreate the job")
        self.assertFalse(any(path.startswith(source.rstrip("/") + "/") for path in alist.files))
        self.assertFalse(any(path.startswith(staging_prefix) for path in alist.files))
        if archive_name is not None:
            self.assertIsNotNone(processed)
        return record, events

    def test_positive_fixture_records_the_complete_converged_chain(self) -> None:
        cases = (
            ("video", None, (Member("Movie/movie.mkv"),)),
            ("zip", "movie.zip", (Member("Movie/movie.mkv"),)),
            ("sevenzip", "movie.7z", (Member("Movie/movie.mkv"),)),
            ("rar", "movie.rar", (Member("Movie/movie.mkv"),)),
            ("sfx", "movie.exe", (Member("Movie/movie.mkv"),)),
            ("bin", "movie.bin", (Member("Movie/movie.mkv"),)),
            ("dat-sidecar", "movie.dat", (Member("Movie/movie.mkv"), Member("Movie/movie.zh.srt", SUBTITLE))),
        )
        records = {}
        for name, archive_name, members in cases:
            with self.subTest(name=name):
                record, events = self._positive(name, archive_name, members)
                records[name] = record
                self.assertEqual(events, ["preprocess", "identity", "planning", "writer"])
                self.assertEqual(record["input_tree"], [archive_name or "movie.mkv"])
                self.assertEqual(
                    record["preprocessing"],
                    {"changed": archive_name is not None,
                     "source": "task_staging" if archive_name is not None else "input"},
                )
                self.assertEqual(
                    record["identity"],
                    {"media_type": "movie", "tmdb_id": 1, "title": "Movie", "year": "2020"},
                )
                self.assertEqual(record["writer_allowed"], True)
                self.assertEqual(record["source_fate"], "moved_to_processed")
                self.assertEqual(record["staging_fate"], "empty_task_owned" if archive_name else "not_created")
                self.assertEqual(record["restart_phase"], "executed")
                self.assertEqual(record["cleanup_rescan_jobs"], [])
        expected_video = [(TARGET_VIDEO, len(VIDEO))]
        for name in ("video", "zip", "sevenzip", "rar", "sfx", "bin"):
            self.assertEqual(records[name]["formal"], expected_video)
            self.assertEqual(records[name]["formal_copy_count"], 1)
            self.assertEqual(records[name]["phase"], "executed")
        self.assertEqual(
            records["dat-sidecar"]["formal"],
            [(TARGET_VIDEO, len(VIDEO)), (TARGET_SUBTITLE, len(SUBTITLE))],
        )
        self.assertEqual(records["dat-sidecar"]["plan"], {"mode": "movie", "files": 2, "problems": 0})

    def _negative(
        self, name: str, members: tuple[Member, ...], *, limits=None,
        required_password=None, retry_password=None, problem=False,
    ) -> dict[str, object]:
        alist = FixtureAList()
        source = f"/library/待刮削/{name}"
        archive = f"{source}/movie.7z"
        alist.add_file(archive, _magic("movie.7z"))
        events: list[str] = []
        runner, executor = self._runner(
            alist,
            Fixture7zRunner(
                members, required_password=required_password, archive_format="7z",
            ),
            events, limits=limits, problem=problem,
        )
        job = runner.create_automatic_job(source, job_id=f"negative-{name}")
        try:
            with patch("engine.scraper.auto_match_tmdb", side_effect=self._identity(events)):
                planned = runner.plan_automatic_job(job.id, retry_password=retry_password)
            if problem:
                with self.assertRaises(EngineExecutionError):
                    runner.execute_job(job.id)
            else:
                self.fail(f"negative fixture unexpectedly planned: {name}")
        except ArchiveError:
            # Archive-domain rejection is the expected fail-closed stop for
            # traversal/link/collision/budget/password fixtures.
            if problem:
                self.fail(f"problem fixture rejected before planning: {name}")
        final = runner.get_job(job.id)
        return {
            "input_tree": ["movie.7z"],
            "preprocessing": "passed" if "identity" in events else "rejected",
            "identity": "resolved" if "identity" in events else "not_called",
            "planning": "completed" if "planning" in events else "not_called",
            "writer_allowed": False,
            "formal": sorted(path for path in alist.files if path.startswith(TARGET_ROOT + "/")),
            "source_fate": "preserved" if archive in alist.files else "missing",
            "staging_fate": "task_owned_only",
            "phase": final.phase,
            "writer_calls": executor.calls,
        }

    def test_negative_fixtures_fail_closed_and_preserve_the_source(self) -> None:
        many = tuple(Member(f"Movie/{index}.mkv", b"x") for index in range(3))
        cases = {
            "path-traversal": ((Member("../escape.mkv"),), ArchiveLimits(min_free_bytes=0), None, None, False),
            "link": ((Member("Movie/movie.mkv", link=True),), ArchiveLimits(min_free_bytes=0), None, None, False),
            "unicode-collision": ((Member("Movie/E\u0301.mkv", b"a"), Member("movie/é.mkv", b"b")), ArchiveLimits(min_free_bytes=0), None, None, False),
            "member-budget": (many, ArchiveLimits(max_members=2, min_free_bytes=0), None, None, False),
            "expanded-budget": ((Member("Movie/movie.mkv"),), ArchiveLimits(max_expanded_bytes=100, min_free_bytes=0), None, None, False),
            "disk-budget": ((Member("Movie/movie.mkv"),), ArchiveLimits(min_free_bytes=10**18), None, None, False),
            "password": ((Member("Movie/movie.mkv"),), ArchiveLimits(min_free_bytes=0), "correct", "wrong", False),
            "mixed-works": ((Member("MovieA/a.mkv"), Member("MovieB/b.mkv")), ArchiveLimits(min_free_bytes=0, max_expansion_ratio=1000), None, None, True),
            "problem-files": ((Member("Movie/movie.mkv"),), ArchiveLimits(min_free_bytes=0, max_expansion_ratio=1000), None, None, True),
        }
        for name, (members, limits, password, retry, problem) in cases.items():
            with self.subTest(name=name):
                record = self._negative(
                    name, members, limits=limits, required_password=password,
                    retry_password=retry, problem=problem,
                )
                self.assertEqual(record["writer_allowed"], False)
                self.assertEqual(record["formal"], [])
                self.assertEqual(record["source_fate"], "preserved")
                self.assertEqual(record["writer_calls"], 0)
                self.assertIn(record["phase"], {"archive_preprocessing", "failed"})


if __name__ == "__main__":
    unittest.main()
